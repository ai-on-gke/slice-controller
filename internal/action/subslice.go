// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package action

import (
	"context"
	"fmt"
	"time"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/action/gcehelper"
	"slice-controller/internal/cache"
	"slice-controller/internal/util"
	betacompute "google.golang.org/api/compute/v0.beta"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// GCE API call timeout
	gceCallTimeout   = 60 * time.Second
	actionActivate   = "ACTIVATE"
	actionDeactivate = "DEACTIVATE"
)

// FormSubsliceAction creates a new action to form a sub-slice.
func FormSubsliceAction(gceClient gcehelper.GCEClient, rc cache.SliceRelationalCache, project, region, clusterHash string) util.Action {
	return &StepActions{
		steps: []util.Step{
			&FormSubsliceStep{
				gceClient:   gceClient,
				rc:          rc,
				project:     project,
				region:      region,
				clusterHash: clusterHash,
			},
		},
	}
}

// FormSubsliceStep is a step that forms a sub-slice.
type FormSubsliceStep struct {
	gceClient   gcehelper.GCEClient
	rc          cache.SliceRelationalCache
	project     string
	region      string
	clusterHash string
}

// Name returns the name of the step.
func (s *FormSubsliceStep) Name() string {
	return "FormSubsliceStep"
}

// Do performs the action of forming a sub-slice.
func (s *FormSubsliceStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	return executeSubsliceConfig(ctx, s.gceClient, s.rc, s.project, s.clusterHash, slice, actionActivate)
}

func executeSubsliceConfig(ctx context.Context, gceClient gcehelper.GCEClient, rc cache.SliceRelationalCache, project, clusterHash string, slice *slicev1beta1.Slice, action string, explicitPartitionId ...string) error {
	log := klog.FromContext(ctx).WithValues("slice", slice.Name, "action", action)

	// Use explicit partition ID if provided (e.g., from provisioned-partition annotation
	// during deletion after scheduler preemption clears Spec.PartitionIds).
	var partitionId string
	if len(explicitPartitionId) > 0 && explicitPartitionId[0] != "" {
		partitionId = explicitPartitionId[0]
	} else if len(slice.Spec.PartitionIds) > 0 {
		partitionId = slice.Spec.PartitionIds[0]
	} else {
		return fmt.Errorf("no partition ID available for %s action on slice %s", action, slice.Name)
	}

	log.Info("Executing sub-slice configuration", "partitionId", partitionId)

	// 1. Find the MIG and zone using the relational cache.
	migInfo, found := rc.GetMIGByPartitionID(partitionId)
	if !found {
		// The slice will be deleted by the reconciler after this action returns nil.
		if action == actionActivate {
			log.Info("No MIG found for partitionId yet in cache, requeueing", "partitionId", partitionId)
			return fmt.Errorf("no MIG found for partitionId %s", partitionId)
		}
		// If no MIG is found during deactivation, we can assume cleanup is done or not possible.
		log.Info("No MIG found for partitionId in cache, assuming deform is complete.", "partitionId", partitionId)
		return nil
	}

	migURL := migInfo.Name
	migName, zone, err := util.ParseMigDetails(migURL)
	if err != nil {
		return fmt.Errorf("failed to parse zone from MIG name %s: %w", migURL, err)
	}

	// 2. Send the configuration request.
	log.Info("Sending configuration request for sub-slice", "topology", slice.Spec.Topology, "mig", migName, "zone", zone)

	sliceID := util.GetResourceName(slice.Name, clusterHash)
	req := &betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest{
		AcceleratorTopologyConfigurations: map[string]betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequestAcceleratorTopologyConfiguration{
			partitionId: {
				Action:     action,
				ExternalId: sliceID,
			},
		},
	}

	var opType util.OperationType
	switch action {
	case actionActivate:
		opType = util.ActivateOperation
	case actionDeactivate:
		opType = util.DeactivateOperation
	}

	retryAttempt := util.GetRetryAttempt(slice)

	reqID := util.GenerateRequestID(slice.UID, slice.Status.ObservedGeneration, migName, opType, retryAttempt)

	attemptCtx, cancel := context.WithTimeout(ctx, gceCallTimeout)
	defer cancel()

	// Use a closure to capture the operation object even when waitForOperation
	// times out. RetryGCECall returns (zero, err) on failure, losing the operation,
	// but we need it to track in-flight activations during the deletion flow.
	var lastOp *betacompute.Operation
	call := func() (*betacompute.Operation, error) {
		op, err := gceClient.ConfigureAcceleratorTopology(attemptCtx, project, zone, migName, req, reqID)
		if op != nil {
			lastOp = op // Capture the operation even if waitForOperation times out
		}
		return op, err
	}

	op, err := util.RetryGCECall(attemptCtx, call)

	// Persist the operation name on the slice so the deletion flow can check
	// whether this activate operation completed via checkSubsliceOperationBarrier
	// before sending a conflicting deactivate (LIFO race prevention).
	if action == actionActivate && lastOp != nil && lastOp.Name != "" {
		if slice.Annotations == nil {
			slice.Annotations = make(map[string]string)
		}
		slice.Annotations[util.SubsliceLastOpAnnotation] = lastOp.Name
		log.Info("Stored last activate operation on slice annotation", "SliceName", slice.Name, "action", action, "operation", lastOp.Name, "zone", zone)
	}

	if err != nil {
		return fmt.Errorf("failed to configure accelerator topologies: %w", err)
	}

	// Check if the operation itself failed (e.g., synchronous failure or already completed with error).
	if op != nil && op.Error != nil && len(op.Error.Errors) > 0 {
		var errMsgs []string
		for _, e := range op.Error.Errors {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %s", e.Code, e.Message))
		}
		return fmt.Errorf("configure accelerator topologies operation failed: %v", errMsgs)
	}

	log.Info("Successfully dispatched request.", "operation", op)
	return nil
}

// DeformSubsliceAction creates a new action to deform a sub-slice.
// partitionId is the effective partition to deactivate; it may differ from
// Spec.PartitionIds when the scheduler has cleared partitionIds during preemption.
func DeformSubsliceAction(gceClient gcehelper.GCEClient, rc cache.SliceRelationalCache, project, region, clusterHash, partitionId string) util.Action {
	return &StepActions{
		steps: []util.Step{
			&DeformSubsliceStep{
				gceClient:   gceClient,
				rc:          rc,
				project:     project,
				region:      region,
				clusterHash: clusterHash,
				partitionId: partitionId,
			},
		},
	}
}

// DeformSubsliceStep is a step that deforms a sub-slice.
type DeformSubsliceStep struct {
	gceClient   gcehelper.GCEClient
	rc          cache.SliceRelationalCache
	project     string
	region      string
	clusterHash string
	partitionId string // explicit partition from getEffectivePartition
}

// Name returns the name of the step.
func (s *DeformSubsliceStep) Name() string {
	return "DeformSubsliceStep"
}

// Do performs the action of deforming a sub-slice.
func (s *DeformSubsliceStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	return executeSubsliceConfig(ctx, s.gceClient, s.rc, s.project, s.clusterHash, slice, actionDeactivate, s.partitionId)
}

// UpdateFormSubsliceAction creates an action to form a new sub-slice partition during an update.
func UpdateFormSubsliceAction(gceClient gcehelper.GCEClient, kubeClient client.Client, rc cache.SliceRelationalCache, project, clusterHash, partitionId string) util.Action {
	return &StepActions{
		steps: []util.Step{
			&UpdateFormSubsliceStep{
				gceClient:   gceClient,
				kubeClient:  kubeClient,
				rc:          rc,
				project:     project,
				clusterHash: clusterHash,
				partitionId: partitionId,
			},
		},
	}
}

// UpdateFormSubsliceStep is a step that forms a specific new sub-slice partition.
type UpdateFormSubsliceStep struct {
	gceClient   gcehelper.GCEClient
	kubeClient  client.Client
	rc          cache.SliceRelationalCache
	project     string
	clusterHash string
	partitionId string
}

// Name returns the name of the step.
func (s *UpdateFormSubsliceStep) Name() string {
	return "UpdateFormSubsliceStep"
}

// Do performs the action of forming the new sub-slice partition.
func (s *UpdateFormSubsliceStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	log := klog.FromContext(ctx).WithValues("slice", slice.Name, "partitionId", s.partitionId)
	log.Info("Starting UpdateFormSubsliceStep")
	return executeUpdateSubsliceConfig(ctx, s.gceClient, s.kubeClient, s.rc, s.project, s.clusterHash, slice, actionActivate, s.partitionId)
}

// UpdateDeformSubsliceAction creates an action to deform an old sub-slice partition during an update.
func UpdateDeformSubsliceAction(gceClient gcehelper.GCEClient, kubeClient client.Client, rc cache.SliceRelationalCache, project, clusterHash, partitionId string) util.Action {
	return &StepActions{
		steps: []util.Step{
			&UpdateDeformSubsliceStep{
				gceClient:   gceClient,
				kubeClient:  kubeClient,
				rc:          rc,
				project:     project,
				clusterHash: clusterHash,
				partitionId: partitionId,
			},
		},
	}
}

// UpdateDeformSubsliceStep is a step that deforms a specific old sub-slice partition.
type UpdateDeformSubsliceStep struct {
	gceClient   gcehelper.GCEClient
	kubeClient  client.Client
	rc          cache.SliceRelationalCache
	project     string
	clusterHash string
	partitionId string
}

// Name returns the name of the step.
func (s *UpdateDeformSubsliceStep) Name() string {
	return "UpdateDeformSubsliceStep"
}

// Do performs the action of deforming the old sub-slice partition.
func (s *UpdateDeformSubsliceStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	log := klog.FromContext(ctx).WithValues("slice", slice.Name, "partitionId", s.partitionId)
	log.Info("Starting UpdateDeformSubsliceStep")
	return executeUpdateSubsliceConfig(ctx, s.gceClient, s.kubeClient, s.rc, s.project, s.clusterHash, slice, actionDeactivate, s.partitionId)
}

// executeUpdateSubsliceConfig is an isolated execution helper specifically for SubSlice updates.
// Unlike the standard executeSubsliceConfig, it expects an explicit partitionId parameter
// (allowing it to deform old partitions that no longer exist in the Spec) and strictly logs
// the resulting Operation ID to the K8s API to fuel the update workflow's LIFO barrier.
func executeUpdateSubsliceConfig(ctx context.Context, gceClient gcehelper.GCEClient, kubeClient client.Client, rc cache.SliceRelationalCache, project, clusterHash string, slice *slicev1beta1.Slice, action string, partitionId string) error {
	log := klog.FromContext(ctx).WithValues("slice", slice.Name, "action", action, "partitionId", partitionId)
	log.Info("Executing sub-slice configuration update")

	migInfo, found := rc.GetMIGByPartitionID(partitionId)
	if !found {
		if action == actionActivate {
			return fmt.Errorf("no MIG found for partitionId %s", partitionId)
		}
		return nil
	}

	migURL := migInfo.Name
	migName, zone, err := util.ParseMigDetails(migURL)
	if err != nil {
		return fmt.Errorf("failed to parse zone from MIG name %s: %w", migURL, err)
	}

	sliceID := util.GetResourceName(slice.Name, clusterHash)

	log.Info("Sending configuration request for sub-slice update", "mig", migName, "zone", zone, "externalId", sliceID)

	req := &betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest{
		AcceleratorTopologyConfigurations: map[string]betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequestAcceleratorTopologyConfiguration{
			partitionId: {
				Action:     action,
				ExternalId: sliceID,
			},
		},
	}

	var opType util.OperationType
	if action == actionActivate {
		opType = util.ActivateOperation
	} else {
		opType = util.DeactivateOperation
	}

	reqID := util.GenerateRequestID(slice.UID, slice.Status.ObservedGeneration, migName, opType, util.GetRetryAttempt(slice))

	attemptCtx, cancel := context.WithTimeout(ctx, gceCallTimeout)
	defer cancel()

	call := func() (*betacompute.Operation, error) {
		return gceClient.ConfigureAcceleratorTopology(attemptCtx, project, zone, migName, req, reqID)
	}

	op, err := util.RetryGCECall(attemptCtx, call)
	if err != nil {
		return fmt.Errorf("failed to configure accelerator topologies: %w", err)
	}

	if op != nil && op.Error != nil && len(op.Error.Errors) > 0 {
		var errMsgs []string
		for _, e := range op.Error.Errors {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %s", e.Code, e.Message))
		}
		return fmt.Errorf("configure accelerator topologies operation failed: %v", errMsgs)
	}

	if op != nil && op.Name != "" {
		patchBaseOp := client.MergeFrom(slice.DeepCopy())
		if slice.Annotations == nil {
			slice.Annotations = make(map[string]string)
		}
		slice.Annotations[util.SubsliceLastOpAnnotation] = op.Name
		if kubeClient != nil {
			if err := kubeClient.Patch(ctx, slice, patchBaseOp); err != nil {
				return fmt.Errorf("failed to save operation ID: %w", err)
			}
		} else {
			log.Info("WARNING: kubeClient is nil, skipping saving operation ID annotation. This should only happen in unit tests.")
		}
	}

	log.Info("Successfully dispatched update request", "operationName", op.Name)

	return nil
}
