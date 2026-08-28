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

package gcehelper

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"sync"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/cache"
	"slice-controller/internal/metrics"
	"slice-controller/internal/util"
	"slice-controller/services"
	betacompute "google.golang.org/api/compute/v0.beta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

const (
	// Op timeout
	opTimeoutPeriod = 60 * time.Second
	// Op poll interval
	opPollInterval = 2 * time.Second

	// Workload policy type
	highThroughput = "HIGH_THROUGHPUT"
)

// MigAction represents the action performed on a MIG (e.g., attach, detach).
type MigAction string

const (
	// MigActionAttach represents an attach action.
	MigActionAttach MigAction = "attach"
	// MigActionDetach represents a detach action.
	MigActionDetach MigAction = "detach"
)

// Client is an interface for a GCE client.
type GCEClient interface {
	CreateWorkloadPolicy(ctx context.Context, projectID string, region string, slice *slicev1beta1.Slice, clusterHash string) error
	DeleteWorkloadPolicy(ctx context.Context, projectID string, region string, sliceName string, clusterHash string) error
	CreateMMIG(ctx context.Context, projectID string, region string, slice *slicev1beta1.Slice, clusterHash string) error
	DeleteMMIG(ctx context.Context, projectID string, region string, sliceName string, clusterHash string) error
	AttachMIGsToMMIG(ctx context.Context, projectID string, region string, sliceName string, cubes []util.CubeInfo, clusterHash string, uid types.UID, generation int64, retryAttempt int64) error
	DetachMIGsFromMMIG(ctx context.Context, projectID string, region string, sliceName string, cubes []util.CubeInfo, clusterHash string, uid types.UID, generation int64) error
	DetachPivotMIGFromMMIG(ctx context.Context, projectID, region string, pivotCube util.CubeInfo, uid types.UID, generation int64) (string, error)
	GetAvailableAcceleratorTopologies(ctx context.Context, projectID, zone, igmName string) (cache.AcceleratorTopologies, error)
	GetZonalOperation(ctx context.Context, project, zone, operationName string) (*betacompute.Operation, error)
	GetMMIGHealthStatus(ctx context.Context, projectID string, region string, sliceName string) (cache.SuperSliceHealthStatus, error)
	ListMultiMigMembers(ctx context.Context, projectID, region, mmigName string) ([]*betacompute.MultiMigMember, error)
	ConfigureAcceleratorTopology(ctx context.Context, projectID, zone, igmName string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error)
}

type getOperationFunc func(ctx context.Context, project, location, operationName string) (*betacompute.Operation, error)

type gceClientImpl struct {
	enableHighGCEConcurrency bool
	enableSubSlicing         bool
	computeService           services.ComputeServiceInterface
}

// NewClient creates a new GCE client.
func NewClient(computeService services.ComputeServiceInterface, enableHighGCEConcurrency bool, enableSubSlicing bool) GCEClient {
	return &gceClientImpl{
		computeService:           computeService,
		enableHighGCEConcurrency: enableHighGCEConcurrency,
		enableSubSlicing:         enableSubSlicing,
	}
}

func (c *gceClientImpl) CreateWorkloadPolicy(ctx context.Context, projectID string, region string, slice *slicev1beta1.Slice, clusterHash string) error {
	policy := &betacompute.ResourcePolicy{
		Name: util.GetResourceName(slice.Name, clusterHash),
		WorkloadPolicy: &betacompute.ResourcePolicyWorkloadPolicy{
			Type:                highThroughput,
			AcceleratorTopology: slice.Spec.Topology,
		},
	}
	apiCall := func() (*betacompute.Operation, error) {
		return c.computeService.CreateResourcePolicy(projectID, region, policy)
	}
	op, err := util.RetryGCECall(ctx, apiCall, util.ResourceAlreadyExists)
	if err != nil {
		return &util.APICallError{Err: fmt.Errorf("failed API call: %w", err)}
	}
	if op == nil {
		return nil
	}
	if err := c.waitForOperation(ctx, projectID, region, op.Name, c.GetRegionalOperation); err != nil {
		return &util.OperationError{Err: fmt.Errorf("failed waiting for operation: %w", err)}
	}
	return nil
}

func (c *gceClientImpl) DeleteWorkloadPolicy(ctx context.Context, projectID string, region string, sliceName string, clusterHash string) error {
	apiCall := func() (*betacompute.Operation, error) {
		return c.computeService.DeleteResourcePolicy(projectID, region, util.GetResourceName(sliceName, clusterHash))
	}
	op, err := util.RetryGCECall(ctx, apiCall, util.ResourceNotFound)
	if err != nil {
		return &util.APICallError{Err: fmt.Errorf("failed API call: %w", err)}
	}
	if op == nil {
		return nil
	}
	if err := c.waitForOperation(ctx, projectID, region, op.Name, c.GetRegionalOperation); err != nil {
		return &util.OperationError{Err: fmt.Errorf("failed waiting for operation: %w", err)}
	}
	return nil
}

func (c *gceClientImpl) CreateMMIG(ctx context.Context, projectID string, region string, slice *slicev1beta1.Slice, clusterHash string) error {
	policyName := util.GetResourceName(slice.Name, clusterHash)
	policyURL := fmt.Sprintf("projects/%s/regions/%s/resourcePolicies/%s", projectID, region, policyName)
	mmig := &betacompute.MultiMig{
		Name: policyName,
		ResourcePolicies: &betacompute.MultiMigResourcePolicies{
			WorkloadPolicy: policyURL,
		},
	}
	apiCall := func() (*betacompute.Operation, error) {
		return c.computeService.CreateMMIG(projectID, region, mmig)
	}
	op, err := util.RetryGCECall(ctx, apiCall, util.ResourceAlreadyExists)
	if err != nil {
		return &util.APICallError{Err: fmt.Errorf("failed API call: %w", err)}
	}
	if op == nil {
		return nil
	}
	if err := c.waitForOperation(ctx, projectID, region, op.Name, c.GetRegionalOperation); err != nil {
		return &util.OperationError{Err: fmt.Errorf("failed waiting for operation: %w", err)}
	}
	return nil
}

func (c *gceClientImpl) DeleteMMIG(ctx context.Context, projectID string, region string, sliceName string, clusterHash string) error {
	apiCall := func() (*betacompute.Operation, error) {
		return c.computeService.DeleteMMIG(projectID, region, util.GetResourceName(sliceName, clusterHash))
	}
	op, err := util.RetryGCECall(ctx, apiCall, util.ResourceNotFound)
	if err != nil {
		return &util.APICallError{Err: fmt.Errorf("failed API call: %w", err)}
	}
	if op == nil {
		return nil
	}
	if err := c.waitForOperation(ctx, projectID, region, op.Name, c.GetRegionalOperation); err != nil {
		return &util.OperationError{Err: fmt.Errorf("failed waiting for operation: %w", err)}
	}
	return nil
}

type cubeError struct {
	cube util.CubeInfo
	err  error
}

// handleCubeErrors processes errors received from cube operations and returns a consolidated error if any occurred.
func handleCubeErrors(action MigAction, errCh <-chan cubeError) error {
	count := len(errCh)
	if count == 0 {
		return nil
	}

	failedPartitions := make([]string, 0, count)
	errStrs := make([]string, 0, count)
	errs := make([]error, 0, count) // Slice to hold the actual error objects

	for ce := range errCh {
		errStrs = append(errStrs, ce.err.Error())
		failedPartitions = append(failedPartitions, ce.cube.PartitionId)
		errs = append(errs, ce.err) // Preserve the original typed error
	}

	if len(errs) > 0 {
		klog.Errorf("handleCubeErrors: failed to %s %s: [%s]", action, strings.Join(failedPartitions, ", "), strings.Join(errStrs, "; "))

		// Combine all typed errors into a single error tree
		joinedErr := errors.Join(errs...)

		// Wrap the joined error using %w so the reconciler can unwrap and inspect it
		return fmt.Errorf("failed to %s %s: %w", action, strings.Join(failedPartitions, ", "), joinedErr)
	}

	return nil
}

func (c *gceClientImpl) AttachMIGsToMMIG(ctx context.Context, projectID string, region string, sliceName string, cubes []util.CubeInfo, clusterHash string, uid types.UID, generation int64, retryAttempt int64) error {
	mmigName := fmt.Sprintf("projects/%s/regions/%s/multiMigs/%s", projectID, region, util.GetResourceName(sliceName, clusterHash))
	jobs := make(chan util.CubeInfo, len(cubes))
	errCh := make(chan cubeError, len(cubes))
	numWorkers := 10
	if c.enableHighGCEConcurrency {
		numWorkers = len(cubes)
	}
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cube := range jobs {
				startTime := time.Now() // Start time for this specific MIG operation
				igm := &betacompute.InstanceGroupManager{
					MultiMig: mmigName,
				}
				// Generate a unique request ID for this operation to ensure idempotency and easier debugging in GCE logs.
				// The ID is based on the Slice UID, generation, operation type, a hash of the MIG name, and the retry attempt count.
				// Including the retry attempt ensures that retried operations get a new request ID and are not incorrectly deduplicated by GCE.
				reqID := util.GenerateRequestID(uid, generation, cube.MigName, util.AttachOperation, (int)(retryAttempt))
				klog.FromContext(ctx).Info("Generated request ID for attaching MIG to MMIG", "requestId", reqID, "migName", cube.MigName)
				apiCall := func() (*betacompute.Operation, error) {
					return c.computeService.PatchInstanceGroupManager(ctx, projectID, cube.Zone, cube.MigName, igm, reqID)
				}
				klog.FromContext(ctx).Info("Attaching MIG to MMIG", "mig", cube.MigName, "zone", cube.Zone, "mmig", mmigName)
				op, err := util.RetryGCECall(ctx, apiCall)
				if err != nil {
					// Wrap as APICallError and use %w to preserve the trace
					wrappedErr := &util.APICallError{Err: fmt.Errorf("partition %q (patch): %w", cube.PartitionId, err)}
					errCh <- cubeError{cube: cube, err: wrappedErr}
					metrics.RecordGceApiCall("mig_attach", startTime, err)
					continue
				}

				if err := c.waitForOperation(ctx, projectID, cube.Zone, op.Name, c.GetZonalOperation); err != nil {
					// Wrap as OperationError and use %w
					wrappedErr := &util.OperationError{Err: fmt.Errorf("partition %q (wait): %w", cube.PartitionId, err)}
					errCh <- cubeError{cube: cube, err: wrappedErr}
				}

				metrics.RecordGceApiCall("mig_attach", startTime, err)
			}
		}()
	}

	for _, cube := range cubes {
		jobs <- cube
	}
	close(jobs)

	wg.Wait()
	close(errCh)

	return handleCubeErrors(MigActionAttach, errCh)
}

func (c *gceClientImpl) DetachMIGsFromMMIG(ctx context.Context, projectID string, region string, sliceName string, cubes []util.CubeInfo, clusterHash string, uid types.UID, generation int64) error {
	jobs := make(chan util.CubeInfo, len(cubes))
	errCh := make(chan cubeError, len(cubes))
	numWorkers := 10
	if c.enableHighGCEConcurrency {
		numWorkers = len(cubes)
	}
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cube := range jobs {
				startTime := time.Now() // Start time for this specific MIG operation
				igm := &betacompute.InstanceGroupManager{
					// Explicitly set this field to null in the JSON request.
					NullFields: []string{"MultiMig"},
				}
				// Generate a unique request ID for this operation to ensure idempotency and easier debugging in GCE logs.
				// The ID is based on the Slice UID, generation, operation type, and a hash of the MIG name.
				// For detaching operations in superslicing, retry count is not an important factor for request ID as detach operation validation happens without creating the operations in GCE.
				reqID := util.GenerateRequestID(uid, generation, cube.MigName, util.DetachOperation, 0)
				klog.FromContext(ctx).Info("Generated request ID for detaching MIG from MMIG", "requestId", reqID, "migName", cube.MigName)
				apiCall := func() (*betacompute.Operation, error) {
					return c.computeService.PatchInstanceGroupManager(ctx, projectID, cube.Zone, cube.MigName, igm, reqID)
				}
				klog.FromContext(ctx).Info("Detaching MIG from MMIG", "mig", cube.MigName, "zone", cube.Zone)
				op, err := util.RetryGCECall(ctx, apiCall)
				if err != nil {
					wrappedErr := &util.APICallError{Err: fmt.Errorf("Partition %q in zone %q (detach patch): %w", cube.PartitionId, cube.Zone, err)}
					errCh <- cubeError{cube: cube, err: wrappedErr}
					metrics.RecordGceApiCall("mig_detach", startTime, err)
					continue
				}
				if err := c.waitForOperation(ctx, projectID, cube.Zone, op.Name, c.GetZonalOperation); err != nil {
					wrappedErr := &util.OperationError{Err: fmt.Errorf("Partition %q in zone %q (detach wait): %w", cube.PartitionId, cube.Zone, err)}
					errCh <- cubeError{cube: cube, err: wrappedErr}
				}
				metrics.RecordGceApiCall("mig_detach", startTime, err)
			}
		}()
	}

	for _, cube := range cubes {
		jobs <- cube
	}
	close(jobs)

	wg.Wait()
	close(errCh)

	return handleCubeErrors(MigActionDetach, errCh)
}

func (c *gceClientImpl) DetachPivotMIGFromMMIG(ctx context.Context, projectID, region string, pivotCube util.CubeInfo, uid types.UID, generation int64) (string, error) {
	startTime := time.Now() // Start time for this specific MIG operation
	var opErr error
	defer func() {
		metrics.RecordGceApiCall("mig_detach_pivot", startTime, opErr)
	}()
	klog.FromContext(ctx).Info("Attempting to detach pivot MIG %q in zone %q from MMIG", pivotCube.MigName, pivotCube.Zone)

	igm := &betacompute.InstanceGroupManager{
		// Explicitly set this field to null in the JSON request.
		NullFields: []string{"MultiMig"},
	}
	// Generate a unique request ID for this operation to ensure idempotency and easier debugging in GCE logs.
	// The ID is based on the Slice UID, generation, operation type, and a hash of the MIG name.
	// For detaching operations in superslicing, retry count is not an important factor for request ID as detach operation validation happens without creating the operations in GCE.
	reqID := util.GenerateRequestID(uid, generation, pivotCube.MigName, util.DetachOperation, 0)
	klog.FromContext(ctx).Info("Generated request ID for detaching pivot MIG from MMIG", "requestId", reqID, "migName", pivotCube.MigName)
	apiCall := func() (*betacompute.Operation, error) {
		return c.computeService.PatchInstanceGroupManager(ctx, projectID, pivotCube.Zone, pivotCube.MigName, igm, reqID)
	}
	op, err := util.RetryGCECall(ctx, apiCall)
	if err != nil {
		opErr = &util.APICallError{Err: err}
		return "", opErr
	}
	return op.Name, nil
}

func (c *gceClientImpl) waitForOperation(ctx context.Context, project, location, operationName string, getFunc getOperationFunc) error {
	opCtx, cancel := context.WithTimeout(ctx, opTimeoutPeriod)
	defer cancel()

	ticker := time.NewTicker(opPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-opCtx.Done():
			return &util.TimeoutError{
				Message: fmt.Sprintf("parent context cancelled or timed out waiting for operation %s", operationName),
			}
		case <-ticker.C:
			apiCall := func() (*betacompute.Operation, error) {
				return getFunc(opCtx, project, location, operationName)
			}
			op, err := util.RetryGCECall(opCtx, apiCall)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return &util.TimeoutError{
						Message: fmt.Sprintf("parent context cancelled or timed out waiting for operation %s", operationName),
					}
				}
				return &util.APICallError{Err: fmt.Errorf("failed to get operation status: %w", err)}
			}

			if op.Status == util.OperationDone {
				if op.Error != nil {
					var details []util.GCEErrorDetail
					for _, e := range op.Error.Errors {
						details = append(details, util.GCEErrorDetail{
							Code:    e.Code,
							Message: e.Message,
						})
					}
					return &util.GCEOperationFailureError{
						OperationName: operationName,
						Details:       details,
					}
				}
				return nil
			}
		}
	}
}

func (c *gceClientImpl) GetRegionalOperation(ctx context.Context, project, region, operationName string) (*betacompute.Operation, error) {
	return c.computeService.GetRegionalOperation(ctx, project, region, operationName)
}

func (c *gceClientImpl) GetZonalOperation(ctx context.Context, project, zone, operationName string) (*betacompute.Operation, error) {
	return c.computeService.GetZonalOperation(ctx, project, zone, operationName)
}

func (c *gceClientImpl) GetAvailableAcceleratorTopologies(ctx context.Context, projectID, zone, igmName string) (cache.AcceleratorTopologies, error) {
	apiCall := func() (*betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse, error) {
		return c.computeService.GetAvailableAcceleratorTopologies(projectID, zone, igmName)
	}

	resp, err := util.RetryGCECall(ctx, apiCall)
	if err != nil {
		return cache.AcceleratorTopologies{}, err
	}

	topologiesInfo := make(map[string]cache.AcceleratorTopologyInfo)

	for topoID, apiTopoDetails := range resp.AcceleratorTopologiesInfo {
		if c.enableSubSlicing {
			// Sub-slicing logic: Combine topology health and instance health, and include detailed state.
			healthStatus := cache.MIGHealthStatus(apiTopoDetails.AcceleratorTopologyHealth)
			if apiTopoDetails.InstancesHealth == string(cache.MIGInstancesHealthUnhealthyOrMissing) {
				healthStatus = cache.MIGHealthStatusUnhealthy
			}

			var currentState string
			var sliceID string
			var errors []cache.AcceleratorTopologyError
			if apiTopoDetails.AcceleratorTopologyState != nil {
				currentState = apiTopoDetails.AcceleratorTopologyState.CurrentState
				sliceID = apiTopoDetails.AcceleratorTopologyState.ExternalId
				if apiTopoDetails.AcceleratorTopologyState.Error != nil {
					for _, e := range apiTopoDetails.AcceleratorTopologyState.Error.Errors {
						errors = append(errors, cache.AcceleratorTopologyError{
							Code:    e.Code,
							Message: e.Message,
						})
					}
				}
			}

			topologiesInfo[topoID] = cache.AcceleratorTopologyInfo{
				AcceleratorTopology:             apiTopoDetails.AcceleratorTopology,
				AcceleratorTopologyHealthStatus: healthStatus,
				AcceleratorTopologyState:        cache.MIGPartitionState(currentState),
				SliceID:                         sliceID,
				InstancesHealth:                 apiTopoDetails.InstancesHealth,
				Errors:                          errors,
			}
		} else {
			// Super-slicing logic: Focus on topology health as reported by the API.
			topologiesInfo[topoID] = cache.AcceleratorTopologyInfo{
				AcceleratorTopology:             apiTopoDetails.AcceleratorTopology,
				AcceleratorTopologyHealthStatus: cache.MIGHealthStatus(apiTopoDetails.AcceleratorTopologyHealth),
			}
		}
	}

	return cache.AcceleratorTopologies{
		MigName:        igmName,
		TopologiesInfo: topologiesInfo,
	}, nil
}

// GetMMIGHealthStatus retrieves the health status of a specific MMIG and its associated topologies.
func (c *gceClientImpl) GetMMIGHealthStatus(ctx context.Context, projectID string, region string, mmigName string) (cache.SuperSliceHealthStatus, error) {
	// A zero-value struct to return in case of errors.
	var zeroStatus cache.SuperSliceHealthStatus

	apiCall := func() (*betacompute.MultiMig, error) {
		return c.computeService.GetMMIG(projectID, region, mmigName)
	}

	mmig, err := util.RetryGCECall(ctx, apiCall)
	if err != nil {
		return zeroStatus, err
	}

	if mmig == nil || mmig.Status == nil || len(mmig.Status.AppliedAcceleratorTopologies) == 0 {
		return zeroStatus, fmt.Errorf("MMIG '%s' status or applied topologies are missing from the API response", mmigName)
	}

	superSliceStatus := cache.SuperSliceHealthStatus{
		MMIGName:        mmig.Name,
		TopologyHealths: []*cache.SuperSliceTopologyHealth{},
	}

	for _, topo := range mmig.Status.AppliedAcceleratorTopologies {
		if topo == nil {
			continue
		}

		topologyInfo := &cache.SuperSliceTopologyHealth{
			AcceleratorTopology: topo.AcceleratorTopology,
			HealthStatus:        cache.MMIGHealthStatus(topo.AcceleratorTopologyState),
		}

		if topo.AcceleratorTopologyStateLastCheck != nil {
			topologyInfo.LastCheckTime = topo.AcceleratorTopologyStateLastCheck.Timestamp
			if topo.AcceleratorTopologyStateLastCheck.Error != nil && len(topo.AcceleratorTopologyStateLastCheck.Error.Errors) > 0 {
				var errorMessages []string
				for _, e := range topo.AcceleratorTopologyStateLastCheck.Error.Errors {
					errorMessages = append(errorMessages, e.Message)
				}
				topologyInfo.LastCheckErrorMessages = errorMessages
			}
		}
		superSliceStatus.TopologyHealths = append(superSliceStatus.TopologyHealths, topologyInfo)
	}

	return superSliceStatus, nil
}

// ListMultiMigMembers lists all the members of a given MMIG, handling API pagination automatically.
func (c *gceClientImpl) ListMultiMigMembers(ctx context.Context, projectID, region, mmigName string) ([]*betacompute.MultiMigMember, error) {
	var allMembers []*betacompute.MultiMigMember
	pageToken := ""

	for {
		// Capture the current pageToken in the closure.
		// If RetryGCECall triggers a retry, it correctly re-uses the token for the failed page.
		currentToken := pageToken
		apiCall := func() (*betacompute.MultiMigMemberList, error) {
			return c.computeService.ListMultiMigMembers(ctx, projectID, region, mmigName, currentToken)
		}

		resp, err := util.RetryGCECall(ctx, apiCall, util.ResourceNotFound)
		if err != nil {
			return nil, fmt.Errorf("failed to list members for MMIG %q in region %q: %w", mmigName, region, err)
		}

		// Guard against nil responses
		if resp == nil {
			break
		}

		if len(resp.Items) > 0 {
			allMembers = append(allMembers, resp.Items...)
		}

		// The GCP API returns an empty NextPageToken when there are no more pages
		if resp.NextPageToken == "" {
			break
		}

		// Advance the token for the next iteration
		pageToken = resp.NextPageToken
	}

	return allMembers, nil
}
func (c *gceClientImpl) ConfigureAcceleratorTopology(ctx context.Context, projectID, zone, igmName string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
	op, err := c.computeService.ConfigureAcceleratorTopology(ctx, projectID, zone, igmName, req, requestId)
	if err != nil {
		return nil, &util.APICallError{Err: fmt.Errorf("failed to configure topology: %w", err)}
	}
	if op == nil {
		return nil, nil
	}
	if err := c.waitForOperation(ctx, projectID, zone, op.Name, c.GetZonalOperation); err != nil {
		return op, &util.OperationError{Err: fmt.Errorf("failed waiting for configure topology operation: %w", err)}
	}
	return op, nil
}
