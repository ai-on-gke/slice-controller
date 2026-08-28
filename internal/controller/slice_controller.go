/*
Copyright 2025 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package controller contains the controller logic for the slice-controller.
package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	slicev1beta1 "slice-controller/api/v1beta1"

	"slice-controller/internal/action"
	"slice-controller/internal/cache"
	"slice-controller/internal/metrics"
	"slice-controller/internal/util"
	"slice-controller/services"
	"golang.org/x/time/rate"
	"google.golang.org/api/googleapi"

	"slice-controller/internal/action/gcehelper"
	"slice-controller/internal/action/kubehelper"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const sliceFormationFailedMessage = "Slice Formation Failed due to Infrastructure failure"

const (
	// sliceFinalizerName is the name of the finalizer used by the Slice controller.
	sliceFinalizerName = "accelerator.gke.io/slice-finalizer"
)

// SliceReconciler reconciles a Slice object
type SliceReconciler struct {
	client.Client
	Scheme                            *runtime.Scheme
	ComputeService                    services.ComputeServiceInterface
	ProjectNumber                     string
	Location                          string
	Region                            string
	ClusterName                       string
	ClusterHash                       string
	KubeHelper                        kubehelper.Client
	GCEHelper                         gcehelper.GCEClient
	SliceEngine                       util.Engine
	RelationalCache                   cache.SliceRelationalCache
	EnableHighGCEConcurrency          bool
	EnableHighNodeLabelingConcurrency bool
	MetricsExporter                   *metrics.MetricsExporter
	EnableLinkedRunner                bool
	EnableSliceUpdate                 bool
	MinRetryDelay                     time.Duration
	MaxRetryDelay                     time.Duration
	WorkQueueRateLimit                float64
	WorkQueueBurst                    int
	EnableSubSlicing                  bool
	SupportSubsliceUpdate             bool
}

var sliceLogger = ctrl.Log.WithName("sliceController")

// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="authentication.k8s.io",resources=tokenreviews,verbs=create

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *SliceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sliceLogger := sliceLogger.WithValues("SliceName", req.Name)
	ctx = ctrl.LoggerInto(ctx, sliceLogger)
	sliceLogger.Info("Reconciling Slice")

	// Fetch the Slice instance
	slice := &slicev1beta1.Slice{}
	err := r.Get(ctx, req.NamespacedName, slice)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			sliceLogger.Info("Slice resource not found. Ignoring since object must be deleted")
			if err := r.RelationalCache.DeleteSlice(req.Name); err != nil {
				sliceLogger.Error(err, "Failed to remove deleted slice from relational cache")
				return ctrl.Result{}, err // Requeue to retry cache deletion
			}
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		sliceLogger.Error(err, "Failed to get Slice")
		return ctrl.Result{}, err
	}
	if condition, found := util.FindSliceCondition(slice, slicev1beta1.SliceStateConditionType); found {
		sliceLogger.Info("Get slice successfully; current slice condition", "reason", condition.Reason, "message", condition.Message)
		if cacheErr := r.cacheSyncUp(ctx, slice); cacheErr != nil {
			sliceLogger.Error(cacheErr, "Failed to sync up relational cache for Slice")
			return ctrl.Result{}, cacheErr
		}
	} else {
		sliceLogger.Info("Slice condition not yet initialized")
	}

	// Migration guard: if observedGeneration is 0, the field was either
	// pruned by the API server (CRD schema not yet updated) or never set
	// because the slice skipped initialization. In either case, do NOT
	// enter the update path — try to backfill instead.
	if r.EnableSliceUpdate && slice.Status.ObservedGeneration == 0 &&
		!r.isSliceInitializing(slice) && !r.isSliceDeleting(slice) &&
		!r.sliceNeedsForming(ctx, slice) && !r.shouldRunPreflightChecks(slice) {
		if err := r.backfillObservedGeneration(ctx, slice); err != nil {
			sliceLogger.Error(err, "Failed to backfill observedGeneration")
			return ctrl.Result{}, err
		}
		if slice.Status.ObservedGeneration == 0 {
			// If still 0, it was pruned by the API server (schema not updated).
			// Requeue periodically to check if the CRD has been upgraded/updated.
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{Requeue: true}, nil
	}

	switch {
	case r.isSliceDeleting(slice):
		return r.handleSliceDeletion(ctx, slice)

	case r.isSliceInitializing(slice):
		sliceLogger.Info("Received slice spec", "partition_id_count", len(slice.Spec.PartitionIds), "partition_ids", slice.Spec.PartitionIds)
		return r.handleSliceInitialization(ctx, slice)

	case r.sliceNeedsUpdate(slice):
		return r.handleSliceUpdate(ctx, slice)

	case r.shouldRunPreflightChecks(slice):
		// If checks pass, 'shouldProceed' is true.
		// If checks fail, we patched the status to 'SliceCreationFailed' and 'shouldProceed' is false.
		shouldProceed, err := r.handlePreflightChecks(ctx, slice)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !shouldProceed {
			// Stop processing.
			return ctrl.Result{}, nil
		}
		// Fallthrough to immediate formation since preflight checks passed.
		fallthrough

	case r.sliceNeedsForming(ctx, slice):
		return r.handleSliceFormation(ctx, slice)

	default:
		// Slice is in sync, no action needed
		sliceLogger.Info("Slice is in sync, no action needed")
		return ctrl.Result{}, nil
	}
}

// backfillObservedGeneration patches ObservedGeneration to 1.
// Prior to the introduction of slice updates, all slices are guaranteed to be
// at Generation 1 because no spec updates could have occurred. Thus, backfilling
// ObservedGeneration to 1 aligns it with the current generation and bypasses update logic.
func (r *SliceReconciler) backfillObservedGeneration(ctx context.Context, slice *slicev1beta1.Slice) error {
	sliceLogger := ctrl.LoggerFrom(ctx)
	patchCopy := slice.DeepCopy()
	baseCopy := slice.DeepCopy()

	patchCopy.Status.ObservedGeneration = 1

	// In controller-runtime, Status().Patch updates the patchCopy object in-place
	// with the response returned by the API server. If the API server supports the
	// observedGeneration field, it will contain the new value (1). If the schema has not yet
	// been upgraded, the API server prunes the field, and patchCopy.Status.ObservedGeneration
	// remains 0.
	if err := r.Status().Patch(ctx, patchCopy, client.MergeFrom(baseCopy)); err != nil {
		return err
	}
	slice.Status.ObservedGeneration = patchCopy.Status.ObservedGeneration

	if slice.Status.ObservedGeneration == 0 {
		sliceLogger.Info("observedGeneration was pruned — CRD schema not yet updated")
	} else {
		sliceLogger.Info("observedGeneration backfilled", "observedGeneration", slice.Status.ObservedGeneration)
	}
	return nil
}

// recordSliceDeformLatency exports the E2E latency of slice deformation to metrics.
func (r *SliceReconciler) recordSliceDeformLatency(ctx context.Context, slice *slicev1beta1.Slice, duration time.Duration) {
	if r.MetricsExporter == nil {
		return
	}
	blockID := ""
	if len(slice.Spec.PartitionIds) > 0 {
		blockID, _ = r.RelationalCache.GetBlockID(slice.Spec.PartitionIds[0])
	}
	r.MetricsExporter.RecordSliceDeformLatency(ctx, slice.Name, slice.Namespace, string(slice.Spec.Type), slice.Spec.Topology, blockID, string(slice.UID), duration)
}

// handleSliceDeletion handles the deletion process of a Slice.
func (r *SliceReconciler) handleSliceDeletion(ctx context.Context, slice *slicev1beta1.Slice) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Slice State: Deleting")
	if !controllerutil.ContainsFinalizer(slice, sliceFinalizerName) {
		return ctrl.Result{}, nil
	}

	var result ctrl.Result
	var err error

	if util.IsSubslice(&slice.Spec) {
		if !r.EnableSubSlicing {
			log.Error(nil, "SubSlicing is disabled")
			return ctrl.Result{}, nil
		}
		result, err = r.handleSubsliceDeletion(ctx, slice)
		if err != nil || result.Requeue || result.RequeueAfter > 0 {
			return result, err
		}

	} else {
		log.Info("Detected super-slice deletion request.")
		condition, _ := util.FindSliceCondition(slice, slicev1beta1.SliceStateConditionType)
		reason := condition.Reason

		// --- STAGE 1: Trigger Deformation (if not already triggered) ---
		if r.shouldTriggerDeformation(reason) {
			triggerAction := action.NewTriggerDeformationAction(
				r.GCEHelper, r.ProjectNumber, r.Region, r.ClusterHash, r.RelationalCache,
			)

			if err := r.SliceEngine.Execute(ctx, r.Client, slice, triggerAction); err != nil {
				if errors.Is(err, util.ErrDeformationSkipped) {
					log.Info("Deformation skipped because MMIG has no members, proceeding to cleanup")
				} else {
					log.Error(err, "Failed to trigger deformation for slice")
					return ctrl.Result{}, err
				}
			} else {
				log.Info("Triggered deformation for slice")
				return ctrl.Result{}, nil
			}
		}

		// if still deactivating, wait, do not proceed to detach the rest of the migs.
		if reason == string(util.MMIGHealthStatusDeactivating) {
			// If we've been deactivating for >10 minutes, something might be stuck.
			// Requeue gently to ensure we don't sleep forever if an event is missed.
			if !condition.LastTransitionTime.IsZero() && time.Since(condition.LastTransitionTime.Time) > 10*time.Minute {
				log.Info("Deformation taking longer than expected. Checking state again.")
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}

			log.Info("Waiting for deformation to complete...")
			return ctrl.Result{}, nil
		}

		labelsToCheck := util.NodeLabels{
			RegularLabels: map[string]string{
				util.TopologyLabelKey: slice.Spec.Topology,
				util.SliceLabelKey:    slice.Name,
			},
		}
		if r.EnableLinkedRunner {
			labelsToCheck.LinkedRunnerLabels = map[string]string{
				fmt.Sprintf("%s-%s", util.SliceLabelKey, slice.Name): "true",
			}
		}
		allUnlabeled, err := r.KubeHelper.CheckNodeLabels(ctx, slice.Spec.PartitionIds, labelsToCheck, false)
		if err != nil {
			log.Error(err, "Failed to check node labels for slice")
			return ctrl.Result{}, err
		}

		if !allUnlabeled {
			keysToUnlabel := util.NodeLabelKeys{
				RegularLabels: []string{util.TopologyLabelKey, util.SliceLabelKey},
			}
			if r.EnableLinkedRunner {
				keysToUnlabel.LinkedRunnerLabels = []string{fmt.Sprintf("%s-%s", util.SliceLabelKey, slice.Name)}
			}
			unlabelAction := action.NewUnlabelNodesAction(r.KubeHelper, keysToUnlabel, r.EnableHighNodeLabelingConcurrency)
			if err := r.SliceEngine.Execute(ctx, r.Client, slice, unlabelAction); err != nil {
				log.Error(err, "Failed to unlabel nodes for slice")
				return ctrl.Result{}, err
			}
			// Update NodeLabeled condition
			if _, err := util.PatchSliceCondition(ctx, r.Client, slice, util.NodeLabeled, metav1.ConditionFalse, util.NodeLabelIsRemoved, util.NodeLabelingCompleted, r.MetricsExporter); err != nil {
				log.Error(err, "Failed to update NodeLabeled condition during slice deletion")
				return ctrl.Result{}, err
			}
		}

		// 2b. Detach, Delete MMIG
		cleanupAction := action.NewCleanupGCEAction(
			r.GCEHelper, r.ProjectNumber, r.Region, r.Location, r.ClusterName, r.ClusterHash, r.RelationalCache, r.KubeHelper)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, cleanupAction); err != nil {
			log.Error(err, "Failed to cleanup GCE resources for slice")
			if _, patchErr := util.PatchSliceCondition(
				ctx, r.Client,
				slice, slicev1beta1.SliceStateConditionType, "",
				"", err.Error(),
				r.MetricsExporter,
			); patchErr != nil {
				log.Error(patchErr, "Failed to patch slice condition during deletion")
				return ctrl.Result{}, patchErr
			}
			return ctrl.Result{}, err
		}
	}

	// 3c. Finalize
	if err := r.RelationalCache.DeleteSlice(slice.Name); err != nil {
		log.Error(err, "Failed to remove slice from relational cache")
	}

	log.Info("Cleanup complete. Removing finalizer.")
	patch := client.MergeFrom(slice.DeepCopy())
	controllerutil.RemoveFinalizer(slice, sliceFinalizerName)
	if err := r.Patch(ctx, slice, patch); err != nil {
		log.Error(err, "Failed to remove finalizer from Slice")
		return ctrl.Result{}, err
	}
	if !slice.DeletionTimestamp.IsZero() {
		duration := time.Since(slice.DeletionTimestamp.Time)
		r.recordSliceDeformLatency(ctx, slice, duration)
		log.Info("Slice deformation is done", "duration_sec", duration.Seconds())
		r.emitSliceMetadata(ctx, slice, 0)
	}
	return ctrl.Result{}, nil
}

// handleSubsliceDeletion handles the specific logic for deleting a sub-slice.
func (r *SliceReconciler) handleSubsliceDeletion(ctx context.Context, slice *slicev1beta1.Slice) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Detected sub-slice deletion request.")

	condition, _ := util.FindSliceCondition(slice, slicev1beta1.SliceStateConditionType)
	reason := condition.Reason

	// --- STAGE 0: Wait for in-flight activate operation to complete ---
	// Reuse the generic LIFO barrier (checkSubsliceOperationBarrier) to ensure
	// the last activate op finishes before we send a deactivate.
	if r.checkSubsliceOperationBarrier(ctx, slice, util.SubsliceLastOpAnnotation) {
		log.Info("Activate operation still in-flight, waiting before deletion")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// --- STAGE 1: Trigger Deactivation (if hardware is Active/Activating) ---
	partitionID := r.getEffectivePartition(slice)
	_, found := r.RelationalCache.GetMIGByPartitionID(partitionID)
	log.Info("Sub-slice status and MIG lookup result", "reason", reason, "migFound", found)

	var topo cache.AcceleratorTopologyInfo
	var getErr error
	if reason == util.SliceCreationFailed && partitionID != "" {
		topo, getErr = r.getPartitionTopologyFromGCE(ctx, partitionID)
		if getErr != nil {
			log.Error(getErr, "Failed to get partition topology from GCE for failed sub-slice deactivation check")
			return ctrl.Result{}, getErr
		}
	}

	expectedID := util.GetResourceName(slice.Name, r.ClusterHash)
	// Trigger DEACTIVATE when:
	// 1. State is ACTIVE/ACTIVATING (normal deletion path)
	// 2. State is DynamicSliceFailed and MIG is found
	// 3. State is SliceCreationFailed and GCE topology matches
	// 4. Partition annotation exists and MIG is found — covers preemption:
	//    scheduler clears PartitionIds → state becomes SliceNotCreated,
	//    but the GCE partition is still ACTIVATED and must be deactivated.
	if r.shouldTriggerSubsliceDeformation(reason) || (found && reason == util.DynamicSliceFailed) || (reason == util.SliceCreationFailed && (topo.SliceID == expectedID || topo.SliceID == "")) || (partitionID != "" && found) {
		log.Info("Sending DEACTIVATE request for sub-slice", "status", reason)
		deformAction := action.DeformSubsliceAction(r.GCEHelper, r.RelationalCache, r.ProjectNumber, r.Region, r.ClusterHash, partitionID)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, deformAction); err != nil {
			if deactErr := r.handleSubsliceDeactivationFailure(ctx, slice, err, reason, partitionID, topo); deactErr != nil {
				return ctrl.Result{}, deactErr
			}
		} else {
			log.Info("Triggered deactivation for sub-slice")
		}
	}

	// --- STAGE 2: Wait for Deactivation to complete ---
	if reason == util.DynamicSliceDeactivating {
		if !condition.LastTransitionTime.IsZero() && time.Since(condition.LastTransitionTime.Time) > 10*time.Minute {
			log.Info("Sub-slice deactivation taking longer than expected. Re-checking state.")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		log.Info("Waiting for sub-slice deactivation to complete...")
	}

	// --- STAGE 3: Unlabel Nodes ---
	if err := r.unlabelSubsliceNodes(ctx, slice, partitionID); err != nil {
		return ctrl.Result{}, err
	}

	if found {
		topo, err := r.getPartitionTopologyFromGCE(ctx, partitionID)
		if err != nil {
			log.Error(err, "Failed to get real-time state from GCE for finalization")
			return ctrl.Result{}, err
		}

		if topo.SliceID != expectedID {
			if topo.SliceID == "" {
				log.Info("Sub-slice not owned by this slice in GCE (empty ID), proceeding to delete.", "partition", partitionID)
			} else {
				log.Info("Sub-slice is owned by another slice in GCE, implying a new slice has been formed. Proceeding to delete.", "partition", partitionID)
			}
		} else {
			state := string(topo.AcceleratorTopologyState)
			if state == util.DynamicSliceInactive {
				log.Info("Sub-slice state in GCE implies it can be deleted.", "partition", partitionID, "gceState", state)
			} else {
				log.Info("Sub-slice is still active or failed or activating in GCE, waiting for deactivation.", "partition", partitionID, "gceState", state)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
		}
	} else {
		// If not found, the MIG was deleted, and we will proceed directly to delete the sub-slice.
		log.Info("MIG not found for sub-slice, proceeding to delete.", "partition", partitionID)
	}

	return ctrl.Result{}, nil
}

// handleSliceInitialization handles the initialization process of a Slice.
func (r *SliceReconciler) handleSliceInitialization(ctx context.Context, slice *slicev1beta1.Slice) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Slice State: Initializing")

	// 1. Add Finalizer First
	if !controllerutil.ContainsFinalizer(slice, sliceFinalizerName) {
		patch := client.MergeFrom(slice.DeepCopy())
		controllerutil.AddFinalizer(slice, sliceFinalizerName)

		if err := r.Patch(ctx, slice, patch); err != nil {
			log.Error(err, "Failed to add finalizer to Slice")
			return ctrl.Result{}, err
		}
		log.Info("Finalizer added to Slice.")

		return ctrl.Result{}, nil
	}

	if util.FindStateCondition(slice) == "" {
		baseSlice := slice.DeepCopy()
		condition := metav1.Condition{
			Type:               slicev1beta1.SliceStateConditionType,
			Status:             metav1.ConditionUnknown,
			LastTransitionTime: metav1.Now(),
			Reason:             util.SliceNotCreated,
			Message:            "",
		}
		meta.SetStatusCondition(&slice.Status.Conditions, condition)
		slice.Status.ObservedGeneration = slice.ObjectMeta.Generation

		patch := client.MergeFrom(baseSlice)
		if err := r.Status().Patch(ctx, slice, patch); err != nil {
			log.Error(err, "Failed to patch slice condition and observedGeneration during initialization", "generation", slice.ObjectMeta.Generation)
			return ctrl.Result{}, err
		}
		log.Info("Slice status initialized.")
		r.emitSliceMetadata(ctx, slice, 1)
	}

	return ctrl.Result{}, nil
}

// handlePreflightChecks performs preflight checks for the slice.
func (r *SliceReconciler) handlePreflightChecks(ctx context.Context, slice *slicev1beta1.Slice) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Slice State: Performing Preflight Checks")

	// 1. Validate sub-slice.
	if util.IsSubslice(&slice.Spec) {
		if !r.EnableSubSlicing {
			return r.reportPreflightError(ctx, slice, fmt.Sprintf("%s: SubSlicing is disabled", util.PreflightCheckError))
		}
		if len(slice.Spec.PartitionIds) > 1 {
			return r.reportPreflightError(ctx, slice, fmt.Sprintf("%s: sub-slice allows at most one partitionId", util.PreflightCheckError))
		}
	}

	var errMsg string
	var expectedReservationName string
	var expectedBlockID string
	var isReferenceSet bool
	var preflightPassed bool = true

	// 2. Validate partition consistency.

	for _, partitionID := range slice.Spec.PartitionIds {
		// Get MIG from Cache
		MIGInfo, found := r.RelationalCache.GetMIGByPartitionID(partitionID)
		if !found {
			log.Error(fmt.Errorf("MIG info not found"), "Preflight check failed", "PartitionID", partitionID)
			errMsg = fmt.Sprintf("%s: MIG info not found for partition ID %s", util.PreflightCheckError, partitionID)
			preflightPassed = false
			break
		}

		// Get ONE representative node from the map
		// Since MIGs are homogeneous, checking one node is sufficient.
		var node *cache.NodeInfo
		var matchedTopo string
		if util.IsSubslice(&slice.Spec) && r.EnableSubSlicing {
			// Search for the node that contains the partitionID in its topology mappings.
			// This is necessary for subslices where nodes in a MIG have different partition IDs.
			var found bool
			for _, n := range MIGInfo.Nodes {
				for topo, ids := range n.TopologyPartitionIds {
					for _, id := range ids {
						if id == partitionID {
							node = n
							matchedTopo = topo
							found = true
							break
						}
					}
					if found {
						break
					}
				}
				if found {
					break
				}
			}
			if matchedTopo != slice.Spec.Topology {
				log.Error(fmt.Errorf("Topology mismatch"), "Preflight check failed", "PartitionID", partitionID, "SpecTopology", slice.Spec.Topology)
				errMsg = fmt.Sprintf("%s: Topology in spec (%s) does not match topology of partition %s (%s)", util.PreflightCheckError, slice.Spec.Topology, partitionID, matchedTopo)
				preflightPassed = false
				break
			}
		} else {
			// For super-slices, MIGs are homogeneous, picking any node is sufficient.
			for _, n := range MIGInfo.Nodes {
				node = n
				break
			}
		}

		// Safety check.
		if node == nil {
			log.Error(fmt.Errorf("No nodes found in partition"), "Preflight check failed", "PartitionID", partitionID)
			errMsg = fmt.Sprintf("%s: Partition %s has no nodes to validate", util.PreflightCheckError, partitionID)
			preflightPassed = false
			break
		}

		// Ensure MIG has enough nodes
		// For Linked Runners, there is only ONE control node in the MIG.
		expectedNodeCount := util.NumNodesInCube
		if info, ok := util.SupportedTopologiesInfo[slice.Spec.Topology]; ok {
			expectedNodeCount = info.NodeCount / len(slice.Spec.PartitionIds)
		}

		if !node.IsLinkedRunnerControlNode && len(MIGInfo.Nodes) < expectedNodeCount {
			log.Error(fmt.Errorf("Not enough nodes in MIG"), "Preflight check failed",
				"PartitionID", partitionID, "NodeCount", len(MIGInfo.Nodes), "ExpectedCount", expectedNodeCount)
			errMsg = fmt.Sprintf("%s: Partition %s has not enough nodes (expected %d, found %d)",
				util.PreflightCheckError, partitionID, expectedNodeCount, len(MIGInfo.Nodes))
			preflightPassed = false
			break
		}

		if node.IsLinkedRunnerControlNode {
			log.V(1).Info("Skipping reservation checks for Linked Runner control node", "PartitionID", partitionID, "Node", node.Name)
			continue
		}

		// Comparison Logic
		if !isReferenceSet {
			// First pass: Set the expected values based on the first MIG
			expectedReservationName = node.ReservationName
			expectedBlockID = node.ReservationBlockID
			isReferenceSet = true
		} else {
			// Subsequent passes: Compare this MIG's representative against the reference
			if node.ReservationName != expectedReservationName {
				log.Error(fmt.Errorf("Inconsistent ReservationName"), "Preflight check failed", "PartitionID", partitionID, "Expected", expectedReservationName, "Found", node.ReservationName)
				errMsg = fmt.Sprintf("%s: Inconsistent ReservationName. Expected '%s', found '%s' in partition %s",
					util.PreflightCheckError, expectedReservationName, node.ReservationName, partitionID)
				preflightPassed = false
				break
			}

			if node.ReservationBlockID != expectedBlockID {
				log.Error(fmt.Errorf("Inconsistent BlockID"), "Preflight check failed", "PartitionID", partitionID, "Expected", expectedBlockID, "Found", node.ReservationBlockID)
				errMsg = fmt.Sprintf("%s: Inconsistent BlockID. Expected '%s', found '%s' in partition %s",
					util.PreflightCheckError, expectedBlockID, node.ReservationBlockID, partitionID)
				preflightPassed = false
				break
			}
		}
	}

	// Patch Status
	if !preflightPassed {
		return r.reportPreflightError(ctx, slice, errMsg)
	}

	return true, nil
}

// reportPreflightError patches the slice status with a preflight error message and returns.
func (r *SliceReconciler) reportPreflightError(ctx context.Context, slice *slicev1beta1.Slice, errMsg string) (bool, error) {
	condition, _ := util.FindSliceCondition(slice, slicev1beta1.SliceStateConditionType)
	if condition.Reason != util.SliceCreationFailed || condition.Message != errMsg {
		sliceLogger.Info("Preflight check failed.", "Error", errMsg)
		if _, err := util.PatchSliceCondition(ctx, r.Client, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.SliceCreationFailed, errMsg, r.MetricsExporter); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "Failed to patch slice condition")
			return false, err
		}
	}
	return false, nil
}

// handleSliceUpdate handles updates to the slice spec.
func (r *SliceReconciler) handleSliceUpdate(ctx context.Context, slice *slicev1beta1.Slice) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	if !r.EnableSliceUpdate {
		log.Info("Slice update is disabled by feature flag")
		return ctrl.Result{}, nil
	}
	log.Info("Slice State: Updating")

	// Concurrency Guard: Capture generation and partition IDs
	expectedGeneration := slice.ObjectMeta.Generation
	targetPartitions := slice.Spec.PartitionIds
	log.Info("Captured slice state at beginning of update", "generation", expectedGeneration, "partitions", targetPartitions)

	// 2. Clean up existing resources or stale labels.
	// We always execute the cleanup action. The action itself checks if the MMIG exists in GCE
	// and skips GCE deletion if it's already gone, but always ensures nodes are unlabeled.
	labelKeys := util.NodeLabelKeys{
		RegularLabels: []string{util.TopologyLabelKey, util.SliceLabelKey},
	}
	var sliceToClean *slicev1beta1.Slice = slice

	cleanupAction := action.NewCleanupMMIGAndUnlabelAction(
		r.GCEHelper, r.KubeHelper, r.ComputeService, r.ProjectNumber, r.Region, r.Location, r.ClusterName, r.ClusterHash, r.RelationalCache,
		labelKeys, r.EnableHighNodeLabelingConcurrency,
		slice.Name,
	)
	if err := r.SliceEngine.Execute(ctx, r.Client, sliceToClean, cleanupAction); err != nil {
		log.Error(err, "Failed to cleanup existing MMIG and unlabel nodes")
		return ctrl.Result{}, err
	}
	log.Info("Successfully executed cleanup action")

	if util.IsSubslice(&slice.Spec) {
		if !r.SupportSubsliceUpdate {
			log.Info("Sub-slice update is disabled by feature flag")
			return ctrl.Result{}, nil
		}
		return r.handleSubsliceUpdate(ctx, slice, expectedGeneration)
	}

	// 3. Handle 0-N Update
	if len(targetPartitions) > 0 {
		log.Info("Detected update from 0 to partitions", "count", len(targetPartitions))

		// Run preflight checks
		shouldProceed, err := r.handlePreflightChecks(ctx, slice)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !shouldProceed {
			baseSlice := slice.DeepCopy()
			slice.Status.ObservedGeneration = expectedGeneration
			if patchErr := r.Status().Patch(ctx, slice, client.MergeFrom(baseSlice)); patchErr != nil {
				log.Error(patchErr, "Failed to patch observedGeneration after preflight failure")
				return ctrl.Result{}, patchErr
			}
			return ctrl.Result{}, nil
		}

		// Proceed with WP and MMIG Creation
		createWPAndMMIGAction := action.NewCreateWPAndMMIGAction(
			r.GCEHelper,
			r.ProjectNumber,
			r.Region,
			r.ClusterHash,
		)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, createWPAndMMIGAction); err != nil {
			if _, patchErr := util.PatchSliceCondition(
				ctx, r.Client,
				slice, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse,
				util.SliceCreationFailed, err.Error(),
				r.MetricsExporter,
			); patchErr != nil {
				log.Error(patchErr, "Failed to patch slice condition")
				return ctrl.Result{}, patchErr
			}

			if slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation {
				log.Info("WP/MMIG creation failed; will be retried", "error", err)
				return ctrl.Result{}, err
			}

			// NOT retrying: advance observedGeneration to avoid infinite loops
			baseSlice := slice.DeepCopy()
			slice.Status.ObservedGeneration = expectedGeneration
			if patchErr := r.Status().Patch(ctx, slice, client.MergeFrom(baseSlice)); patchErr != nil {
				log.Error(patchErr, "Failed to patch observedGeneration after WP/MMIG creation failure")
				return ctrl.Result{}, patchErr
			}

			log.Info("WP/MMIG creation failed; will not be retried", "error", err)
			return ctrl.Result{}, nil
		}

		// Run Attachment Step
		attachMIGsAction := action.NewAttachMIGsAction(
			r.GCEHelper,
			r.ProjectNumber,
			r.Region,
			r.Location,
			r.ClusterName,
			r.ClusterHash,
			r.RelationalCache,
		)
		attachErr := r.SliceEngine.Execute(ctx, r.Client, slice, attachMIGsAction)

		if attachErr != nil {
			if _, patchErr := util.PatchSliceCondition(
				ctx, r.Client,
				slice, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse,
				util.SliceCreationFailed, attachErr.Error(),
				r.MetricsExporter,
			); patchErr != nil {
				log.Error(patchErr, "Failed to patch slice condition after attachment failure")
				return ctrl.Result{}, patchErr
			}

			if slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation {
				log.Info("Attachment failed; will be retried", "error", attachErr)
				return ctrl.Result{}, attachErr
			}

			// NOT retrying: advance observedGeneration to avoid infinite loops
			baseSlice := slice.DeepCopy()
			slice.Status.ObservedGeneration = expectedGeneration
			if patchErr := r.Status().Patch(ctx, slice, client.MergeFrom(baseSlice)); patchErr != nil {
				log.Error(patchErr, "Failed to patch observedGeneration after attachment failure")
				return ctrl.Result{}, patchErr
			}

			log.Info("Attachment failed; will not be retried", "error", attachErr)
			return ctrl.Result{}, nil
		}

		// Request 2: Early return after successful attachment
		log.Info("Attachment successful. Advancing observedGeneration and returning for next reconcile.")

		// Add slice to relational cache after successful 0-N update
		if err := r.RelationalCache.AddSlice(slice.Name, slice.Spec.PartitionIds, slice.Spec.Topology); err != nil {
			log.Error(err, "Failed to add slice to relational cache after successful 0-N update")
			return ctrl.Result{}, err
		}

		baseSlice := slice.DeepCopy()
		slice.Status.ObservedGeneration = expectedGeneration
		patch := client.MergeFrom(baseSlice)
		if err := r.Status().Patch(ctx, slice, patch); err != nil {
			log.Error(err, "Failed to patch observedGeneration after successful attachment")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// 4. Handle N-0 Update
	if len(targetPartitions) == 0 {
		log.Info("Detected update from N to 0 partitions")

		// Note: It is a prerequisite to deform the slice, clean the MMIG, and unlabel the
		// nodes before updating the conditions. This is handled by the cleanupAction
		// executed at the beginning of this function.

		baseSlice := slice.DeepCopy()
		condition := metav1.Condition{
			Type:               slicev1beta1.SliceStateConditionType,
			Status:             metav1.ConditionUnknown,
			LastTransitionTime: metav1.Now(),
			Reason:             util.SliceNotCreated,
			Message:            "Slice update completed successfully",
		}
		meta.SetStatusCondition(&slice.Status.Conditions, condition)

		nodeLabeledCondition := metav1.Condition{
			Type:               util.NodeLabeled,
			Status:             metav1.ConditionFalse,
			LastTransitionTime: metav1.Now(),
			Reason:             util.NodeLabelIsRemoved,
			Message:            util.NodeLabelingCompleted,
		}
		meta.SetStatusCondition(&slice.Status.Conditions, nodeLabeledCondition)

		slice.Status.ObservedGeneration = expectedGeneration

		patch := client.MergeFrom(baseSlice)
		if err := r.Status().Patch(ctx, slice, patch); err != nil {
			log.Error(err, "Failed to patch slice status after N-0 update success")
			return ctrl.Result{}, err
		}

		log.Info("N-0 update complete.")
		return ctrl.Result{}, nil
	}

	// 5. Fallback for unsupported updates or no-op
	if slice.Status.ObservedGeneration == expectedGeneration {
		return ctrl.Result{}, nil
	}
	log.Info("Unsupported update request, skipping partition update and advancing observedGeneration", "generation", expectedGeneration)
	patch := client.MergeFrom(slice.DeepCopy())
	slice.Status.ObservedGeneration = expectedGeneration
	if err := r.Status().Patch(ctx, slice, patch); err != nil {
		log.Error(err, "Failed to patch observedGeneration for unhandled update")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *SliceReconciler) handleSliceFormation(ctx context.Context, slice *slicev1beta1.Slice) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Slice State: Forming")

	if util.IsSubslice(&slice.Spec) {
		if !r.EnableSubSlicing {
			log.Error(nil, "SubSlicing is disabled")
			return ctrl.Result{}, nil
		}
		return r.handleSubsliceFormation(ctx, slice)
	}

	log.Info("Detected super-slice request, proceeding with existing formation logic.")
	reason := util.FindStateCondition(slice)
	isReady := reason == string(util.MMIGHealthStatusActive) || reason == string(util.MMIGHealthStatusActiveDegraded)
	mmigName := util.GetResourceName(slice.Name, r.ClusterHash)

	if !isReady {
		if reason == string(util.MMIGHealthStatusActivating) {
			log.Info("Waiting for MMIG to become Active...", "CurrentState", reason, "MMIGName", mmigName)
			return ctrl.Result{}, nil
		}
		// Create resources
		createAction := action.NewCreateGCEResourcesAction(
			r.GCEHelper,
			r.ProjectNumber,
			r.Region,
			r.Location,
			r.ClusterName,
			r.ClusterHash,
			r.RelationalCache,
			r.KubeHelper,
		)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, createAction); err != nil {
			// Update condition to GCECreationFailure
			if _, patchErr := util.PatchSliceCondition(
				ctx, r.Client,
				slice, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse,
				util.SliceCreationFailed, err.Error(),
				r.MetricsExporter,
			); patchErr != nil {

				// Log the failure so it's not silent
				log.Error(patchErr, "Failed to patch slice condition")
				return ctrl.Result{}, patchErr
			}
			// Check for Retry Annotation
			// If annotation is set to "true", we want to retry by returning the original error.
			if slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation {
				log.Info("Formation failed; will be retried", "error", err)

				var opErr *util.OperationError
				var timeoutErr *util.TimeoutError
				var apiErr *util.APICallError
				if errors.As(err, &opErr) && !errors.As(err, &timeoutErr) && !errors.As(err, &apiErr) {
					log.Info("Operation error detected, incrementing retry attempt for next loop")

					patchBaseMeta := client.MergeFrom(slice.DeepCopy())
					util.IncrementRetryAttempt(slice)

					// Patch ONLY the metadata
					if patchErr := r.Client.Patch(ctx, slice, patchBaseMeta); patchErr != nil {
						log.Error(patchErr, "Failed to patch slice annotations with new retry attempt")
						return ctrl.Result{}, patchErr
					}
				}
				return ctrl.Result{}, err
			}

			log.Info("Formation failed; will not be retried", "error", err)
			return ctrl.Result{}, nil
		}

		// Fetch the latest status from GCE
		mmigStatus, err := r.GCEHelper.GetMMIGHealthStatus(ctx, r.ProjectNumber, r.Region, mmigName)
		if err != nil || len(mmigStatus.TopologyHealths) == 0 {
			log.Error(err, "Failed to get MMIG health status after creation", "MMIGName", mmigName)
			return ctrl.Result{}, err
		}
		// Determine the state from the fetched status
		mmigState := util.MMIGHealthStatus(mmigStatus.TopologyHealths[0].HealthStatus)
		status := util.ConvertReasonToStatus(string(mmigState))

		message := util.SliceFormationInProgress
		if mmigState == util.MMIGHealthStatusFailed {
			message = sliceFormationFailedMessage
		}
		if _, err := util.PatchSliceCondition(
			ctx,
			r.Client,
			slice,
			slicev1beta1.SliceStateConditionType,
			status,
			string(mmigState),
			message,
			r.MetricsExporter,
		); err != nil {
			log.Error(err, "Failed to patch slice condition after formation")
			return ctrl.Result{}, err
		}
		// Add slice to relational cache
		partitionIDs := []string{}
		partitionIDs = append(partitionIDs, slice.Spec.PartitionIds...)
		if err := r.RelationalCache.AddSlice(slice.Name, partitionIDs, slice.Spec.Topology); err != nil {
			log.Error(err, "Failed to add slice to relational cache after formation")
			return ctrl.Result{}, err
		}

	} else {
		// Condition is True (Active), check labels
		partitionIDs := []string{}
		partitionIDs = append(partitionIDs, slice.Spec.PartitionIds...)
		if err := r.RelationalCache.AddSlice(slice.Name, partitionIDs, slice.Spec.Topology); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "Failed to update slice in relational cache when active")
			return ctrl.Result{}, err
		}

		labelsToCheck := util.NodeLabels{
			RegularLabels: map[string]string{
				util.TopologyLabelKey: slice.Spec.Topology,
				util.SliceLabelKey:    slice.Name,
			},
		}
		if r.EnableLinkedRunner {
			labelsToCheck.LinkedRunnerLabels = map[string]string{
				fmt.Sprintf("%s-%s", util.SliceLabelKey, slice.Name): "true",
			}
		}
		labelAction := action.NewLabelNodesAction(r.KubeHelper, labelsToCheck, r.EnableHighNodeLabelingConcurrency)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, labelAction); err != nil {
			log.Error(err, "Failed to label nodes for slice")
			return ctrl.Result{}, err
		}
		// Explicitly update NodeLabeled condition to true
		if _, err := util.PatchSliceCondition(ctx, r.Client, slice, util.NodeLabeled, metav1.ConditionTrue, util.NodeLabelIsAdded, util.NodeLabelingCompleted, r.MetricsExporter); err != nil {
			log.Error(err, "Failed to update NodeLabeled condition")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// handleSubsliceFormation handles the formation process of a sub-slice.
func (r *SliceReconciler) handleSubsliceFormation(ctx context.Context, slice *slicev1beta1.Slice) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Detected sub-slice request, dispatching to FormSubsliceAction.")

	reason := util.FindStateCondition(slice)
	isReady := reason == util.DynamicSliceActive

	if !isReady {
		if reason == util.DynamicSliceActivating {
			log.Info("Waiting for sub-slice to become Active...", "CurrentState", reason)
			return ctrl.Result{}, nil
		}

		formAction := action.FormSubsliceAction(r.GCEHelper, r.RelationalCache, r.ProjectNumber, r.Region, r.ClusterHash)
		formErr := r.SliceEngine.Execute(ctx, r.Client, slice, formAction)

		// Persist the operation annotation (set in-memory by the action step) even on error,
		// since the GCE operation may have been dispatched before timeout.
		if persistErr := r.persistSubsliceOperationAnnotation(ctx, slice); persistErr != nil {
			log.Error(persistErr, "Failed to persist subslice operation annotation")
		}

		if formErr != nil {
			log.Error(formErr, "Failed to execute form action for sub-slice")

			if _, patcherr := util.PatchSliceCondition(ctx, r.Client, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.SliceCreationFailed, formErr.Error(), r.MetricsExporter); patcherr != nil {
				log.Error(patcherr, "Failed to patch slice condition")
				return ctrl.Result{}, patcherr
			}

			if slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation {
				log.Info("Formation failed; will be retried", "error", formErr)

				var opErr *util.OperationError
				var timeoutErr *util.TimeoutError
				var apiErr *util.APICallError
				if errors.As(formErr, &opErr) && !errors.As(formErr, &timeoutErr) && !errors.As(formErr, &apiErr) {
					log.Info("Operation error detected, incrementing retry attempt for next loop")

					patchBaseMeta := client.MergeFrom(slice.DeepCopy())
					util.IncrementRetryAttempt(slice)

					// Patch ONLY the metadata
					if patchErr := r.Client.Patch(ctx, slice, patchBaseMeta); patchErr != nil {
						log.Error(patchErr, "Failed to patch slice annotations with new retry attempt")
						return ctrl.Result{}, patchErr
					}
				}

				return ctrl.Result{}, formErr
			}

			log.Info("Formation failed; will not be retried", "error", formErr)
			return ctrl.Result{}, nil
		}

		// Add sub-slice to relational cache.
		if err := r.RelationalCache.AddSlice(slice.Name, slice.Spec.PartitionIds, slice.Spec.Topology); err != nil {
			log.Error(err, "Failed to add sub-slice to relational cache after formation")
			return ctrl.Result{}, err
		}

		// Re-fetch state after sending ACTIVATE.
		topo, err := r.getPartitionTopologyFromGCE(ctx, slice.Spec.PartitionIds[0])
		if err != nil {
			log.Error(err, "Failed to get real-time state after sub-slice formation")
			return ctrl.Result{}, err
		}

		expectedID := util.GetResourceName(slice.Name, r.ClusterHash)
		realTimeState := string(topo.AcceleratorTopologyState)
		var validationErr error

		if topo.SliceID != expectedID {
			realTimeState = util.SliceCreationFailed
			validationErr = fmt.Errorf("the current slice id does not match, current is: %q, expected is: %q. Current topology state: %s", topo.SliceID, expectedID, string(topo.AcceleratorTopologyState))
		}

		status := util.ConvertReasonToStatus(realTimeState)
		message := util.SliceFormationInProgress
		if validationErr != nil {
			message = validationErr.Error()
		}

		if _, patchErr := util.PatchSliceCondition(ctx, r.Client, slice, slicev1beta1.SliceStateConditionType, status, realTimeState, message, r.MetricsExporter); patchErr != nil {
			log.Error(patchErr, "Failed to patch slice condition after sub-slice formation")
			return ctrl.Result{}, patchErr
		}
		if validationErr != nil {
			if realTimeState == util.SliceCreationFailed {
				log.Error(validationErr, "Formation failed (Creation Failed); unexpected state")
				return ctrl.Result{}, nil
			}
			log.Error(validationErr, "Sub-slice formation pending; will not be retried via error")
			return ctrl.Result{}, nil
		}
	} else {
		// Slice is ACTIVE, ensure nodes are labeled.
		labelsToCheck := util.NodeLabels{
			RegularLabels: map[string]string{
				util.TopologyLabelKey: slice.Spec.Topology,
				util.SliceLabelKey:    slice.Name,
			},
		}
		if r.EnableLinkedRunner {
			labelsToCheck.LinkedRunnerLabels = map[string]string{
				fmt.Sprintf("%s-%s", util.SliceLabelKey, slice.Name): "true",
			}
		}
		labelAction := action.NewLabelNodesAction(r.KubeHelper, labelsToCheck, r.EnableHighNodeLabelingConcurrency)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, labelAction); err != nil {
			log.Error(err, "Failed to label nodes for sub-slice")
			return ctrl.Result{}, err
		}
		if _, err := util.PatchSliceCondition(ctx, r.Client, slice, util.NodeLabeled, metav1.ConditionTrue, util.NodeLabelIsAdded, util.NodeLabelingCompleted, r.MetricsExporter); err != nil {
			log.Error(err, "Failed to update NodeLabeled condition")
			return ctrl.Result{}, err
		}

		// Stamp provisioned partition annotation for robust crash recovery
		partitionID := r.getEffectivePartition(slice)
		if partitionID != "" && slice.Annotations[util.SubsliceProvisionedPartitionAnnotation] == "" {
			patch := client.MergeFrom(slice.DeepCopy())
			if slice.Annotations == nil {
				slice.Annotations = make(map[string]string)
			}
			slice.Annotations[util.SubsliceProvisionedPartitionAnnotation] = partitionID
			if err := r.Client.Patch(ctx, slice, patch); err != nil {
				log.Error(err, "Failed to stamp provisioned partition annotation")
				return ctrl.Result{}, err
			}
			log.Info("Stamped Slice with provisioned partition annotation upon initial ACTIVE state", "partition", partitionID)
		}
	}

	return ctrl.Result{}, nil
}

// persistSubsliceOperationAnnotation persists the in-memory SubsliceLastOpAnnotation to the API server.
// The annotation is set in-memory by executeSubsliceConfig during the FormSubsliceAction step.
// We read the current slice from the API server to get a clean base for the merge patch.
func (r *SliceReconciler) persistSubsliceOperationAnnotation(ctx context.Context, slice *slicev1beta1.Slice) error {
	opName, hasOp := slice.Annotations[util.SubsliceLastOpAnnotation]
	if !hasOp || opName == "" {
		return nil
	}
	log := ctrl.LoggerFrom(ctx)
	log.Info("Persisting subslice operation annotation", "operation", opName)

	// Read the current slice from the API server to get a clean base for patching.
	current := &slicev1beta1.Slice{}
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(slice), current); err != nil {
		return fmt.Errorf("failed to get current slice for annotation patch: %w", err)
	}

	patchBase := client.MergeFrom(current.DeepCopy())
	if current.Annotations == nil {
		current.Annotations = make(map[string]string)
	}
	current.Annotations[util.SubsliceLastOpAnnotation] = opName

	return r.Client.Patch(ctx, current, patchBase)
}

// cacheSyncUp performs an initial synchronization of the relational cache with existing Slices.
func (r *SliceReconciler) cacheSyncUp(ctx context.Context, slice *slicev1beta1.Slice) error {
	log := ctrl.LoggerFrom(ctx)
	// For Sub-slices, we don't have an MMIG to query.
	// Instead, we check if the partition is configured for this slice in GCE.
	if util.IsSubslice(&slice.Spec) && r.EnableSubSlicing {
		return r.cacheSyncUpSubslice(ctx, slice)
	}

	_, hasEntry := r.RelationalCache.GetSliceInfo(slice.Name)
	condition, _ := util.FindSliceCondition(slice, slicev1beta1.SliceStateConditionType)
	reason := condition.Reason
	message := condition.Message
	mmigName := util.GetResourceName(slice.Name, r.ClusterHash)

	// health monitor detected MMIG not found, need to sync up cache
	if reason == util.SliceCreationFailed && message == util.MMIGNotFound {
		if hasEntry {
			log.Info("Status explicitly indicates MMIG not found. Removing stale entry from relational cache.", "MMIGName", mmigName)
			if err := r.RelationalCache.DeleteSlice(slice.Name); err != nil {
				log.Error(err, "Failed to remove stale entry from relational cache", "MMIGName", mmigName)
				return err
			}
		}
		return nil
	}

	// EXPLICIT SKIP ON RESTART:
	// If the controller restarted (or cache is empty for this slice) and the slice already failed creation,
	// we explicitly skip querying GCE and backfilling it into the cache, and only add slice information to relational cache
	// if the slice has passed creation step that created WP/MMIG and attached MIGs.
	if !hasEntry && reason == util.SliceCreationFailed {
		log.Info("Skipping cache backfill because Slice creation previously failed", "SliceName", slice.Name, "MMIGName", mmigName)
		return nil
	}

	// Logic Gate 1: Potential Stale Entry
	// (If reason == SliceCreationFailed here, hasEntry must be true due to the check above)
	shouldVerifyDeletion := (reason == util.SliceCreationFailed || reason == util.SliceNotCreated) && hasEntry

	// Logic Gate 2: Potential Missing Entry
	shouldVerifyAddition := !hasEntry && reason != util.SliceNotCreated

	if shouldVerifyDeletion || shouldVerifyAddition {
		_, err := r.ComputeService.GetMMIG(r.ProjectNumber, r.Region, mmigName)
		// Case A: Resource NOT FOUND in GCE
		if err != nil {
			var gceErr *googleapi.Error
			if errors.As(err, &gceErr) && gceErr.Code == util.ResourceNotFound {
				// If it's not in GCE, but it IS in our cache, we must delete it.
				if hasEntry {
					log.Info("MMIG not found in GCE, removing from relational cache", "MMIGName", mmigName)
					if err := r.RelationalCache.DeleteSlice(slice.Name); err != nil {
						log.Error(err, "Failed to remove stale entry from relational cache", "MMIGName", mmigName)
						return err
					}
				}
				return nil
			}

			// Case C: Actual API Error (Network, Auth, etc)
			return err
		}

		// Case B: Resource FOUND in GCE
		// If it exists in GCE, but NOT in our cache, we must add it.
		if !hasEntry {
			partitionIDs := []string{}
			partitionIDs = append(partitionIDs, slice.Spec.PartitionIds...)
			if err := r.RelationalCache.AddSlice(slice.Name, partitionIDs, slice.Spec.Topology); err != nil {
				log.Error(err, "Failed to backfill slice into relational cache", "MMIGName", mmigName)
				return err
			}
			log.Info("Backfilled Slice into Relational Cache", "MMIGName", mmigName)
		}
		return nil
	}

	return nil
}

// cacheSyncUpSubslice performs cache synchronization specifically for sub-slices.
func (r *SliceReconciler) cacheSyncUpSubslice(ctx context.Context, slice *slicev1beta1.Slice) error {
	log := ctrl.LoggerFrom(ctx)
	_, hasEntry := r.RelationalCache.GetSliceInfo(slice.Name)
	condition, _ := util.FindSliceCondition(slice, slicev1beta1.SliceStateConditionType)
	reason := condition.Reason

	shouldVerifyDeletion := (reason == util.SliceCreationFailed || reason == util.SliceNotCreated) && hasEntry
	shouldVerifyAddition := !hasEntry && reason != util.SliceNotCreated

	if shouldVerifyDeletion || shouldVerifyAddition {
		partitionID := r.getEffectivePartition(slice)
		if partitionID == "" {
			return nil
		}
		expectedID := util.GetResourceName(slice.Name, r.ClusterHash)
		topo, err := r.getPartitionTopologyFromGCE(ctx, partitionID)

		// If error or not owned by this slice
		if err != nil || topo.SliceID != expectedID {
			if hasEntry {
				log.Info("Sub-slice not owned by this slice in GCE, removing from relational cache", "SliceName", slice.Name)
				if err := r.RelationalCache.DeleteSlice(slice.Name); err != nil {
					log.Error(err, "Failed to remove stale entry from relational cache", "SliceName", slice.Name)
					return err
				}
			}
			return nil
		}

		// Owned by this slice in GCE
		if !hasEntry {
			log.Info("Sub-slice owned by this slice in GCE, adding to relational cache", "SliceName", slice.Name)
			if err := r.RelationalCache.AddSlice(slice.Name, []string{partitionID}, slice.Spec.Topology); err != nil {
				log.Error(err, "Failed to add sub-slice to relational cache during sync up", "SliceName", slice.Name)
				return err
			}
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SliceReconciler) SetupWithManager(mgr ctrl.Manager, workerCount int) error {
	r.GCEHelper = gcehelper.NewClient(r.ComputeService, r.EnableHighGCEConcurrency, r.EnableSubSlicing)

	// Define the predicate to filter events before they hit the workqueue
	slicePredicate := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSlice, okOld := e.ObjectOld.(*slicev1beta1.Slice)
			newSlice, okNew := e.ObjectNew.(*slicev1beta1.Slice)

			// Safety check: if casting fails, allow reconciliation to avoid missing an event
			if !okOld || !okNew {
				return true
			}

			// 1. SPEC CHANGE: Always reconcile if the user changes the Spec.
			if oldSlice.Generation != newSlice.Generation {
				return true
			}

			// 2. DELETION: Reconcile any update when the object is marked for deletion.
			if !newSlice.DeletionTimestamp.IsZero() {
				return true
			}

			// 3. FINALIZERS: Reconcile if finalizers are added or removed.
			if len(oldSlice.Finalizers) != len(newSlice.Finalizers) {
				return true
			}

			// 4. ANNOTATIONS: Reconcile if annotations change (e.g., someone adds the RetryAnnotation).
			// Ignore updates to LastReadyConditionTransitionTimeAnnotation and RetryAttemptAnnotationKey to avoid infinite reconciliation loops.
			if !reflect.DeepEqual(oldSlice.Annotations, newSlice.Annotations) && annotationsChangedIgnoringTransitionTime(oldSlice.Annotations, newSlice.Annotations) {
				return true
			}

			// 5. STATE TRANSITION: Reconcile if meaningful status changes occur (Reason/Status).
			// This specifically ignores changes to the condition Message or LastTransitionTime.
			if conditionsChanged(oldSlice.Status.Conditions, newSlice.Status.Conditions) {
				return true
			}

			// IGNORE: If we reach here, it's likely a status message/timestamp update.
			// Returning false allows the workqueue's exponential back-off to stay in control.
			return false
		},
		// Create and Delete events should always trigger reconciliation.
		CreateFunc: func(e event.CreateEvent) bool { return true },
		DeleteFunc: func(e event.DeleteEvent) bool { return true },
	}

	// Add rate limiting to the workqueue, the retry delay is exponential backoff
	// and the rate limit is token bucket, min retry delay is 5 seconds and max retry delay is 60 seconds
	// and the rate limit is 200 QPS and burst size is 500. It will apply to all slice reconciliations.
	return ctrl.NewControllerManagedBy(mgr).
		For(&slicev1beta1.Slice{}, builder.WithPredicates(slicePredicate)).
		Named("slice").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: workerCount,
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter[ctrl.Request](
				workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](r.MinRetryDelay, r.MaxRetryDelay),
				&workqueue.TypedBucketRateLimiter[ctrl.Request]{Limiter: rate.NewLimiter(rate.Limit(r.WorkQueueRateLimit), r.WorkQueueBurst)},
			),
		}).
		Complete(r)
}

// conditionsChanged checks if the core attributes (Type, Status, Reason)
// of any condition have changed, ignoring Message and LastTransitionTime.
func conditionsChanged(oldConds, newConds []metav1.Condition) bool {
	if len(oldConds) != len(newConds) {
		return true // A condition was added or removed
	}

	// Map old conditions by Type for quick O(1) lookup
	oldMap := make(map[string]metav1.Condition, len(oldConds))
	for _, c := range oldConds {
		oldMap[c.Type] = c
	}

	// Compare new conditions against old
	for _, newC := range newConds {
		oldC, exists := oldMap[newC.Type]
		if !exists {
			return true // New condition type was added
		}
		// Only care about meaningful state transitions
		if oldC.Status != newC.Status || oldC.Reason != newC.Reason {
			return true
		}
	}

	return false
}

// annotationsChangedIgnoringTransitionTime checks if any annotation other than LastReadyConditionTransitionTimeAnnotation has changed.
func annotationsChangedIgnoringTransitionTime(oldAnn, newAnn map[string]string) bool {
	oldFiltered := make(map[string]string)
	for k, v := range oldAnn {
		if k != util.LastReadyConditionTransitionTimeAnnotation && k != util.RetryAttemptAnnotationKey {
			oldFiltered[k] = v
		}
	}
	newFiltered := make(map[string]string)
	for k, v := range newAnn {
		if k != util.LastReadyConditionTransitionTimeAnnotation && k != util.RetryAttemptAnnotationKey {
			newFiltered[k] = v
		}
	}
	return !reflect.DeepEqual(oldFiltered, newFiltered)
}

// Determine if the slice is at deleting state
func (r *SliceReconciler) isSliceDeleting(slice *slicev1beta1.Slice) bool {
	return !slice.ObjectMeta.DeletionTimestamp.IsZero()
}

// Determine if the slice is just created and pending initialization
// for finalizer and default state
func (r *SliceReconciler) isSliceInitializing(slice *slicev1beta1.Slice) bool {
	return slice.ObjectMeta.DeletionTimestamp.IsZero() &&
		(!controllerutil.ContainsFinalizer(slice, sliceFinalizerName) || util.FindStateCondition(slice) == "")
}

// Determine if preflight checks should be run for the slice
func (r *SliceReconciler) shouldRunPreflightChecks(slice *slicev1beta1.Slice) bool {
	reason := util.FindStateCondition(slice)
	if len(slice.Spec.PartitionIds) == 0 {
		return false
	}
	isRetrying := reason == util.SliceCreationFailed && slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation
	return reason == util.SliceNotCreated || isRetrying
}

// Determine if the slice needs update based on generation
func (r *SliceReconciler) sliceNeedsUpdate(slice *slicev1beta1.Slice) bool {
	if !r.EnableSliceUpdate {
		return false
	}
	if slice.Status.ObservedGeneration == 0 {
		return false
	}
	return slice.ObjectMeta.Generation > slice.Status.ObservedGeneration
}

// Determine if the slice is done with forming
// The last step of network formation is the labeling
func (r *SliceReconciler) sliceNeedsForming(ctx context.Context, slice *slicev1beta1.Slice) bool {
	reason := util.FindStateCondition(slice)

	// If slice has no partitions, it does not need forming.
	if len(slice.Spec.PartitionIds) == 0 {
		return false
	}

	// If the retry annotation is set to "true", we override the error guard
	// and return true to attempt formation again.
	if reason == util.SliceCreationFailed && slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation {
		return true
	}

	// 1. Error Guard
	// If the slice state is Deactivating, and this check is triggered, it likely means MIG is detached bypassing the Slice controller.
	// We want to avoid triggering formation process and do nothing.
	if reason == util.SliceCreationFailed ||
		reason == string(util.MMIGHealthStatusFailed) ||
		reason == util.SliceHealthProbeFailed ||
		reason == string(util.MMIGHealthStatusDeactivating) ||
		reason == util.DynamicSliceFailed {
		return false
	}

	// 2. Pre-Active
	if reason != string(util.MMIGHealthStatusActive) &&
		reason != string(util.MMIGHealthStatusActiveDegraded) &&
		reason != string(util.MMIGHealthStatusActivating) &&
		reason != util.DynamicSliceActive &&
		reason != util.DynamicSliceActivating {
		return true
	}

	// 3. Active State Check
	labelsToCheck := util.NodeLabels{
		RegularLabels: map[string]string{
			util.TopologyLabelKey: slice.Spec.Topology,
			util.SliceLabelKey:    slice.Name,
		},
	}
	if r.EnableLinkedRunner {
		labelsToCheck.LinkedRunnerLabels = map[string]string{
			fmt.Sprintf("%s-%s", util.SliceLabelKey, slice.Name): "true",
		}
	}
	allLabeled, err := r.KubeHelper.CheckNodeLabels(ctx, slice.Spec.PartitionIds, labelsToCheck, true)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Failed to check node labels")
		return true // Retry on error
	}
	return !allLabeled
}

func (r *SliceReconciler) getPartitionTopologyFromGCE(ctx context.Context, partitionID string) (cache.AcceleratorTopologyInfo, error) {

	migInfo, found := r.RelationalCache.GetMIGByPartitionID(partitionID)
	if !found {
		return cache.AcceleratorTopologyInfo{}, fmt.Errorf("MIG info not found for partition %s", partitionID)
	}

	migName, zone, err := util.ParseMigDetails(migInfo.Name)
	if err != nil {
		return cache.AcceleratorTopologyInfo{}, err
	}

	topologies, err := r.GCEHelper.GetAvailableAcceleratorTopologies(ctx, r.ProjectNumber, zone, migName)
	if err != nil {
		return cache.AcceleratorTopologyInfo{}, err
	}

	if topo, exists := topologies.TopologiesInfo[partitionID]; exists {
		return topo, nil
	}

	return cache.AcceleratorTopologyInfo{}, fmt.Errorf("topology for partition %s not found", partitionID)
}

// shouldTriggerDeformation checks if the slice is in a state where we must
// actively detach a MIG (Deformation) or deactivate hardware before proceeding to full deletion.
func (r *SliceReconciler) shouldTriggerDeformation(state string) bool {
	switch state {
	// Only trigger if we are currently healthy or trying to become healthy.
	case string(util.MMIGHealthStatusActive),
		string(util.MMIGHealthStatusActivating),
		string(util.MMIGHealthStatusActiveDegraded):
		return true

	default:
		// For everything else (Incomplete, Failed, Deactivating, Unknown, etc.)
		// we assume deformation is either already done, impossible, or unsafe.
		return false
	}
}

// shouldTriggerSubsliceDeformation checks if the sub-slice is in a state where we must
// actively deactivate hardware before proceeding to full deletion.
func (r *SliceReconciler) shouldTriggerSubsliceDeformation(state string) bool {
	switch state {
	case util.DynamicSliceActive,
		util.DynamicSliceActivating:
		return true

	default:
		return false
	}
}

// emitSliceMetadata lists nodes for the slice, pivots data by node pool, and emits the metric.
func (r *SliceReconciler) emitSliceMetadata(ctx context.Context, slice *slicev1beta1.Slice, value int64) {
	if r.MetricsExporter == nil {
		return
	}
	log := ctrl.LoggerFrom(ctx)

	for _, pid := range slice.Spec.PartitionIds {
		migInfo, found := r.RelationalCache.GetMIGByPartitionID(pid)
		if !found {
			log.V(1).Info("MIG info not found in cache for partition", "partitionID", pid)
			continue
		}

		var nodePool string
		for _, nodeInfo := range migInfo.Nodes {
			nodePool = nodeInfo.NodePoolName
			break
		}

		if nodePool != "" {
			r.MetricsExporter.RecordSliceMetadata(ctx, slice.Name, slice.Namespace, string(slice.Spec.Type), slice.Spec.Topology, nodePool, string(slice.UID), pid, value)
		}
	}
}

// handleSubsliceDeactivationFailure handles subslice deactivation dispatch failure and manages retries/timeouts.
func (r *SliceReconciler) handleSubsliceDeactivationFailure(ctx context.Context, slice *slicev1beta1.Slice, err error, reason string, partitionID string, topo cache.AcceleratorTopologyInfo) error {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Deactivation failed, checking current topology state to see if it was already deactivated", "error", err)

	// If GCE external ID was empty initially under CreationFailed state, we attempted deactivate once.
	// Regardless of deactivation success/failure, we do not retry or block, and proceed with CRD deletion.
	if reason == util.SliceCreationFailed && topo.SliceID == "" {
		log.Info("Deactivation failed on empty GCE external ID. Regardless of errors, proceeding with sub-slice deletion.", "partition", partitionID, "error", err)
		return nil
	}

	topo2, getErr := r.getPartitionTopologyFromGCE(ctx, partitionID)
	if getErr != nil {
		log.Error(getErr, "Failed to get partition topology from GCE for verification; retrying deactivation")
		return err
	}

	expectedID := util.GetResourceName(slice.Name, r.ClusterHash)
	if topo2.SliceID == expectedID {
		log.Error(err, "Failed to execute deform action for sub-slice; check for retries")

		var opErr *util.OperationError
		var timeoutErr *util.TimeoutError
		var apiErr *util.APICallError
		if errors.As(err, &opErr) && !errors.As(err, &timeoutErr) && !errors.As(err, &apiErr) {
			log.Info("Operation error detected, incrementing retry attempt for next loop")

			patchBaseMeta := client.MergeFrom(slice.DeepCopy())
			util.IncrementRetryAttempt(slice)

			// Patch ONLY the metadata
			if patchErr := r.Client.Patch(ctx, slice, patchBaseMeta); patchErr != nil {
				log.Error(patchErr, "Failed to patch slice annotations with new retry attempt")
				return patchErr
			}
		}
		return err
	}
	log.Info("Sub-slice partition not found or assigned to another slice in GCE, ignoring deactivation error")
	return nil
}

// handleSubsliceUpdate handles the N->0->M update process of a sub-slice.
// This workflow relies on a LIFO operation barrier (checkSubsliceOperationBarrier)
// to ensure any pending activations or deactivations fully complete before
// the controller issues the next sequence. This avoids race conditions in GCE.
func (r *SliceReconciler) handleSubsliceUpdate(ctx context.Context, slice *slicev1beta1.Slice, expectedGeneration int64) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Detected sub-slice update request")

	if len(slice.Spec.PartitionIds) > 1 {
		err := fmt.Errorf("sub-slice update allows at most 1 partition ID")
		log.Error(err, "Invalid update")
		return r.patchObservedGeneration(ctx, slice, expectedGeneration)
	}
	var targetPartitionId string
	if len(slice.Spec.PartitionIds) > 0 {
		targetPartitionId = slice.Spec.PartitionIds[0]
	}

	shouldProceed, err := r.handlePreflightChecks(ctx, slice)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !shouldProceed {
		return r.patchObservedGeneration(ctx, slice, expectedGeneration)
	}

	provisionedPartition := r.getEffectivePartition(slice)

	if r.checkSubsliceOperationBarrier(ctx, slice, util.SubsliceLastOpAnnotation) {
		log.Info("Waiting for pending operation to finish before updating (relying on HealthMonitor events)")
		return ctrl.Result{}, nil
	}

	if provisionedPartition != "" && provisionedPartition != targetPartitionId {
		handled, err := r.processSubsliceUpdateDeformPhase(ctx, slice, provisionedPartition)
		if handled {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
	}

	// Phase 2: Form New Partition (0->M)
	if targetPartitionId != "" {
		handled, err := r.processSubsliceUpdateFormPhase(ctx, slice, targetPartitionId, expectedGeneration)
		if handled {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
	}

	// Phase 3: Finalize
	// Add the new target partition to relational cache now that formation is active/initiated
	if err := r.RelationalCache.AddSlice(slice.Name, slice.Spec.PartitionIds, slice.Spec.Topology); err != nil {
		log.Error(err, "Failed to add new partition to relational cache during update")
		return ctrl.Result{}, err
	}

	patch := client.MergeFrom(slice.DeepCopy())
	if slice.Annotations == nil {
		slice.Annotations = make(map[string]string)
	}
	if targetPartitionId != "" {
		slice.Annotations[util.SubsliceProvisionedPartitionAnnotation] = targetPartitionId
	} else {
		delete(slice.Annotations, util.SubsliceProvisionedPartitionAnnotation)
		// For pure N->0 update, update the condition to SliceNotCreated to match super-slice
		if _, patchErr := util.PatchSliceCondition(ctx, r.Client, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, util.SliceNotCreated, "Slice update completed successfully", r.MetricsExporter); patchErr != nil {
			return ctrl.Result{}, patchErr
		}
		if _, patchErr := util.PatchSliceCondition(ctx, r.Client, slice, util.NodeLabeled, metav1.ConditionFalse, util.NodeLabelIsRemoved, util.NodeLabelingCompleted, r.MetricsExporter); patchErr != nil {
			return ctrl.Result{}, patchErr
		}
	}
	if err := r.Client.Patch(ctx, slice, patch); err != nil {
		return ctrl.Result{}, err
	}

	return r.patchObservedGeneration(ctx, slice, expectedGeneration)
}

func (r *SliceReconciler) patchObservedGeneration(ctx context.Context, slice *slicev1beta1.Slice, expectedGeneration int64) (ctrl.Result, error) {
	patch := client.MergeFrom(slice.DeepCopy())
	slice.Status.ObservedGeneration = expectedGeneration
	if err := r.Status().Patch(ctx, slice, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// handleSubsliceActionError centralizes the retry and condition patching logic for subslice actions.
// If an operation fails, it checks the retry annotation, patches the condition to SliceCreationFailed,
// and correctly increments the retry backoff.
func (r *SliceReconciler) handleSubsliceActionError(ctx context.Context, slice *slicev1beta1.Slice, actionErr error) error {
	log := ctrl.LoggerFrom(ctx)
	if _, patcherr := util.PatchSliceCondition(ctx, r.Client, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.SliceCreationFailed, actionErr.Error(), r.MetricsExporter); patcherr != nil {
		log.Error(patcherr, "Failed to patch slice condition after action error")
		return patcherr
	}

	if slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation {
		log.Info("Action failed; will be retried", "error", actionErr)
		var opErr *util.OperationError
		var timeoutErr *util.TimeoutError
		var apiErr *util.APICallError
		if errors.As(actionErr, &opErr) && !errors.As(actionErr, &timeoutErr) && !errors.As(actionErr, &apiErr) {
			log.Info("Operation error detected, incrementing retry attempt for next loop")
			patchBaseMeta := client.MergeFrom(slice.DeepCopy())
			util.IncrementRetryAttempt(slice)
			if patchErr := r.Client.Patch(ctx, slice, patchBaseMeta); patchErr != nil {
				log.Error(patchErr, "Failed to patch slice annotations with new retry attempt")
				return patchErr
			}
		}
		return actionErr
	}

	log.Info("Action failed; will not be retried", "error", actionErr)
	return nil
}

// getEffectivePartition resolves the most accurate physical partition ID for a sub-slice.
// During crash recoveries or rapid spec updates, the Spec might point to a new partition (M),
// while the physically allocated hardware is still the old partition (N).
// This helper falls back securely: Annotation (Truth) -> Cache (Backup).
func (r *SliceReconciler) getEffectivePartition(slice *slicev1beta1.Slice) string {
	if p := slice.Annotations[util.SubsliceProvisionedPartitionAnnotation]; p != "" {
		return p
	}
	if sliceInfo, found := r.RelationalCache.GetSliceInfo(slice.Name); found && len(sliceInfo.PartitionIDs) > 0 {
		return sliceInfo.PartitionIDs[0]
	}
	if len(slice.Spec.PartitionIds) > 0 {
		return slice.Spec.PartitionIds[0]
	}
	return ""
}

// checkSubsliceOperationBarrier acts as a global LIFO mutex for a sub-slice.
// It queries the GCE Operations API for the `opAnnotation` stored on the Slice CR.
// If the operation is IN-PROGRESS, it returns true, signaling the reconciler to wait
// before issuing any conflicting state transitions (like activating while deactivating).
func (r *SliceReconciler) checkSubsliceOperationBarrier(ctx context.Context, slice *slicev1beta1.Slice, opAnnotation string) bool {
	if slice.Annotations == nil {
		return false
	}
	opName, ok := slice.Annotations[opAnnotation]
	if !ok || opName == "" {
		return false
	}

	var migZone string
	partitionId := r.getEffectivePartition(slice)
	if partitionId != "" {
		if migInfo, found := r.RelationalCache.GetMIGByPartitionID(partitionId); found {
			_, migZone, _ = util.ParseMigDetails(migInfo.Name)
		}
	}

	if migZone == "" {
		return false
	}

	log := ctrl.LoggerFrom(ctx)
	op, err := r.GCEHelper.GetZonalOperation(ctx, r.ProjectNumber, migZone, opName)
	if err != nil {
		var gceErr *googleapi.Error
		if errors.As(err, &gceErr) && gceErr.Code == util.ResourceNotFound {
			log.Info("Operation not found in GCE, assuming it completed and was garbage collected. Unblocking barrier.", "operation", opName)
			return false
		}
		log.Error(err, "Failed to query GCE operation status, blocking to prevent conflicting operations", "operation", opName)
		return true
	}

	if op.Status != "DONE" {
		return true
	}

	return false
}

// processSubsliceUpdateDeformPhase handles Phase 1 of the update workflow (N->0).
// Returns true if the workflow should exit (handled), along with the error.
func (r *SliceReconciler) processSubsliceUpdateDeformPhase(ctx context.Context, slice *slicev1beta1.Slice, provisionedPartition string) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	if _, found := r.RelationalCache.GetMIGByPartitionID(provisionedPartition); !found {
		log.Info("Old partition MIG not found in cache, assuming it is inactive and safe to clean up", "partition", provisionedPartition)
		patch := client.MergeFrom(slice.DeepCopy())
		if slice.Annotations != nil {
			delete(slice.Annotations, util.SubsliceProvisionedPartitionAnnotation)
		}
		if patchErr := r.Client.Patch(ctx, slice, patch); patchErr != nil {
			return true, patchErr
		}
		if deleteErr := r.RelationalCache.DeleteSlice(slice.Name); deleteErr != nil {
			log.Error(deleteErr, "Failed to delete slice from relational cache")
		}
		return false, nil
	}

	topo, err := r.getPartitionTopologyFromGCE(ctx, provisionedPartition)
	if err != nil {
		log.Error(err, "Transient error checking old partition topology, requeueing deform phase")
		return true, err
	}

	expectedID := util.GetResourceName(slice.Name, r.ClusterHash)
	if topo.SliceID == expectedID && topo.AcceleratorTopologyState != util.DynamicSliceInactive {
		log.Info("Deactivating old partition before updating", "oldPartition", provisionedPartition)
		updateDeformAction := action.UpdateDeformSubsliceAction(r.GCEHelper, r.Client, r.RelationalCache, r.ProjectNumber, r.ClusterHash, provisionedPartition)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, updateDeformAction); err != nil {
			return true, r.handleSubsliceActionError(ctx, slice, err)
		}

		return true, nil
	} else if topo.AcceleratorTopologyState == util.DynamicSliceInactive || topo.SliceID != expectedID {
		log.Info("Old partition is inactive or unowned, clearing provisioned-partition to allow new formation")
		patch := client.MergeFrom(slice.DeepCopy())
		if slice.Annotations != nil {
			delete(slice.Annotations, util.SubsliceProvisionedPartitionAnnotation)
		}
		if err := r.Client.Patch(ctx, slice, patch); err != nil {
			return true, err
		}
		if err := r.RelationalCache.DeleteSlice(slice.Name); err != nil {
			log.Error(err, "Failed to delete slice from relational cache")
		}
	}
	return false, nil
}

// processSubsliceUpdateFormPhase handles Phase 2 of the update workflow (0->M).
// Returns true if the workflow should exit (handled), along with the error.
func (r *SliceReconciler) processSubsliceUpdateFormPhase(ctx context.Context, slice *slicev1beta1.Slice, targetPartitionId string, expectedGeneration int64) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	topo, err := r.getPartitionTopologyFromGCE(ctx, targetPartitionId)
	if err != nil {
		return true, err
	}

	expectedID := util.GetResourceName(slice.Name, r.ClusterHash)
	if topo.SliceID != expectedID || topo.AcceleratorTopologyState == util.DynamicSliceInactive {
		log.Info("Activating new partition for update", "targetPartition", targetPartitionId)
		updateFormAction := action.UpdateFormSubsliceAction(r.GCEHelper, r.Client, r.RelationalCache, r.ProjectNumber, r.ClusterHash, targetPartitionId)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, updateFormAction); err != nil {
			actionErr := r.handleSubsliceActionError(ctx, slice, err)
			// Handoff to normal formation loop: advance ObservedGeneration so the next reconciler loop
			// drops out of the update workflow and into handleSliceFormation for native retries.
			if _, patchErr := r.patchObservedGeneration(ctx, slice, expectedGeneration); patchErr != nil {
				log.Error(patchErr, "Failed to patch observed generation after formation failure")
				return true, patchErr
			}
			return true, actionErr
		}

		// 2. Stamp provisioned-partition immediately after successful ACTIVATE dispatch.
		// This is critical: if the scheduler changes partitionIds before the next reconcile
		// (e.g., [A]→[]→[B] coalesced into a single generation skip), the controller must
		// be able to find partition A via getEffectivePartition() to DEACTIVATE it.
		// Without this, the deform phase skips cleanup and the GCE partition leaks.
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Fetch the latest Slice object to ensure we patch against the latest ResourceVersion
			var latestSlice slicev1beta1.Slice
			if getErr := r.Client.Get(ctx, types.NamespacedName{Name: slice.Name, Namespace: slice.Namespace}, &latestSlice); getErr != nil {
				return getErr
			}

			if latestSlice.Annotations == nil {
				latestSlice.Annotations = make(map[string]string)
			}
			latestSlice.Annotations[util.SubsliceProvisionedPartitionAnnotation] = targetPartitionId

			return r.Client.Update(ctx, &latestSlice)
		})

		if err != nil {
			log.Error(err, "Failed to stamp provisioned-partition after ACTIVATE dispatch after retries", "partition", targetPartitionId)
			// Return the error to force a requeue. The ACTIVATE dispatch is idempotent, so
			// it's safe to retry the formation loop if we failed to persist the annotation.
			return true, err
		} else {
			log.Info("Stamped provisioned-partition after ACTIVATE dispatch", "partition", targetPartitionId)
		}

		// Also add to relational cache so getEffectivePartition() can find it via cache lookup.
		if err := r.RelationalCache.AddSlice(slice.Name, []string{targetPartitionId}, slice.Spec.Topology); err != nil {
			log.Error(err, "Failed to add partition to relational cache after ACTIVATE dispatch")
		}

		// 3. Fetch fresh state from GCE to update Slice CR Status
		freshTopo, freshErr := r.getPartitionTopologyFromGCE(ctx, targetPartitionId)
		if freshErr == nil {
			realTimeState := string(freshTopo.AcceleratorTopologyState)
			status := util.ConvertReasonToStatus(realTimeState)
			if _, patchErr := util.PatchSliceCondition(ctx, r.Client, slice, slicev1beta1.SliceStateConditionType, status, realTimeState, util.SliceFormationInProgress, r.MetricsExporter); patchErr != nil {
				log.Error(patchErr, "Failed to patch slice condition after sub-slice update formation")
				return true, patchErr
			}
		} else {
			log.Error(freshErr, "Failed to fetch fresh topology state after sending ACTIVATE")
		}

		// 4. Advance observedGeneration so the next reconcile doesn't re-enter a stale
		// update loop dispatching the same ACTIVATE indefinitely.
		if _, patchErr := r.patchObservedGeneration(ctx, slice, expectedGeneration); patchErr != nil {
			log.Error(patchErr, "Failed to patch observed generation after successful formation")
			return true, patchErr
		}
		return true, nil
	}
	return false, nil
}

func (r *SliceReconciler) unlabelSubsliceNodes(ctx context.Context, slice *slicev1beta1.Slice, partitionID string) error {
	log := ctrl.LoggerFrom(ctx)
	labelsToCheck := util.NodeLabels{
		RegularLabels: map[string]string{
			util.TopologyLabelKey: slice.Spec.Topology,
			util.SliceLabelKey:    slice.Name,
		},
	}
	if r.EnableLinkedRunner {
		labelsToCheck.LinkedRunnerLabels = map[string]string{
			fmt.Sprintf("%s-%s", util.SliceLabelKey, slice.Name): "true",
		}
	}
	// Fallback to partitionID for checking labels if Spec is empty
	partitionsToCheck := slice.Spec.PartitionIds
	if len(partitionsToCheck) == 0 && partitionID != "" {
		partitionsToCheck = []string{partitionID}
	}
	allUnlabeled, err := r.KubeHelper.CheckNodeLabels(ctx, partitionsToCheck, labelsToCheck, false)
	if err != nil {
		log.Error(err, "Failed to check node labels for sub-slice")
		return err
	}

	if !allUnlabeled {
		labelKeys := util.NodeLabelKeys{
			RegularLabels: []string{util.TopologyLabelKey, util.SliceLabelKey},
		}
		if r.EnableLinkedRunner {
			labelKeys.LinkedRunnerLabels = []string{
				fmt.Sprintf("%s-%s", util.SliceLabelKey, slice.Name),
			}
		}
		unlabelAction := action.NewUnlabelNodesAction(r.KubeHelper, labelKeys, r.EnableHighNodeLabelingConcurrency)
		if err := r.SliceEngine.Execute(ctx, r.Client, slice, unlabelAction); err != nil {
			log.Error(err, "Failed to unlabel nodes for sub-slice")
			return err
		}
		// Update NodeLabeled condition
		if _, err := util.PatchSliceCondition(ctx, r.Client, slice, util.NodeLabeled, metav1.ConditionFalse, util.NodeLabelIsRemoved, util.NodeLabelingCompleted, r.MetricsExporter); err != nil {
			log.Error(err, "Failed to update NodeLabeled condition during sub-slice deletion")
			return err
		}
	}
	return nil
}
