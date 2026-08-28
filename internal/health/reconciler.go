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

package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"slice-controller/internal/action/kubehelper"
	"slice-controller/internal/cache"
	"slice-controller/internal/metrics"
	"slice-controller/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	slicev1beta1 "slice-controller/api/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
)

type GCEResourceType string

const (
	// Health status values
	healthStatusIncomplete = "INCOMPLETE"
	healthStatusHealthy    = "HEALTHY"
	healthStatusUnhealthy  = "UNHEALTHY"
	healthStatusDegraded   = "DEGRADED"
	healthStatusUnknown    = "UNKNOWN"

	// retry settings
	maxRetries = 3

	// Workqueue rate limiting
	workqueueBaseDelay = 1 * time.Second
	workqueueMaxDelay  = 60 * time.Second

	MIG  GCEResourceType = "MIG"
	MMIG GCEResourceType = "MMIG"

	// heartbeatInterval defines the interval at which health heartbeats are sent
	heartbeatInterval = 1 * time.Minute

	// metricsReportInterval defines the interval at which slice and partition metrics are reported
	metricsReportInterval = 15 * time.Second

	linkedRunnerHealthAnnotation = "cloud.google.com/gke-tpu-partition-health-status"
)

// WorkItem represents an item to be processed by the reconciler
type WorkItem struct {
	ResourceType GCEResourceType
	ResourceURL  string
}

// Key returns a unique string key for deduplication in workqueue
func (w WorkItem) Key() string {
	return fmt.Sprintf("%s:%s", w.ResourceType, w.ResourceURL)
}

// HealthReconciler reconciles node labels and Slice CR conditions based on health status changes.
type HealthReconciler struct {
	client          client.Client
	KubeClient      kubehelper.Client
	relationalCache cache.ReconcilerRelationalCacheInterface
	healthCache     cache.ReconcilerHealthCacheInterface

	// healthReconcilerWorkerCount is the number of workers for the health reconciler
	healthReconcilerWorkerCount int
	metricsExporter             *metrics.MetricsExporter

	// Workqueue for processing health changes
	workqueue workqueue.TypedRateLimitingInterface[WorkItem]

	clusterHash        string
	enableLinkedRunner bool

	enableSubSlicing bool
}

// NewHealthReconciler creates a new HealthReconciler
func NewHealthReconciler(
	mgr manager.Manager,
	relationalCache *cache.RelationalCache,
	healthCache *cache.HealthCache,
	healthReconcilerWorkerCount int,
	clusterHash string,
	metricsExporter *metrics.MetricsExporter,
	enableLinkedRunner bool,
	enableSubSlicing bool,
) (*HealthReconciler, error) {
	if mgr == nil {
		return nil, fmt.Errorf("manager cannot be nil")
	}
	if relationalCache == nil {
		return nil, fmt.Errorf("relational cache cannot be nil")
	}
	if healthCache == nil {
		return nil, fmt.Errorf("health cache cannot be nil")
	}

	// Create rate-limiting workqueue
	rateLimiter := workqueue.NewTypedItemExponentialFailureRateLimiter[WorkItem](
		workqueueBaseDelay,
		workqueueMaxDelay,
	)

	return &HealthReconciler{
		client:                      mgr.GetClient(),
		relationalCache:             relationalCache,
		healthCache:                 healthCache,
		KubeClient:                  kubehelper.NewClient(mgr.GetClient(), relationalCache, enableLinkedRunner),
		healthReconcilerWorkerCount: healthReconcilerWorkerCount,
		workqueue:                   workqueue.NewTypedRateLimitingQueue(rateLimiter),
		clusterHash:                 clusterHash,
		metricsExporter:             metricsExporter,
		enableLinkedRunner:          enableLinkedRunner,
		enableSubSlicing:            enableSubSlicing,
	}, nil
}

// OnMIGHealthStatusChange implements HealthStatusChangeListener interface.
// Called by HealthCache when MIG health status changes.
func (r *HealthReconciler) OnMIGHealthStatusChange(resourceURL string) {
	r.workqueue.Add(WorkItem{
		ResourceType: MIG,
		ResourceURL:  resourceURL,
	})
}

// OnMIGPartitionStateChange implements HealthStatusChangeListener interface.
// Called by HealthCache when MIG topologies change.
func (r *HealthReconciler) OnMIGPartitionStateChange(resourceURL string) {
	r.workqueue.Add(WorkItem{
		ResourceType: MIG,
		ResourceURL:  resourceURL,
	})
}

// OnMMIGHealthChange implements HealthChangeListener interface.
// Called by HealthCache when MMIG health status changes.
func (r *HealthReconciler) OnMMIGHealthStatusChange(mmigURL string) {
	r.workqueue.Add(WorkItem{
		ResourceType: MMIG,
		ResourceURL:  mmigURL,
	})
}

// Start implements the controller-runtime Runnable interface.
// This is the main event loop that:
// 1. Processes work items from the queue (event-driven, low latency)
// 2. Handles graceful shutdown on context cancellation
func (r *HealthReconciler) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("health-reconciler")
	logger.Info("Starting Health Reconciler", "workers", r.healthReconcilerWorkerCount)

	var wg sync.WaitGroup
	defer r.workqueue.ShutDown()

	for i := 0; i < r.healthReconcilerWorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wait.UntilWithContext(ctx, r.worker, time.Second)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.runHeartbeat(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.runMetricsReport(ctx)
	}()

	<-ctx.Done()
	logger.Info("Health Reconciler stopping... shutting down queue")

	logger.Info("Waiting for workers to exit...")
	wg.Wait()
	logger.Info("All workers exited. Health Reconciler stopped.")

	return nil
}

// runHeartbeat periodically enqueues all MIGs to ensure all nodes' health status is up-to-date
func (r *HealthReconciler) runHeartbeat(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("health-heartbeat")
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	logger.Info("Heartbeat loop started", "interval", heartbeatInterval)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Heartbeat loop stopping")
			return
		case <-ticker.C:
			logger.V(1).Info("Heartbeat triggered: Enqueuing all MIGs")
			r.enqueueAllMIGs()
		}
	}
}

// enqueueAllMIGs enqueues all MIGs to ensure their health status is up-to-date
func (r *HealthReconciler) enqueueAllMIGs() {
	migKeys := r.relationalCache.GetAllMIGNames()
	for _, migURL := range migKeys {
		r.workqueue.Add(WorkItem{
			ResourceType: MIG,
			ResourceURL:  migURL,
		})
	}
}

// worker processes items from the workqueue continuously
func (r *HealthReconciler) worker(ctx context.Context) {
	for {
		workItem, shutdown := r.workqueue.Get()
		logger := log.FromContext(ctx).WithName("health-reconciler-worker").WithValues("resourceType", workItem.ResourceType, "resourceURL", workItem.ResourceURL)
		if shutdown {
			logger.Info("Workqueue shutdown, stopping worker")
			return
		}
		err := r.processWorkItem(ctx, workItem)

		if err == nil {
			r.workqueue.Forget(workItem)
		} else if IsTerminal(err) {
			// The reconciler hit a permanent error
			// Forget the item and do not retry.
			logger.Error(err, "Permanent error processing work item, dropping")
			r.workqueue.Forget(workItem)
		} else {
			numRequeues := r.workqueue.NumRequeues(workItem)
			if numRequeues < maxRequeues {
				// Requeue with rate limiting
				r.workqueue.AddRateLimited(workItem)
				logger.Error(err, "Error processing work item, requeuing",
					"requeues", numRequeues)
			} else {
				logger.Error(err, "Max retries exceeded, dropping work item",
					"requeues", numRequeues)
				r.workqueue.Forget(workItem)
			}
		}

		// Mark item as done
		r.workqueue.Done(workItem)
	}
}

// processWorkItem processes a single work item from the queue
func (r *HealthReconciler) processWorkItem(ctx context.Context, workItem WorkItem) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Processing work item", "key", workItem.Key(), "time", metav1.Now())

	switch workItem.ResourceType {
	case MIG:
		return r.reconcileMIG(ctx, workItem.ResourceURL)
	case MMIG:
		return r.reconcileMMIG(ctx, workItem.ResourceURL)
	default:
		return NewTerminalError(fmt.Errorf("unknown resource type: %s", workItem.ResourceType))
	}
}

// updatePartitionSliceCR updates the Slice CR status for a specific partition.
func (r *HealthReconciler) updatePartitionSliceCR(ctx context.Context, slice *slicev1beta1.Slice, partitionInfo cache.AcceleratorTopologyInfo) error {
	logger := log.FromContext(ctx).WithValues("sliceName", slice.Name)
	var (
		status  metav1.ConditionStatus
		reason  string
		message string
	)

	originalState := util.FindStateCondition(slice)

	switch partitionInfo.AcceleratorTopologyState {
	case cache.MIGPartitionStateActive:
		status = metav1.ConditionTrue
		reason = string(partitionInfo.AcceleratorTopologyState)
		message = ""
	case cache.MIGPartitionStateActivating:
		status = metav1.ConditionFalse
		reason = string(partitionInfo.AcceleratorTopologyState)
		message = "Partition is activating"
	case cache.MIGPartitionStateDeactivating:
		status = metav1.ConditionFalse
		reason = string(partitionInfo.AcceleratorTopologyState)
		message = "Partition is deactivating"
	case cache.MIGPartitionStateInactive:
		status = metav1.ConditionFalse
		reason = string(partitionInfo.AcceleratorTopologyState)
		message = "Partition is inactive"
	case cache.MIGPartitionStateFailed:
		status = metav1.ConditionFalse
		reason = string(partitionInfo.AcceleratorTopologyState)
		message = "Partition is in failed state"
	default:
		status = metav1.ConditionUnknown
		reason = string(partitionInfo.AcceleratorTopologyState)
		message = "Unexpected partition state"
	}

	changed, err := util.PatchSliceCondition(
		ctx,
		r.client,
		slice,
		slicev1beta1.SliceStateConditionType,
		status,
		reason,
		message,
		r.metricsExporter,
	)
	if err != nil {
		return fmt.Errorf("failed to update Slice status: %w", err)
	}

	if changed {
		logger.Info("Successfully updated Slice CR status for partition", "originalState", originalState, "reason", reason)
	}

	// Record formation latency for sub-slices
	r.recordSliceFormationLatencyIfNeeded(ctx, slice, originalState, reason)
	r.recordPartitionFailuresIfNeeded(ctx, slice, originalState, reason)

	return nil
}

// updatePartitionSliceWithRetry updates the Slice CR for a specific partition with retries.
func (r *HealthReconciler) updatePartitionSliceWithRetry(ctx context.Context, partitionID string, partitionInfo cache.AcceleratorTopologyInfo) error {
	sliceName, exists := r.relationalCache.GetSliceNameByPartitionID(partitionID)
	logger := log.FromContext(ctx).WithValues("sliceName", sliceName)
	if !exists {
		return nil // Not managed by a Slice CR yet
	}

	objectKey := client.ObjectKey{Name: sliceName}
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		slice := &slicev1beta1.Slice{}
		if err := r.client.Get(ctx, objectKey, slice); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to fetch Slice resource: %w", err)
		}

		originalState := util.FindStateCondition(slice)
		if originalState == util.DynamicSliceFailed && r.enableSubSlicing {
			logger.Info("Sub-slice is already in a failed state, skipping health status update", "state", originalState)
			return nil
		}

		// Guard against stale/transient INACTIVE updates during sub-slice activation.
		// This is to avoid a situation where an earlier health monitoring reconcile happened after
		// getting the latest state of the slice to avoid overriding.
		if originalState == util.DynamicSliceActivating && partitionInfo.AcceleratorTopologyState == cache.MIGPartitionStateInactive {
			if cond, found := util.FindSliceCondition(slice, slicev1beta1.SliceStateConditionType); found {
				if time.Since(cond.LastTransitionTime.Time) < 2*time.Minute {
					logger.Info("Skipping health status update to INACTIVE as sub-slice recently transitioned to ACTIVATING", "transitionTime", cond.LastTransitionTime.Time)
					return nil
				}
			}
		}

		if partitionInfo.AcceleratorTopologyState == cache.MIGPartitionStateActive && !slice.ObjectMeta.DeletionTimestamp.IsZero() {
			logger.V(4).Info("Skipping status update for deleting slice")
			return nil
		}

		err := r.updatePartitionSliceCR(ctx, slice, partitionInfo)
		if err == nil {
			return nil
		}

		if apierrors.IsConflict(err) {
			lastErr = err
			time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
			continue
		}
		return err
	}
	return lastErr
}

func (r *HealthReconciler) reconcileMIG(ctx context.Context, migURL string) error {
	logger := log.FromContext(ctx).WithName("reconcile-mig").WithValues("migURL", migURL)

	// 1. Fetch from Relational Cache
	migInfo, exists := r.relationalCache.GetMIG(migURL)
	if !exists {
		logger.V(1).Info("No MIG info found in relational cache")
		return NewTerminalError(errors.New("no MIG info found in relational cache"))
	}

	// healthStatus is currently just a single value for the MIG and does not include individual partitions
	if r.enableLinkedRunner {
		isLinkedRunner, node, err := util.IsLinkedRunnerMIG(ctx, migInfo, r.client)
		if err != nil {
			return fmt.Errorf("failed to check if MIG is for Linked VM: %w", err)
		}
		if isLinkedRunner {
			// For LinkedRunners we apply a single JSON annotation to the control node containing
			// the health statuses for all its managed partitions
			if err := r.reconcileLinkedRunnerMIG(ctx, migURL, node); err != nil {
				return err
			}

			// Fetch topologies specifically for the linked runner sub-slices
			topologiesHealth, topologiesExist := r.healthCache.GetMIGPartitionsStatus(migURL)

			// Update Slice CR status for each linked runner partition
			if r.enableSubSlicing && topologiesExist {
				for uid, info := range topologiesHealth {
					if err := r.updatePartitionSliceWithRetry(ctx, uid, info); err != nil {
						logger.Error(err, "Failed to update Slice CR for partition", "partitionID", uid)
						// Continue to other partitions
					}
				}
			}

			logger.V(1).Info("Linked Runner MIG reconciliation completed")
			return nil
		}
	}

	nodeNameList := make([]string, 0, len(migInfo.Nodes))
	for k := range migInfo.Nodes {
		nodeNameList = append(nodeNameList, k)
	}

	if len(nodeNameList) == 0 {
		logger.V(1).Info("No nodes in MIG to reconcile")
		return nil
	}

	healthStatus, healthExists := r.healthCache.GetMIGHealth(migURL)
	topologiesHealth, topologiesExist := r.healthCache.GetMIGPartitionsStatus(migURL)

	// 1. Process the partition health for sub-partitions
	if r.enableSubSlicing {
		if err := r.reconcileSubPartitions(ctx, &migInfo, topologiesHealth, topologiesExist); err != nil {
			logger.Error(err, "Failed to reconcile sub-partitions, proceeding to MIG level")
		}
	}

	// 2. Process MIG-level status (Primary Partition)
	if err := r.reconcileMIGHealth(ctx, migURL, &migInfo, nodeNameList, healthStatus, healthExists); err != nil {
		return err
	}

	// 3. Update Slice CR status for each partition
	if r.enableSubSlicing && topologiesExist {
		for uid, info := range topologiesHealth {
			if err := r.updatePartitionSliceWithRetry(ctx, uid, info); err != nil {
				logger.Error(err, "Failed to update Slice CR for partition", "partitionID", uid)
				// Continue to other partitions
			}
		}
	}

	logger.V(1).Info("MIG reconciliation completed")
	return nil
}

// PartitionHealthInfo holds the health information for a single partition.
type PartitionHealthInfo struct {
	AcceleratorTopology       string `json:"acceleratorTopology"`
	AcceleratorTopologyHealth string `json:"acceleratorTopologyHealth"`
}

// reconcileLinkedRunnerMIG reconciles a Linked Runner MIG by annotating its control node.
func (r *HealthReconciler) reconcileLinkedRunnerMIG(ctx context.Context, migURL string, node *corev1.Node) error {
	logger := log.FromContext(ctx).WithValues("reconcile linkedvm mig", migURL, "node", node.Name)

	patch := client.MergeFrom(node.DeepCopy())

	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}

	ipToTopologyPartitionId, err := util.ExtractRunnerPartitionsFromControlNode(node)
	if err != nil {
		return fmt.Errorf("failed to extract partition IDs from Linked VM control node %s: %w", node.Name, err)
	}

	healthStatus, healthExists := r.healthCache.GetMIGHealth(migURL)
	topologiesHealth, topologiesExist := r.healthCache.GetMIGPartitionsStatus(migURL)

	healthStatusMap := make(map[string]PartitionHealthInfo)
	for _, topologyPartitionId := range ipToTopologyPartitionId {
		for topology, partitionID := range topologyPartitionId {
			if topologyInfo, ok := util.SupportedTopologiesInfo[topology]; ok {
				var health string

				// If sub-slicing is enabled, try to get the specific health for this partition.
				if r.enableSubSlicing && topologiesExist {
					if status, ok := r.getPartitionHealthStatus(partitionID, topologiesHealth); ok {
						health = status
					}
				}

				if health == "" {
					if !healthExists {
						health = healthStatusIncomplete
					} else {
						switch healthStatus.CurrentStatus {
						case cache.MIGHealthStatusHealthy:
							health = healthStatusHealthy
						case cache.MIGHealthStatusUnhealthy:
							health = healthStatusUnhealthy
						default:
							health = string(healthStatus.CurrentStatus)
						}
					}
				}
				healthStatusMap[partitionID] = PartitionHealthInfo{
					AcceleratorTopology:       topologyInfo.LabelKey,
					AcceleratorTopologyHealth: health,
				}
			}
		}
	}

	if len(healthStatusMap) > 0 {
		healthStatusJSON, err := json.Marshal(healthStatusMap)
		if err != nil {
			return fmt.Errorf("failed to marshal health status map for node %s: %w", node.Name, err)
		}
		node.Annotations[linkedRunnerHealthAnnotation] = string(healthStatusJSON)
	} else {
		delete(node.Annotations, linkedRunnerHealthAnnotation)
	}

	if err := r.client.Patch(ctx, node, patch); err != nil {
		return fmt.Errorf("failed to patch node %s with health annotation: %w", node.Name, err)
	}

	logger.V(1).Info("Linked Runner MIG reconciliation completed")
	return nil
}

func (r *HealthReconciler) reconcileSubPartitions(ctx context.Context, migInfo *cache.MIGInfo, topologiesHealth map[string]cache.AcceleratorTopologyInfo, topologiesExist bool) error {
	logger := log.FromContext(ctx)
	if topologiesExist {
		for uid, info := range topologiesHealth {
			// Skip 4x4x4 and 16x16 if they accidentally got into the cache; they're handled separately.
			if util.IsFullTopology(info.AcceleratorTopology) {
				continue
			}

			stateLabel, err := r.getPartitionStateLabelFromUID(uid, migInfo)
			if err != nil {
				logger.V(1).Info("Could not find label for partition UID", "uid", uid)
				continue
			}

			// Find all nodes that belong to this partition
			nodesInPartition := make([]string, 0)
			for nodeName, nodeInfo := range migInfo.Nodes {
				for _, nodeUIDs := range nodeInfo.TopologyPartitionIds {
					found := false
					for _, nodeUID := range nodeUIDs {
						if nodeUID == uid {
							nodesInPartition = append(nodesInPartition, nodeName)
							found = true
							break
						}
					}
					if found {
						break
					}
				}
			}

			if len(nodesInPartition) > 0 {
				sliceName, exists := r.relationalCache.GetSliceNameByPartitionID(uid)
				if exists {
					sliceInfo, found := r.relationalCache.GetSliceInfo(sliceName)
					if found && util.IsSubsliceTopology(sliceInfo.Topology) && !r.enableSubSlicing {
						logger.V(1).Info("Skipping node labeling for sub-slice partition, monitoring is disabled", "partition", uid, "slice", sliceName)
						continue
					}
				}
				status, _ := r.getPartitionHealthStatus(uid, topologiesHealth)
				labels := map[string]string{stateLabel: status}
				if err := r.KubeClient.LabelNodeGroupByNodeNames(ctx, labels, nodesInPartition); err != nil {
					return fmt.Errorf("failed to label nodes for partition %s: %w", uid, err)
				}
			}
		}
	} else {
		// If no topologies info exists, mark all sub-partition labels as INCOMPLETE for all nodes.
		// For efficiency, we group nodes by their sub-partition labels.
		subPartitionLabelsToNodes := make(map[string][]string)
		for nodeName, nodeInfo := range migInfo.Nodes {
			labels := make([]string, 0)
			for topoLabel := range nodeInfo.TopologyPartitionIds {
				if util.IsFullTopology(topoLabel) {
					continue
				}
				if topoInfo, ok := util.SupportedTopologiesInfo[topoLabel]; ok {
					labels = append(labels, r.getPartitionStateLabel(topoInfo.LabelKey))
				} else {
					labels = append(labels, r.getPartitionStateLabel(topoLabel))
				}
			}
			if len(labels) > 0 {
				sort.Strings(labels)
				key := strings.Join(labels, ",")
				subPartitionLabelsToNodes[key] = append(subPartitionLabelsToNodes[key], nodeName)
			}
		}

		for labelKey, nodes := range subPartitionLabelsToNodes {
			labels := strings.Split(labelKey, ",")
			labelsToAdd := make(map[string]string)
			for _, l := range labels {
				labelsToAdd[l] = healthStatusIncomplete
			}
			if err := r.KubeClient.LabelNodeGroupByNodeNames(ctx, labelsToAdd, nodes); err != nil {
				return fmt.Errorf("failed to mark labels incomplete for nodes %v: %w", nodes, err)
			}
		}
	}
	return nil
}

func (r *HealthReconciler) reconcileMIGHealth(ctx context.Context, migURL string, migInfo *cache.MIGInfo, nodeNameList []string, healthStatus cache.TimestampedHealth[cache.MIGHealthStatus], healthExists bool) error {
	migStatus := healthStatusIncomplete
	if healthExists {
		migStatus = r.mapMIGHealthStatusToString(healthStatus.CurrentStatus)
	}

	// Find the appropriate topology label for this MIG
	var topoLabelKey string
	for _, nodeInfo := range migInfo.Nodes {
		for topo := range nodeInfo.TopologyPartitionIds {
			if util.IsFullTopology(topo) {
				if topoInfo, ok := util.SupportedTopologiesInfo[topo]; ok {
					topoLabelKey = topoInfo.LabelKey
					break
				}
			}
		}
		if topoLabelKey != "" {
			break
		}
	}

	if topoLabelKey == "" {
		return fmt.Errorf("no supported topology found for MIG %s", migURL)
	}

	stateLabel := r.getPartitionStateLabel(topoLabelKey)
	if err := r.KubeClient.LabelNodeGroupByNodeNames(ctx, map[string]string{stateLabel: migStatus}, nodeNameList); err != nil {
		return fmt.Errorf("failed to update health labels for MIG %s: %w", migURL, err)
	}
	return nil
}

func (r *HealthReconciler) getPartitionStateLabelFromUID(uid string, migInfo *cache.MIGInfo) (string, error) {
	for _, nodeInfo := range migInfo.Nodes {
		for topoLabel, nodeUIDs := range nodeInfo.TopologyPartitionIds {
			for _, nodeUID := range nodeUIDs {
				if nodeUID == uid {
					if topoInfo, ok := util.SupportedTopologiesInfo[topoLabel]; ok {
						return r.getPartitionStateLabel(topoInfo.LabelKey), nil
					}
					return r.getPartitionStateLabel(topoLabel), nil
				}
			}
		}
	}
	return "", fmt.Errorf("partition UID %s not found in MIG", uid)
}

func (r *HealthReconciler) getPartitionHealthStatus(uid string, topologiesHealth map[string]cache.AcceleratorTopologyInfo) (string, bool) {
	info, ok := topologiesHealth[uid]
	if !ok {
		return "", false
	}
	switch {
	case info.AcceleratorTopologyState == cache.MIGPartitionStateIncomplete ||
		info.AcceleratorTopologyHealthStatus == cache.MIGHealthStatusIncomplete:
		return healthStatusIncomplete, true
	default:
		return r.mapMIGHealthStatusToString(info.AcceleratorTopologyHealthStatus), true
	}
}

func (r *HealthReconciler) mapMIGHealthStatusToString(status cache.MIGHealthStatus) string {
	switch status {
	case cache.MIGHealthStatusHealthy:
		return healthStatusHealthy
	case cache.MIGHealthStatusDegraded:
		return healthStatusDegraded
	case cache.MIGHealthStatusUnhealthy:
		return healthStatusUnhealthy
	case cache.MIGHealthStatusIncomplete:
		return healthStatusIncomplete
	case cache.MIGHealthStatusUnknown:
		return healthStatusUnknown
	default:
		return string(status)
	}
}

func (r *HealthReconciler) getPartitionStateLabel(topoLabel string) string {
	res := strings.Replace(topoLabel, "-id", "-state", 1)
	return strings.Replace(res, "-slice-", "-partition-", 1)
}

// reconcileMMIG reconciles Slice CR condition for a single MMIG
func (r *HealthReconciler) reconcileMMIG(ctx context.Context, mmigURL string) error {
	logger := log.FromContext(ctx).WithName("reconcile-mmig").WithValues("mmigURL", mmigURL)

	// Get latest health from cache
	healthStatus, exists := r.healthCache.GetMMIGHealth(mmigURL)
	if !exists {
		logger.V(1).Info("MMIG not found in health cache (may have been deleted)")
		return NewTerminalError(errors.New("MMIG not found in health cache (may have been deleted)"))
	}
	_, _, mmigName, err := parseMmigDetails(mmigURL)
	if err != nil {
		logger.Error(err, "failed to parse MMIG details")
		return NewTerminalError(errors.New("failed to parse MMIG details"))
	}

	sliceName := util.ExtractSliceNameFromResourceName(mmigName, r.clusterHash)
	_, exists = r.relationalCache.GetSliceInfo(sliceName)
	if !exists {
		logger.V(1).Info("No Slice info found for MMIG")
		return NewTerminalError(errors.New("no Slice info found for MMIG"))
	}

	// Update Slice CR status with retries
	if err := r.updateSliceWithRetry(ctx, sliceName, healthStatus); err != nil {
		return err
	}

	logger.V(1).Info("MMIG reconciliation completed")
	return nil
}

// recordSliceFormLatency exports the E2E latency of slice formation to metrics.
func (r *HealthReconciler) recordSliceFormLatency(ctx context.Context, slice *slicev1beta1.Slice, newState string, duration time.Duration) {
	blockID := ""
	if len(slice.Spec.PartitionIds) > 0 {
		blockID, _ = r.relationalCache.GetBlockID(slice.Spec.PartitionIds[0])
	}
	r.metricsExporter.RecordSliceFormLatency(ctx, slice.Name, slice.Namespace, newState, string(slice.Spec.Type), slice.Spec.Topology, blockID, string(slice.UID), duration)
}

// updateSliceWithRetry updates the Slice CR with retries.
func (r *HealthReconciler) updateSliceWithRetry(ctx context.Context, sliceName string, currentHealthStatus cache.TimestampedHealth[cache.MMIGHealthStatus]) error {
	logger := log.FromContext(ctx).WithValues("sliceName", sliceName)
	var lastErr error
	objectKey := client.ObjectKey{Name: sliceName}

	for attempt := 0; attempt < maxRetries; attempt++ {
		// check context cancellation before each attempt
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get fresh slice CR for retry attempt
		slice := &slicev1beta1.Slice{}
		if err := r.client.Get(ctx, objectKey, slice); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Slice resource not found during retry attempt")
				return NewTerminalError(errors.New("slice not found"))
			}
			return fmt.Errorf("failed to fetch Slice resource during retry attempt: %w", err)
		}

		if (currentHealthStatus.CurrentStatus == cache.MMIGHealthStatusActive ||
			currentHealthStatus.CurrentStatus == cache.MMIGHealthStatusActiveDegraded) && !slice.ObjectMeta.DeletionTimestamp.IsZero() {
			logger.V(4).Info("Skipping status update for deleting slice")
			return nil
		}

		err := r.updateSliceCR(ctx, slice, currentHealthStatus)
		if err == nil {
			logger.Info("Successfully updated Slice CR")
			return nil
		}

		if apierrors.IsConflict(err) {
			logger.Info("Conflict error while updating Slice CR, will retry", "attempt", attempt+1, "maxAttempts", maxRetries)
			lastErr = err
			time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
			continue
		}

		return fmt.Errorf("failed to update Slice CR status: %w", err)
	}

	logger.Error(lastErr, "Failed to update Slice CR", "attempt", maxRetries, "maxAttempts", maxRetries)
	return lastErr
}

// updateSliceCR updates the Slice CR with retries.
func (r *HealthReconciler) updateSliceCR(ctx context.Context, slice *slicev1beta1.Slice, currentHealthStatus cache.TimestampedHealth[cache.MMIGHealthStatus]) error {
	var (
		status  metav1.ConditionStatus
		reason  string
		message string
	)
	originalState := util.FindStateCondition(slice)

	switch currentHealthStatus.CurrentStatus {
	case cache.MMIGHealthStatusActive, cache.MMIGHealthStatusActiveDegraded:
		status = metav1.ConditionTrue
		reason = string(currentHealthStatus.CurrentStatus)
		message = "" // Explicitly clear old error messages on success

	case cache.MMIGHealthStatusNotFound:
		status = metav1.ConditionFalse
		// [CRITICAL] We map this to SliceCreationFailed so the main Reconciler
		reason = util.SliceCreationFailed
		message = util.MMIGNotFound

	case cache.MMIGHealthStatusUnknown:
		status = metav1.ConditionUnknown
		reason = string(currentHealthStatus.CurrentStatus)
		message = currentHealthStatus.ErrorMessage

	default:
		// Covers Failed, ProbeFailed, and any other error states
		status = metav1.ConditionFalse
		reason = string(currentHealthStatus.CurrentStatus)
		message = currentHealthStatus.ErrorMessage
	}

	if _, err := util.PatchSliceCondition(
		ctx,
		r.client,
		slice,
		slicev1beta1.SliceStateConditionType,
		status,
		reason,
		message,
		r.metricsExporter,
	); err != nil {
		return fmt.Errorf("failed to update SliceState condition: %w", err)
	}

	r.recordSliceFormationLatencyIfNeeded(ctx, slice, originalState, reason)
	r.recordPartitionFailuresIfNeeded(ctx, slice, originalState, reason)
	return nil
}

// recordSliceFormationLatencyIfNeeded checks if the slice transitioned from Activating to a final state
// and records the formation latency if so.
func (r *HealthReconciler) recordSliceFormationLatencyIfNeeded(ctx context.Context, slice *slicev1beta1.Slice, originalState, newState string) {
	logger := log.FromContext(ctx)
	retryEnabled := slice.Annotations[util.RetryAnnotation] == util.EnableRetryAnnotation

	if ((originalState == string(util.MMIGHealthStatusActivating) || originalState == util.SliceNotCreated || originalState == util.SliceCreationFailed) && IsFinalState(newState)) ||
		(originalState == util.SliceNotCreated && newState == util.SliceCreationFailed && !retryEnabled) {
		formationDuration := time.Since(slice.CreationTimestamp.Time)
		r.recordSliceFormLatency(ctx, slice, newState, formationDuration)
		logger.Info("Slice formation is done", "end_state", newState, "duration_sec", formationDuration.Seconds(), "sliceUID", string(slice.UID), "sliceName", slice.Name)
	}
}

// recordPartitionFailuresIfNeeded checks if the slice entered a failed state and records partition failures.
func (r *HealthReconciler) recordPartitionFailuresIfNeeded(ctx context.Context, slice *slicev1beta1.Slice, originalState, newState string) {
	if originalState == newState {
		return
	}
	if newState != string(cache.MMIGHealthStatusFailed) && newState != util.DynamicSliceFailed && newState != string(cache.MIGPartitionStateFailed) {
		return
	}

	// Capture necessary data before goroutine
	partitionIDs := make([]string, len(slice.Spec.PartitionIds))
	copy(partitionIDs, slice.Spec.PartitionIds)
	sliceName := slice.Name
	sliceNamespace := slice.Namespace
	sliceUID := string(slice.UID)

	go func() {
		// Wait 15 seconds to allow HealthMonitor to poll the latest partition status.
		select {
		case <-ctx.Done():
			// Controller is shutting down, abort
			return
		case <-time.After(15 * time.Second):
			r.checkAndRecordPartitionFailures(ctx, partitionIDs, sliceName, sliceNamespace, sliceUID)
		}
	}()
}

// checkAndRecordPartitionFailures checks the health of each partition and records a metric if it is not HEALTHY.
// If all partitions are HEALTHY, it records a single metric indicating an unidentified cause.
func (r *HealthReconciler) checkAndRecordPartitionFailures(ctx context.Context, partitionIDs []string, sliceName, sliceNamespace, sliceUID string) {
	if r.metricsExporter == nil {
		return
	}

	metricsRecorded := false
	allHealthy := len(partitionIDs) > 0

	for _, partitionID := range partitionIDs {
		migInfo, found := r.relationalCache.GetMIGByPartitionID(partitionID)
		if !found {
			r.metricsExporter.RecordSlicePartitionFailure(ctx, sliceName, sliceNamespace, sliceUID, partitionID, healthStatusUnknown, metrics.UnknownNodePool)
			allHealthy = false
			metricsRecorded = true
			continue
		}

		nodePoolName := metrics.UnknownNodePool
		for _, node := range migInfo.Nodes {
			nodePoolName = node.NodePoolName
			break
		}

		var pHealth string
		if r.enableSubSlicing {
			if topologiesHealth, ok := r.healthCache.GetMIGPartitionsStatus(migInfo.Name); ok {
				if pStatus, exists := r.getPartitionHealthStatus(partitionID, topologiesHealth); exists {
					pHealth = pStatus
				}
			}
		}

		if pHealth == "" || pHealth == healthStatusIncomplete || pHealth == healthStatusUnknown {
			if healthStatus, healthExists := r.healthCache.GetMIGHealth(migInfo.Name); healthExists {
				pHealth = r.mapMIGHealthStatusToString(healthStatus.CurrentStatus)
			}
		}

		if pHealth == "" {
			pHealth = healthStatusUnknown
		}

		if pHealth == healthStatusUnhealthy || pHealth == healthStatusDegraded || pHealth == healthStatusIncomplete || pHealth == healthStatusUnknown {
			r.metricsExporter.RecordSlicePartitionFailure(ctx, sliceName, sliceNamespace, sliceUID, partitionID, pHealth, nodePoolName)
			metricsRecorded = true
		}

		if pHealth != healthStatusHealthy {
			allHealthy = false
		}
	}

	// If all partitions were explicitly healthy despite the slice failing, record an UNIDENTIFIED_CAUSE metric.
	if allHealthy && !metricsRecorded {
		r.metricsExporter.RecordSlicePartitionFailure(ctx, sliceName, sliceNamespace, sliceUID, "unidentified", "UNIDENTIFIED_CAUSE", metrics.UnknownNodePool)
	}
}

// runMetricsReport periodically reports metrics for slices and partitions
func (r *HealthReconciler) runMetricsReport(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("metrics-report")
	ticker := time.NewTicker(metricsReportInterval)
	defer ticker.Stop()

	logger.V(2).Info("Metrics report loop started", "interval", metricsReportInterval)

	for {
		select {
		case <-ctx.Done():
			logger.V(2).Info("Metrics report loop stopping")
			return
		case <-ticker.C:
			r.reportSliceMetrics(ctx)
			r.reportPartitionMetrics(ctx)
			if r.metricsExporter != nil {
				r.metricsExporter.ExportCumulativeFailures()
			}
		}
	}
}

// reportSliceMetrics reports metrics for all slices
func (r *HealthReconciler) reportSliceMetrics(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("slice-metrics-report")

	sliceList := &slicev1beta1.SliceList{}
	if err := r.client.List(ctx, sliceList); err != nil {
		logger.Error(err, "failed to list slices for metrics report")
		return
	}

	existingSlices := make(map[string]bool)
	for i := range sliceList.Items {
		slice := &sliceList.Items[i]
		existingSlices[string(slice.UID)] = true
		condition := meta.FindStatusCondition(slice.Status.Conditions, slicev1beta1.SliceStateConditionType)
		reason := "Unknown"
		if condition != nil {
			reason = condition.Reason
		}

		if r.metricsExporter != nil {
			blockID := ""
			if len(slice.Spec.PartitionIds) > 0 {
				blockID, _ = r.relationalCache.GetBlockID(slice.Spec.PartitionIds[0])
			}
			r.metricsExporter.RecordSliceState(ctx, slice.Name, slice.Namespace, reason, string(slice.Spec.Type), slice.Spec.Topology, blockID, string(slice.UID))
		}
	}

	if r.metricsExporter != nil {
		r.metricsExporter.GarbageCollectSlices(existingSlices)
	}
}

// reportPartitionMetrics reports metrics for all partitions
func (r *HealthReconciler) reportPartitionMetrics(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("partition-metrics-report")

	sliceList := &slicev1beta1.SliceList{}
	if err := r.client.List(ctx, sliceList); err != nil {
		logger.Error(err, "failed to list slices for metrics report")
		return
	}
	sliceMap := make(map[string]*slicev1beta1.Slice)
	for i := range sliceList.Items {
		s := &sliceList.Items[i]
		sliceMap[s.Name] = s
	}

	migNames := r.relationalCache.GetAllMIGNames()
	reportedPIDs := make(map[string]bool)

	for _, migName := range migNames {
		migInfo, exists := r.relationalCache.GetMIG(migName)
		if !exists {
			continue
		}
		healthStatus, found := r.healthCache.GetMIGHealth(migInfo.Name)
		statusStr := string(healthStatus.CurrentStatus)
		if !found {
			statusStr = "INCOMPLETE"
			logger.Info("MIG health not found in cache (MIG might not have enough nodes), logging as INCOMPLETE", "mig", migInfo.Name)
		}

		r.reportMIGPartitionsMetrics(ctx, migInfo, sliceMap, reportedPIDs, statusStr)
	}

	if r.metricsExporter != nil {
		r.metricsExporter.GarbageCollectPartitions(reportedPIDs)
	}
}

// reportMIGPartitionsMetrics iterates over all nodes in a MIG and reports metrics for each unique partition.
func (r *HealthReconciler) reportMIGPartitionsMetrics(ctx context.Context, migInfo cache.MIGInfo, sliceMap map[string]*slicev1beta1.Slice, reportedPIDs map[string]bool, statusStr string) {
	for _, nodeInfo := range migInfo.Nodes {
		for topo, pids := range nodeInfo.TopologyPartitionIds {
			for _, pid := range pids {
				// Avoid reporting duplicate metrics for the same partition across different nodes.
				if reportedPIDs[pid] {
					continue
				}
				reportedPIDs[pid] = true

				var slice *slicev1beta1.Slice
				if sliceName, found := r.relationalCache.GetSliceNameByPartitionID(pid); found {
					slice = sliceMap[sliceName]
				}

				var sliceUID string
				if slice != nil {
					sliceUID = string(slice.UID)
				}

				partitionStatusStr := statusStr
				if r.enableSubSlicing {
					if topologiesHealth, ok := r.healthCache.GetMIGPartitionsStatus(migInfo.Name); ok {
						if pStatus, exists := r.getPartitionHealthStatus(pid, topologiesHealth); exists {
							partitionStatusStr = pStatus
						}
					}
				}

				if r.metricsExporter != nil {
					r.metricsExporter.RecordPartitionState(ctx, nodeInfo.NodePoolName, partitionStatusStr, nodeInfo.AcceleratorType, topo, pid, sliceUID, nodeInfo.ReservationBlockID)
				}
			}
		}
	}
}
