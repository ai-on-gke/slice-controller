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
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	internalCache "slice-controller/internal/cache"
	"slice-controller/internal/util"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	logr "github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
)

const (
	// Labels used to identify TPU nodes and their provisioning modes.
	ProvisioningModeLabel = "cloud.google.com/gke-accelerator-topology-mode"

	// PROVISION_ONLY indicates that the ICI links are configured dynamically.
	ProvisionOnly               = "PROVISION_ONLY"
	TpuAcceleratorLabel         = "cloud.google.com/gke-tpu-accelerator"
	TpuV7XAcceleratorLabelValue = "tpu7x"

	// Labels used for preflight checks.
	// NodePoolLabel is the label key for the GKE node pool name.
	NodePoolLabel = "cloud.google.com/gke-nodepool"
	// Reservation Name that the node pool is associated with.
	ReservationNameLabel = "cloud.google.com/reservation-name"
	// Reservation Block Hash ID that the node pool is associated with.
	ReservationBlockIDLabel = "cloud.google.com/gce-topology-block"
)

var requiredV7XTopologies = []string{"2x2x1", "2x2x2", "2x2x4", "2x4x4", "4x4x4"}

// NodeInformer watches for Node creation/deletion/updates to keep the relational cache up to date.
type NodeInformer struct {
	cache              ctrlcache.Cache
	relationalCache    internalCache.NodeInformerRelationalCacheInterface
	informer           ctrlcache.Informer
	ctx                context.Context
	processedNodes     sync.Map
	initialSyncDone    chan struct{}
	enableLinkedRunner bool
}

// NewNodeInformer creates a new NodeInformer.
func NewNodeInformer(mgr manager.Manager, rc internalCache.NodeInformerRelationalCacheInterface, enableLinkedRunner bool) (*NodeInformer, error) {
	if mgr == nil {
		return nil, fmt.Errorf("manager cannot be nil")
	}
	if rc == nil {
		return nil, fmt.Errorf("relational cache cannot be nil")
	}
	sharedCache := mgr.GetCache()

	ni := &NodeInformer{
		cache:              sharedCache,
		relationalCache:    rc,
		initialSyncDone:    make(chan struct{}),
		enableLinkedRunner: enableLinkedRunner,
	}

	informer, err := sharedCache.GetInformerForKind(context.TODO(), corev1.SchemeGroupVersion.WithKind("Node"))
	if err != nil {
		return nil, fmt.Errorf("failed to get Node informer: %v", err)
	}
	ni.informer = informer

	if _, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ni.handleNodeAdd,
		UpdateFunc: ni.handleNodeUpdate,
		DeleteFunc: ni.handleNodeDelete,
	}); err != nil {
		return nil, fmt.Errorf("failed to add event handlers to Node informer: %v", err)
	}

	return ni, nil
}

// Startup sequence:
// 1. Get the node informer from the shared cache with label selector filter
// 2. Register event handlers for add/update/delete
// 3. Wait for cache to sync (triggers AddFunc for all existing incremental nodes)
// 4. Mark informer as synced
// 5. Start processing events as they come in
func (ni *NodeInformer) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("node-informer")
	logger.Info("Starting Node Informer")

	ni.ctx = ctx

	if !cache.WaitForCacheSync(ctx.Done(), ni.informer.HasSynced) {
		return fmt.Errorf("timed out waiting for Node informer cache to sync")
	}

	logger.Info("Cache synced, all existing nodes have been processed")

	close(ni.initialSyncDone)

	<-ctx.Done()
	logger.Info("Stopping Node Informer")
	return nil
}

// If enableLinkedRunner is true and the node is a Linked Runner control node, shouldProcessNode checks for the presence
// of linked-runner.runner-instance/ annotations. Otherwise, it checks for standard TPU node labels.
func (ni *NodeInformer) shouldProcessNode(node *corev1.Node) bool {
	if node == nil {
		return false
	}

	if ni.enableLinkedRunner && util.IsLinkedRunnerControlNode(node) {
		if node.Annotations != nil {
			for key := range node.Annotations {
				if strings.HasPrefix(key, util.LinkedRunnerAnnotationPrefix) {
					return true
				}
			}
		}
		return false
	}

	if node.Labels == nil {
		return false
	}

	ProvisioningModeLabelValue, exists := node.Labels[ProvisioningModeLabel]
	if !exists || ProvisioningModeLabelValue != ProvisionOnly {
		return false
	}

	accelType, exists := node.Labels[util.TpuAcceleratorLabel]
	if !exists || accelType != TpuV7XAcceleratorLabelValue {
		return false
	}

	return true
}

// If enableLinkedRunner is true and the node is a Linked Runner control node, isNodeReady verifies that all associated runner instances
// have a RUNNING status, valid PartitionIDs, and all required topology labels are present. Otherwise, it checks for
// standard TPU topology labels directly on the node.
func (ni *NodeInformer) isNodeReady(node *corev1.Node) (bool, error) {
	if node == nil || node.Annotations == nil || node.Labels == nil {
		return false, fmt.Errorf("node, annotations or labels cannot be nil")
	}

	if _, err := util.GetMIGAnnotationFromNode(node, ni.enableLinkedRunner); err != nil {
		return false, fmt.Errorf("failed to get MIG name from node %s: %w", node.Name, err)
	}

	if ni.enableLinkedRunner && util.IsLinkedRunnerControlNode(node) {
		runnerInfos, err := util.ExtractRunnerInfo(node)
		if err != nil {
			return false, fmt.Errorf("failed to extract runner infos from LinkedRunner control node %s: %w", node.Name, err)
		}

		if len(runnerInfos) == 0 {
			return false, nil
		}

		atLeastOneRunning := false
		for instance, info := range runnerInfos {
			if info.Status == "RUNNING" {
				atLeastOneRunning = true
				if info.PartitionIDs == "" {
					return false, fmt.Errorf("runner instance %s on Linked Runner control node %s is RUNNING but is missing PartitionIDs", instance, node.Name)
				}
			}
		}

		if !atLeastOneRunning {
			return false, fmt.Errorf("no runner instances on Linked Runner control node %s are RUNNING", node.Name)
		}

		ipToTopologyPartitionId, err := util.ExtractRunnerPartitionsFromControlNode(node)
		if err != nil {
			return false, fmt.Errorf("failed to extract partition IDs from Linked Runner control node %s: %w", node.Name, err)
		}

		for ip, topologyPartitionId := range ipToTopologyPartitionId {
			for _, topology := range requiredV7XTopologies {
				partitionId, ok := topologyPartitionId[topology]
				if !ok || partitionId == "" {
					return false, fmt.Errorf("runner node with IP %s missing or is missing required topology: %s", ip, topology)
				}
			}
		}
	} else {
		ReservationNameLabelValue, resNameExists := node.Labels[ReservationNameLabel]
		if !resNameExists || ReservationNameLabelValue == "" {
			return false, fmt.Errorf("failed to get reservation name from node %s", node.Name)
		}

		ReservationBlockIDLabelValue, resBlockIDExists := node.Labels[ReservationBlockIDLabel]
		if !resBlockIDExists || ReservationBlockIDLabelValue == "" {
			return false, fmt.Errorf("failed to get reservation block id from node %s", node.Name)
		}

		requiredTopologies := requiredV7XTopologies

		for _, topology := range requiredTopologies {
			if topologyInfo, ok := util.SupportedTopologiesInfo[topology]; ok {
				value, ok := node.Labels[topologyInfo.LabelKey]
				if !ok || value == "" {
					return false, fmt.Errorf("node %s missing or has empty required topology: %s", node.Name, topology)
				}
			}
		}
	}
	return true, nil
}

// handleNodeAdd processes a Node Add event.
func (ni *NodeInformer) handleNodeAdd(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		log.FromContext(ni.ctx).Error(fmt.Errorf("invalid object type in onAdd"), "Expected *corev1.Node")
		return
	}

	logger := log.FromContext(ni.ctx).WithValues("node-informer-on-add", node.Name)

	if !ni.shouldProcessNode(node) {
		logger.V(1).Info("Node does not meet processing criteria, skipping")
		return
	}

	logger.Info("Handling Node Add event")

	ready, err := ni.isNodeReady(node)
	if err != nil {
		logger.V(1).Info("Node is not ready for processing", "error", err)
		return
	}
	if !ready {
		logger.V(1).Info("Node is not ready for processing, skipping")
		return
	}

	nodeHash, err := ni.computeNodeHash(node)
	if err != nil {
		logger.Error(err, "Failed to compute node hash")
		return
	}
	ni.executeNodeUpdate(node, nodeHash, logger)
}

// handleNodeUpdate processes a Node Update event.
func (ni *NodeInformer) handleNodeUpdate(oldObj, newObj interface{}) {
	newNode, ok := newObj.(*corev1.Node)
	if !ok {
		log.FromContext(ni.ctx).Error(fmt.Errorf("invalid object type in onUpdate"), "Expected *corev1.Node for newObj")
		return
	}
	oldNode, ok := oldObj.(*corev1.Node)
	if !ok {
		log.FromContext(ni.ctx).Error(fmt.Errorf("invalid object type in onUpdate"), "Expected *corev1.Node for oldObj")
		return
	}

	logger := log.FromContext(ni.ctx).WithValues("node-informer-handle-node-update", newNode.Name)
	logger.V(1).Info("Handling Node Update event")

	shouldProcessNew := ni.shouldProcessNode(newNode)
	shouldProcessOld := ni.shouldProcessNode(oldNode)

	// CASE 1: Became unprocessable
	if shouldProcessOld && !shouldProcessNew {
		logger.Info("Node no longer meets processing criteria, handling as delete")
		ni.executeNodeRemoval(newNode, logger)
		return
	}

	if !shouldProcessNew {
		logger.V(1).Info("Node does not meet processing criteria, skipping")
		return
	}

	newReady, newReadyErr := ni.isNodeReady(newNode)
	oldReady, oldReadyErr := ni.isNodeReady(oldNode)

	if newReadyErr != nil {
		logger.Error(newReadyErr, "Error checking new node readiness")
		newReady = false
	}
	if oldReadyErr != nil {
		logger.Error(oldReadyErr, "Error checking old node readiness")
		oldReady = false
	}

	// CASE 2: Became processable OR Became ready
	if (!shouldProcessOld && shouldProcessNew) || (!oldReady && newReady) {
		if !newReady {
			logger.V(1).Info("Node is not ready for processing, skipping")
			return
		}
		nodeHash, err := ni.computeNodeHash(newNode)
		if err != nil {
			logger.Error(err, "Failed to compute node hash for new node")
			return
		}
		ni.executeNodeUpdate(newNode, nodeHash, logger)
		return
	}

	// CASE 3: Standard Update (Ready -> Ready)
	if oldReady && newReady {
		oldHash, err := ni.computeNodeHash(oldNode)
		if err != nil {
			logger.Error(err, "Failed to compute node hash for old node")
			return
		}
		newHash, err := ni.computeNodeHash(newNode)
		if err != nil {
			logger.Error(err, "Failed to compute node hash for new node")
			return
		}

		if oldHash == newHash {
			logger.V(2).Info("No relevant changes detected in Node, skipping")
			return
		}

		logger.V(1).Info("Relevant changes detected in Node, updating relational cache")

		ni.processedNodes.Store(newNode.Name, newHash)

		opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := ni.UpdateNodeInRelationalCache(opCtx, newNode); err != nil {
			logger.Error(err, "Failed to update Node in relational cache")
			ni.processedNodes.Store(newNode.Name, oldHash)
			return
		}
		logger.Info("Node Update event processed successfully")
		return
	}

	// CASE 4: Became unready
	if oldReady && !newReady {
		logger.Info("Node is no longer ready for processing, removing from relational cache")
		ni.executeNodeRemoval(newNode, logger)
		return
	}
}

// handleNodeDelete processes a Node Delete event.
func (ni *NodeInformer) handleNodeDelete(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}

	node, ok := obj.(*corev1.Node)
	if !ok {
		log.FromContext(ni.ctx).Error(fmt.Errorf("invalid object type in onDelete"), "Expected *corev1.Node")
		return
	}

	if !ni.shouldProcessNode(node) {
		log.FromContext(ni.ctx).WithValues("node-informer-on-delete", node.Name).V(1).Info("Node does not meet processing criteria, skipping")
		return
	}

	log.FromContext(ni.ctx).WithValues("node-informer-on-delete", node.Name).Info("Handling Node Delete event")
	opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // 10s timeout
	defer cancel()
	if err := ni.removeNodeFromRelationalCache(opCtx, node); err != nil {
		log.FromContext(ni.ctx).WithValues("node-informer-on-delete", node.Name).Error(err, "Failed to process Node Delete event")
		return
	}

	ni.processedNodes.Delete(node.Name)
	log.FromContext(ni.ctx).WithValues("node-informer-on-delete", node.Name).Info("Node Delete event processed successfully")
}

// UpdateNodeInRelationalCache updates or adds the given node in the relational cache.
func (ni *NodeInformer) UpdateNodeInRelationalCache(ctx context.Context, node *corev1.Node) error {
	if node == nil {
		return fmt.Errorf("node cannot be nil")
	}

	log.FromContext(ctx).WithValues("node", node.Name).Info("Updating node in relational cache")

	ready, err := ni.isNodeReady(node)
	if err != nil {
		return fmt.Errorf("error checking node %s readiness: %w", node.Name, err)
	}
	if !ready {
		return fmt.Errorf("node %s is not ready for processing", node.Name)
	}

	migURL, err := util.GetMIGAnnotationFromNode(node, ni.enableLinkedRunner)
	if err != nil {
		return fmt.Errorf("failed to get MIG annotation from node %s: %w", node.Name, err)
	}
	if migURL == "" {
		return fmt.Errorf("MIG annotation is empty for node %s", node.Name)
	}
	reservationName := node.Labels[ReservationNameLabel]
	reservationBlockID := node.Labels[ReservationBlockIDLabel]
	nodePoolName := node.Labels[NodePoolLabel]
	uniqueTopologyPartitionIds := ni.extractTopologyUniquePartitionIds(node)
	isLinkedRunnerControlNode := ni.enableLinkedRunner && util.IsLinkedRunnerControlNode(node)
	acceleratorType := node.Labels[util.TpuAcceleratorLabel]

	if err := ni.relationalCache.UpdateNodeInfo(migURL, node.Name, reservationName, reservationBlockID, nodePoolName, uniqueTopologyPartitionIds, isLinkedRunnerControlNode, acceleratorType); err != nil {
		log.FromContext(ctx).WithValues("node", node.Name).Error(err, "Failed to update node info in relational cache", "migURL", migURL)
		return fmt.Errorf("failed to update node info in relational cache: %v", err)
	}

	log.FromContext(ctx).WithValues("node", node.Name).V(1).Info("Node processed and added to cache", "mig", migURL, "num_partitions", len(uniqueTopologyPartitionIds), "partition_details", uniqueTopologyPartitionIds)
	return nil
}

// removeNodeFromRelationalCache removes the given node from the relational cache.
func (ni *NodeInformer) removeNodeFromRelationalCache(ctx context.Context, node *corev1.Node) error {
	if node == nil {
		return fmt.Errorf("node cannot be nil")
	}

	log.FromContext(ctx).WithValues("node", node.Name).Info("Removing node from relational cache")

	if _, err := ni.relationalCache.DeleteNodeAndCleanUpMIG(node.Name); err != nil {
		log.FromContext(ctx).WithValues("node", node.Name).Error(err, "Failed to remove node info from relational cache")
		return fmt.Errorf("failed to remove node info from relational cache: %v", err)
	}

	log.FromContext(ctx).WithValues("node", node.Name).V(1).Info("Node removed from cache successfully")
	return nil
}

// extractTopologyUniquePartitionIds extracts topologies to their corresponding unique and IP-sorted partition IDs
// from a given node. Linked VM control nodes can have multiple unique partition IDs under each topology, coming from runner nodes,
// and these are sorted by the runner node's IP address. Regular nodes have just a single partition ID per topology.
func (ni *NodeInformer) extractTopologyUniquePartitionIds(node *corev1.Node) map[string][]string {
	if ni.enableLinkedRunner && util.IsLinkedRunnerControlNode(node) {
		ipToTopologyPartitionId, err := util.ExtractRunnerPartitionsFromControlNode(node)
		if err != nil {
			log.FromContext(ni.ctx).WithValues("node", node.Name).Error(err, "failed to extract partition UIDs from control nodes")
			return make(map[string][]string)
		}

		type runnerNodePartition struct {
			ip          string
			partitionID string
		}
		partitionsByTopology := make(map[string][]runnerNodePartition)

		for ip, topologyPartitionId := range ipToTopologyPartitionId {
			for topology, partitionID := range topologyPartitionId {
				partitionsByTopology[topology] = append(partitionsByTopology[topology], runnerNodePartition{ip: ip, partitionID: partitionID})
			}
		}

		topologyUniquePartitionIds := make(map[string][]string)
		// for each topology, sort the partition ids by the runner node IP the partition ID corresponds to
		for topology, runnerNodePartitions := range partitionsByTopology {
			sort.Slice(runnerNodePartitions, func(i, j int) bool {
				return runnerNodePartitions[i].ip < runnerNodePartitions[j].ip
			})

			// extracts unique partition IDs, preserving the order from the ip sort
			uniquePartitionIDs := make([]string, 0)
			seenIDs := make(map[string]bool)
			for _, p := range runnerNodePartitions {
				if !seenIDs[p.partitionID] {
					uniquePartitionIDs = append(uniquePartitionIDs, p.partitionID)
					seenIDs[p.partitionID] = true
				}
			}
			topologyUniquePartitionIds[topology] = uniquePartitionIDs
		}
		return topologyUniquePartitionIds
	} else {
		requiredTopologies := requiredV7XTopologies

		topologyPartitionIds := make(map[string][]string, len(requiredTopologies)) // Pre-size map
		for _, topology := range requiredTopologies {
			if topologyInfo, ok := util.SupportedTopologiesInfo[topology]; ok {
				if value, ok := node.Labels[topologyInfo.LabelKey]; ok {
					topologyPartitionIds[topology] = []string{value}
				}
			}
		}
		return topologyPartitionIds
	}
}

// computeNodeHash computes a hash string for the node based on relevant labels and annotations.
// For regular nodes, the hash is a concatenation of all singular partition IDs from 2x2x1 up.
// For Linked Runner nodes, it concatenates the unique and IP-sorted partition IDs for each topology.
func (ni *NodeInformer) computeNodeHash(node *corev1.Node) (string, error) {
	if node == nil || node.Labels == nil || node.Annotations == nil {
		return "", fmt.Errorf("node, labels or annotations cannot be nil")
	}

	migURL, err := util.GetMIGAnnotationFromNode(node, ni.enableLinkedRunner)
	if err != nil {
		return "", fmt.Errorf("failed to get MIG annotation from node %s: %w", node.Name, err)
	}
	if migURL == "" {
		return "", fmt.Errorf("MIG annotation is empty for node %s", node.Name)
	}
	parts := []string{migURL}

	requiredTopologies := requiredV7XTopologies

	topologyUniquePartitionIds := ni.extractTopologyUniquePartitionIds(node)
	for _, topology := range requiredTopologies {
		if ids, exists := topologyUniquePartitionIds[topology]; exists {
			parts = append(parts, ids...)
		}
	}

	return strings.Join(parts, ","), nil
}

// executeNodeUpdate performs the exact update sequence used in both handlers.
func (ni *NodeInformer) executeNodeUpdate(node *corev1.Node, nodeHash string, logger logr.Logger) {
	if existingHash, exists := ni.processedNodes.Load(node.Name); exists {
		if existingHash == nodeHash {
			logger.V(2).Info("Node already processed with same hash, skipping")
			return
		}
	}

	opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := ni.UpdateNodeInRelationalCache(opCtx, node); err != nil {
		logger.Error(err, "Failed to update Node in relational cache")
		return
	}

	ni.processedNodes.Store(node.Name, nodeHash)
	logger.Info("Node processed successfully")
}

// executeNodeRemoval performs the exact removal sequence used in Update handler.
func (ni *NodeInformer) executeNodeRemoval(node *corev1.Node, logger logr.Logger) {
	opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := ni.removeNodeFromRelationalCache(opCtx, node); err != nil {
		logger.Error(err, "Failed to remove Node from relational cache")
		return
	}

	ni.processedNodes.Delete(node.Name)
	logger.Info("Node removed from relational cache successfully")
}

// NeedLeaderElection indicates whether this informer requires leader election.
func (ni *NodeInformer) NeedLeaderElection() bool {
	return true
}
