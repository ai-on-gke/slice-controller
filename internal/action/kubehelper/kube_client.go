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

package kubehelper

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"slice-controller/internal/cache"
	"slice-controller/internal/metrics"
	"slice-controller/internal/util"
	"golang.org/x/sync/errgroup"
	"gomodules.xyz/jsonpatch/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	kubeBackOffRetryDuration = 100 * time.Millisecond
	kubeRetrySteps           = 5
)

// Client is the interface for a Kubernetes helper client.
type Client interface {
	// Add labels to all nodes with partitionId
	LabelNodeGroup(ctx context.Context, labels util.NodeLabels, partitionId string) error
	// Remove labels from all nodes with partitionId
	UnlabelNodeGroup(ctx context.Context, labelKeys util.NodeLabelKeys, partitionId string) error
	// Check Node Labels existence on nodeGroup
	CheckNodeLabels(ctx context.Context, partitionIds []string, labels util.NodeLabels, shouldExist bool) (bool, error)
	// Gets all nodes with nodeGroupLabelValue
	GetNodesByPartitionId(ctx context.Context, partitionId string) ([]corev1.Node, error)
	// Get a node by name
	GetNode(ctx context.Context, nodeName string) (*corev1.Node, error)
	LabelNodeGroupByNodeNames(ctx context.Context, labels map[string]string, nodeNames []string) error
	UnlabelNodeGroupByNames(ctx context.Context, labelKeys []string, nodeNames []string) error
	ListNodesBySliceName(ctx context.Context, sliceName string) ([]corev1.Node, error)
}

type kubeHelper struct {
	client             client.Client
	cache              cache.SliceRelationalCache
	enableLinkedRunner bool
}

// NewClient creates a new Kubernetes helper client.
func NewClient(client client.Client, cache cache.SliceRelationalCache, enableLinkedRunner bool) Client {
	return &kubeHelper{
		client:             client,
		cache:              cache,
		enableLinkedRunner: enableLinkedRunner,
	}
}

func (c *kubeHelper) TaintNodeGroup(ctx context.Context, taints []corev1.Taint, partitionId string) error {
	nodes, err := c.GetNodesByPartitionId(ctx, partitionId)
	if err != nil {
		return err
	}

	for i := range nodes {
		node := &nodes[i]
		err := c.patchNodeWithRetry(ctx, node, func(n *corev1.Node) {
			n.Spec.Taints = append(n.Spec.Taints, taints...)
		})
		if err != nil {
			return fmt.Errorf("failed to taint node %s: %w", node.Name, err)
		}
	}
	return nil
}

func (c *kubeHelper) UntaintNodeGroup(ctx context.Context, taints []corev1.Taint, partitionId string) error {
	nodes, err := c.GetNodesByPartitionId(ctx, partitionId)
	if err != nil {
		return err
	}

	for i := range nodes {
		node := &nodes[i]
		err := c.patchNodeWithRetry(ctx, node, func(freshNode *corev1.Node) {
			var newTaints []corev1.Taint
			for _, currentTaint := range freshNode.Spec.Taints {
				found := false
				for _, t := range taints {
					if currentTaint.MatchTaint(&t) {
						found = true
						break
					}
				}
				if !found {
					newTaints = append(newTaints, currentTaint)
				}
			}
			freshNode.Spec.Taints = newTaints
		})
		if err != nil {
			return fmt.Errorf("failed to untaint node %s: %w", node.Name, err)
		}
	}
	return nil
}

func (c *kubeHelper) LabelNodeGroup(ctx context.Context, labels util.NodeLabels, partitionId string) error {
	nodes, err := c.GetNodesByPartitionId(ctx, partitionId)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		klog.FromContext(ctx).WithName("LabelNodeGroup").Info("no nodes found for partitionId %s", partitionId)
		return nil
	}

	for i := range nodes {
		node := &nodes[i]
		err := c.patchNodeWithRetry(ctx, node, func(freshNode *corev1.Node) {
			if freshNode.Labels == nil {
				freshNode.Labels = make(map[string]string)
			}
			if c.enableLinkedRunner && util.IsLinkedRunnerControlNode(freshNode) {
				for k, v := range labels.LinkedRunnerLabels {
					freshNode.Labels[k] = v
				}
			} else {
				for k, v := range labels.RegularLabels {
					freshNode.Labels[k] = v
				}
			}
		})
		if err != nil {
			return fmt.Errorf("failed to label node %s: %w", node.Name, err)
		}
	}
	return nil
}

// getNodesByNames fetches nodes by name sequentially.
func (c *kubeHelper) getNodesByNames(ctx context.Context, nodeNames []string) ([]corev1.Node, error) {
	nodes := make([]corev1.Node, 0, len(nodeNames))
	for _, name := range nodeNames {
		node := &corev1.Node{}
		if err := c.client.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
			return nil, fmt.Errorf("failed to get node %s: %w", name, err)
		}
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

// getNodesByNamesParallel fetches nodes by name in parallel.
func (c *kubeHelper) getNodesByNamesParallel(ctx context.Context, nodeNames []string) ([]corev1.Node, error) {
	nodeChan := make(chan corev1.Node, len(nodeNames))

	// Use an ErrorGroup to manage parallel workers and error handling.
	g, gctx := errgroup.WithContext(ctx)

	for _, name := range nodeNames {
		nodeName := name

		g.Go(func() error {
			node := &corev1.Node{}
			err := c.client.Get(gctx, client.ObjectKey{Name: nodeName}, node)
			if err != nil {
				return fmt.Errorf("failed to get node %s: %w", nodeName, err)
			}
			nodeChan <- *node
			return nil
		})
	}

	// Wait for all goroutines to finish or for the first error.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Close the channel, since all writing is done.
	close(nodeChan)

	// Collect all results from the channel into a slice.
	nodes := make([]corev1.Node, 0, len(nodeNames))
	for node := range nodeChan {
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// LabelNodeGroupByNodeNames adds labels to nodes specified by their names in parallel.
func (c *kubeHelper) LabelNodeGroupByNodeNames(ctx context.Context, labels map[string]string, nodeNames []string) error {
	nodes, err := c.getNodesByNamesParallel(ctx, nodeNames)
	if err != nil {
		return err
	}

	// Use an ErrorGroup to manage parallel patching.
	g, gctx := errgroup.WithContext(ctx)

	for _, n := range nodes {
		node := n

		// [OPTIMIZATION]
		alreadyMatches := true
		for k, v := range labels {
			if val, ok := node.Labels[k]; !ok || val != v {
				alreadyMatches = false
				break
			}
		}
		if alreadyMatches {
			continue
		}

		g.Go(func() error {
			err := c.patchNodeWithRetry(gctx, &node, func(freshNode *corev1.Node) {
				if freshNode.Labels == nil {
					freshNode.Labels = make(map[string]string)
				}
				for k, v := range labels {
					freshNode.Labels[k] = v
				}
			})
			if err != nil {
				return fmt.Errorf("failed to label node %s: %w", node.Name, err)
			}
			return nil
		})
	}

	// Wait for all patches to complete or for the first error.
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// UnlabelNodeGroupByNames removes labels from nodes specified by their names in parallel.
func (c *kubeHelper) UnlabelNodeGroupByNames(ctx context.Context, labelKeys []string, nodeNames []string) error {
	nodes, err := c.getNodesByNamesParallel(ctx, nodeNames)
	if err != nil {
		return err
	}

	// Use an ErrorGroup to manage parallel patching.
	g, gctx := errgroup.WithContext(ctx)

	for _, n := range nodes {
		node := n

		// [OPTIMIZATION]
		labelsAlreadyGone := true
		if node.Labels != nil {
			for _, key := range labelKeys {
				if _, exists := node.Labels[key]; exists {
					// Found a label that needs deleting, so we must patch.
					labelsAlreadyGone = false
					break
				}
			}
		}

		if labelsAlreadyGone {
			continue
		}

		g.Go(func() error {
			err := c.patchNodeWithRetry(gctx, &node, func(freshNode *corev1.Node) {
				if freshNode.Labels == nil {
					return
				}
				for _, key := range labelKeys {
					delete(freshNode.Labels, key)
				}
			})
			if err != nil {
				return fmt.Errorf("failed to unlabel node %s: %w", node.Name, err)
			}
			return nil
		})
	}

	// Wait for all patches to complete or for the first error.
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *kubeHelper) UnlabelNodeGroup(ctx context.Context, labelKeys util.NodeLabelKeys, partitionId string) error {
	nodes, err := c.GetNodesByPartitionId(ctx, partitionId)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		klog.FromContext(ctx).WithName("UnlabelNodeGroup").Info("no nodes found for partitionId %s", partitionId)
		return nil
	}

	for i := range nodes {
		node := &nodes[i]
		err := c.patchNodeWithRetry(ctx, node, func(freshNode *corev1.Node) {
			if freshNode.Labels == nil {
				return
			}
			if c.enableLinkedRunner && util.IsLinkedRunnerControlNode(freshNode) {
				for _, key := range labelKeys.LinkedRunnerLabels {
					delete(freshNode.Labels, key)
				}
			} else {
				for _, key := range labelKeys.RegularLabels {
					delete(freshNode.Labels, key)
				}
			}
		})
		if err != nil {
			return fmt.Errorf("failed to unlabel node %s: %w", node.Name, err)
		}
	}
	return nil
}

// getNodesByPartitionIdFallback finds nodes for a partitionId when cache lookup fails.
// If enableLinkedRunner is true, it first tries to find the partitionId in linked runner control nodes.
// If not found or if enableLinkedRunner is false, it falls back to the original flow (getNodesByPartitionIdFallbackDefault).
// This supports clusters with a mix of linked and non-linked VMs.
func (c *kubeHelper) getNodesByPartitionIdFallback(ctx context.Context, partitionId string) ([]corev1.Node, error) {
	if c.enableLinkedRunner {
		controlNodes, err := c.getLinkedRunnerControlNodes()
		if err != nil {
			klog.Warningf("failed to get linked runner control nodes, falling back to default lookup: %v", err)
		}

		if err == nil && len(controlNodes) > 0 {
			controlNode := c.getControlNodeByPartitionIdFallbackForLinkedVM(controlNodes, partitionId)
			if controlNode != nil {
				return []corev1.Node{*controlNode}, nil
			}
		}
	}
	return c.getNodesByPartitionIdFallbackDefault(ctx, partitionId)
}

func (c *kubeHelper) getNodesByPartitionIdFallbackDefault(ctx context.Context, partitionId string) ([]corev1.Node, error) {
	var result []corev1.Node
	for _, topologyInfo := range util.SupportedTopologiesInfo {
		nodeList := &corev1.NodeList{}
		selector := client.MatchingLabels{topologyInfo.LabelKey: partitionId}

		if err := c.client.List(ctx, nodeList, selector); err != nil {
			return nil, fmt.Errorf("failed to list nodes with label %s=%s: %w", topologyInfo.LabelKey, partitionId, err)
		}

		if len(nodeList.Items) > 0 {
			if len(nodeList.Items) != topologyInfo.NodeCount {
				klog.Warningf("unexpected number of nodes found for label %s=%s: expected %d, got %d",
					topologyInfo.LabelKey, partitionId, topologyInfo.NodeCount, len(nodeList.Items))
			}
			result = nodeList.Items
			break
		}
	}

	if len(result) == 0 {
		klog.Infof("No nodes found for partitionId %s", partitionId)
	}

	return result, nil
}

func (c *kubeHelper) getControlNodeByPartitionIdFallbackForLinkedVM(controlNodes []corev1.Node, partitionId string) *corev1.Node {
	for i := range controlNodes {
		controlNode := &controlNodes[i]
		runnerPartitions, err := util.ExtractRunnerPartitionsFromControlNode(controlNode)
		if err != nil {
			klog.Warningf("Could not extract runner partitions from control node %s, skipping: %v", controlNode.Name, err)
			continue
		}

		for _, partitions := range runnerPartitions {
			for topology, pId := range partitions {
				if pId == partitionId {
					klog.V(4).Infof("Found partitionId %s in control node %s", partitionId, controlNode.Name)

					topologyInfo, ok := util.SupportedTopologiesInfo[topology]
					if !ok {
						klog.Warningf("unknown topology %s for partitionId %s on control node %s", topology, partitionId, controlNode.Name)
					} else {
						runnerCount := 0
						for _, rp := range runnerPartitions {
							if val, ok := rp[topology]; ok && val == partitionId {
								runnerCount++
							}
						}

						if runnerCount != topologyInfo.NodeCount {
							klog.Warningf("unexpected number of linked runner VMs found for partitionId %s (topology %s) on control node %s: expected %d, got %d",
								partitionId, topology, controlNode.Name, topologyInfo.NodeCount, runnerCount)
						}
					}

					return controlNode
				}
			}
		}
	}

	klog.Warningf("No nodes found for partitionId %s in any control node annotations", partitionId)
	return nil
}

func (c *kubeHelper) GetNodesByPartitionId(ctx context.Context, partitionId string) ([]corev1.Node, error) {
	migInfo, found := c.cache.GetMIGByPartitionID(partitionId)
	if !found {
		klog.Warningf("No MIG found for partitionId %s in cache, falling back to direct lookup.", partitionId)
		return c.getNodesByPartitionIdFallback(ctx, partitionId)
	}

	var associatedTopology string
	for _, nodeInfo := range migInfo.Nodes {
		for topology, partitionIds := range nodeInfo.TopologyPartitionIds {
			if slices.Contains(partitionIds, partitionId) {
				associatedTopology = topology
				break
			}
		}
		if associatedTopology != "" {
			break
		}
	}

	if associatedTopology == "" {
		klog.Warningf("PartitionId %s not found within MIG %s", partitionId, migInfo.Name)
		return nil, nil
	}

	var nodeNames []string
	for nodeName, nodeInfo := range migInfo.Nodes {
		if partitionIds, ok := nodeInfo.TopologyPartitionIds[associatedTopology]; ok {
			if slices.Contains(partitionIds, partitionId) {
				nodeNames = append(nodeNames, nodeName)
			}
		}
	}

	if len(nodeNames) == 0 {
		klog.Warningf("No nodes with partitionId %s found in MIG %s, though MIG was mapped from partitionId.", partitionId, migInfo.Name)
		return nil, nil
	}

	nodes, err := c.getNodesByNames(ctx, nodeNames)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes by name for partitionId %s: %w", partitionId, err)
	}

	nodeCountForValidation := len(nodes)
	if c.enableLinkedRunner && nodeCountForValidation == 1 {
		singleNode := nodes[0]
		isLinkedRunnerControlNode := util.IsLinkedRunnerControlNode(&singleNode)
		if isLinkedRunnerControlNode {
			runnerPartitions, err := util.ExtractRunnerPartitionsFromControlNode(&singleNode)
			if err != nil {
				klog.Warningf("failed to extract runner partitions from control node %s: %v", singleNode.Name, err)
			}
			nodeCountForValidation = len(runnerPartitions)
		}
	}

	if err := util.ValidateNodeCountForTopology(associatedTopology, nodeCountForValidation); err != nil {
		klog.Warningf("validation of node count failed for partitionId %s: %v", partitionId, err)
	}

	return nodes, nil
}

func (c *kubeHelper) patchNodeWithRetry(ctx context.Context, node *corev1.Node, modifyFunc func(*corev1.Node)) error {
	backoff := retry.DefaultRetry
	backoff.Duration = kubeBackOffRetryDuration
	backoff.Steps = kubeRetrySteps
	logger := klog.FromContext(ctx).WithName("patcher").WithValues("node", node.Name)

	return retry.RetryOnConflict(backoff, func() error {
		// Get the latest version of the node.
		latestNode := &corev1.Node{}
		if err := c.client.Get(ctx, client.ObjectKeyFromObject(node), latestNode); err != nil {
			return fmt.Errorf("failed to get latest node: %w", err)
		}

		// Clone the node object to generate the patch.
		oldNode := latestNode.DeepCopy()

		// Apply the modifications.
		modifyFunc(latestNode)

		// Create a JSON patch.
		oldNodeBytes, err := json.Marshal(oldNode)
		if err != nil {
			return fmt.Errorf("failed to marshal oldNode: %w", err)
		}
		latestNodeBytes, err := json.Marshal(latestNode)
		if err != nil {
			return fmt.Errorf("failed to marshal latestNode: %w", err)
		}
		patchOps, err := jsonpatch.CreatePatch(oldNodeBytes, latestNodeBytes)
		if err != nil {
			return fmt.Errorf("failed to create JSON patch: %w", err)
		}
		patchBytes, err := json.Marshal(patchOps)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON patch: %w", err)
		}

		// Apply the patch.
		startTime := time.Now()
		err = c.client.Patch(ctx, oldNode, client.RawPatch(types.JSONPatchType, patchBytes))
		metrics.RecordKubeApiCall("Node.Patch", startTime, err)
		if err != nil {
			if errors.IsConflict(err) {
				logger.V(4).Info("Conflict detected when patching node, retrying", "err", err)
			}
			return err
		}
		logger.Info("Successfully patched", "node", node.Name)
		return nil
	})
}

func (c *kubeHelper) GetNode(ctx context.Context, nodeName string) (*corev1.Node, error) {
	node := &corev1.Node{}
	err := c.client.Get(ctx, client.ObjectKey{Name: nodeName}, node)
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}
	return node, nil
}

// CheckNodeLabels checks if all nodes matching the selector have the required labels.
func (c *kubeHelper) CheckNodeLabels(ctx context.Context, partitionIds []string, labels util.NodeLabels, shouldExist bool) (bool, error) {
	var allNodes []corev1.Node
	for _, partitionId := range partitionIds {
		nodes, err := c.GetNodesByPartitionId(ctx, partitionId)
		if err != nil {
			return false, fmt.Errorf("failed to get nodes for partitionId %s: %w", partitionId, err)
		}
		allNodes = append(allNodes, nodes...)
	}

	if len(allNodes) == 0 {
		klog.FromContext(ctx).WithName("CheckNodeLabels").Info("No nodes found")
		return true, nil // No nodes found, the check is considered done.
	}

	for _, node := range allNodes {
		labelsToCheck := make(map[string]string)
		if c.enableLinkedRunner && util.IsLinkedRunnerControlNode(&node) {
			for k, v := range labels.LinkedRunnerLabels {
				labelsToCheck[k] = v
			}
		} else {
			for k, v := range labels.RegularLabels {
				labelsToCheck[k] = v
			}
		}

		nodeLabels := node.GetLabels()
		if nodeLabels == nil {
			nodeLabels = make(map[string]string)
		}

		for key, value := range labelsToCheck {
			nodeVal, ok := nodeLabels[key]
			if shouldExist {
				if !ok || nodeVal != value {
					return false, nil // Label not found or value mismatch
				}
			} else {
				if ok {
					if c.enableLinkedRunner && util.IsLinkedRunnerControlNode(&node) {
						return false, nil // Label should not exist, but it does
					}
					// For regular labels, we only consider it labeled for our slice if the SliceLabelKey matches our slice name.
					expectedSlice := labels.RegularLabels[util.SliceLabelKey]
					if val, exists := nodeLabels[util.SliceLabelKey]; exists && val == expectedSlice {
						return false, nil // Label belongs to our slice, so it's not "unlabeled"
					}
				}
			}
		}
	}

	return true, nil // All nodes have the required label state
}

func (c *kubeHelper) getLinkedRunnerControlNodes() ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	selector := client.MatchingLabels{util.LinkedRunnerNodeTypeLabel: "control-node"}
	if err := c.client.List(context.Background(), nodeList, selector); err != nil {
		return nil, fmt.Errorf("failed to list control nodes: %w", err)
	}
	klog.V(2).Infof("Found %d control nodes", len(nodeList.Items))
	return nodeList.Items, nil
}

// ListNodesBySliceName lists all nodes that have the slice label set to the given slice name.
// Note: This is a fallback mechanism used during N->0 updates when the MMIG has already been
// deleted from GCE and the relational cache no longer holds the MIG mapping. In this case,
// we cannot efficiently look up nodes by partition ID, so we fall back to scanning all nodes
// by the slice label. This might be slow in very large clusters if the label is not indexed
// in the API server cache, but it is not on the hot path.
func (c *kubeHelper) ListNodesBySliceName(ctx context.Context, sliceName string) ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	selector := client.MatchingLabels{util.SliceLabelKey: sliceName}
	if err := c.client.List(ctx, nodeList, selector); err != nil {
		return nil, fmt.Errorf("failed to list nodes for slice %s: %w", sliceName, err)
	}
	return nodeList.Items, nil
}
