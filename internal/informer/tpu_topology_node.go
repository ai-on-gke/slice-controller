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

package informer

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
)

// TopologyNode represents a node in a tree structure.
// The tree structure represents the TPU topology, where each node represents a
// specific topology shape (e.g., 2x2, 2x4, 4x4, etc.).
// The top-level node of the tree can be any topology shape, depending on the
// allocatable nodes. For example, if there are only 4x4 nodes, then the root
// will be a 4x4 node. If there are 16x16 nodes, then the root will be a 16x16
// node.
//
// Example of a tree structure for a v5e/v6e TPU:
//
//	16x16
//	├── 8x16
//	│   ├── 8x8
//	│   │   ├── 4x8
//	│   │   │   ├── 4x4
//	│   │   │   |     └── 2x4--- 2x2
//	│   │   │   |          └──-- 2x2
//	│   │   │   └── 4x4
//	│   │   │         └── 2x4--- 2x2
//	│   │   │              └──-- 2x2
//	│   │   └── 4x8
//	│   │       ├── ...
//	│   └── 8x8
//	│       ├── ...
//	└── 8x16
//	    ├── ....
type TopologyNode struct {
	// UID uniquely identifies a group of VMs that belongs to a topology
	UID      string
	Type     string
	Topology string
	Block    string
	SubBlock string
	IsReady  bool
	// Value of (accelerator.gke.io/slice-id) reflected on the nodes.
	// Only set if both the child have this value set to the same value.
	ClaimBySliceID  string
	AllocationState AllocationState
	Children        []*TopologyNode
	Parent          *TopologyNode `json:"-"`
	// Shared lock for the whole tree
	rootLock *sync.RWMutex `json:"-"`
	// Only populated for leaf nodes. In future we can expand it to be a list
	NodeName string
}

// DeepCopy returns a deep copy of the TopologyNode.
// The Parent and rootLock fields are not copied to prevent cycles and data races.
func (tn *TopologyNode) DeepCopy() *TopologyNode {
	if tn == nil {
		return nil
	}
	newTn := &TopologyNode{
		UID:             tn.UID,
		Type:            tn.Type,
		Topology:        tn.Topology,
		Block:           tn.Block,
		SubBlock:        tn.SubBlock,
		IsReady:         tn.IsReady,
		ClaimBySliceID:  tn.ClaimBySliceID,
		AllocationState: tn.AllocationState,
		NodeName:        tn.NodeName,
	}
	if tn.Children != nil {
		newTn.Children = make([]*TopologyNode, len(tn.Children))
		for i, child := range tn.Children {
			newTn.Children[i] = child.DeepCopy()
			// The parent of the copied child is intentionally not set to newTn to avoid cycles.
		}
	}
	return newTn
}

// setAllocationStateRecursive sets the allocation state for a node and all its descendants.
func setAllocationStateRecursive(tn *TopologyNode, state AllocationState) {
	q := []*TopologyNode{tn}
	for len(q) > 0 {
		curr := q[0]
		q = q[1:]
		curr.AllocationState = state
		q = append(q, curr.Children...)
	}
}

// findRoot traverses up the tree to find the root of a given topology node.
func findRoot(tn *TopologyNode) *TopologyNode {
	if tn == nil {
		return nil
	}
	curr := tn
	for curr.Parent != nil {
		curr = curr.Parent
	}
	return curr
}

// updateTreeHealth updates health across the TopologyNode tree starting
// from the root node.
func updateTreeHealth(root *TopologyNode) {
	if root == nil {
		return
	}
	updateHealthRecursive(root)
}

// Helper function that recursively updates the health of the parent based on
// the health of the parent.
//   - If a node is a leaf node, return the health of the leaf.
//   - If the # of children != 2, then the parent is not ready.
//   - Else it recursively calls the updateHealthRecursive on its children. An
//     && operation is performed on the results of the children.
func updateHealthRecursive(tn *TopologyNode) bool {
	if len(tn.Children) == 0 {
		return tn.IsReady
	} else if len(tn.Children) != 2 {
		tn.IsReady = false
	} else {
		tn.IsReady = updateHealthRecursive(tn.Children[0]) && updateHealthRecursive(tn.Children[1])
	}
	return tn.IsReady
}

// updateAllocationState updates AllocationState & ClaimBySliceId across the
// TopologyNode tree
// This function recursively traverses the tree, updating the allocation state
// of each node based on its children.
//
//   - If a node is a leaf node, it returns its current allocation state and
//     claimBySliceId.
//   - If a node has children:
//   - It recursively calls updateAllocationState on its children.
//   - It determines its own allocation state based on the states of its
//     children:
//   - If both children are 'Allocatable', the node is 'Allocatable'.
//   - If both children are 'Allocated', the node is 'Allocated'.
//   - Otherwise, the node is 'PartiallyAllocated'.
//   - It determines its own claimBySliceId based on the claimBySliceId of its
//     children:
//   - If both children have the same claimBySliceId, the node takes that
//     value.
//   - Otherwise, the node's claimBySliceId is set to "".
//   - It returns its updated allocation state and claimBySliceId.
func updateAllocationState(tn *TopologyNode) (AllocationState, string) {
	if tn == nil {
		return StateAllocatable, ""
	}
	if len(tn.Children) == 0 { // Leaf node
		return tn.AllocationState, tn.ClaimBySliceID
	}

	leftChildState, leftChildClaimBySliceId := updateAllocationState(tn.Children[0])
	// By default the child state is Allocatable and claimBySliceId is empty
	rightChildState := StateAllocatable
	rightChildClaimBySliceId := ""
	if len(tn.Children) == 2 {
		rightChildState, rightChildClaimBySliceId = updateAllocationState(tn.Children[1])
	}

	// If both the children are Allocatable, only then the parent is Allocatable
	if leftChildState == StateAllocatable && rightChildState == StateAllocatable {
		// <Allocatable, Allocatable> -> <Allocatable>
		// If both child are Allocatable, parent -> Allocatable
		tn.AllocationState = StateAllocatable
	} else if leftChildState == StateAllocated && rightChildState == StateAllocated {
		// <Allocated, Allocated> -> <Allocated>
		// If both child are allocated, parent -> Allocated
		tn.AllocationState = StateAllocated
	} else {
		// All other cases it is PartiallyAllocated
		tn.AllocationState = StatePartiallyAllocated
	}

	// If both children have the same claimBySlice string, then parent takes that
	// <claimBySlice: “slice-1”, claimBySlice: “slice-1”> -> <claimBySlice: “slice-1”>
	if leftChildClaimBySliceId == rightChildClaimBySliceId {
		tn.ClaimBySliceID = leftChildClaimBySliceId
	} else {
		// All other cases we set it to empty
		tn.ClaimBySliceID = ""
	}

	return tn.AllocationState, tn.ClaimBySliceID
}

// This function traverses a Tree from root node and collects all the leaf
// TopologyNode. If any of the leaf nodes are missing from the visited then
// those are marked for deletion.
// This function first prunes the in-memory tree structure (which is protected
// by the tree's rootLock held by the caller), collecting the UIDs of all
// pruned nodes. It then acquires the global repoMutex once to perform a
// single, atomic batch deletion of all pruned nodes from the repository maps.
func pruneTree(root *TopologyNode, visited map[string]struct{}) map[string]string {
	if root == nil {
		return nil
	}
	toRemoveTNs := []*TopologyNode{}
	q := []*TopologyNode{root}
	for len(q) > 0 {
		curr := q[0]
		q = q[1:]
		// If this is not leaf node, then add children and move on
		if len(curr.Children) != 0 {
			q = append(q, curr.Children...)
			continue
		}
		// Since this is the leaf node, check if we have this visited. If not then
		// add this leaf node for deletion
		if _, ok := visited[curr.UID]; !ok {
			toRemoveTNs = append(toRemoveTNs, curr)
		}
	}
	if len(toRemoveTNs) == 0 {
		return map[string]string{}
	}

	// Recursively prune the tree structure and collect all UIDs that were removed.
	prunedUIDs := make(map[string]string) // Using a map to avoid duplicate UIDs
	for _, tn := range toRemoveTNs {
		deleteAndCollectRecursive(tn, prunedUIDs)
	}
	return prunedUIDs
}

// deleteAndCollectRecursive prunes a given leaf node and its parent nodes
// recursively if they become childless. It does NOT interact with the global
// repository maps. Instead, it records the UIDs of all nodes it prunes into
// the `pruned` map provided by the caller.
// The caller MUST hold the write lock for the tree being modified.
func deleteAndCollectRecursive(s *TopologyNode, pruned map[string]string) {
	// If this node was already pruned as part of another leaf's deletion, stop.
	if _, ok := pruned[s.UID]; ok {
		return
	}

	pruned[s.UID] = s.NodeName // Record the node as pruned.

	parentTN := s.Parent
	if parentTN == nil {
		// This was the root of a tree. We are done with this branch.
		return
	}

	// Find and remove the current node from its parent's children list.
	index := -1
	for i, child := range parentTN.Children {
		if child.UID == s.UID {
			index = i
			break
		}
	}
	if index != -1 {
		parentTN.Children = append(parentTN.Children[:index], parentTN.Children[index+1:]...)
	}

	// If the parent has no more children, recurse up to prune it as well.
	if len(parentTN.Children) == 0 {
		deleteAndCollectRecursive(parentTN, pruned)
	}
}

// getLabelOrDefault returns the value of a label from a map, or "unknown" if the label is not present or empty.
func getLabelOrDefault(labels map[string]string, key string) string {
	if value, ok := labels[key]; ok && value != "" {
		return value
	}
	return "unknown"
}

// Returns the expected number of nodes for a given topology shape and type.
func numNodes(accelType, topology string) int {
	shapes, ok := topologyOrder[accelType]
	if !ok {
		return 0
	}
	for i, shape := range shapes {
		if shape == topology {
			// The number of nodes doubles at each step up in topology size.
			// Smallest (index 0) has 1 node (2^0), next has 2 (2^1), and so on.
			return 1 << i
		}
	}
	return 0
}

// Extracts a key from the node that would be used for workerQueue.
// Finds the largest topology that is present as a label on the node. Usually a
// 16x16 for v5e/v6e and 4x4x4 for v7x.
// The key is a string of the form: tpus.gke.io/topology-<topology>-id=<UID>.
// For example: tpus.gke.io/topology-16x16-id=abcdefg1234567890.
func getRootKeyForNode(node corev1.Node) string {
	acceleratorType := getLabelOrDefault(node.Labels, tpuAcceleratorLabel)
	shapes, ok := topologyOrder[acceleratorType]
	if !ok {
		return ""
	}

	for i := len(shapes) - 1; i >= 0; i-- {
		shape := shapes[i]
		label := tpuTopologyLabelPrefix + shape + "-id"
		if topologyUID, ok := node.Labels[label]; ok {
			return label + "=" + topologyUID
		}
	}
	return ""
}
