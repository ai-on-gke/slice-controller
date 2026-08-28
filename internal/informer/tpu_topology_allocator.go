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

package informer

import (
	"fmt"
	"sync"

	"github.com/go-logr/logr"
)

type TPUType string
type TPUTopology string

// TopologyAllocator manages the pool of allocatable topology nodes.
// It is responsible for finding and reserving topologies for allocation
// and releasing them back to the pool. Its methods are thread-safe.
type TopologyAllocator struct {
	log logr.Logger
	// repo provides access to the global topology node repository.
	// The allocator needs this to re-validate nodes after acquiring locks.
	repo *TopologyRepo
	// Allocatable topologies. This represents the largest topology that is
	// allocatable to be allocated for every TopologyTree.
	// Key: Type -> Topology -> *TopologyNode
	// Example: If we have 3 16x16 slices with 1 16x16 fully free (slice-A),
	// 1 16x16 (slice-B) has an 8x16 free and the other one (slice-C) has a 4x4
	// (on slice-C-4x4-11) running on it.
	// The `allocatableTopologyNodeMap` will look like:
	// v5e -> 16x16 -> [slice-A-16x16-1]
	// v5e -> 8x16 -> [slice-B–8x16-2, slice-C-8x16-1]
	// v5e -> 8x8 -> [slice-C-8x8-4]
	// v5e -> 4x8-> [slice-C-4x8-5]
	// v5e -> 4x4 -> [slice-C-4x4-12]
	allocatableTopologyNodeMap map[TPUType]map[TPUTopology][]*TopologyNode
	// allocatableTopologiesMutex protects allocatableTopologyNodeMap.
	allocatableTopologiesMutex sync.RWMutex
	// allocationMutex serializes the Allocate and Deallocate operations to
	// prevent race conditions where multiple goroutines might select the
	// same resource for allocation.
	allocationMutex sync.Mutex
}

// NewTopologyAllocator creates a new allocator.
func NewTopologyAllocator(log logr.Logger, repo *TopologyRepo) *TopologyAllocator {
	return &TopologyAllocator{
		log:                        log.WithName("topology-allocator"),
		repo:                       repo,
		allocatableTopologyNodeMap: make(map[TPUType]map[TPUTopology][]*TopologyNode),
	}
}

// Allocate finds and reserves a topology, returning the constituent topology
// UIDs.
// This function follows a multi-step process to allocate a TPU topology:
//  1. Input Validation: It first checks if both tpuType and topology are provided.
//     If either is missing, it returns nil, indicating no allocation is needed.
//  2. Candidate Search: It attempts to find a suitable topology node (TN) for
//     allocation. This involves:
//     -   Reading the allocatable topologies (using a read lock).
//     -   Searching for an exact match or a larger topology that can be broken down.
//  3. Locking: Once a candidate is found, it acquires the necessary locks:
//     -   A write lock for the specific topology tree (to prevent concurrent
//     modifications to that tree).
//  4. Re-check Availability: After acquiring the locks, it re-checks if the
//     candidate TN is still allocatable. This is to prevent race conditions.
//  5. State Propagation: If the candidate is allocatable, it proceeds to:
//     -   Mark all descendants of the candidate as 'Allocated'.
//     -   Update the allocation state of the entire tree, propagating changes
//     upwards.
//     -   Update the allocatable topologies based on the new state.
//  6. Return: Finally, it returns the UID of the allocated topology and nil error.
//     If any error happens during the process, it returns nil and the error.
//
// Example (Continuing from the example in `allocatableTopologyNodeMap`):
//   - Lets say a request comes in to allocate a 2x4.
//   - First we will see if we have any allocatable candidate matching the topology.
//   - Next, we will go through the map to find the next biggest allocatable topology
//     i.e. `slice-C-4x4-12`
//   - Now we will remove the `slice-C-4x4-12` entry from the map and add entries
//     to 2x4 and 2x2.
//   - The map now will look like:
//     v5e -> 16x16 -> [slice-A-16x16-1]
//     v5e -> 8x16 -> [slice-B–8x16-2, slice-C-8x16-2]
//     v5e -> 8x8 -> [slice-C-8x8-2]
//     v5e -> 4x8-> [slice-C-4x8-2]
//     v5e -> 4x4 -> []
//     v5e -> 2x4 -> [slice-C-2x4-25]
//     v5e -> 2x2 -> [slice-C-2x2-49]
//
// This whole process allows us to efficiently (reducing fragmentation) allocate
func (a *TopologyAllocator) Allocate(tpuType, topology string) ([]string, error) {
	if tpuType == "" || topology == "" {
		return nil, nil
	}

	// Serialize the entire allocation process to prevent race conditions.
	a.allocationMutex.Lock()
	defer a.allocationMutex.Unlock()

	candidateTN, err := a.findAllocationCandidate(tpuType, topology)
	if err != nil {
		return nil, err
	}

	// Lock the specific topology tree
	candidateTN.rootLock.Lock()
	defer candidateTN.rootLock.Unlock()

	// **Critical Re-validation**
	// After acquiring the write lock, we must confirm the node still exists in the
	// repository and is still in the correct state. This closes the race window
	// between finding the candidate and modifying its tree.
	latestCandidateTN := a.repo.TopologyNode(candidateTN.UID)
	if latestCandidateTN == nil || latestCandidateTN != candidateTN {
		// The node we have a lock on was removed from the repository.
		return nil, fmt.Errorf("candidate topology %s was deleted during allocation", candidateTN.UID)
	}

	// Re-check availability after acquiring the tree lock
	if !(candidateTN.IsReady && candidateTN.AllocationState == StateAllocatable) {
		// This should be a retryable error by the caller. Its a race condition and
		// should be rare.
		return nil, fmt.Errorf("candidate topology %s became unallocatable", candidateTN.UID)
	}

	root := findRoot(candidateTN)
	// First, Propagate the StateAllocated to all descendants of the candidate
	setAllocationStateRecursive(candidateTN, StateAllocated)
	// Next, Propagate the StateAllocated up the tree, by updating it for the
	// whole tree.
	updateAllocationState(root)
	// Next update the allocatableTopologiesNodeMap variable with the latest state.
	a.updateAllocatableTopologies(root)

	a.log.Info("Allocated", "Type", candidateTN.Type, "Topology", candidateTN.Topology, "UID", candidateTN.UID)

	return []string{candidateTN.UID}, nil
}

// This function is responsible for releasing a previously allocated TPU topology,
// making it allocatable for future allocation. It performs the following steps:
//
//  1. Input Validation: It checks if the provided `topologyUID` is empty. If so,
//     it returns without performing any action.
//  2. Node Retrieval: It attempts to retrieve the `TopologyNode` associated
//     with the given `topologyUID` from the `allTopologyNodes` map. It uses a
//     read lock to avoid contention. If the node is not found, it returns an error.
//  3. Lock Acquisition: It acquires a write lock on the `rootLock` of the
//     retrieved `TopologyNode`.
//  4. State Reset: It resets the allocation state of the target `TopologyNode`
//     and all its descendants to `StateAllocatable`.
//  5. State Propagation: It propagates the updated allocation state up the
//     topology tree, updating the state of all parent nodes.
//  6. Allocatable Topologies Update: It updates the `allocatableTopologiesNodeMap`
//     map to reflect the newly allocatable topology.
//  7. Logging: It logs the successful deallocation of the topology.
//
// This function ensures that the TPU topology is correctly marked as allocatable
// after it is no longer in use, preventing resource exhaustion and enabling
// efficient allocation of TPU resources.
//
// Note: Deallocate should always be called after the labels from the node are
// removed which indicates which slice the node belongs to.
// Example (Continuing from the example in `Allocate`):
//   - Lets say we DeAllocate slice-B-8x16-1.
//   - Mark the the topologyCandidate (slice-B-8x16-1) to be `Allocatable`
//   - Next propagate this information down the tree and up the tree.
//   - Update the `allocatableTopologyNodeMap` map to look like:
//     v5e -> 16x16 -> [slice-A-16x16-1, slice-B–16x16-1]
//     v5e -> 8x16 -> [slice-C-8x16-2]
//     v5e -> 8x8 -> [slice-C-8x8-2]
//     v5e -> 4x8-> [slice-C-4x8-2]
//     v5e -> 4x4 -> []
//     v5e -> 2x4 -> [slice-C-2x4-25]
//     v5e -> 2x2 -> [slice-C-2x2-49]
func (a *TopologyAllocator) Deallocate(topologyUID string) error {
	if topologyUID == "" {
		return nil
	}

	// Serialize the entire deallocation process to prevent race conditions.
	a.allocationMutex.Lock()
	defer a.allocationMutex.Unlock()

	// Find the node without a global lock first
	tn := a.repo.TopologyNode(topologyUID)
	if tn == nil {
		return fmt.Errorf("topology with UID %s not found", topologyUID)
	}

	// Now acquire lock for the tree.
	tn.rootLock.Lock()
	defer tn.rootLock.Unlock()

	// **Critical Re-validation**
	latestTN := a.repo.TopologyNode(topologyUID)
	if latestTN == nil || latestTN != tn {
		return fmt.Errorf("topology with UID %s not found or was deleted during deallocation", topologyUID)
	}

	root := findRoot(tn)
	// First, Propagate the AllocatedState to all descendants of the candidate
	setAllocationStateRecursive(tn, StateAllocatable)
	// Next, Propagate the AllocatedState up the tree, by updating it for the
	// whole tree.
	updateAllocationState(root)
	// Next update the allocatableTopologiesNodeMap variable with the latest state.
	a.updateAllocatableTopologies(root)

	a.log.Info("DeAllocated", "Type", tn.Type, "Topology", tn.Topology, "UID", tn.UID)

	return nil
}

// updateAllocatableTopologies updates the allocatable topologies for a given tree.
// It finds the largest allocatable sub-topologies to reduce fragmentation.
// This function assumes caller has the tree lock.
//
// This function performs the following steps:
//  1. Identify Nodes in the Current Tree:
//     - It traverses the provided `root` node and its descendants to collect
//     all `TopologyNode` objects that belong to the same tree.
//  2. Remove Existing Nodes from `allocatableTopologiesNodeMap`:
//     - It iterates through all the topology shapes found in the tree.
//     - For each shape, it filters out any `TopologyNode` from the
//     `allocatableTopologiesNodeMap` map that belongs to the current tree.
//     - This ensures that the `allocatableTopologiesNodeMap` map does not contain
//     stale entries from the current tree.
//  3. Assign Maximal allocatable Nodes:
//     - It calls the `assignMaximalAllocatable` function to traverse the tree
//     and add the maximal allocatable nodes to the `allocatableTopologies` map.
//
// Example:
//
// `allocatableTopologiesNodeMap` may look like this:
// v5e -> 16x16 -> [slice-A-16x16-1]
// v5e -> 8x16 -> [slice-B–8x16-2, slice-C-8x16-1]
// v5e -> 8x8 -> [slice-C-8x8-4]
// v5e -> 4x8-> [slice-C-4x8-5]
// v5e -> 4x4 -> [slice-C-4x4-12]
//
// - Lets say now we allocate a 2x4, which resulted in the allocation of
// `slice-C-2x4-24`. Then updateAllocatableTopologies was called on root of
// `slice-C`
//   - First step we will gather all the UIDs of slice-C
//   - Next we will remove all entries of slice-C UIDs from `allocatableTopologiesNodeMap`
//     The map will look like:
//     v5e -> 16x16 -> [slice-A-16x16-1]
//     v5e -> 8x16 -> [slice-B–8x16-2]
//   - Next a call will be made to assignMaximalAllocatable for slice-C
//   - This will find all the maximal topologies that are Allocatable, resulting
//     in the map:
//     v5e -> 16x16 -> [slice-A-16x16-1]
//     v5e -> 8x16 -> [slice-B–8x16-2, slice-C-8x16-2]
//     v5e -> 8x8 -> [slice-C-8x8-2]
//     v5e -> 4x8-> [slice-C-4x8-2]
//     v5e -> 4x4 -> []
//     v5e -> 2x4 -> [slice-C-2x4-25]
//     v5e -> 2x2 -> [slice-C-2x2-49]
func (a *TopologyAllocator) updateAllocatableTopologies(root *TopologyNode) {
	if root == nil {
		return
	}

	a.allocatableTopologiesMutex.Lock()
	defer a.allocatableTopologiesMutex.Unlock()

	tpuType := TPUType(root.Type)
	if _, ok := a.allocatableTopologyNodeMap[tpuType]; !ok {
		a.allocatableTopologyNodeMap[tpuType] = make(map[TPUTopology][]*TopologyNode)
	}

	// --- Step 1: Gather all the UIDs in the tree: `root`` ---
	q := []*TopologyNode{root}
	nodesInTree := make(map[string]bool)

	for len(q) > 0 {
		curr := q[0]
		q = q[1:]
		nodesInTree[curr.UID] = true
		q = append(q, curr.Children...)
	}

	// --- Step 2: Gather all the topologyUID and topologies in the tree *root* ---
	// For each topology shape present in the tree, filter out the nodes from this tree.

	// Save all the nodes that do not map to the current tree, and then add
	// them back to the allocatableTopologyNodeMap. This is to remove all the nodes
	// from the curret tree from allocatableTopologyNodeMap.
	if typeMap, ok := a.allocatableTopologyNodeMap[tpuType]; ok {
		for topoShape := range typeMap {
			if nodes, ok := typeMap[topoShape]; ok {
				newNodes := make([]*TopologyNode, 0, len(nodes))
				for _, node := range nodes {
					if !nodesInTree[node.UID] {
						newNodes = append(newNodes, node)
					}
				}
				a.allocatableTopologyNodeMap[tpuType][topoShape] = newNodes
			}
		}
	}

	// --- Step 3: Find and add the new maximal allocatable topologies ---
	a.assignMaximalAllocatable(root)
}

// Recursively traverses down the tree starting from root (ideally).
// If the topologyNode is Allocatable and IsReady then add to map and stop
// If the topologyNode is PartiallyAllocated and IsReady then recurse on children
// Other cases (IsReady==false OR Allocated) recursion stops.
// Example:
//
//	Assume a tree with the following structure:
//	  16x16 (Not Ready)
//	  ├── 8x16-1 (Ready, PartiallyAllocated)
//	  │   ├── 8x8-1  (Ready, Allocatable)
//	  │   └── 8x8-2  (Ready, Allocated)
//	  ├── 8x16-2 (Ready, Allocatable)
//	      ├── ....
//	The function will add the following nodes to the availableTopologies map:
//	- v5e -> 8x16 -> [8x16-2] (Ready, Allocatable)
//	- v5e -> 8x8 -> [8x8-1]  (Ready, Allocatable)
func (a *TopologyAllocator) assignMaximalAllocatable(tn *TopologyNode) {
	if tn == nil {
		return
	}

	if tn.IsReady && tn.AllocationState == StateAllocatable {
		if _, ok := a.allocatableTopologyNodeMap[TPUType(tn.Type)]; !ok {
			a.allocatableTopologyNodeMap[TPUType(tn.Type)] = make(map[TPUTopology][]*TopologyNode)
		}
		a.allocatableTopologyNodeMap[TPUType(tn.Type)][TPUTopology(tn.Topology)] = append(a.allocatableTopologyNodeMap[TPUType(tn.Type)][TPUTopology(tn.Topology)], tn)
		return
	}
	if tn.IsReady && tn.AllocationState == StatePartiallyAllocated {
		for _, child := range tn.Children {
			a.assignMaximalAllocatable(child)
		}
	}
}

// findAllocationCandidate finds a suitable topology node for allocation.
func (a *TopologyAllocator) findAllocationCandidate(tpuType, topology string) (*TopologyNode, error) {
	a.allocatableTopologiesMutex.RLock()
	if topologies, ok := a.allocatableTopologyNodeMap[TPUType(tpuType)]; ok {
		if candidates, ok := topologies[TPUTopology(topology)]; ok && len(candidates) > 0 {
			a.allocatableTopologiesMutex.RUnlock()
			return candidates[0], nil
		}
	}
	a.allocatableTopologiesMutex.RUnlock()

	return a.findAndBreakApartLargerTopology(tpuType, topology)
}

// findAndBreakApartLargerTopology finds the smallest larger topology to break apart.
func (a *TopologyAllocator) findAndBreakApartLargerTopology(tpuType, topology string) (*TopologyNode, error) {
	order, ok := topologyOrder[tpuType]
	if !ok {
		return nil, fmt.Errorf("unknown tpu type: %s", tpuType)
	}
	orderedMap, ok := topologyOrderedMap[tpuType]
	if !ok {
		return nil, fmt.Errorf("unknown tpu type for ordered map: %s", tpuType)
	}
	startIndex, ok := orderedMap[topology]
	if !ok {
		return nil, fmt.Errorf("unknown topology %s for tpu type %s", topology, tpuType)
	}

	for i := startIndex + 1; i < len(order); i++ {
		largerTopology := order[i]

		a.allocatableTopologiesMutex.RLock()
		var candidates []*TopologyNode
		if topologies, ok := a.allocatableTopologyNodeMap[TPUType(tpuType)]; ok {
			if c, ok := topologies[TPUTopology(largerTopology)]; ok {
				candidates = c
			}
		}
		a.allocatableTopologiesMutex.RUnlock()

		if len(candidates) > 0 {
			if foundNode, err := checkCandidates(candidates, topology); err == nil {
				return foundNode, nil
			}
		}
	}
	return nil, fmt.Errorf("no allocatable topology found for %s/%s", tpuType, topology)
}

// checkCandidates iterates through a list of candidate parent topologies,
// locks them safely, and checks for a suitable sub-topology.
//
// This function is designed to be resilient to race conditions where a
// candidate's state might change after the initial candidate list was generated.
// For example, a background reconciliation process could mark a node as unhealthy,
// making a previously valid candidate tree unallocatable.
//
// By iterating through a list, if the first candidate is found to be stale or
// no longer allocatable upon acquiring its lock, the function can gracefully
// move to the next candidate in the list without failing the entire allocation
// operation.
func checkCandidates(candidates []*TopologyNode, targetTopology string) (*TopologyNode, error) {
	for _, candidate := range candidates {
		candidate.rootLock.RLock()
		if !(candidate.IsReady && candidate.AllocationState == StateAllocatable) {
			candidate.rootLock.RUnlock()
			continue
		}

		foundNode, err := findAllocatableSubTopology(candidate, targetTopology)
		candidate.rootLock.RUnlock()

		if err == nil {
			return foundNode, nil
		}
	}
	return nil, fmt.Errorf("no suitable sub-topology found in candidates")
}

// findAllocatableSubTopology performs a DFS to find an allocatable node of a
// specific topology within a larger topology tree.
// Example: Lets say we are looking for a 2x4 and the candidate tree we got is
// an 8x16 topologyNode from which we need to select a 2x4.
// - We start with 8x16 and appends the children 8x8-1 and 8x8-2.
// - The next item would be 8x8-1 which will append children 4x8-1 and 4x8-2
// - The next item would be 4x8-1 which will append children 4x4-1 and 4x4-2
// - The next item would be 4x4-1 which will append children 2x4-1 and 2x4-2
// - The next item would be 2x4-1 which will match the `targetTopology`
// - We stop here and return the 2x4-1 TopologyNode
func findAllocatableSubTopology(tn *TopologyNode, targetTopology string) (*TopologyNode, error) {
	q := []*TopologyNode{tn}

	for len(q) > 0 {
		curr := q[len(q)-1]
		q = q[:len(q)-1]
		if curr.Topology == targetTopology {
			if curr.IsReady && curr.AllocationState == StateAllocatable {
				return curr, nil
			}
			continue
		}
		if topologyOrderedMap[curr.Type][curr.Topology] < topologyOrderedMap[curr.Type][targetTopology] {
			continue
		}
		q = append(q, curr.Children...)
	}
	return nil, fmt.Errorf("could not find an allocatable sub-topology %s in %s", targetTopology, tn.UID)
}
