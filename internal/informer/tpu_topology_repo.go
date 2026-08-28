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

	"github.com/go-logr/logr"
)

// RootsByUID maps a topology UID to its root TopologyNode.
type RootsByUID map[string]*TopologyNode

// TopologiesByShape maps a topology shape (e.g., "4x4") to a map of its root nodes.
type TopologiesByShape map[string]RootsByUID

// RepoSnapshot is a snapshot of the repository, structured for the dashboard.
type RepoSnapshot map[string]TopologiesByShape

type TopologyRepo struct {
	Log logr.Logger
	// Key: UID -> TopologyNode
	// allTopologies provides O(1) access to any TopologyNode by its ID.
	allTopologyNodes map[string]*TopologyNode
	// Key: NodeName -> (root) TopologyNode
	// This map allows direct access from any noed to the root of the TopologyNode
	// tree.
	nodeToTopologyRoot map[string]*TopologyNode
	// Mutex for the two variables above.
	mu sync.RWMutex
}

func NewTopologyRepo(log logr.Logger) *TopologyRepo {
	return &TopologyRepo{
		Log:                log.WithName("topology-repo"),
		allTopologyNodes:   make(map[string]*TopologyNode),
		nodeToTopologyRoot: make(map[string]*TopologyNode),
	}
}

func (tr *TopologyRepo) GetOrAddTree(root *TopologyNode) *TopologyNode {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if tn, ok := tr.allTopologyNodes[root.UID]; ok {
		return tn
	}
	tr.addTree(root)
	return root
}

func (tr *TopologyRepo) addTree(root *TopologyNode) {
	if tr.allTopologyNodes == nil {
		tr.allTopologyNodes = make(map[string]*TopologyNode)
	}
	if tr.allTopologyNodes[root.UID] != nil {
		return
	}
	tr.allTopologyNodes[root.UID] = root
}

// Acquires a read lock and returns the TopologyNode object given the topology
// UID
func (tr *TopologyRepo) TopologyNode(uid string) *TopologyNode {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if tn, ok := tr.allTopologyNodes[uid]; ok {
		return tn
	}
	return nil
}

// Acquires a read lock and returns the TopologyNode object for the root, given
// the k8s nodeName
func (tr *TopologyRepo) RootTNFromNode(nodeName string) *TopologyNode {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if tn, ok := tr.nodeToTopologyRoot[nodeName]; ok {
		return tn
	}
	return nil
}

// Acquires the lock and then adds new TopologyNodes and root Topology Nodes to
// allTopologyNodes and nodeToTopologyRoot maps.
func (tr *TopologyRepo) Update(newTN *map[string]*TopologyNode, rootTN *TopologyNode) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, tn := range *newTN {
		tr.Log.Info("Added to allTopologyNodes", "tn", tn.UID)
		tr.allTopologyNodes[tn.UID] = tn
		// If its a leaf node then add it to the map nodeToTopologyRoot
		if tn.NodeName != "" && rootTN != nil {
			tr.Log.Info("Added to nodeToTopologyRoot", "NodeName", tn.NodeName, "rootTN", rootTN.UID)
			tr.nodeToTopologyRoot[tn.NodeName] = rootTN
		}
	}
}

func (tr *TopologyRepo) DeleteFromRepo(prunedUIDs map[string]string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for uid, nodeName := range prunedUIDs {
		tr.Log.Info("Deleted from allTopologyNodes", "tn", uid)
		delete(tr.allTopologyNodes, uid)
		tr.Log.Info("Deleted from nodeToTopologyRoot", "NodeName", nodeName)
		delete(tr.nodeToTopologyRoot, nodeName)
	}
}

// GetTopologyRepoSnapshot returns a deep copy of the topology repository for safe concurrent access.
// It transforms the flat node map into a structured map of root nodes: AcceleratorType -> Topology -> UID -> RootNode.
func (tr *TopologyRepo) GetTopologyRepoSnapshot() RepoSnapshot {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	repoCopy := make(RepoSnapshot)

	for _, node := range tr.allTopologyNodes {
		// A root node is a node without a parent.
		if node.Parent == nil {
			// Lock the specific tree to ensure a consistent snapshot during the deep copy.
			node.rootLock.RLock()

			// Ensure nested maps are initialized.
			if _, ok := repoCopy[node.Type]; !ok {
				repoCopy[node.Type] = make(TopologiesByShape)
			}
			if _, ok := repoCopy[node.Type][node.Topology]; !ok {
				repoCopy[node.Type][node.Topology] = make(RootsByUID)
			}

			// Add a deep copy of the entire tree to the snapshot.
			repoCopy[node.Type][node.Topology][node.UID] = node.DeepCopy()

			node.rootLock.RUnlock()
		}
	}
	return repoCopy
}
