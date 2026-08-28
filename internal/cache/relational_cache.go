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

package cache

import (
	"fmt"
	"slices"
	"sync"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// NodeInfo represents information about a node in the cluster.
type NodeInfo struct {
	Name                      string
	MIGName                   string              // back reference to the parent MIG
	TopologyPartitionIds      map[string][]string // TopologyPartitionIds maps a topology to its partition IDs. For the non-linked runner case, on each node, for each partition label, there is only one hash value. For the linked runner case, a partition label can have multiple hash values.
	ReservationName           string
	ReservationBlockID        string
	NodePoolName              string
	IsLinkedRunnerControlNode bool
	AcceleratorType           string // Accelerator type from node label
}

// MonitoredMIG represents a monitored Managed Instance Group.
type MIGInfo struct {
	Name  string
	Nodes map[string]*NodeInfo // key: node name
}

// SliceInfo represents a Slice CR, its partition mappings, and its topology information.
type SliceInfo struct {
	Name         string
	PartitionIDs []string // List of Partition IDs for this Slice
	Topology     string   // Topology for this Slice
}

// RelationalCache provides thread-safe storage and lookups for MIG/Node/Slice relationships.
// It is thread-safe for concurrent access.
type RelationalCache struct {
	// --- Slice Data ---
	sliceMap              map[string]*SliceInfo // Slice Name -> SliceInfo
	partitionIDToSliceMap map[string]string     // partitionID -> Slice Name
	sliceMu               sync.RWMutex          // Lock for sliceMap and partitionIDToSliceMap

	// --- MIG/Node Data ---
	migMap              map[string]*MIGInfo  // MIG Name -> MIGInfo
	nodeMap             map[string]*NodeInfo // Node name -> NodeInfo
	partitionIDToMIGMap map[string]*MIGInfo  // partitionId -> MIGInfo
	mu                  sync.RWMutex         // Lock for migMap, nodeMap, partitionIDToMIGMap

	logger logr.Logger
}

// ### Memory Consumption Estimation (New Structure)
//
// The cache's memory footprint is primarily determined by the total number of
// nodes in the cluster, as the cache stores a `NodeInfo` object for each one.
//
// **Example Scenario: 1 full superpod claimed by a single Slice CR.**
//
// **Assumptions:**
// * 1 Slice CR = 1 `SliceInfo`
// * 1 Superpod = 144 Cubes
// * 1 Cube = 1 MIG = 1 `MIGInfo`
// * 1 Cube = 16 Nodes
// * 1 Cube = 1 primary partition ID (e.g., `topologyLabel_4x4x4`)
// * Each Node has ~5 partition labels (in its `PartitionIDs` map)
// * String identifiers (URLs, names) average **100 bytes**.
// * Go map/slice/string overhead is factored in (e.g., a `map[string]string` entry is `key+val+overhead` ~= 250 bytes).
//
// **Calculated Entities for 1 Max-Size Slice:**
// * **Slices:** 1
// * **MIGs:** 144 (1 per cube)
// * **Nodes:** 144 cubes * 16 nodes/cube = **2304 nodes**
// * **Partitions (Primary):** 144 (1 per cube)
//
// ### Estimated Breakdown (per max-size Slice)
//
// 1.  **Primary Store: `nodeMap` (Structs)**
//   - This is the largest component, holding all 2304 `NodeInfo` structs.
//   - **Size per `NodeInfo`:**
//   - 4 strings (`Name`, `MIGName`, `ResName`, `ResBlockID`): 4 * ~100 bytes = 400 bytes
//   - `PartitionIDs` map (`~5` entries): 5 * ~250 bytes/entry = 1.25 KB
//   - *Total per NodeInfo*: ~1.65 KB
//   - **Sub-Total (NodeInfo Structs):** 2304 nodes * 1.65 KB/node = **~3.80 MB**
//
// 2.  **Primary Store: `nodeMap` (Map)**
//   - The `nodeMap` (`map[string]*NodeInfo`) itself holds pointers to the structs.
//   - 2304 entries (string key + pointer + overhead) * ~175 bytes/entry = **~403 KB**
//
// 3.  **Primary Store: `migMap` (Structs + Maps)**
//   - 144 `MIGInfo` structs.
//   - Each `MIGInfo` holds a `Nodes` map (`map[string]*NodeInfo`) with 16 entries.
//   - *Size of 144 `Nodes` maps*: 144 maps * 16 entries/map * ~175 bytes/entry = ~403 KB
//   - *Size of 144 `MIGInfo` structs*: 144 * (string name + map header) ~= ~20 KB
//   - **Sub-Total:** **~423 KB**
//
// 4.  **Primary Store: `migMap` (Map)**
//   - The `migMap` (`map[string]*MIGInfo`) itself.
//   - 144 entries (string key + pointer + overhead) * ~175 bytes/entry = **~25 KB**
//
// 5.  **Indexes (`sliceMap`, `partitionIDToMIGMap`)**
//   - `sliceMap`: 1 entry with a `SliceInfo` struct holding a slice of 144 strings ~= **~16 KB**
//   - `partitionIDToMIGMap`: 144 entries (string key + pointer) * ~175 bytes/entry = **~25 KB**
//   - **Sub-Total:** **~41 KB**
//
// ---
//
// **Total Estimated Upper Bound: ~4.7 MB per max-size Slice CR.**
//
// * (3.80 MB + 403 KB) (`nodeMap` structs + map
// * + (423 KB + 25 KB) (`migMap` structs + map)
// * + 41 KB (All other maps and indexes)
// * **Total ~= 4.69 MB (Round up to 4.7 MB)**
func NewRelationalCache() *RelationalCache {
	return &RelationalCache{
		migMap:                make(map[string]*MIGInfo),
		sliceMap:              make(map[string]*SliceInfo),
		partitionIDToSliceMap: make(map[string]string),
		nodeMap:               make(map[string]*NodeInfo),
		partitionIDToMIGMap:   make(map[string]*MIGInfo),
		logger:                log.Log.WithName("RelationalCache"),
	}
}

// AddSlice adds a new SliceInfo to the relational cache.
func (rc *RelationalCache) AddSlice(sliceName string, partitionIDs []string, topology string) error {
	if sliceName == "" {
		return fmt.Errorf("EMPTY SLICE NAME")
	}

	rc.sliceMu.Lock()
	defer rc.sliceMu.Unlock()

	si, exists := rc.sliceMap[sliceName]
	if !exists {
		si = &SliceInfo{
			Name:         sliceName,
			PartitionIDs: []string{},
		}
		rc.sliceMap[sliceName] = si
	}

	// Clean up old mappings in partitionIDToSliceMap to avoid memory leaks
	for _, pid := range si.PartitionIDs {
		if !slices.Contains(partitionIDs, pid) {
			if currentSlice, exists := rc.partitionIDToSliceMap[pid]; exists && currentSlice == sliceName {
				delete(rc.partitionIDToSliceMap, pid)
			}
		}
	}

	// Update to new partitions and topology
	si.PartitionIDs = make([]string, len(partitionIDs))
	copy(si.PartitionIDs, partitionIDs)
	si.Topology = topology

	// Update partitionID to SliceName index
	for _, pid := range partitionIDs {
		if existingSlice, exists := rc.partitionIDToSliceMap[pid]; exists && existingSlice != sliceName {
			rc.logger.Error(fmt.Errorf("partition already assigned to another slice"), "Conflict detected, overwriting mapping",
				"partitionID", pid,
				"existingSlice", existingSlice,
				"attemptedNewSlice", sliceName)
		}
		rc.partitionIDToSliceMap[pid] = sliceName
	}

	rc.logger.Info("Added Slice in relational cache", "Slice", sliceName, "Partitions", partitionIDs)
	return nil
}

// DeleteSlice removes a SliceInfo from the relational cache.
func (rc *RelationalCache) DeleteSlice(sliceName string) error {
	rc.sliceMu.Lock()
	defer rc.sliceMu.Unlock()

	si, exists := rc.sliceMap[sliceName]
	if !exists {
		rc.logger.Info("Slice not found, skipping delete", "Slice", sliceName)
		return nil
	}

	// Clean up partitionID to SliceName index
	for _, pid := range si.PartitionIDs {
		// Only delete mapping if it still points to this slice.
		// This prevents deleting mappings that a competing slice has taken over.
		if currentSlice, exists := rc.partitionIDToSliceMap[pid]; exists && currentSlice == sliceName {
			delete(rc.partitionIDToSliceMap, pid)
		}
	}

	delete(rc.sliceMap, sliceName)

	rc.logger.Info("Deleted Slice from relational cache", "Slice", sliceName)
	return nil
}

// GetAllSliceNames returns a list of all Slice names in the relational cache.
func (rc *RelationalCache) GetAllSliceNames() []string {
	rc.sliceMu.RLock()
	defer rc.sliceMu.RUnlock()

	names := make([]string, 0, len(rc.sliceMap))
	for name := range rc.sliceMap {
		names = append(names, name)
	}
	return names
}

// GetSliceInfo retrieves a *copy* of SliceInfo for a given slice name.
func (rc *RelationalCache) GetSliceInfo(sliceName string) (SliceInfo, bool) {
	rc.sliceMu.RLock()
	defer rc.sliceMu.RUnlock()

	sliceInfo, exists := rc.sliceMap[sliceName]
	if !exists {
		return SliceInfo{}, false
	}

	// Return a copy to prevent external modification
	return deepCopySliceInfo(sliceInfo), true
}

// GetSliceNameByPartitionID returns the slice name associated with a given partition ID.
func (rc *RelationalCache) GetSliceNameByPartitionID(partitionID string) (string, bool) {
	rc.sliceMu.RLock()
	defer rc.sliceMu.RUnlock()

	sliceName, found := rc.partitionIDToSliceMap[partitionID]
	return sliceName, found
}

// GetBlockID returns the block ID for a given partition ID.
// It assumes that all nodes in the MIG associated with the partition ID have the same block ID.
func (rc *RelationalCache) GetBlockID(partitionID string) (string, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	migInfo, found := rc.partitionIDToMIGMap[partitionID]
	if !found {
		return "", false
	}

	// Iterate over the nodes in the MIG and return the first block ID found.
	for _, nodeInfo := range migInfo.Nodes {
		if nodeInfo.ReservationBlockID != "" {
			return nodeInfo.ReservationBlockID, true
		}
	}

	return "", false
}

// deleteMIG removes a MIG and all its associated nodes from the relational cache.
// This function is private and assumes the caller holds the write lock.
func (rc *RelationalCache) deleteMIG_unsafe(migName string) {
	mig, exists := rc.migMap[migName]
	if !exists {
		rc.logger.V(1).Info("MIG not found, skipping delete", "MIG", migName)
		return
	}

	nodesToDelete := make([]string, 0, len(mig.Nodes))
	for nodeName := range mig.Nodes {
		nodesToDelete = append(nodesToDelete, nodeName)
	}

	for _, nodeName := range nodesToDelete {
		// deleteNode_unsafe will clean up mig.Nodes, nodeMap, and indexes
		if nodeInfo, nodeExists := rc.nodeMap[nodeName]; nodeExists {
			rc.deleteNode_unsafe(nodeInfo)
		}
	}

	// finally remove the mig itself
	delete(rc.migMap, migName)
	rc.logger.V(1).Info("Deleted MIG from relational cache", "MIG", migName)
}

// GetMIG retrieves a *copy* of a MIGInfo by its name.
func (rc *RelationalCache) GetMIG(migName string) (MIGInfo, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	mig, found := rc.migMap[migName]
	if !found {
		return MIGInfo{}, false
	}

	// Create a deep copy to return to the caller
	return deepCopyMIGInfo(mig), true
}

// GetAllMIGNames returns a list of all MIG names in the relational cache.
func (rc *RelationalCache) GetAllMIGNames() []string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	names := make([]string, 0, len(rc.migMap))
	for name := range rc.migMap {
		names = append(names, name)
	}
	return names
}

// GetMIGByPartitionID returns a *copy* of MIGInfo for a given partition ID
func (rc *RelationalCache) GetMIGByPartitionID(partitionID string) (MIGInfo, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	migInfoPtr, found := rc.partitionIDToMIGMap[partitionID]
	if !found {
		return MIGInfo{}, false
	}

	// Return deep copy
	return deepCopyMIGInfo(migInfoPtr), true
}

// ### Node Functions ###

// UpdateNodeInfo adds or updates a node (control node for Linked VM) in the relational cache.
func (rc *RelationalCache) UpdateNodeInfo(migName, nodeName, reservationName, reservationBlockID, nodePoolName string, topologyPartitionIds map[string][]string, isLinkedRunnerControlNode bool, acceleratorType string) error {
	if migName == "" {
		return fmt.Errorf("MIG name cannot be empty")
	}
	if nodeName == "" {
		return fmt.Errorf("node name cannot be empty")
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	// If node already exists, we must first delete it to clean up old indexes
	if oldNodeInfo, exists := rc.nodeMap[nodeName]; exists {
		oldMIGName := oldNodeInfo.MIGName
		// Log if the MIG is changing, as this is a notable event.
		if oldMIGName != migName {
			rc.logger.Info("Node is changing MIG. This may indicate an error.", "Node", nodeName, "OldMIG", oldMIGName, "NewMIG", migName)
		}

		// In all cases, delete the old node info to clean up its old indexes.
		rc.deleteNode_unsafe(oldNodeInfo)

		// If the old MIG is now empty, clean it up as well to avoid "zombie" MIGs in the monitor.
		if oldMIG, exists := rc.migMap[oldMIGName]; exists && len(oldMIG.Nodes) == 0 {
			rc.deleteMIG_unsafe(oldMIGName)
		}
	}
	// Get or create the parent MIG
	mig, exists := rc.migMap[migName]

	// If it does NOT exist, create it and add it to the map
	if !exists {
		rc.logger.V(1).Info("MIG not found, creating new one", "MIG", migName)
		mig = &MIGInfo{
			Name:  migName,
			Nodes: make(map[string]*NodeInfo),
		}
		rc.migMap[migName] = mig
	}

	// Create new NodeInfo
	nodeInfo := &NodeInfo{
		Name:                      nodeName,
		MIGName:                   migName,
		ReservationName:           reservationName,
		ReservationBlockID:        reservationBlockID,
		NodePoolName:              nodePoolName,
		IsLinkedRunnerControlNode: isLinkedRunnerControlNode,
		AcceleratorType:           acceleratorType,
		TopologyPartitionIds:      make(map[string][]string, len(topologyPartitionIds)),
	}
	for topology, partitionIds := range topologyPartitionIds {
		nodeInfo.TopologyPartitionIds[topology] = make([]string, len(partitionIds))
		copy(nodeInfo.TopologyPartitionIds[topology], partitionIds)
	}

	// Add node to primary stores
	// This is now safe because 'mig' is guaranteed to be non-nil.
	mig.Nodes[nodeName] = nodeInfo
	rc.nodeMap[nodeName] = nodeInfo

	// Update indexes
	for _, partitionIds := range topologyPartitionIds {
		for _, partitionId := range partitionIds {
			// Add this MIG to the partition's map
			if existingMIG, exists := rc.partitionIDToMIGMap[partitionId]; exists && existingMIG.Name != mig.Name {
				rc.logger.Error(fmt.Errorf("partition ID %s already mapped to MIG %s, but new node %s maps it to MIG %s", partitionId, existingMIG.Name, nodeName, mig.Name), "Partition ID conflict detected, overwriting")
			}
			rc.partitionIDToMIGMap[partitionId] = mig
		}
	}

	rc.logger.V(1).Info("Added/Updated node in relational cache", "Node", nodeName, "MIG", migName)
	return nil
}

// deleteNode_unsafe removes a node and its indexes, assuming the caller holds the write lock.
// It finds the parent MIG using the nodeInfo.
func (rc *RelationalCache) deleteNode_unsafe(nodeInfo *NodeInfo) {
	// Look up the parent MIG
	mig, exists := rc.migMap[nodeInfo.MIGName]
	if !exists {
		// This indicates data inconsistency, but we should still clean up the node
		// from nodeMap, as the deleteNode_unsafe logic below will do that.
		rc.logger.Error(fmt.Errorf("MIG %s for node %s not found", nodeInfo.MIGName, nodeInfo.Name), "Data inconsistency detected during node delete")
		// Even if MIG is gone, continue to delete from nodeMap and indexes.
		// We pass 'nil' for the MIG to skip the first part.
	}

	if mig != nil {
		if _, exists := mig.Nodes[nodeInfo.Name]; !exists {
			// Node already gone from this MIG, but might still be in nodeMap
			rc.logger.V(1).Info("Node not found in MIG, but cleaning up indexes", "Node", nodeInfo.Name, "MIG", mig.Name)
		} else {
			// Delete node from MIG
			delete(mig.Nodes, nodeInfo.Name)
		}
	}

	// Delete from primary node map
	delete(rc.nodeMap, nodeInfo.Name)

	// --- Index Cleanup Logic ---
	// This part needs to know which other nodes are in the *same MIG*.
	// If the MIG doesn't exist (mig == nil), we can't check other nodes,
	// so we must assume we can clean up all indexes.

	remainingPartitions := make(map[string]bool)
	if mig != nil {
		// Check which partitions are still referenced by *other* nodes in this MIG
		for _, node := range mig.Nodes {
			for _, partitionIds := range node.TopologyPartitionIds {
				for _, partitionId := range partitionIds {
					remainingPartitions[partitionId] = true
				}
			}
		}
	}

	// Clean up all node-specific indexes
	for _, partitionIds := range nodeInfo.TopologyPartitionIds {
		for _, partitionId := range partitionIds {
			// Clean up partitionIDToMIGMap *only* if no other nodes in this MIG reference it
			// (or if the MIG is gone entirely)
			if !remainingPartitions[partitionId] {
				// This MIG is no longer associated with this partition.

				// We must check if the index points to this MIG before deleting.
				// If mig is nil, we use nodeInfo.MIGName for the check.
				migName := nodeInfo.MIGName

				if mappedMIG, exists := rc.partitionIDToMIGMap[partitionId]; exists && mappedMIG.Name == migName {
					delete(rc.partitionIDToMIGMap, partitionId)
				}
			}
		}
	}
	rc.logger.V(1).Info("Deleted node via delete_unsafe", "Node", nodeInfo.Name, "MIG", nodeInfo.MIGName)
}

// GetAllNodesFromMIG retrieves all node names associated with a given MIG.
func (rc *RelationalCache) GetAllNodesFromMIG(migName string) ([]string, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	mig, exists := rc.migMap[migName]
	if !exists {
		return nil, fmt.Errorf("MIG %s not found in cache", migName)
	}

	nodeNames := make([]string, 0, len(mig.Nodes))
	for nodeName := range mig.Nodes {
		nodeNames = append(nodeNames, nodeName)
	}
	return nodeNames, nil
}

// GetAllNodesFromPartitionID retrieves all node names associated with a given partition ID.
func (rc *RelationalCache) GetAllNodesFromPartitionID(partitionId string) []string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	mig, found := rc.partitionIDToMIGMap[partitionId]
	if !found {
		return []string{}
	}

	nodeNameSet := make(map[string]struct{})
	// Iterate over every node in the MIG
	for nodeName, nodeInfo := range mig.Nodes {
		for _, partitionIds := range nodeInfo.TopologyPartitionIds {
			// Check if the specific partitionId exists in this slice
			if slices.Contains(partitionIds, partitionId) {
				nodeNameSet[nodeName] = struct{}{}
				break
			}
		}
	}

	// Convert set to slice
	nodeNames := make([]string, 0, len(nodeNameSet))
	for nodeName := range nodeNameSet {
		nodeNames = append(nodeNames, nodeName)
	}
	return nodeNames
}

// GetNodeInfo retrieves a *copy* of NodeInfo for a given node name.
func (rc *RelationalCache) GetNodeInfo(nodeName string) (NodeInfo, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	nodeInfo, exists := rc.nodeMap[nodeName]
	if !exists {
		return NodeInfo{}, false
	}

	// Return a copy to prevent external modification
	return deepCopyNodeInfo(nodeInfo), true
}

// DeleteNodeAndCleanUpMIG deletes a node and cleans up its MIG if it becomes empty.
func (rc *RelationalCache) DeleteNodeAndCleanUpMIG(nodeName string) (bool, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	nodeInfo, exists := rc.nodeMap[nodeName]
	if !exists {
		rc.logger.V(1).Info("Node not found in cache, skipping delete", "Node", nodeName)
		return false, nil
	}

	migName := nodeInfo.MIGName
	mig, exists := rc.migMap[migName]
	if !exists {
		// Data inconsistency
		rc.logger.Error(fmt.Errorf("MIG %s for node %s not found", migName, nodeName), "Data inconsistency detected")
		// Clean up the node from nodeMap anyway
		delete(rc.nodeMap, nodeName)
		return false, nil
	}

	// This will clean up nodeMap, mig.Nodes, and all indexes
	rc.deleteNode_unsafe(nodeInfo)

	if len(mig.Nodes) == 0 {
		// If it was the last node, remove the now-empty MIG
		// This will see the MIG has 0 nodes and just delete it from the map.
		rc.deleteMIG_unsafe(migName)
		return true, nil // Return true indicating the MIG was cleaned up.
	}

	return false, nil
}

// copyTopologyPartitionIdsMap creates a deep copy of a map[string][]string.
func copyTopologyPartitionIdsMap(m map[string][]string) map[string][]string {
	if m == nil {
		return nil
	}
	result := make(map[string][]string, len(m))
	for k, v := range m {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		result[k] = newSlice
	}
	return result
}

// deepCopyNodeInfo creates a deep copy of a NodeInfo struct.
func deepCopyNodeInfo(ni *NodeInfo) NodeInfo {
	return NodeInfo{
		Name:                      ni.Name,
		MIGName:                   ni.MIGName,
		ReservationName:           ni.ReservationName,
		ReservationBlockID:        ni.ReservationBlockID,
		NodePoolName:              ni.NodePoolName,
		TopologyPartitionIds:      copyTopologyPartitionIdsMap(ni.TopologyPartitionIds),
		IsLinkedRunnerControlNode: ni.IsLinkedRunnerControlNode,
		AcceleratorType:           ni.AcceleratorType,
	}
}

// deepCopyMIGInfo creates a deep copy of a MIGInfo struct.
func deepCopyMIGInfo(mi *MIGInfo) MIGInfo {
	migCopy := MIGInfo{
		Name:  mi.Name,
		Nodes: make(map[string]*NodeInfo, len(mi.Nodes)),
	}

	// Also deep-copy the nodes map
	for nodeName, nodeInfo := range mi.Nodes {
		// When returning a copy, we must copy the NodeInfo structs themselves.
		nodeInfoCopy := deepCopyNodeInfo(nodeInfo)
		migCopy.Nodes[nodeName] = &nodeInfoCopy
	}
	return migCopy
}

// deepCopySliceInfo creates a deep copy of a SliceInfo struct.
func deepCopySliceInfo(si *SliceInfo) SliceInfo {
	sliceCopy := SliceInfo{
		Name:         si.Name,
		PartitionIDs: make([]string, len(si.PartitionIDs)),
		Topology:     si.Topology,
	}
	copy(sliceCopy.PartitionIDs, si.PartitionIDs)
	return sliceCopy
}
