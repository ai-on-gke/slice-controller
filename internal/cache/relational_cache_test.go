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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// newTestCache creates a new cache with a null logger for testing.
func newTestCache() *RelationalCache {
	cache := NewRelationalCache()
	cache.logger = zap.New(zap.UseDevMode(true), zap.WriteTo(new(nullWriter))) // Suppress logs
	return cache
}

// nullWriter is used to discard logs during testing.
type nullWriter struct{}

func (w *nullWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func TestSliceOperations(t *testing.T) {
	cache := newTestCache()
	partitions := []string{"p1", "p2"}

	// Test Add
	err := cache.AddSlice("slice-a", partitions, "4x4x4")
	require.NoError(t, err)

	// Test Get
	slice, ok := cache.GetSliceInfo("slice-a")
	require.True(t, ok)
	assert.Equal(t, "slice-a", slice.Name)
	assert.Equal(t, partitions, slice.PartitionIDs)

	// Test modifying copy (should not affect original)
	slice.PartitionIDs[0] = "p-mod"
	slice, ok = cache.GetSliceInfo("slice-a")
	require.True(t, ok)
	assert.Equal(t, partitions, slice.PartitionIDs, "GetSliceInfo must return a deep copy")

	// Test Get All
	names := cache.GetAllSliceNames()
	assert.Equal(t, []string{"slice-a"}, names)

	// Test GetSliceNameByPartitionID
	sn, found := cache.GetSliceNameByPartitionID("p1")
	require.True(t, found)
	assert.Equal(t, "slice-a", sn)

	sn, found = cache.GetSliceNameByPartitionID("p2")
	require.True(t, found)
	assert.Equal(t, "slice-a", sn)

	_, found = cache.GetSliceNameByPartitionID("non-existent")
	assert.False(t, found)

	// Test Add Update (was Idempotency)
	err = cache.AddSlice("slice-a", []string{"p3"}, "4x4x4")
	require.NoError(t, err)
	slice, ok = cache.GetSliceInfo("slice-a")
	require.True(t, ok)
	assert.Equal(t, []string{"p3"}, slice.PartitionIDs, "AddSlice should update partitions")

	// Test Delete
	err = cache.DeleteSlice("slice-a")
	require.NoError(t, err)

	// Test Get after delete
	_, ok = cache.GetSliceInfo("slice-a")
	assert.False(t, ok)

	// Test GetSliceNameByPartitionID after delete
	_, found = cache.GetSliceNameByPartitionID("p1")
	assert.False(t, found)

	// Test Get All after delete
	names = cache.GetAllSliceNames()
	assert.Empty(t, names)

	// Test Delete Idempotency
	err = cache.DeleteSlice("slice-a")
	require.NoError(t, err)
}

func TestAddSliceUpdate(t *testing.T) {
	cache := newTestCache()
	partitions := []string{"p1", "p2"}

	// 1. Setup initial mapping
	require.NoError(t, cache.AddSlice("slice-a", partitions, "4x4x4"))

	sn, found := cache.GetSliceNameByPartitionID("p1")
	require.True(t, found)
	assert.Equal(t, "slice-a", sn)

	// 2. Update slice-a with new partitions (should clean up p1, p2)
	require.NoError(t, cache.AddSlice("slice-a", []string{"p3"}, "4x4x4"))

	// Verify mapping for p1 is gone
	_, found = cache.GetSliceNameByPartitionID("p1")
	assert.False(t, found)

	// Verify mapping points to slice-a for p3
	sn, found = cache.GetSliceNameByPartitionID("p3")
	require.True(t, found)
	assert.Equal(t, "slice-a", sn)
}

func TestAddSlicePartialUpdate(t *testing.T) {
	cache := newTestCache()
	partitions := []string{"p1", "p2"}

	// 1. Setup initial mapping
	require.NoError(t, cache.AddSlice("slice-a", partitions, "4x4x4"))

	// 2. Update slice-a with partial new partitions (keep p1, add p3, drop p2)
	require.NoError(t, cache.AddSlice("slice-a", []string{"p1", "p3"}, "4x4x4"))

	// Verify mapping for p1 still exists
	sn, found := cache.GetSliceNameByPartitionID("p1")
	require.True(t, found)
	assert.Equal(t, "slice-a", sn)

	// Verify mapping for p2 is gone
	_, found = cache.GetSliceNameByPartitionID("p2")
	assert.False(t, found)

	// Verify mapping points to slice-a for p3
	sn, found = cache.GetSliceNameByPartitionID("p3")
	require.True(t, found)
	assert.Equal(t, "slice-a", sn)
}

func TestSliceConflictResolution(t *testing.T) {
	cache := newTestCache()

	// 1. Add SliceA with partition p1
	require.NoError(t, cache.AddSlice("slice-a", []string{"p1"}, "4x4x4"))

	// 2. Add SliceB with partition p1 (conflict)
	require.NoError(t, cache.AddSlice("slice-b", []string{"p1"}, "4x4x4"))

	// Verify mapping points to slice-b for p1
	sn, found := cache.GetSliceNameByPartitionID("p1")
	require.True(t, found)
	assert.Equal(t, "slice-b", sn)

	// 3. Update slice-a to use p2 instead (should NOT delete p1 mapping for slice-b)
	require.NoError(t, cache.AddSlice("slice-a", []string{"p2"}, "4x4x4"))

	// Verify mapping for p1 STILL points to slice-b
	sn, found = cache.GetSliceNameByPartitionID("p1")
	require.True(t, found)
	assert.Equal(t, "slice-b", sn)

	// 4. Delete slice-a (should NOT delete p1 mapping for slice-b)
	require.NoError(t, cache.DeleteSlice("slice-a"))

	// Verify mapping for p1 STILL points to slice-b
	sn, found = cache.GetSliceNameByPartitionID("p1")
	require.True(t, found)
	assert.Equal(t, "slice-b", sn)
}

func TestNodeAndMIGOperations(t *testing.T) {
	cache := newTestCache()
	pids := map[string][]string{"topo-a": {"p1"}}

	// Test Add Node (should create MIG)
	err := cache.UpdateNodeInfo("mig-a", "node-1", "resv-1", "block-1", "test-nodepool", pids, false, "v4")
	require.NoError(t, err)

	// Test Get Node
	node, ok := cache.GetNodeInfo("node-1")
	require.True(t, ok)
	assert.Equal(t, "node-1", node.Name)
	assert.Equal(t, "mig-a", node.MIGName)
	assert.Equal(t, pids, node.TopologyPartitionIds)

	// Test Get MIG
	mig, ok := cache.GetMIG("mig-a")
	require.True(t, ok)
	assert.Equal(t, "mig-a", mig.Name)
	require.Contains(t, mig.Nodes, "node-1")
	assert.Equal(t, "node-1", mig.Nodes["node-1"].Name)

	// Test Get All Migs
	migNames := cache.GetAllMIGNames()
	assert.Equal(t, []string{"mig-a"}, migNames)

	// Test Get All Nodes from MIG
	nodeNames, err := cache.GetAllNodesFromMIG("mig-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"node-1"}, nodeNames)

	// Test Indexes
	mig, ok = cache.GetMIGByPartitionID("p1")
	require.True(t, ok)
	assert.Equal(t, "mig-a", mig.Name)

	nodes := cache.GetAllNodesFromPartitionID("p1")
	assert.Equal(t, []string{"node-1"}, nodes)

	// Test Delete Node
	_, err = cache.DeleteNodeAndCleanUpMIG("node-1")
	require.NoError(t, err)

	// Test Get Node after delete
	_, ok = cache.GetNodeInfo("node-1")
	assert.False(t, ok)

	// Test MIG still exists
	mig, ok = cache.GetMIG("mig-a")
	require.False(t, ok)
	assert.Empty(t, mig.Nodes, "MIG should be cleaned up after last node deletion")

	// Test Indexes are cleaned up
	_, ok = cache.GetMIGByPartitionID("p1")
	assert.False(t, ok, "Index should be cleaned up")
	nodes = cache.GetAllNodesFromPartitionID("p1")
	assert.Empty(t, nodes, "Index should be cleaned up")

	// Test Delete Idempotency
	_, err = cache.DeleteNodeAndCleanUpMIG("node-1")
	require.NoError(t, err)
}

func TestIndexCleanupWithSharedPartitions(t *testing.T) {
	cache := newTestCache()
	pids1 := map[string][]string{"topo-a": {"p1"}, "topo-b": {"p-shared"}}
	pids2 := map[string][]string{"topo-c": {"p3"}, "topo-b": {"p-shared"}}

	// Add two nodes to the same MIG sharing a partition
	require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-1", "resv-1", "block-1", "nodepool", pids1, false, "v4"))
	require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-2", "resv-", "block-2", "nodepool", pids2, false, "v4"))

	// Check indexes
	_, ok := cache.GetMIGByPartitionID("p1")
	assert.True(t, ok, "p1 index should exist")
	_, ok = cache.GetMIGByPartitionID("p3")
	assert.True(t, ok, "p3 index should exist")
	_, ok = cache.GetMIGByPartitionID("p-shared")
	assert.True(t, ok, "p-shared index should exist")

	// Delete node-1
	_, err := cache.DeleteNodeAndCleanUpMIG("node-1")
	require.NoError(t, err)

	// Check indexes after deleting node-1
	_, ok = cache.GetMIGByPartitionID("p1")
	assert.False(t, ok, "p1 index should be gone")
	_, ok = cache.GetMIGByPartitionID("p3")
	assert.True(t, ok, "p3 index should still exist")
	_, ok = cache.GetMIGByPartitionID("p-shared")
	assert.True(t, ok, "p-shared index should still exist (used by node-2)")

	// Delete node-2
	_, err = cache.DeleteNodeAndCleanUpMIG("node-2")
	require.NoError(t, err)

	// Check indexes after deleting node-2
	_, ok = cache.GetMIGByPartitionID("p3")
	assert.False(t, ok, "p3 index should be gone")
	_, ok = cache.GetMIGByPartitionID("p-shared")
	assert.False(t, ok, "p-shared index should be gone")
}

func TestUpdateNodeInfoLogic(t *testing.T) {
	t.Run("Node update in same MIG", func(t *testing.T) {
		cache := newTestCache()
		pidsOld := map[string][]string{"topo-a": {"p-old"}, "topo-b": {"p-shared"}}
		pidsNew := map[string][]string{"topo-c": {"p-new"}, "topo-b": {"p-shared"}}

		require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-1", "resv-1", "block-1", "test-nodepool", pidsOld, false, "v4"))
		require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-shared", "resv-1", "block-1", "test-nodepool", map[string][]string{"s": {"p-old"}}, false, "v4")) // Keep p-old alive

		// Check pre-update state
		_, ok := cache.GetMIGByPartitionID("p-old")
		assert.True(t, ok)
		_, ok = cache.GetMIGByPartitionID("p-shared")
		assert.True(t, ok)
		_, ok = cache.GetMIGByPartitionID("p-new")
		assert.False(t, ok)

		// Perform the update
		require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-1", "resv-1", "block-1", "test-nodepool", pidsNew, false, "v4"))

		// Check post-update state
		node, ok := cache.GetNodeInfo("node-1")
		require.True(t, ok)
		assert.Equal(t, pidsNew, node.TopologyPartitionIds)

		// Check indexes
		_, ok = cache.GetMIGByPartitionID("p-old")
		assert.True(t, ok, "p-old index should still exist (used by node-shared)")
		_, ok = cache.GetMIGByPartitionID("p-shared")
		assert.True(t, ok, "p-shared index should still exist")
		_, ok = cache.GetMIGByPartitionID("p-new")
		assert.True(t, ok, "p-new index should be created")
	})

	t.Run("Node moves to new MIG", func(t *testing.T) {
		cache := newTestCache()
		pids1 := map[string][]string{"topo-a": {"p1"}}
		pids2 := map[string][]string{"topo-b": {"p2"}}

		require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-1", "resv-1", "block-1", "test-nodepool", pids1, false, "v4"))
		require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-2", "resv-1", "block-1", "test-nodepool", pids1, false, "v4")) // Share p1

		// Move node-1 to mig-b
		require.NoError(t, cache.UpdateNodeInfo("mig-b", "node-1", "resv-1", "block-2", "test-nodepool", pids2, false, "v4"))
		// Check mig-a
		migA, ok := cache.GetMIG("mig-a")
		require.True(t, ok)
		assert.NotContains(t, migA.Nodes, "node-1")
		assert.Contains(t, migA.Nodes, "node-2")

		// Check mig-b
		migB, ok := cache.GetMIG("mig-b")
		require.True(t, ok)
		assert.Contains(t, migB.Nodes, "node-1")
		assert.NotContains(t, migB.Nodes, "node-2")

		// Check node-1 info
		node1, ok := cache.GetNodeInfo("node-1")
		require.True(t, ok)
		assert.Equal(t, "mig-b", node1.MIGName)
		assert.Equal(t, pids2, node1.TopologyPartitionIds)

		// Check indexes
		mig, ok := cache.GetMIGByPartitionID("p1")
		require.True(t, ok, "p1 should still exist")
		assert.Equal(t, "mig-a", mig.Name, "p1 should point to mig-a (from node-2)")

		mig, ok = cache.GetMIGByPartitionID("p2")
		require.True(t, ok, "p2 should be created")
		assert.Equal(t, "mig-b", mig.Name, "p2 should point to mig-b")
	})
}

func TestDeleteNodeAndCleanUpMIG(t *testing.T) {
	cache := newTestCache()
	pids1 := map[string][]string{"topo-a": {"p1"}}
	pids2 := map[string][]string{"topo-b": {"p2"}}

	require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-1", "resv-1", "block-1", "test-nodepool", pids1, false, "v4"))
	require.NoError(t, cache.UpdateNodeInfo("mig-a", "node-2", "resv-1", "block-1", "test-nodepool", pids2, false, "v4"))

	// Delete node-1, MIG should remain
	cleaned, err := cache.DeleteNodeAndCleanUpMIG("node-1")
	require.NoError(t, err)
	assert.False(t, cleaned, "MIG should not be cleaned up")

	_, ok := cache.GetMIG("mig-a")
	assert.True(t, ok, "MIG should still exist")
	_, ok = cache.GetMIGByPartitionID("p1")
	assert.False(t, ok, "p1 index should be gone")
	_, ok = cache.GetMIGByPartitionID("p2")
	assert.True(t, ok, "p2 index should still exist")

	// Delete node-2, MIG should be cleaned up
	cleaned, err = cache.DeleteNodeAndCleanUpMIG("node-2")
	require.NoError(t, err)
	assert.True(t, cleaned, "MIG should be cleaned up")

	_, ok = cache.GetMIG("mig-a")
	assert.False(t, ok, "MIG should be gone")
	_, ok = cache.GetMIGByPartitionID("p2")
	assert.False(t, ok, "p2 index should be gone")
}

// TestConcurrencyRaces hammers the cache from multiple goroutines.
// Run this with `go test -race -v ./...` to detect race conditions.
func TestConcurrencyRaces(t *testing.T) {
	cache := newTestCache()

	// Pre-populate with some slices and MIGs
	require.NoError(t, cache.AddSlice("slice-1", []string{"s1"}, "4x4x4"))
	require.NoError(t, cache.UpdateNodeInfo("mig-1", "node-1", "resv-1", "block-1", "nodepool", map[string][]string{"t": {"p1"}}, false, "v4"))

	// This test hammers the cache with concurrent reads and writes
	// to both the slice map and the mig/node maps.
	// The separate locks should prevent races and deadlocks.
	t.Run("Mixed R/W", func(t *testing.T) {
		t.Parallel()
		var wg sync.WaitGroup
		numGoroutines := 100
		numOpsPerGoroutine := 50

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOpsPerGoroutine; j++ {
					op := (id + j) % 10 // Distribute operations

					switch op {
					case 0:
						// Write Slice
						_ = cache.AddSlice(fmt.Sprintf("s-%d-%d", id, j), []string{fmt.Sprintf("p-%d", j)}, "4x4x4")
					case 1:
						// Read Slice
						_, _ = cache.GetSliceInfo("slice-1")
					case 2:
						// Read All Slices
						_ = cache.GetAllSliceNames()
					case 3:
						// Delete Slice
						_ = cache.DeleteSlice(fmt.Sprintf("s-%d-%d", id, j-1))
					case 4:
						// Write Node
						pids := map[string][]string{"t": {fmt.Sprintf("p-%d-%d", id, j)}}
						_ = cache.UpdateNodeInfo(fmt.Sprintf("m-%d", id), fmt.Sprintf("n-%d-%d", id, j), "resv-1", "block-1", "test-nodepool", pids, false, "v4")
					case 5:
						// Read Node
						_, _ = cache.GetNodeInfo("node-1")
					case 6:
						// Read MIG
						_, _ = cache.GetMIG("mig-1")
					case 7:
						// Read Index
						_, _ = cache.GetMIGByPartitionID("p1")
					case 8:
						// Read All MIGs
						_ = cache.GetAllMIGNames()
					case 9:
						// Delete Node
						_, _ = cache.DeleteNodeAndCleanUpMIG(fmt.Sprintf("n-%d-%d", id, j-1))
					}
				}
			}(i)
		}
		wg.Wait()
	})
}

func TestDeleteSlice_ShouldNotDeleteIfPartitionAssignedToOtherSlice(t *testing.T) {
	cache := newTestCache()

	// 1. Add slice-a with p1
	err := cache.AddSlice("slice-a", []string{"p1"}, "4x4x4")
	require.NoError(t, err)

	// 2. Add slice-b with p1 (conflict, slice-b takes over)
	err = cache.AddSlice("slice-b", []string{"p1"}, "4x4x4")
	require.NoError(t, err)

	// Verify state: p1 points to slice-b
	sn, found := cache.GetSliceNameByPartitionID("p1")
	require.True(t, found)
	assert.Equal(t, "slice-b", sn)

	// 3. Delete slice-a. It should NOT delete p1 from partitionIDToSliceMap because it belongs to slice-b.
	err = cache.DeleteSlice("slice-a")
	require.NoError(t, err)

	// 4. Verify partitionIDToSliceMap["p1"] still exists and points to "slice-b"
	sn, found = cache.GetSliceNameByPartitionID("p1")
	assert.True(t, found, "p1 should still be in partitionIDToSliceMap")
	assert.Equal(t, "slice-b", sn, "p1 should still point to slice-b")
}
