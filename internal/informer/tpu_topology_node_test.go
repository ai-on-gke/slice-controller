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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TopologyNode Helpers", func() {
	const (
		sliceA = "slice-a"
		sliceB = "slice-b"
	)

	Context("updateTreeHealth", func() {
		It("should correctly aggregate health up the tree", func() {
			gc1 := &TopologyNode{UID: "gc1", IsReady: true}
			gc2 := &TopologyNode{UID: "gc2", IsReady: false} // Unhealthy grandchild
			c1 := &TopologyNode{UID: "c1", Children: []*TopologyNode{gc1, gc2}}
			c2 := &TopologyNode{UID: "c2", Children: []*TopologyNode{}} // Leaf node, IsReady is initially false
			c2.IsReady = true                                           // Set leaf to ready
			root := &TopologyNode{UID: "root", Children: []*TopologyNode{c1, c2}}

			updateTreeHealth(root)

			Expect(c1.IsReady).To(BeFalse())   // Because gc2 is not ready
			Expect(c2.IsReady).To(BeTrue())    // Unaffected
			Expect(root.IsReady).To(BeFalse()) // Because c1 is not ready
		})
	})

	Context("updateAllocationState", func() {
		var (
			child1, child2, parent *TopologyNode
		)

		BeforeEach(func() {
			child1 = &TopologyNode{UID: "c1"}
			child2 = &TopologyNode{UID: "c2"}
			parent = &TopologyNode{UID: "p1", Children: []*TopologyNode{child1, child2}}
		})

		It("should set parent to Allocatable if both children are Allocatable", func() {
			child1.AllocationState = StateAllocatable
			child2.AllocationState = StateAllocatable
			updateAllocationState(parent)
			Expect(parent.AllocationState).To(Equal(StateAllocatable))
		})

		It("should set parent to Allocated if both children are Allocated", func() {
			child1.AllocationState = StateAllocated
			child2.AllocationState = StateAllocated
			updateAllocationState(parent)
			Expect(parent.AllocationState).To(Equal(StateAllocated))
		})

		It("should set parent to PartiallyAllocated if one child is Allocatable and one is Allocated", func() {
			child1.AllocationState = StateAllocatable
			child2.AllocationState = StateAllocated
			updateAllocationState(parent)
			Expect(parent.AllocationState).To(Equal(StatePartiallyAllocated))
		})

		It("should propagate ClaimBySliceID if both children have the same one", func() {
			child1.AllocationState = StateAllocated
			child1.ClaimBySliceID = sliceA
			child2.AllocationState = StateAllocated
			child2.ClaimBySliceID = sliceA
			updateAllocationState(parent)
			Expect(parent.AllocationState).To(Equal(StateAllocated))
			Expect(parent.ClaimBySliceID).To(Equal(sliceA))
		})

		It("should not propagate ClaimBySliceID if children have different ones", func() {
			child1.AllocationState = StateAllocated
			child1.ClaimBySliceID = sliceA
			child2.AllocationState = StateAllocated
			child2.ClaimBySliceID = sliceB
			updateAllocationState(parent)
			Expect(parent.AllocationState).To(Equal(StateAllocated))
			Expect(parent.ClaimBySliceID).To(BeEmpty())
		})
	})

	Context("pruneTree", func() {
		It("should identify and collect UIDs of missing leaf nodes and their childless parents", func() {
			// Setup a tree: root -> (c1, c2), c1 -> (gc1, gc2), c2 -> (gc3)
			gc1 := &TopologyNode{UID: "gc1"}
			gc2 := &TopologyNode{UID: "gc2"}
			gc3 := &TopologyNode{UID: "gc3"}
			c1 := &TopologyNode{UID: "c1", Children: []*TopologyNode{gc1, gc2}}
			c2 := &TopologyNode{UID: "c2", Children: []*TopologyNode{gc3}}
			root := &TopologyNode{UID: "root", Children: []*TopologyNode{c1, c2}}
			gc1.Parent = c1
			gc2.Parent = c1
			gc3.Parent = c2
			c1.Parent = root
			c2.Parent = root

			// Simulate that gc2 and gc3 are missing
			visited := map[string]struct{}{
				"gc1": {},
			}

			prunedUIDs := pruneTree(root, visited)

			// Expect gc2, gc3, and their now-childless parent c2 to be pruned
			Expect(prunedUIDs).To(HaveLen(3))
			Expect(prunedUIDs).To(HaveKeyWithValue("gc2", ""))
			Expect(prunedUIDs).To(HaveKeyWithValue("gc3", ""))
			Expect(prunedUIDs).To(HaveKeyWithValue("c2", ""))

			// Check that the tree structure was modified correctly
			Expect(c1.Children).To(HaveLen(1))
			Expect(c1.Children[0].UID).To(Equal("gc1"))
			Expect(root.Children).To(HaveLen(1))
			Expect(root.Children[0].UID).To(Equal("c1"))
		})
	})
})
