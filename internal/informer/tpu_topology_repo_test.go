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
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var _ = Describe("TopologyRepo", func() {
	var (
		repo *TopologyRepo
	)

	BeforeEach(func() {
		logger := zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true))
		repo = NewTopologyRepo(logger)
	})

	It("should initialize correctly", func() {
		Expect(repo).NotTo(BeNil())
		Expect(repo.allTopologyNodes).NotTo(BeNil())
		Expect(repo.nodeToTopologyRoot).NotTo(BeNil())
	})

	It("should get or add a tree", func() {
		root := &TopologyNode{UID: "root1"}

		// First time, should add
		addedNode := repo.GetOrAddTree(root)
		Expect(addedNode).To(BeIdenticalTo(root))
		Expect(repo.allTopologyNodes).To(HaveKeyWithValue("root1", root))

		// Second time, should get the existing one
		sameNode := repo.GetOrAddTree(&TopologyNode{UID: "root1"})
		Expect(sameNode).To(BeIdenticalTo(root))
	})

	It("should retrieve a topology node by UID", func() {
		root := &TopologyNode{UID: "root1"}
		repo.GetOrAddTree(root)

		// Found
		node := repo.TopologyNode("root1")
		Expect(node).To(BeIdenticalTo(root))

		// Not found
		node = repo.TopologyNode("non-existent")
		Expect(node).To(BeNil())
	})

	It("should retrieve a root topology node by node name", func() {
		root := &TopologyNode{UID: "root1"}
		leaf := &TopologyNode{UID: "leaf1", NodeName: "k8s-node-1"}

		newNodes := map[string]*TopologyNode{
			"leaf1": leaf,
		}
		repo.Update(&newNodes, root)

		// Found
		foundRoot := repo.RootTNFromNode("k8s-node-1")
		Expect(foundRoot).To(BeIdenticalTo(root))

		// Not found
		foundRoot = repo.RootTNFromNode("non-existent-node")
		Expect(foundRoot).To(BeNil())
	})

	It("should update the repository with new nodes", func() {
		root := &TopologyNode{UID: "root1"}
		leaf1 := &TopologyNode{UID: "leaf1", NodeName: "k8s-node-1"}
		leaf2 := &TopologyNode{UID: "leaf2", NodeName: "k8s-node-2"}

		newNodes := map[string]*TopologyNode{
			"leaf1": leaf1,
			"leaf2": leaf2,
		}
		repo.Update(&newNodes, root)

		Expect(repo.allTopologyNodes).To(HaveKeyWithValue("leaf1", leaf1))
		Expect(repo.allTopologyNodes).To(HaveKeyWithValue("leaf2", leaf2))
		Expect(repo.nodeToTopologyRoot).To(HaveKeyWithValue("k8s-node-1", root))
		Expect(repo.nodeToTopologyRoot).To(HaveKeyWithValue("k8s-node-2", root))
	})

	It("should delete entries from the repository", func() {
		root := &TopologyNode{UID: "root1"}
		leaf1 := &TopologyNode{UID: "leaf1", NodeName: "k8s-node-1"}
		leaf2 := &TopologyNode{UID: "leaf2", NodeName: "k8s-node-2"}

		newNodes := map[string]*TopologyNode{
			"leaf1": leaf1,
			"leaf2": leaf2,
		}
		repo.Update(&newNodes, root)

		pruned := map[string]string{
			"leaf1": "k8s-node-1",
		}
		repo.DeleteFromRepo(pruned)

		Expect(repo.allTopologyNodes).NotTo(HaveKey("leaf1"))
		Expect(repo.allTopologyNodes).To(HaveKey("leaf2"))
		Expect(repo.nodeToTopologyRoot).NotTo(HaveKey("k8s-node-1"))
		Expect(repo.nodeToTopologyRoot).To(HaveKey("k8s-node-2"))
	})
})
