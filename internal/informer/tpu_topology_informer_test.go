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
	"context"
	"errors"
	"sync"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2" //lint:ignore ST1001 this is ginkgo's preferred way
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clientmocks "slice-controller/internal/mocks/client"
	queueMocks "slice-controller/internal/mocks/queue"
)

var _ = Describe("TpuTopologyInformer", func() {
	var (
		mockCtrl   *gomock.Controller
		mockClient *clientmocks.MockClient
		mockQueue  *queueMocks.MockStringRateLimitingQueue
		sut        *TpuTopologyInformer
		ctx        context.Context
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockClient = clientmocks.NewMockClient(mockCtrl)
		mockQueue = queueMocks.NewMockStringRateLimitingQueue(mockCtrl)
		logger := zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true))
		sut = NewTpuTopologyInformer(mockClient, logger, 1) // Add worker count
		sut.workqueue = mockQueue
		ctx = context.Background()
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	// Helper to create a test node
	makeNode := func(name, acceleratorType, blockID, subBlockID string, isReady, isClaimed bool, topologies map[string]string) *corev1.Node {
		labels := map[string]string{
			"tpus.gke.io/accelerator": acceleratorType,
			"tpus.gke.io/block":       blockID,
			"tpus.gke.io/sub-block":   subBlockID,
		}
		for k, v := range topologies {
			labels[k] = v
		}
		if isClaimed {
			labels["accelerator.gke.io/slice-id"] = "some-slice-claim"
		}

		status := corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		}
		if !isReady {
			status.Conditions[0].Status = corev1.ConditionFalse
		}

		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Status:     status,
		}
	}

	Context("Reconcile", func() {
		var req reconcile.Request
		var nodeName string

		BeforeEach(func() {
			nodeName = "test-node-1"
			req = reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeName}}
		})

		It("should enqueue the node's largest present topology label", func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						"tpus.gke.io/accelerator":       "tpu7x",
						"tpus.gke.io/topology-2x2x1-id":   "slice-2x2x1-A1",
						"tpus.gke.io/topology-2x2x2-id":   "slice-2x2x2-A",
						"tpus.gke.io/topology-4x4x4-id": "slice-4x4x4-A",
					},
				},
			}

			mockClient.EXPECT().Get(ctx, req.NamespacedName, gomock.Any()).DoAndReturn(
				func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					node.DeepCopyInto(obj.(*corev1.Node))
					return nil
				},
			)
			// Expect the largest topology to be enqueued
			mockQueue.EXPECT().Add("tpus.gke.io/topology-4x4x4-id=slice-4x4x4-A")

			_, err := sut.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should enqueue the correct topology on node deletion", func() {
			// Simulate that the node existed in a 2x2x4 topology before deletion
			rootNode := &TopologyNode{UID: "slice-2x2x4-A", Type: "tpu7x", Topology: "2x2x4", rootLock: &sync.RWMutex{}}
			sut.repo.mu.Lock()
			sut.repo.nodeToTopologyRoot[nodeName] = rootNode
			sut.repo.mu.Unlock()

			mockClient.EXPECT().Get(ctx, req.NamespacedName, gomock.Any()).Return(
				apierrors.NewNotFound(corev1.Resource("nodes"), nodeName),
			)
			// Expect the key to be constructed from the cached topology info
			mockQueue.EXPECT().Add("tpus.gke.io/topology-2x2x4-id=slice-2x2x4-A")

			_, err := sut.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("reconcileTopologyNode", func() {
		var queueKey string
		BeforeEach(func() {
			queueKey = "tpus.gke.io/topology-2x2x2-id=slice-2x2x2-A"
		})

		mockList := func(nodes ...*corev1.Node) {
			mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
					nodeList := list.(*corev1.NodeList)
					for _, n := range nodes {
						nodeList.Items = append(nodeList.Items, *n)
					}
					return nil
				},
			)
		}

		It("should create a new topology tree correctly", func() {
			node1 := makeNode("node-1", "tpu7x", "b", "sb", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A1",
				"tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A",
			})
			node2 := makeNode("node-2", "tpu7x", "b", "sb", true, true, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A2",
				"tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A",
			})
			mockList(node1, node2)

			err := sut.reconcileTopologyNode(ctx, queueKey)
			Expect(err).NotTo(HaveOccurred())

			root := sut.repo.TopologyNode("slice-2x2x2-A")
			Expect(root).NotTo(BeNil())
			Expect(root.AllocationState).To(Equal(StatePartiallyAllocated))

			child1 := sut.repo.TopologyNode("slice-2x2x1-A1")
			Expect(child1.AllocationState).To(Equal(StateAllocatable))

			child2 := sut.repo.TopologyNode("slice-2x2x1-A2")
			Expect(child2.AllocationState).To(Equal(StateAllocated))
		})

		It("should prune a missing leaf node", func() {
			node1 := makeNode("node-1", "tpu7x", "b", "sb", true, false, map[string]string{"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A1", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A"})
			node2 := makeNode("node-2", "tpu7x", "b", "sb", true, false, map[string]string{"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A2", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A"})
			mockList(node1, node2)
			Expect(sut.reconcileTopologyNode(ctx, queueKey)).To(Succeed())

			mockList(node1) // node-2 is gone
			Expect(sut.reconcileTopologyNode(ctx, queueKey)).To(Succeed())

			root := sut.repo.TopologyNode("slice-2x2x2-A")
			Expect(root).NotTo(BeNil())
			Expect(sut.repo.TopologyNode("slice-2x2x1-A2")).To(BeNil())
			Expect(root.Children).To(HaveLen(1))
			Expect(root.Children[0].UID).To(Equal("slice-2x2x1-A1"))
		})
	})

	Context("Allocate and Deallocate", func() {
		BeforeEach(func() {
			// Manually set synced for these tests, as we are not testing the initial
			// sync flow here.
			sut.hasSynced = true
			queueKey := "tpus.gke.io/topology-2x2x4-id=slice-2x2x4-A"
			node1 := makeNode("node-1", "tpu7x", "b", "sb", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A1", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
			})
			node2 := makeNode("node-2", "tpu7x", "b", "sb", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A2", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
			})
			node3 := makeNode("node-3", "tpu7x", "b", "sb", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-B1", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-B", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
			})
			node4 := makeNode("node-4", "tpu7x", "b", "sb", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-B2", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-B", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
			})

			mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
					nodeList := list.(*corev1.NodeList)
					nodeList.Items = []corev1.Node{*node1, *node2, *node3, *node4}
					return nil
				},
			)
			Expect(sut.reconcileTopologyNode(ctx, queueKey)).To(Succeed())
		})

		It("should allocate and deallocate correctly", func() {
			By("Allocating a smaller topology from a larger one")
			uids, err := sut.Allocate("tpu7x", "2x2x2")
			Expect(err).NotTo(HaveOccurred())
			Expect(uids).To(HaveLen(1))
			allocatedUID := uids[0]

			root := sut.repo.TopologyNode("slice-2x2x4-A")
			Expect(root.AllocationState).To(Equal(StatePartiallyAllocated))
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x2")]).To(HaveLen(1))

			By("Deallocating the smaller topology")
			err = sut.Deallocate(allocatedUID)
			Expect(err).NotTo(HaveOccurred())

			Expect(root.AllocationState).To(Equal(StateAllocatable))
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x4")]).To(ConsistOf(root))
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x2")]).To(BeEmpty())
		})
	})
	Context("Initial Sync", func() {
		It("should return an error if Allocate is called before sync is complete", func() {
			_, err := sut.Allocate("tpu7x", "2x2x1")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("informer has not synced yet"))
		})

		It("should return an error if Deallocate is called before sync is complete", func() {
			err := sut.Deallocate("some-uid")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("informer has not synced yet"))
		})

		It("should correctly perform the initial sync", func() {
			node1 := makeNode("node-1", "tpu7x", "b", "sb", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A1",
				"tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A",
			})
			node2 := makeNode("node-2", "tpu7x", "b", "sb", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A2",
				"tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A",
			})
			node3 := makeNode("node-3", "tpu7x", "c", "sc", true, false, map[string]string{
				"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-B1",
				"tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-B",
			})

			mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
					nodeList := list.(*corev1.NodeList)
					nodeList.Items = []corev1.Node{*node1, *node2, *node3}
					return nil
				},
			)

			// Expect two unique roots to be enqueued
			mockQueue.EXPECT().Add("tpus.gke.io/topology-2x2x2-id=slice-2x2x2-A")
			mockQueue.EXPECT().Add("tpus.gke.io/topology-2x2x2-id=slice-2x2x2-B")

			sut.performInitialSync(ctx)

			Expect(sut.initialSyncKeys).To(HaveKey("tpus.gke.io/topology-2x2x2-id=slice-2x2x2-A"))
			Expect(sut.initialSyncKeys).To(HaveKey("tpus.gke.io/topology-2x2x2-id=slice-2x2x2-B"))
			Expect(sut.HasSynced()).To(BeFalse())
		})

		It("should mark as synced immediately if no nodes are found", func() {
			mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
					// No nodes
					return nil
				},
			)

			sut.performInitialSync(ctx)
			Expect(sut.HasSynced()).To(BeTrue())
		})

		It("should complete the sync after processing all initial keys", func() {
			sut.initialSyncKeys = map[string]struct{}{
				"key-1": {},
				"key-2": {},
			}
			Expect(sut.HasSynced()).To(BeFalse())

			sut.checkInitialSyncComplete("key-1")
			Expect(sut.HasSynced()).To(BeFalse())

			sut.checkInitialSyncComplete("key-2")
			Expect(sut.HasSynced()).To(BeTrue())

			// Check that it doesn't try to re-sync
			sut.checkInitialSyncComplete("key-3")
			Expect(sut.HasSynced()).To(BeTrue())
		})
	})

	Context("when handling high concurrency", func() {
		BeforeEach(func() {
			sut.hasSynced = true
			// Set up a large, fully healthy and allocatable 2x2x4 topology
			queueKey := "tpus.gke.io/topology-2x2x4-id=slice-2x2x4-A"
			nodes := []*corev1.Node{
				makeNode("node-1", "tpu7x", "b", "sb", true, false, map[string]string{
					"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A1", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
				}),
				makeNode("node-2", "tpu7x", "b", "sb", true, false, map[string]string{
					"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A2", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
				}),
				makeNode("node-3", "tpu7x", "b", "sb", true, false, map[string]string{
					"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-B1", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-B", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
				}),
				makeNode("node-4", "tpu7x", "b", "sb", true, false, map[string]string{
					"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-B2", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-B", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
				}),
			}
			mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
					nodeList := list.(*corev1.NodeList)
					nodeList.Items = []corev1.Node{*nodes[0], *nodes[1], *nodes[2], *nodes[3]}
					return nil
				},
			)
			Expect(sut.reconcileTopologyNode(ctx, queueKey)).To(Succeed())
		})

		It("should handle multiple concurrent allocations without deadlocks or races", func() {
			// Attempt to allocate two 2x2x2 slices concurrently from the single 2x2x4 slice.
			// Both should succeed. A third attempt should fail.
			var wg sync.WaitGroup
			wg.Add(3)

			errChan := make(chan error, 3)

			// Goroutine 1: Allocate a 2x2x2
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_, err := sut.Allocate("tpu7x", "2x2x2")
				errChan <- err
			}()

			// Goroutine 2: Allocate another 2x2x2
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_, err := sut.Allocate("tpu7x", "2x2x2")
				errChan <- err
			}()

			// Goroutine 3: Allocate a third 2x2x2 (this one should fail)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_, err := sut.Allocate("tpu7x", "2x2x2")
				errChan <- err
			}()

			wg.Wait()
			close(errChan)

			// We don't assert the exact number of successes and failures, as it can
			// vary depending on the timing of the goroutines. Instead, we assert
			// on the final state of the system.
			root := sut.repo.TopologyNode("slice-2x2x4-A")
			Expect(root.AllocationState).To(Equal(StateAllocated))
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x4")]).To(BeEmpty())
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x2")]).To(BeEmpty())
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x1")]).To(BeEmpty())
		})

		It("should handle a mix of concurrent allocations and deallocations", func() {
			// This test simulates a more chaotic environment with concurrent allocations
			// of different sizes and deallocations.
			var wg sync.WaitGroup
			const numOps = 10 // Number of concurrent operations
			wg.Add(numOps)

			// Perform a series of concurrent operations
			for i := 0; i < numOps; i++ {
				go func(i int) {
					defer GinkgoRecover()
					defer wg.Done()

					if i%3 == 0 { // Every 3rd operation is a 2x2x2 allocation
						uids, err := sut.Allocate("tpu7x", "2x2x2")
						if err == nil {
							// Immediately deallocate to create churn
							Expect(sut.Deallocate(uids[0])).To(Succeed())
						}
					} else { // Other operations are 2x2x1 allocations
						uids, err := sut.Allocate("tpu7x", "2x2x1")
						if err == nil {
							// Immediately deallocate to create churn
							Expect(sut.Deallocate(uids[0])).To(Succeed())
						}
					}
				}(i)
			}

			wg.Wait()

			// After all the churn, the system should return to its initial state
			// because every successful allocation was followed by a deallocation.
			root := sut.repo.TopologyNode("slice-2x2x4-A")
			Expect(root.AllocationState).To(Equal(StateAllocatable))
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x4")]).To(HaveLen(1))
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x2")]).To(BeEmpty())
			Expect(sut.allocator.allocatableTopologyNodeMap[TPUType("tpu7x")][TPUTopology("2x2x1")]).To(BeEmpty())
		})

		It("should handle concurrent allocation and reconciliation safely", func() {
			var wg sync.WaitGroup
			wg.Add(2)

			// Goroutine 1: Tries to allocate a 2x2x2 slice. This will likely be blocked
			// by the reconciler's tree lock.
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				// This allocation might succeed or fail depending on timing, which is okay.
				// The key is that it doesn't panic or deadlock.
				_, _ = sut.Allocate("tpu7x", "2x2x2")
			}()

			// Goroutine 2: A reconciliation runs, finds one node has been deleted,
			// and prunes the tree.
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				// Simulate node-4 being deleted
				node1 := makeNode("node-1", "tpu7x", "b", "sb", true, false, map[string]string{
					"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A1", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
				})
				node2 := makeNode("node-2", "tpu7x", "b", "sb", true, false, map[string]string{
					"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-A2", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-A", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
				})
				node3 := makeNode("node-3", "tpu7x", "b", "sb", true, false, map[string]string{
					"tpus.gke.io/topology-2x2x1-id": "slice-2x2x1-B1", "tpus.gke.io/topology-2x2x2-id": "slice-2x2x2-B", "tpus.gke.io/topology-2x2x4-id": "slice-2x2x4-A",
				})

				// Mock the client to return the reduced set of nodes.
				// Use AnyTimes() because the order of execution isn't guaranteed.
				mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
					func(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
						nodeList := list.(*corev1.NodeList)
						nodeList.Items = []corev1.Node{*node1, *node2, *node3}
						return nil
					},
				)
				Expect(sut.reconcileTopologyNode(ctx, "tpus.gke.io/topology-2x2x4-id=slice-2x2x4-A")).To(Succeed())
			}()

			wg.Wait()

			// Assertions on the final state
			root := sut.repo.TopologyNode("slice-2x2x4-A")
			Expect(root).NotTo(BeNil())
			// The tree should be incomplete because of the pruning
			Expect(root.IsReady).To(BeFalse())
			// The node that was "deleted" should be gone from the repo
			Expect(sut.repo.TopologyNode("slice-2x2x1-B2")).To(BeNil())
			// The parent of the deleted node should now have only one child
			tn2x2x2B := sut.repo.TopologyNode("slice-2x2x2-B")
			Expect(tn2x2x2B).NotTo(BeNil())
			Expect(tn2x2x2B.Children).To(HaveLen(1))
		})
	})

	Context("processTopologyUpdates worker", func() {
		It("should call Done and AddRateLimited after a failed reconcile", func() {
			key := "some-key"
			testErr := errors.New("reconcile failed")
			gomock.InOrder(
				mockQueue.EXPECT().Get().Return(key, false),
				mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).Return(testErr),
				mockQueue.EXPECT().AddRateLimited(key),
				mockQueue.EXPECT().Done(key),
				mockQueue.EXPECT().Get().Return("", true),
			)
			sut.processTopologyUpdates(ctx)
		})
	})
})
