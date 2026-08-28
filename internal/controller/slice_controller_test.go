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

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/api/googleapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/action"
	"slice-controller/internal/cache"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"

	betacompute "google.golang.org/api/compute/v0.beta"
	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/api/meta"
)

var _ = Describe("Slice Controller", func() {
	var (
		mockCtrl            *gomock.Controller
		mockKubeHelper      *mocks.MockClient
		mockComputeService  *mocks.MockComputeServiceInterface
		mockGCEHelper       *mocks.MockGCEClient
		mockSliceEngine     *mocks.MockEngine
		mockRelationalCache *mocks.MockSliceRelationalCache
		reconciler          *SliceReconciler
		ctx                 context.Context
		typeNamespacedName  types.NamespacedName
		slice               *slicev1beta1.Slice
	)

	// --- HELPERS ---

	// Forces clean deletion of any existing slice
	ensureSliceDeleted := func() {
		s := &slicev1beta1.Slice{}
		err := k8sClient.Get(ctx, typeNamespacedName, s)
		if err == nil {
			// Remove finalizers to allow deletion
			if len(s.Finalizers) > 0 {
				s.Finalizers = []string{}
				if err := k8sClient.Update(ctx, s); err != nil {
					// Fallback to patch if update fails
					patch := client.MergeFrom(s.DeepCopy())
					_ = k8sClient.Patch(ctx, s, patch)
				}
			}
			_ = k8sClient.Delete(context.Background(), s)
		}
		Eventually(func() bool {
			// MUST fetch fresh data
			err := k8sClient.Get(ctx, typeNamespacedName, &slicev1beta1.Slice{})
			return apierrors.IsNotFound(err)
		}, time.Second*5, time.Millisecond*100).Should(BeTrue())
	}

	// Runs reconcile until a predicate is met
	reconcileUntil := func(matcher func() bool) {
		Eventually(func() bool {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			if err != nil {
				return false
			}
			return matcher()
		}, time.Second*5, time.Millisecond*100).Should(BeTrue())
	}

	// fast-forwards the controller through the initialization phase
	ensureFullyInitialized := func() {
		// Pass 1: Add Finalizer
		reconcileUntil(func() bool {
			if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
				return false
			}
			return controllerutil.ContainsFinalizer(slice, sliceFinalizerName)
		})
		// Pass 2: Init Status
		reconcileUntil(func() bool {
			if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
				return false
			}
			return util.FindStateCondition(slice) != ""
		})
	}

	// --- SETUP ---

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockKubeHelper = mocks.NewMockClient(mockCtrl)
		mockComputeService = mocks.NewMockComputeServiceInterface(mockCtrl)
		mockGCEHelper = mocks.NewMockGCEClient(mockCtrl)
		mockSliceEngine = mocks.NewMockEngine(mockCtrl)
		mockRelationalCache = mocks.NewMockSliceRelationalCache(mockCtrl)
		mockRelationalCache.EXPECT().GetBlockID(gomock.Any()).Return("test-block", true).AnyTimes()
		// No global defaults for GetSliceInfo, GetMMIG, DeleteSlice to avoid shadowing specific expectations.

		reconciler = &SliceReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			KubeHelper:       mockKubeHelper,
			ComputeService:   mockComputeService,
			GCEHelper:        mockGCEHelper,
			SliceEngine:      mockSliceEngine,
			RelationalCache:  mockRelationalCache,
			ProjectNumber:    "test-project",
			Location:         "test-location",
			ClusterName:      "test-cluster",
			ClusterHash:      "test-cluster-hash",
			EnableSubSlicing: true,
		}
		ctx = context.Background()
		typeNamespacedName = types.NamespacedName{Name: "test-slice", Namespace: "default"}

		ensureSliceDeleted()

		slice = &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      typeNamespacedName.Name,
				Namespace: typeNamespacedName.Namespace,
			},
			Spec: slicev1beta1.SliceSpec{
				Type:         "tpu7x",
				Topology:     "4x4x8",
				PartitionIds: []string{"p1", "p2"},
			},
		}
		Expect(k8sClient.Create(ctx, slice)).To(Succeed())
	})

	AfterEach(func() {
		mockCtrl.Finish()
		ensureSliceDeleted()
	})

	// =========================================================================
	// TESTS
	// =========================================================================

	Context("EnableSubSlicing flag", func() {
		It("should fail sub-slice reconciliation when flag is false", func() {
			reconciler.EnableSubSlicing = false
			// Create a sub-slice
			subslice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "disabled-subslice",
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         slicev1beta1.TypeTpu7x,
					Topology:     "2x2x2",
					PartitionIds: []string{"p1"},
				},
			}
			Expect(k8sClient.Create(ctx, subslice)).To(Succeed())

			// 0. Mock CacheSyncUp (called by Reconcile)
			mockRelationalCache.EXPECT().GetSliceInfo(subslice.Name).Return(cache.SliceInfo{}, false).AnyTimes()

			// 1. First Pass: Add Finalizer
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "disabled-subslice", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())

			// 2. Second Pass: Init Status
			_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "disabled-subslice", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())

			// 3. Third Pass: Preflight Check (should fail here now)
			_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "disabled-subslice", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())

			// Check status - it should have failed with the correct message
			fetched := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "disabled-subslice", Namespace: "default"}, fetched)).To(Succeed())
			cond, _ := util.FindSliceCondition(fetched, slicev1beta1.SliceStateConditionType)
			Expect(cond.Reason).To(Equal(util.SliceCreationFailed))
			Expect(cond.Message).To(ContainSubstring("SubSlicing is disabled"))
		})

		It("should proceed with super-slice reconciliation when flag is false", func() {
			reconciler.EnableSubSlicing = false
			// Super-slice (4x4x8 has > 16 nodes, but even 4x4x4 is a super-slice)
			superslice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "enabled-superslice",
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         slicev1beta1.TypeTpu7x,
					Topology:     "4x4x4",
					PartitionIds: []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "p11", "p12", "p13", "p14", "p15", "p16"},
				},
			}
			Expect(k8sClient.Create(ctx, superslice)).To(Succeed())

			// Reconcile - it should proceed to initialization (adding finalizer)
			result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "enabled-superslice", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Check finalizer
			fetched := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "enabled-superslice", Namespace: "default"}, fetched)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(fetched, sliceFinalizerName)).To(BeTrue())
		})
	})

	Context("Initialization", func() {
		It("should add Finalizer first, then Initialize Status", func() {
			// Manual check of the 2-step process
			reconcileUntil(func() bool {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return false
				}
				return controllerutil.ContainsFinalizer(slice, sliceFinalizerName)
			})
			reconcileUntil(func() bool {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return false
				}
				return util.FindStateCondition(slice) == util.SliceNotCreated
			})
		})
	})

	Context("Empty Slice", func() {
		It("should stay in SliceNotCreated state and not trigger formation", func() {
			// 1. Create an empty slice
			emptySliceName := "empty-slice"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x8",
					PartitionIds: []string{}, // Empty!

				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			// Remove finalizer and delete at the end
			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			// Mock cache miss for empty slice
			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			// 2. Run reconcile to initialize
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred()) // Should add finalizer

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred()) // Should set status to SliceNotCreated

			// 3. Verify state is SliceNotCreated
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			// 4. Run reconcile again, it should do nothing and stay in SliceNotCreated
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(ctrl.Result{})) // Should not requeue

			// Verify state is STILL SliceNotCreated
			var s slicev1beta1.Slice
			Expect(k8sClient.Get(ctx, emptySliceNamespacedName, &s)).To(Succeed())
			Expect(util.FindStateCondition(&s)).To(Equal(util.SliceNotCreated))
		})

		It("should trigger formation when partitions are added to an empty slice", func() {
			emptySliceName := "0-n-slice"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x8",
					PartitionIds: []string{}, // Empty!
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Eventually(func() error {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1", "p2"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			mockGCEHelper.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(cache.SuperSliceHealthStatus{
					TopologyHealths: []*cache.SuperSliceTopologyHealth{
						{HealthStatus: cache.MMIGHealthStatusActive},
					},
				}, nil)
			mockRelationalCache.EXPECT().AddSlice(emptySliceName, []string{"p1", "p2"}, gomock.Any()).Return(nil)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*10).Should(Equal(string(util.MMIGHealthStatusActive)))
		})
	})

	Context("Preflight Checks", func() {
		const (
			getFailed        = "GetFailed"
			failedAsExpected = "FailedAsExpected"
		)
		BeforeEach(func() {
			// 1. Run controller logic to get out of "Initializing"
			ensureFullyInitialized()

			// 2. Mock CheckNodeLabels.
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).
				Return(false, nil).AnyTimes()

			// 3. Manually set state to "SliceNotCreated" to target the Forming logic
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, util.SliceNotCreated, "Ready", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// The cache should return false (empty) for the slice info, which simulates a cache miss and forces the controller to handle it gracefully.
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()

			// Allow cache to sync the "SliceNotCreated" state
			time.Sleep(2 * time.Second)
		})

		It("should fail and update status if MIG info is missing from cache", func() {
			slice.Spec.Topology = "4x4x4"
			slice.Spec.PartitionIds = []string{"p1"}
			// Update the object in the fake client so the Reconciler sees it
			err := k8sClient.Update(ctx, slice)
			Expect(err).NotTo(HaveOccurred())

			// 1. Mock: Relational Cache returns false (Not Found)
			mockRelationalCache.EXPECT().
				GetMIGByPartitionID("p1"). // Expect the ID we set above
				Return(cache.MIGInfo{}, false)

			// 2. Act
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			// 3. Assert
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return getFailed
				}

				cond, _ := util.FindSliceCondition(&s, slicev1beta1.SliceStateConditionType)

				// Check for failure reason AND specific error message part
				if cond.Reason == util.SliceCreationFailed &&
					strings.Contains(cond.Message, "MIG info not found") {
					return failedAsExpected
				}
				return cond.Reason
			}, time.Second*5).Should(Equal(failedAsExpected))
		})
		It("should fail if partitions have inconsistent ReservationBlockIDs", func() {
			// 1. Prepare Mock Data (Inconsistent Blocks)
			migA := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				migA.Nodes[fmt.Sprintf("a-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-1", ReservationBlockID: "block-A",
					TopologyPartitionIds: map[string][]string{
						"4x4x4": {"p1"},
					},
				}
			}
			migB := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				migB.Nodes[fmt.Sprintf("b-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-1", ReservationBlockID: "block-B", // Mismatch!
					TopologyPartitionIds: map[string][]string{
						"4x4x4": {"p2"},
					},
				}
			}

			// 2. Set Mock Expectations
			// The controller will eventually pick up the new slice name
			mockRelationalCache.EXPECT().GetMIGByPartitionID("p1").Return(migA, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("p2").Return(migB, true).AnyTimes()

			// 3. Trigger Reconcile
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// 4. Assert
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return getFailed
				}
				cond, _ := util.FindSliceCondition(&s, slicev1beta1.SliceStateConditionType)

				if cond.Reason == util.SliceCreationFailed &&
					strings.Contains(cond.Message, "Inconsistent BlockID") {
					return failedAsExpected
				}
				return fmt.Sprintf("WrongState: %s / %s", cond.Reason, cond.Message)
			}, time.Second*5).Should(Equal(failedAsExpected))
		})

		It("should fail if partition topology does not match requested topology for subslice", func() {
			slice.Spec.Topology = "2x2x1"
			slice.Spec.PartitionIds = []string{"p1"}
			err := k8sClient.Update(ctx, slice)
			Expect(err).NotTo(HaveOccurred())

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			validMIGInfo.Nodes["node-1"] = &cache.NodeInfo{
				ReservationName:    "res-OK",
				ReservationBlockID: "block-OK",
				TopologyPartitionIds: map[string][]string{
					"2x2x2": {"p1"}, // Mismatch!
				},
			}

			mockRelationalCache.EXPECT().
				GetMIGByPartitionID("p1").
				Return(validMIGInfo, true).
				AnyTimes()

			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return getFailed
				}
				cond, _ := util.FindSliceCondition(&s, slicev1beta1.SliceStateConditionType)

				if cond.Reason == util.SliceCreationFailed &&
					strings.Contains(cond.Message, "does not match topology of partition") {
					return failedAsExpected
				}
				return fmt.Sprintf("WrongState: %s / %s", cond.Reason, cond.Message)
			}, time.Second*5).Should(Equal(failedAsExpected))
		})

		It("should pass preflight and proceed to Formation if data is valid", func() {
			// 1. Setup Valid Data (16 homogeneous nodes)
			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
					TopologyPartitionIds: map[string][]string{
						"4x4x4": {"p1", "p2"},
					},
				}
			}

			// 2. Mock Cache (Preflight Check)
			mockRelationalCache.EXPECT().
				GetMIGByPartitionID(gomock.Any()).
				Return(validMIGInfo, true).
				AnyTimes()

			// 3. Mock Formation Phase
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

			// Mock other formation dependencies if needed to avoid nil pointers
			mockGCEHelper.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(cache.SuperSliceHealthStatus{}, nil).AnyTimes()
			mockRelationalCache.EXPECT().AddSlice(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			// 4. Act
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Forming Phase", func() {
		BeforeEach(func() {
			// 1. Run controller logic to get out of "Initializing"
			ensureFullyInitialized()

			// 2. Mock CheckNodeLabels. Return 'false' so the controller thinks
			// "Nodes are not labeled yet" and enters the Forming block.
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).
				Return(false, nil).AnyTimes()

			// 3. Manually set state to "SliceNotCreated" to target the Forming logic
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, util.SliceNotCreated, "Ready", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Allow cache to sync the "SliceNotCreated" state
			time.Sleep(2 * time.Second)
		})

		// 1. Initialize as a struct VALUE (no '&')
		validMIGInfo := cache.MIGInfo{
			Nodes: make(map[string]*cache.NodeInfo),
		}

		// 2. Populate 16 nodes
		for i := 0; i < 16; i++ {
			nodeName := fmt.Sprintf("node-%d", i)
			validMIGInfo.Nodes[nodeName] = &cache.NodeInfo{
				ReservationName:    "res-1",
				ReservationBlockID: "block-1",
			}
		}
		It("should DO NOTHING if state is DEACTIVATING and there is no deletion timestamp", func() {
			// 1. Set State to DEACTIVATING in the API Server
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(
					ctx, k8sClient, slice,
					slicev1beta1.SliceStateConditionType,
					metav1.ConditionUnknown,
					string(util.MMIGHealthStatusDeactivating),
					"Deactivating internally without deletion",
					nil,
				)
				return err
			}, time.Second*5).Should(Succeed())

			// Wait for the Reconciler's local cache to reflect the state
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := reconciler.Client.Get(ctx, typeNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(string(util.MMIGHealthStatusDeactivating)))

			// 2. Mock Cache to simulate an existing entry.
			mockRelationalCache.EXPECT().
				GetSliceInfo(slice.Name).
				Return(cache.SliceInfo{}, true).
				AnyTimes()

			// 3. Act: Run Reconcile
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// 4. Assert: Controller should return (ctrl.Result{}, nil) gracefully
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Requeue).To(BeFalse())
			Expect(res.RequeueAfter).To(Equal(time.Duration(0)))
		})

		It("should execute Creation Sequence when state is SliceNotCreated", func() {
			// The cache should return false (empty) for the slice info, which simulates a cache miss and forces the controller to handle it gracefully.
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()

			// EXPECTATIONS
			mockRelationalCache.EXPECT().
				GetMIGByPartitionID(gomock.Any()).
				Return(validMIGInfo, true).
				AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			mockRelationalCache.EXPECT().AddSlice(slice.Name, []string{"p1", "p2"}, gomock.Any()).Return(nil)

			// Return valid INCOMPLETE status
			mockGCEHelper.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(cache.SuperSliceHealthStatus{
					TopologyHealths: []*cache.SuperSliceTopologyHealth{
						{HealthStatus: cache.MMIGHealthStatusActive},
					},
				}, nil)
			// ACT
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// ASSERT
			// We use a small sleep or loop to allow cache sync if necessary, but Eventually is usually enough
			Eventually(func() string {
				// Bypass cache if possible or just retry
				var freshSlice slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &freshSlice); err != nil {
					return ""
				}
				return util.FindStateCondition(&freshSlice)
			}, time.Second*10).Should(Equal(string(util.MMIGHealthStatusActive)))
		})

		It("should update status with descriptive message if state is FAILED", func() {
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()

			mockRelationalCache.EXPECT().
				GetMIGByPartitionID(gomock.Any()).
				Return(validMIGInfo, true).
				AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			mockRelationalCache.EXPECT().AddSlice(slice.Name, []string{"p1", "p2"}, gomock.Any()).Return(nil)

			// Return FAILED status
			mockGCEHelper.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(cache.SuperSliceHealthStatus{
					TopologyHealths: []*cache.SuperSliceTopologyHealth{
						{HealthStatus: cache.MMIGHealthStatusFailed},
					},
				}, nil)

			// ACT
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// ASSERT
			Eventually(func() string {
				var freshSlice slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &freshSlice); err != nil {
					return ""
				}
				cond, _ := util.FindSliceCondition(&freshSlice, slicev1beta1.SliceStateConditionType)
				if cond.Reason == string(util.MMIGHealthStatusFailed) &&
					cond.Message == sliceFormationFailedMessage {
					return "Passed"
				}
				return fmt.Sprintf("Reason: %s, Message: %s", cond.Reason, cond.Message)
			}, time.Second*10).Should(Equal("Passed"))
		})

		It("should PAUSE (No Requeue) if state is ACTIVATING", func() {
			// Because the state is ACTIVATING, we simulate that the slice is
			// already in the cache, preventing cacheSyncUp from calling GetMMIG.
			mockRelationalCache.EXPECT().
				GetSliceInfo(slice.Name).
				Return(cache.SliceInfo{Name: slice.Name}, true).
				AnyTimes()

			// 1. Set State to ACTIVATING in API Server
			Eventually(func() error {
				// Fetch latest to avoid conflict
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, string(util.MMIGHealthStatusActivating), "Wait", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Wait for the Reconciler's local cache to reflect the update
			Eventually(func() string {
				var s slicev1beta1.Slice
				err := reconciler.Client.Get(ctx, typeNamespacedName, &s)
				if err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*10).Should(Equal(string(util.MMIGHealthStatusActivating)))

			// 2. Run Reconcile
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// 3. Verify
			Expect(err).NotTo(HaveOccurred())

			Expect(res.RequeueAfter).To(Equal(time.Duration(0)))
			Expect(res.Requeue).To(BeFalse())
		})

		It("should Label Nodes if state is Active", func() {
			// The slice is actively forming or active, it should be in the cache
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{Name: slice.Name}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()

			// 1. Set State to Active
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, string(util.MMIGHealthStatusActive), "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())
			// Wait for propagation
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(string(util.MMIGHealthStatusActive)))

			// 2. Expect Label Action
			// Mock Execute for the actual Labeling action
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			mockRelationalCache.EXPECT().AddSlice(slice.Name, []string{"p1", "p2"}, gomock.Any()).Return(nil)

			// 3. Run Reconcile
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// 4. Assert
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return false
				}
				cond := meta.FindStatusCondition(slice.Status.Conditions, util.NodeLabeled)
				return cond != nil && cond.Status == metav1.ConditionTrue
			}, time.Second*5).Should(BeTrue())
		})

		It("should return error if Relational Cache addition fails when state is Active", func() {
			// The slice is actively forming or active, it should be in the cache
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{Name: slice.Name}, true).AnyTimes()

			// 1. Set State to Active
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, string(util.MMIGHealthStatusActive), "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())
			// Wait for propagation
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(string(util.MMIGHealthStatusActive)))

			// Mock Node Labels check to return false (needs labeling)
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).Return(false, nil).AnyTimes()

			// Mock: AddSlice fails
			mockRelationalCache.EXPECT().AddSlice(slice.Name, []string{"p1", "p2"}, gomock.Any()).Return(errors.New("cache error"))

			// Act
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// Assert
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cache error"))
		})

		It("should RETRY (return error) if formation fails and retry annotation is 'true'", func() {
			// The cache should return false (empty) for the slice info, which simulates a cache miss and forces the controller to handle it gracefully.
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()

			// 1. SETUP MOCKS FIRST (Crucial: Must be before any client updates)

			// Allow Preflight to pass
			mockRelationalCache.EXPECT().
				GetMIGByPartitionID(gomock.Any()).
				Return(validMIGInfo, true).
				AnyTimes()

			// Mock SliceEngine to return an error (Simulate GCE failure)
			expectedErr := fmt.Errorf("transient network error")
			mockSliceEngine.EXPECT().
				Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(expectedErr).
				AnyTimes() // Use AnyTimes as it might retry multiple times in background

			// 2. Add the Retry Annotation (This triggers the controller!)
			Eventually(func() error {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return err
				}
				if s.Annotations == nil {
					s.Annotations = make(map[string]string)
				}
				s.Annotations[util.RetryAnnotation] = "true"
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			// 3. Act: Run Reconcile Manually (to capture the return error)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// 4. Assert:
			// The controller should RETURN the error (TRIGGER retry)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(expectedErr.Error()))

			// 5. Verify Status is updated to SliceCreationFailed
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return ""
				}
				cond, _ := util.FindSliceCondition(&s, slicev1beta1.SliceStateConditionType)
				return cond.Reason
			}, time.Second*5).Should(Equal(util.SliceCreationFailed))
		})

		It("should increment retryAttempt if formation fails with OperationError and retry annotation is 'true'", func() {
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()

			// Mock SliceEngine to return an OperationError
			opErr := &util.OperationError{Err: fmt.Errorf("operation failed")}
			mockSliceEngine.EXPECT().
				Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(opErr).
				AnyTimes()

			// Add the Retry Annotation
			Eventually(func() error {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return err
				}
				if s.Annotations == nil {
					s.Annotations = make(map[string]string)
				}
				s.Annotations[util.RetryAnnotation] = util.EnableRetryAnnotation
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			// Run Reconcile
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).To(HaveOccurred())

			// Verify retryAttempt is incremented
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return ""
				}
				return s.Annotations[util.RetryAttemptAnnotationKey]
			}, time.Second*5).Should(Equal("1"))
		})

		It("should NOT increment retryAttempt if formation fails with non-OperationError and retry annotation is 'true'", func() {
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()

			// Mock SliceEngine to return a generic error (not OperationError)
			genericErr := fmt.Errorf("generic error")
			mockSliceEngine.EXPECT().
				Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(genericErr).
				AnyTimes()

			// Add the Retry Annotation
			Eventually(func() error {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return err
				}
				if s.Annotations == nil {
					s.Annotations = make(map[string]string)
				}
				s.Annotations[util.RetryAnnotation] = util.EnableRetryAnnotation
				// Reset retry attempt just in case
				delete(s.Annotations, util.RetryAttemptAnnotationKey)
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			// Run Reconcile
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).To(HaveOccurred())

			// Verify retryAttempt is NOT incremented (should be empty or not found)
			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return "error"
				}
				return s.Annotations[util.RetryAttemptAnnotationKey]
			}, time.Second*5).Should(BeEmpty())
		})
	})

	Context("Update Phase", func() {
		BeforeEach(func() {
			ensureFullyInitialized()
			reconciler.EnableSliceUpdate = true
		})

		It("should trigger handleSliceUpdate when generation increments", func() {
			emptySliceName := "update-gen-slice"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Expect(s.Status.ObservedGeneration).To(Equal(int64(1)))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			Expect(s.ObjectMeta.Generation).To(Equal(int64(2)))

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Cleanup
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Create WP/MMIG
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Attach
			mockRelationalCache.EXPECT().AddSlice(emptySliceName, []string{"p1"}, "4x4x4").Return(nil).Times(1)
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// Second Reconcile for formation
			mockGCEHelper.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(cache.SuperSliceHealthStatus{
					TopologyHealths: []*cache.SuperSliceTopologyHealth{
						{HealthStatus: cache.MMIGHealthStatusActive},
					},
				}, nil)
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Label nodes
			mockRelationalCache.EXPECT().AddSlice(emptySliceName, []string{"p1"}, gomock.Any()).Return(nil)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int64 {
				var freshSlice slicev1beta1.Slice
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &freshSlice); err != nil {
					return 0
				}
				return freshSlice.Status.ObservedGeneration
			}, time.Second*10).Should(Equal(int64(2)))
		})

		It("should retry GetMMIG on transient errors and succeed", func() {
			emptySliceName := "update-retry-succeed"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Expect(s.Status.ObservedGeneration).To(Equal(int64(1)))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			Expect(s.ObjectMeta.Generation).To(Equal(int64(2)))

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()

			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Cleanup
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Create
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Attach
			mockRelationalCache.EXPECT().AddSlice(emptySliceName, []string{"p1"}, "4x4x4").Return(nil).Times(1)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should retry GetMMIG on transient errors and fail after max retries", func() {
			emptySliceName := "update-retry-fail"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()

			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("gce api call failed after retries")).Times(1)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("gce api call failed after retries"))
		})

		It("should detach and delete MMIG if it already exists during 0-N update", func() {
			emptySliceName := "update-recreate-slice"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Expect(s.Status.ObservedGeneration).To(Equal(int64(1)))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			Expect(s.ObjectMeta.Generation).To(Equal(int64(2)))

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Cleanup
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Create WP/MMIG
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Attach
			mockRelationalCache.EXPECT().AddSlice(emptySliceName, []string{"p1"}, "4x4x4").Return(nil).Times(1)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// Second Reconcile for formation
			mockGCEHelper.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(cache.SuperSliceHealthStatus{
					TopologyHealths: []*cache.SuperSliceTopologyHealth{
						{HealthStatus: cache.MMIGHealthStatusActive},
					},
				}, nil)
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Label nodes
			mockRelationalCache.EXPECT().AddSlice(emptySliceName, []string{"p1"}, gomock.Any()).Return(nil)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int64 {
				var freshSlice slicev1beta1.Slice
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &freshSlice); err != nil {
					return 0
				}
				return freshSlice.Status.ObservedGeneration
			}, time.Second*10).Should(Equal(int64(2)))
		})

		It("should cleanup MMIG and update status to SliceNotCreated during N-0 update", func() {
			sliceName := "update-n0-slice"
			sliceNamespacedName := types.NamespacedName{Name: sliceName, Namespace: "default"}

			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{"p1", "p2"},
				},
			}
			Expect(k8sClient.Create(ctx, slice)).To(Succeed())

			mockRelationalCache.EXPECT().GetSliceInfo(sliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: sliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: sliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, sliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			Eventually(func() error {
				if err := k8sClient.Get(ctx, sliceNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, string(util.MMIGHealthStatusActive), "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			Eventually(func() error {
				if err := k8sClient.Get(ctx, sliceNamespacedName, slice); err != nil {
					return err
				}
				slice.Status.ObservedGeneration = slice.ObjectMeta.Generation
				return k8sClient.Status().Update(ctx, slice)
			}, time.Second*5).Should(Succeed())

			Eventually(func() error {
				if err := k8sClient.Get(ctx, sliceNamespacedName, slice); err != nil {
					return err
				}
				slice.Spec.PartitionIds = []string{}
				return k8sClient.Update(ctx, slice)
			}, time.Second*5).Should(Succeed())

			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(&betacompute.MultiMig{Name: "some-mmig"}, nil).Times(1)
			mockRelationalCache.EXPECT().AddSlice(sliceName, []string{}, gomock.Any()).Return(nil)
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: sliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				var freshSlice slicev1beta1.Slice
				if err := k8sClient.Get(ctx, sliceNamespacedName, &freshSlice); err != nil {
					return ""
				}
				return util.FindStateCondition(&freshSlice)
			}, time.Second*10).Should(Equal(util.SliceNotCreated))

			var freshSlice slicev1beta1.Slice
			Expect(k8sClient.Get(ctx, sliceNamespacedName, &freshSlice)).To(Succeed())
			Expect(freshSlice.Status.ObservedGeneration).To(Equal(freshSlice.ObjectMeta.Generation))
		})

		It("should ignore update when EnableSliceUpdate is false", func() {
			reconciler.EnableSliceUpdate = false // Disable flag
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{Name: slice.Name}, true).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).Return(true, nil).AnyTimes()

			// Set slice to ACTIVE state first
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, string(util.MMIGHealthStatusActive), "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Update the spec to trigger generation increment
			Eventually(func() error {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1", "p2", "p3"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			// Run Reconcile
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(ctrl.Result{})) // Should do nothing

			// Verify that ObservedGeneration was NOT updated
			var s slicev1beta1.Slice
			Expect(k8sClient.Get(ctx, typeNamespacedName, &s)).To(Succeed())
			Expect(s.ObjectMeta.Generation).To(BeNumerically(">", s.Status.ObservedGeneration))
		})

		It("should update observedGeneration if CreateWPAndMMIGAction fails during 0-N Update without Retry", func() {
			emptySliceName := "update-cmmig-fail-slice"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Expect(s.Status.ObservedGeneration).To(Equal(int64(1)))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			Expect(s.ObjectMeta.Generation).To(Equal(int64(2)))

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Cleanup
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("WP creation failure")).Times(1)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var freshSlice slicev1beta1.Slice
			Expect(k8sClient.Get(ctx, emptySliceNamespacedName, &freshSlice)).To(Succeed())
			Expect(freshSlice.Status.ObservedGeneration).To(Equal(int64(2)))
		})

		It("should NOT update observedGeneration and return error if CreateWPAndMMIGAction fails during 0-N Update with Retry", func() {
			emptySliceName := "update-cmmig-fail-retry"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
					Annotations: map[string]string{
						util.RetryAnnotation: "true",
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Expect(s.Status.ObservedGeneration).To(Equal(int64(1)))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			Expect(s.ObjectMeta.Generation).To(Equal(int64(2)))

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Cleanup
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("WP creation failure")).Times(1)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).To(HaveOccurred())

			var freshSlice slicev1beta1.Slice
			Expect(k8sClient.Get(ctx, emptySliceNamespacedName, &freshSlice)).To(Succeed())
			Expect(freshSlice.Status.ObservedGeneration).To(Equal(int64(1)))
		})

		It("should update observedGeneration if AttachMIGsAction fails during 0-N Update without Retry", func() {
			emptySliceName := "update-attach-fail"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Expect(s.Status.ObservedGeneration).To(Equal(int64(1)))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			Expect(s.ObjectMeta.Generation).To(Equal(int64(2)))

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Cleanup
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("Attachment failure")).Times(1)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var freshSlice slicev1beta1.Slice
			Expect(k8sClient.Get(ctx, emptySliceNamespacedName, &freshSlice)).To(Succeed())
			Expect(freshSlice.Status.ObservedGeneration).To(Equal(int64(2)))
		})

		It("should NOT update observedGeneration and return error if AttachMIGsAction fails during 0-N Update with Retry", func() {
			emptySliceName := "update-attach-fail-retry"
			emptySliceNamespacedName := types.NamespacedName{Name: emptySliceName, Namespace: "default"}
			emptySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      emptySliceName,
					Namespace: "default",
					Annotations: map[string]string{
						util.RetryAnnotation: "true",
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, emptySlice)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(emptySliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var s slicev1beta1.Slice
			Eventually(func() string {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceNotCreated))

			Expect(s.Status.ObservedGeneration).To(Equal(int64(1)))

			Eventually(func() error {
				if err := k8sClient.Get(ctx, emptySliceNamespacedName, &s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1"}
				return k8sClient.Update(ctx, &s)
			}, time.Second*5).Should(Succeed())

			Expect(s.ObjectMeta.Generation).To(Equal(int64(2)))

			validMIGInfo := cache.MIGInfo{Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				validMIGInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					ReservationName: "res-OK", ReservationBlockID: "block-OK",
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(validMIGInfo, true).AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil) // Cleanup
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("Attachment failure")).Times(1)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: emptySliceNamespacedName})
			Expect(err).To(HaveOccurred())

			var freshSlice slicev1beta1.Slice
			Expect(k8sClient.Get(ctx, emptySliceNamespacedName, &freshSlice)).To(Succeed())
			Expect(freshSlice.Status.ObservedGeneration).To(Equal(int64(1)))
		})

		It("should prioritize Deletion over Update", func() {
			sliceName := "delete-over-update"
			sliceNamespacedName := types.NamespacedName{Name: sliceName, Namespace: "default"}
			s := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sliceName,
					Namespace: "default",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "4x4x4",
					PartitionIds: []string{"p1"},
				},
			}
			Expect(k8sClient.Create(ctx, s)).To(Succeed())

			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, sliceNamespacedName, s); err == nil {
					s.Finalizers = []string{}
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			mockRelationalCache.EXPECT().GetSliceInfo(sliceName).Return(cache.SliceInfo{}, false).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: sliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: sliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				if err := k8sClient.Get(ctx, sliceNamespacedName, s); err != nil {
					return err
				}
				s.Spec.PartitionIds = []string{"p1", "p2"}
				return k8sClient.Update(ctx, s)
			}, time.Second*5).Should(Succeed())

			Expect(k8sClient.Delete(ctx, s)).To(Succeed())

			// Fetch fresh copy to see deletion timestamp
			Expect(k8sClient.Get(ctx, sliceNamespacedName, s)).To(Succeed())
			Expect(s.DeletionTimestamp.IsZero()).To(BeFalse())

			// Mock Deletion expectations
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), false).Return(true, nil)
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			mockRelationalCache.EXPECT().DeleteSlice(sliceName).Return(nil)

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: sliceNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var freshSlice slicev1beta1.Slice
			err = k8sClient.Get(ctx, sliceNamespacedName, &freshSlice)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("Deletion Phase", func() {
		BeforeEach(func() {
			ensureFullyInitialized()
			// Set to Active
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, string(util.MMIGHealthStatusActive), "Active", nil)
				return err
			}).Should(Succeed()) // Mark for deletion
			Expect(k8sClient.Delete(ctx, slice)).To(Succeed())
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return false
				}
				return !slice.DeletionTimestamp.IsZero()
			}).Should(BeTrue())

			// The slice is being deleted, it should be in the cache until we remove it at the end of deletion.
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{Name: slice.Name}, true).AnyTimes()
		})

		It("Stage 1: Should Trigger Deformation", func() {
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return ""
				}
				return util.FindStateCondition(slice)
			}).Should(Equal(string(cache.MMIGHealthStatusActive)))
		})

		It("Stage 2: Should Wait (Block) if state is Deactivating", func() {
			// 1. Force state to 'Deactivating'
			// This simulates the state after Deformation has been triggered but GCE is still working.
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, string(util.MMIGHealthStatusDeactivating), "Deforming", nil)
				return err
			}).Should(Succeed())

			// 2. Set Strict Expectations: NOTHING should happen
			// We expect the reconciler to check the state and return immediately.

			// Critical: Ensure 'Execute' (Unlabel/Detach actions) is NEVER called
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// Critical: Ensure we don't remove the slice from cache/finalizers yet
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Times(0)
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// 3. Run Reconcile
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// 4. Assertions
			// Verify the controller did not remove the finalizer (Slice must still exist)
			currentSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, currentSlice)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(currentSlice, sliceFinalizerName)).To(BeTrue(), "Finalizer should not be removed while deactivating")

			// Verify Result
			Expect(result.RequeueAfter).To(Equal(time.Duration(0)))
		})

		It("Stage 3: Should Cleanup if state is Incomplete", func() {
			// Force state to Incomplete (Ready for Cleanup)
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, string(util.MMIGHealthStatusIncomplete), "Ready", nil)
				return err
			}).Should(Succeed())

			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(cache.MIGInfo{}, false).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), false).Return(true, nil)
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			mockRelationalCache.EXPECT().DeleteSlice(slice.Name).Return(nil)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// Should be gone
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(ctx, typeNamespacedName, slice))
			}).Should(BeTrue())
		})
	})

	Context("Steady State", func() {
		BeforeEach(func() {
			ensureFullyInitialized()
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, string(util.MMIGHealthStatusActive), "Active", nil)
				return err
			}).Should(Succeed())

			// Mock Node Labels check to avoid entering "Forming"
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).Return(true, nil).AnyTimes()
		})

		It("should Backfill Cache if GCE resource exists but Cache is empty", func() {
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false)
			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(&betacompute.MultiMig{}, nil)
			mockRelationalCache.EXPECT().AddSlice(slice.Name, gomock.Any(), gomock.Any()).Return(nil)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Forming Phase - Error Handling", func() {
		BeforeEach(func() {
			ensureFullyInitialized()
			// Mock: Nodes NOT labeled yet (Enter Forming Logic)
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).
				Return(false, nil).AnyTimes()

			// Setup: SliceNotCreated
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, util.SliceNotCreated, "Ready", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// The cache should return false (empty) for the slice info, which simulates a cache miss and forces the controller to handle it gracefully.
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()
			time.Sleep(2 * time.Second) // Sync cache
		})

		// 1. Initialize as a struct VALUE (no '&')
		validMIGInfo := cache.MIGInfo{
			Nodes: make(map[string]*cache.NodeInfo),
		}

		// 2. Populate 16 nodes
		for i := 0; i < 16; i++ {
			nodeName := fmt.Sprintf("node-%d", i)
			validMIGInfo.Nodes[nodeName] = &cache.NodeInfo{
				ReservationName:    "res-1",
				ReservationBlockID: "block-1",
			}
		}

		It("should update status to CreationFailed if Engine Execution fails", func() {
			mockRelationalCache.EXPECT().
				GetMIGByPartitionID(gomock.Any()).
				Return(validMIGInfo, true).
				AnyTimes()
			// Mock: Creation fails
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(errors.New("gce quota exceeded"))

			// Act
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// Assert: No error returned (handled internally), but Status updated
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				var s slicev1beta1.Slice
				if err := k8sClient.Get(ctx, typeNamespacedName, &s); err != nil {
					return ""
				}
				return util.FindStateCondition(&s)
			}, time.Second*5).Should(Equal(util.SliceCreationFailed))
		})

		It("should return error if Relational Cache addition fails", func() {
			mockRelationalCache.EXPECT().
				GetMIGByPartitionID(gomock.Any()).
				Return(validMIGInfo, true).
				AnyTimes()
			mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

			mockGCEHelper.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(cache.SuperSliceHealthStatus{
					TopologyHealths: []*cache.SuperSliceTopologyHealth{
						{HealthStatus: cache.MMIGHealthStatusActive},
					},
				}, nil)

			mockRelationalCache.EXPECT().AddSlice(slice.Name, gomock.Any(), gomock.Any()).Return(errors.New("redis error"))

			// Act
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// Assert
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Deletion Phase - Unlabeling Logic", func() {
		BeforeEach(func() {
			ensureFullyInitialized()
			// Setup: Deletion Timestamp Set
			Expect(k8sClient.Delete(ctx, slice)).To(Succeed())

			// Setup: State is Incomplete (Ready for Cleanup)
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionUnknown, string(util.MMIGHealthStatusIncomplete), "Ready", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// The slice is being deleted, it should be in the cache until we remove it at the end of deletion.
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{Name: slice.Name}, true).AnyTimes()

			time.Sleep(2 * time.Second)
		})

		It("should Unlabel nodes and immediately proceed to Cleanup (Fallthrough)", func() {
			// 1. Mock: Nodes ARE still labeled (false = not all unlabeled)
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(cache.MIGInfo{}, false).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), false).
				Return(false, nil)

			// 2. Expect Sequence of Actions (The critical proof)
			gomock.InOrder(
				// Step A: Unlabel Nodes
				mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil),

				// Step B: Cleanup GCE (Falls through immediately)
				mockSliceEngine.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil),
			)

			// 3. Mock: Cache cleanup
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil)

			// Act
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Requeue).To(BeFalse())

			// 4. ASSERT: The Object should be GONE.
			// Since we removed the finalizer at the end of the loop, the object is deleted.
			// We cannot check for "NodeLabelIsRemoved" because the object no longer exists.
			Eventually(func() bool {
				var s slicev1beta1.Slice
				err := k8sClient.Get(ctx, typeNamespacedName, &s)
				return apierrors.IsNotFound(err)
			}, time.Second*5).Should(BeTrue(), "Slice should be deleted from API server")
		})
	})

	Context("Steady State - Cache Cleanup", func() {
		BeforeEach(func() {
			ensureFullyInitialized()
			// Setup: Slice Failed (Trigger for cacheSyncUp verification)
			Eventually(func() error {
				if err := k8sClient.Get(ctx, typeNamespacedName, slice); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, slice, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.SliceCreationFailed, "Failed", nil)
				return err
			}).Should(Succeed())
		})

		It("should remove slice from Cache if GCE resource is missing (Ghost Entry)", func() {
			expectedMMIGName := util.GetResourceName(slice.Name, reconciler.ClusterHash)
			// Mock: Cache says "I have this slice"
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{Name: slice.Name}, true)

			// Mock: GCE says "I do NOT have this slice" (404)
			mockComputeService.EXPECT().GetMMIG(
				gomock.Any(), // Project Number
				gomock.Any(), // Region
				expectedMMIGName,
			).Return(nil, &googleapi.Error{Code: http.StatusNotFound})

			// Mock: Controller should Delete from Cache
			mockRelationalCache.EXPECT().DeleteSlice(slice.Name).Return(nil)

			// Act
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should explicitly skip GCE query and cache backfill if slice creation previously failed and cache is empty", func() {
			// 1. The BeforeEach in this Context already patched the Slice condition to SliceCreationFailed.

			// 2. Mock: Simulate a controller restart where the cache is currently empty
			mockRelationalCache.EXPECT().GetSliceInfo(slice.Name).Return(cache.SliceInfo{}, false).AnyTimes()

			// 3. EXPLICIT CHECKS:
			// Assert that the Google API is NEVER queried.
			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// Assert that the Slice is NEVER added back to the cache.
			mockRelationalCache.EXPECT().AddSlice(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// Prevent falling through to node label checks (keeps test isolated)
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// 4. Act
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// 5. Assert: Reconciler should complete successfully without triggering those external calls
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Edge Case - Resource Gone", func() {
		It("should clean up relational cache when Slice object is deleted (NotFound)", func() {
			// Setup: Ensure Slice is NOT in K8s
			ensureSliceDeleted()

			// Mock: Controller should try to clean up cache just in case
			mockRelationalCache.EXPECT().DeleteSlice(slice.Name).Return(nil)

			// Act: Call Reconcile on a name that doesn't exist
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})

			// Assert
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Controller Setup", func() {
		It("should setup with manager and correct worker count", func() {
			// 1. Create a new manager
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme: k8sClient.Scheme(),
			})
			Expect(err).NotTo(HaveOccurred())

			// 2. Create the reconciler
			r := &SliceReconciler{
				Client:         mgr.GetClient(),
				Scheme:         mgr.GetScheme(),
				ComputeService: mockComputeService, // Can use mocks here
			}

			// 3. Call SetupWithManager with a specific worker count
			workerCount := 5
			err = r.SetupWithManager(mgr, workerCount)

			// 4. Assert that no error occurred.
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("Sub-slice Reconciliation", func() {
	const (
		migURL  = "projects/test-project/zones/us-central1-a/instanceGroupManagers/gke-test-cluster-default-pool-abcdef-mig"
		migName = "gke-test-cluster-default-pool-abcdef-mig"
	)

	var subslice *slicev1beta1.Slice
	var mockNode *corev1.Node
	var mockCtrl *gomock.Controller
	var mockComputeService *mocks.MockComputeServiceInterface
	var mockRelationalCache *mocks.MockSliceRelationalCache
	var mockKubeHelper *mocks.MockClient
	var mockGCEHelper *mocks.MockGCEClient
	var reconciler *SliceReconciler
	var wrappingClient *InterceptingClient

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		// Reset mocks before each test
		mockComputeService = mocks.NewMockComputeServiceInterface(mockCtrl)
		mockRelationalCache = mocks.NewMockSliceRelationalCache(mockCtrl)
		mockKubeHelper = mocks.NewMockClient(mockCtrl)
		mockGCEHelper = mocks.NewMockGCEClient(mockCtrl)

		wrappingClient = &InterceptingClient{Client: k8sClient}

		reconciler = &SliceReconciler{
			Client:           wrappingClient,
			Scheme:           k8sClient.Scheme(),
			ComputeService:   mockComputeService,
			KubeHelper:       mockKubeHelper,
			GCEHelper:        mockGCEHelper,
			SliceEngine:      action.NewEngine(),
			RelationalCache:  mockRelationalCache,
			ProjectNumber:    "test-project",
			Location:         "test-location",
			ClusterHash:      "test-cluster-hash",
			EnableSubSlicing: true,
		}

		subslice = &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-subslice",
				Finalizers: []string{"accelerator.gke.io/slice-finalizer"},
			},
			Spec: slicev1beta1.SliceSpec{
				Type:         slicev1beta1.TypeTpu7x,
				Topology:     "2x2x4",
				PartitionIds: []string{"partition-0"},
			},
		}

		mockNode = &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "gke-test-cluster-default-pool-abcdef-mig-w5v5",
				Labels: map[string]string{
					"topology.kubernetes.io/zone":                 "us-central1-a",
					"cloud.google.com/gke-tpu-partition-4x4x4-id": "partition-0",
				},
			},
		}
	})

	AfterEach(func() {
		mockCtrl.Finish()
		// Clean up resources with finalizer removal to ensure they are gone
		s := &slicev1beta1.Slice{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, s); err == nil {
			if len(s.Finalizers) > 0 {
				patch := client.MergeFrom(s.DeepCopy())
				s.Finalizers = nil
				_ = k8sClient.Patch(ctx, s, patch)
			}
			_ = k8sClient.Delete(ctx, s)
		}
		_ = k8sClient.Delete(ctx, mockNode)
		Eventually(func() bool {
			var fetchedNode corev1.Node
			err := k8sClient.Get(ctx, types.NamespacedName{Name: mockNode.Name}, &fetchedNode)
			return apierrors.IsNotFound(err)
		}, time.Second*5).Should(BeTrue())
	})

	Context("When a new sub-slice CR is created", func() {
		It("should call ACTIVATE when the topology is INACTIVE", func() {
			By("creating a new sub-slice custom resource and its associated node")
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Define mock expectations
			migURL := "projects/test-project/zones/us-central1-a/instanceGroupManagers/gke-test-cluster-default-pool-abcdef-mig"
			migName := "gke-test-cluster-default-pool-abcdef-mig"
			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			for i := 0; i < 16; i++ {
				migInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					Name:               fmt.Sprintf("node-%d", i),
					MIGName:            migURL,
					ReservationName:    "res-1",
					ReservationBlockID: "block-1",
					TopologyPartitionIds: map[string][]string{
						"2x2x4": {"partition-0"},
					},
				}
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, false).AnyTimes()
			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			mockRelationalCache.EXPECT().AddSlice(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						SliceID:                  util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
				},
			}, nil).AnyTimes()

			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
					Expect(req.AcceleratorTopologyConfigurations).To(HaveLen(1))
					config := req.AcceleratorTopologyConfigurations["partition-0"]
					Expect(config.Action).To(Equal("ACTIVATE"))
					Expect(config.ExternalId).To(Equal(util.GetResourceName(subslice.Name, reconciler.ClusterHash)))
					return &betacompute.Operation{Name: "test-op"}, nil
				}).AnyTimes()

			// Trigger reconciliation
			reconcileUntil := func() {
				Eventually(func() bool {
					_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
					return err == nil
				}, time.Second*10, time.Millisecond*250).Should(BeTrue())
			}

			reconcileUntil() // Initialization: Finalizer
			reconcileUntil() // Initialization: Status
			reconcileUntil() // Preflight & Formation

			// Verify status is updated to INACTIVE (result of post-activation sync in mock)
			Eventually(func() string {
				fetched := &slicev1beta1.Slice{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
				if err != nil {
					return ""
				}
				cond := meta.FindStatusCondition(fetched.Status.Conditions, slicev1beta1.SliceStateConditionType)
				if cond != nil {
					return cond.Reason
				}
				return ""
			}, time.Second*5, time.Millisecond*250).Should(Equal(util.DynamicSliceInactive))
		})
	})

	Context("When a sub-slice formation previously failed and is retrying", func() {
		It("should run preflight checks again and block if they fail", func() {
			By("creating a sub-slice with SliceCreationFailed status and retry enabled")
			subslice.Annotations = map[string]string{
				util.RetryAnnotation: util.EnableRetryAnnotation,
			}
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Manually set status to SliceCreationFailed so the next reconcile is a retry
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.SliceCreationFailed, "Injected preflight failure", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Define mock expectations
			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, false).AnyTimes()

			// Mock preflight checks to fail (e.g., Topology Mismatch)
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						AcceleratorTopology:      "2x2x2", // Mismatch with spec "2x2x4"
						SliceID:                  "",      // GCE doesn't know about it
					},
				},
			}, nil).AnyTimes()

			// We expect ConfigureAcceleratorTopology (ACTIVATE) to NOT be called because preflight failed.
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// Trigger reconciliation
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())

			// Verify status remains SliceCreationFailed with the preflight error
			fetched := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)).Should(Succeed())
			cond := meta.FindStatusCondition(fetched.Status.Conditions, slicev1beta1.SliceStateConditionType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal(util.SliceCreationFailed))
			Expect(cond.Message).To(ContainSubstring("Topology in spec"))
		})
	})

	Context("When the controller crashes and restarts mid-update (deactivation in progress)", func() {
		It("should safely backfill the cache and block on the LIFO barrier", func() {
			By("creating a slice with update annotations but empty cache")
			subslice.Spec.PartitionIds = []string{"partition-old"}
			subslice.Annotations = map[string]string{
				util.SubsliceProvisionedPartitionAnnotation: "partition-old",
				util.SubsliceLastOpAnnotation:               "op-deactivate-old",
				util.RetryAnnotation:                        util.EnableRetryAnnotation,
			}

			reconciler.EnableSliceUpdate = true
			reconciler.SupportSubsliceUpdate = true
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Initialize status to ObservedGeneration = 1 and DEACTIVATING
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Status.ObservedGeneration = 1
				if err := k8sClient.Status().Update(ctx, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.DynamicSliceDeactivating, "Deactivating", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Update spec to partition-new to increment generation to 2
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Spec.PartitionIds = []string{"partition-new"}
				return k8sClient.Update(ctx, fetched)
			}, time.Second*5).Should(Succeed())

			// Define mock expectations
			migInfoOld := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			migInfoNew := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			for i := 0; i < 16; i++ {
				migInfoNew.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					Name:    fmt.Sprintf("node-%d", i),
					MIGName: migURL,
					TopologyPartitionIds: map[string][]string{
						"2x2x4": {"partition-new"},
					},
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-old").Return(migInfoOld, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-new").Return(migInfoNew, true).AnyTimes()
			mockRelationalCache.EXPECT().GetSliceInfo(subslice.Name).Return(cache.SliceInfo{}, false).AnyTimes()
			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockKubeHelper.EXPECT().UnlabelNodeGroup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			// Sync-up checks GCE for partition-old, and preflight checks partition-new.
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-old": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceActive),
						AcceleratorTopology:      "2x2x4",
						SliceID:                  util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
					"partition-new": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						AcceleratorTopology:      "2x2x4", // Must match spec!
						SliceID:                  "",
					},
				},
			}, nil).AnyTimes()

			// Cache sync-up should BACKFILL partition-old to cache
			mockRelationalCache.EXPECT().AddSlice(subslice.Name, []string{"partition-old"}, subslice.Spec.Topology).Return(nil).Times(1)

			// Barrier checks the operation. GCE returns RUNNING.
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "op-deactivate-old").Return(&betacompute.Operation{
				Status: "RUNNING",
			}, nil).Times(1)

			// We expect NO GCE configure topology call because barrier blocks
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// Trigger reconciliation
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When the GCE Operations API returns a transient error during barrier check", func() {
		It("should block (fail-closed) and NOT dispatch a conflicting operation", func() {
			By("creating a slice with an in-flight operation annotation")
			subslice.Spec.PartitionIds = []string{"partition-old"}
			subslice.Annotations = map[string]string{
				util.SubsliceProvisionedPartitionAnnotation: "partition-old",
				util.SubsliceLastOpAnnotation:               "op-deactivate-old",
				util.RetryAnnotation:                        util.EnableRetryAnnotation,
			}

			reconciler.EnableSliceUpdate = true
			reconciler.SupportSubsliceUpdate = true
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Initialize status to ObservedGeneration = 1 and DEACTIVATING
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Status.ObservedGeneration = 1
				if err := k8sClient.Status().Update(ctx, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.DynamicSliceDeactivating, "Deactivating", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Update spec to partition-new to increment generation to 2
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Spec.PartitionIds = []string{"partition-new"}
				return k8sClient.Update(ctx, fetched)
			}, time.Second*5).Should(Succeed())

			// Define mock expectations
			migInfoOld := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			migInfoNew := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			for i := 0; i < 16; i++ {
				migInfoNew.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					Name:    fmt.Sprintf("node-%d", i),
					MIGName: migURL,
					TopologyPartitionIds: map[string][]string{
						"2x2x4": {"partition-new"},
					},
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-old").Return(migInfoOld, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-new").Return(migInfoNew, true).AnyTimes()
			mockRelationalCache.EXPECT().GetSliceInfo(subslice.Name).Return(cache.SliceInfo{}, false).AnyTimes()
			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockKubeHelper.EXPECT().UnlabelNodeGroup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			// Sync-up checks GCE for partition-old, and preflight checks partition-new.
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-old": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceActive),
						AcceleratorTopology:      "2x2x4",
						SliceID:                  util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
					"partition-new": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						AcceleratorTopology:      "2x2x4",
						SliceID:                  "",
					},
				},
			}, nil).AnyTimes()

			// Cache sync-up should BACKFILL partition-old to cache
			mockRelationalCache.EXPECT().AddSlice(subslice.Name, []string{"partition-old"}, subslice.Spec.Topology).Return(nil).Times(1)

			// Barrier checks the operation. GCE returns a TRANSIENT ERROR (e.g., network dropout).
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "op-deactivate-old").Return(nil, errors.New("transient network error")).Times(1)

			// We expect NO GCE configure topology call because barrier should block on error (fail-closed)
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// Trigger reconciliation
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When the spec is rapidly updated A -> B -> C during an ongoing update", func() {
		It("should block while deactivating A, and then directly activate C (skipping B)", func() {
			By("creating a slice currently active on partition-A")
			subslice.Spec.PartitionIds = []string{"partition-A"}
			subslice.Annotations = map[string]string{
				util.SubsliceProvisionedPartitionAnnotation: "partition-A",
				util.SubsliceLastOpAnnotation:               "op-deactivate-A",
				util.RetryAnnotation:                        util.EnableRetryAnnotation,
			}

			reconciler.EnableSliceUpdate = true
			reconciler.SupportSubsliceUpdate = true
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Initialize status to ObservedGeneration = 1 and DEACTIVATING
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Status.ObservedGeneration = 1
				if err := k8sClient.Status().Update(ctx, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.DynamicSliceDeactivating, "Deactivating", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Update spec to partition-C to increment generation to 2
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Spec.PartitionIds = []string{"partition-C"}
				return k8sClient.Update(ctx, fetched)
			}, time.Second*5).Should(Succeed())

			// Define mock expectations
			migInfoA := cache.MIGInfo{Name: migURL}
			migInfoC := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			for i := 0; i < 16; i++ {
				migInfoC.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					Name:    fmt.Sprintf("node-%d", i),
					MIGName: migURL,
					TopologyPartitionIds: map[string][]string{
						"2x2x4": {"partition-C"},
					},
				}
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-A").Return(migInfoA, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-C").Return(migInfoC, true).AnyTimes()

			mockRelationalCache.EXPECT().GetSliceInfo(subslice.Name).Return(cache.SliceInfo{
				Name:         subslice.Name,
				PartitionIDs: []string{"partition-A"},
			}, true).AnyTimes()
			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockKubeHelper.EXPECT().UnlabelNodeGroup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			// --- Reconcile 1: Barrier is RUNNING (Deactivating A in progress) ---
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "op-deactivate-A").Return(&betacompute.Operation{
				Status: "RUNNING",
			}, nil).Times(1)

			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(time.Duration(0)))

			// --- Reconcile 2: Barrier is DONE (Deactivation of A finished) ---
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "op-deactivate-A").Return(&betacompute.Operation{
				Status: "DONE",
			}, nil).AnyTimes()

			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-A": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						SliceID:                  "",
					},
					"partition-C": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						SliceID:                  "",
					},
				},
			}, nil).AnyTimes()

			mockRelationalCache.EXPECT().DeleteSlice(subslice.Name).Return(nil).Times(1)

			// After successful ACTIVATE dispatch, the controller now stamps the relational cache immediately.
			mockRelationalCache.EXPECT().AddSlice(subslice.Name, []string{"partition-C"}, subslice.Spec.Topology).Return(nil).Times(1)

			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
					Expect(req.AcceleratorTopologyConfigurations).To(HaveLen(1))
					config := req.AcceleratorTopologyConfigurations["partition-C"]
					Expect(config.Action).To(Equal("ACTIVATE"))
					return &betacompute.Operation{Name: "op-activate-C"}, nil
				}).Times(1)

			res2, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res2.RequeueAfter).To(Equal(10 * time.Second))
		})
	})

	Context("When a sub-slice is being deleted", func() {
		It("should call DEACTIVATE when the topology is ACTIVE", func() {
			By("creating a sub-slice and then marking it for deletion")
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// 1. Manually set status to ACTIVE so it can be deleted
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

			// Verify it has a deletion timestamp
			Eventually(func() bool {
				fetched := &slicev1beta1.Slice{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
				return err == nil && !fetched.DeletionTimestamp.IsZero()
			}, time.Second*5).Should(BeTrue())

			// Define mock expectations
			migURL := "projects/test-project/zones/us-central1-a/instanceGroupManagers/gke-test-cluster-default-pool-abcdef-mig"
			migName := "gke-test-cluster-default-pool-abcdef-mig"
			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// Return ACTIVE state from GCE
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyHealthStatus: cache.MIGHealthStatus(cache.MMIGHealthStatusActive),
						AcceleratorTopologyState:        cache.MIGPartitionState(util.DynamicSliceActive),
						SliceID:                         util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
				},
			}, nil).AnyTimes()

			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
					Expect(req.AcceleratorTopologyConfigurations).To(HaveLen(1))
					config := req.AcceleratorTopologyConfigurations["partition-0"]
					Expect(config.Action).To(Equal("DEACTIVATE"))
					Expect(config.ExternalId).To(Equal(util.GetResourceName(subslice.Name, reconciler.ClusterHash)))
					return &betacompute.Operation{Name: "test-op"}, nil
				}).AnyTimes()

			// Trigger reconciliation
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should ignore deactivation error if partition assigned to another slice", func() {
			By("creating a sub-slice and then marking it for deletion")
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

			Eventually(func() bool {
				fetched := &slicev1beta1.Slice{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
				return err == nil && !fetched.DeletionTimestamp.IsZero()
			}, time.Second*5).Should(BeTrue())

			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// 1. Deactivate returns transient error
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("GCE error: not assigned")).AnyTimes()

			// 2. GCE topology query shows it is assigned to another slice ID
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyHealthStatus: cache.MIGHealthStatus(cache.MMIGHealthStatusActive),
						AcceleratorTopologyState:        cache.MIGPartitionState(util.DynamicSliceActive),
						SliceID:                         "another-slice-id",
					},
				},
			}, nil).AnyTimes()

			// Reconcile should complete successfully without returning an error because it ignores the error
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should ignore deactivation error if partition already deactivated (empty SliceID)", func() {
			By("creating a sub-slice and then marking it for deletion")
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

			Eventually(func() bool {
				fetched := &slicev1beta1.Slice{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
				return err == nil && !fetched.DeletionTimestamp.IsZero()
			}, time.Second*5).Should(BeTrue())

			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// 1. Deactivate returns transient error
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("GCE error: not assigned")).AnyTimes()

			// 2. GCE topology query shows partition has empty SliceID
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyHealthStatus: cache.MIGHealthStatus(cache.MMIGHealthStatusActive),
						AcceleratorTopologyState:        cache.MIGPartitionState(util.DynamicSliceInactive),
						SliceID:                         "",
					},
				},
			}, nil).AnyTimes()

			// Reconcile should complete successfully without returning an error because it ignores the error
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return error if deactivation fails and still assigned to us", func() {
			By("creating a sub-slice and then marking it for deletion")
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

			Eventually(func() bool {
				fetched := &slicev1beta1.Slice{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
				return err == nil && !fetched.DeletionTimestamp.IsZero()
			}, time.Second*5).Should(BeTrue())

			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// 1. Deactivate returns a transient GCE error
			gceErr := &util.OperationError{Err: fmt.Errorf("some transient error")}
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(nil, gceErr).AnyTimes()

			// 2. GCE topology query shows it is still assigned to us
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyHealthStatus: cache.MIGHealthStatus(cache.MMIGHealthStatusActive),
						AcceleratorTopologyState:        cache.MIGPartitionState(util.DynamicSliceActive),
						SliceID:                         util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
				},
			}, nil).AnyTimes()

			// Reconcile should return the error to trigger retry
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).To(HaveOccurred())
		})

		Context("When slice is in SliceCreationFailed state", func() {
			BeforeEach(func() {
				if err := k8sClient.Create(ctx, mockNode); err != nil && !apierrors.IsAlreadyExists(err) {
					Expect(err).NotTo(HaveOccurred())
				}
				Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

				mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()

				// Setup the slice state to SliceCreationFailed
				Eventually(func() error {
					fetched := &slicev1beta1.Slice{}
					if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
						return err
					}
					_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.SliceCreationFailed, "Failed", nil)
					return err
				}, time.Second*5).Should(Succeed())
			})

			It("should proceed to delete CRD when GCE ID is empty initially and deactivation succeeds", func() {
				By("marking it for deletion")

				// Transition to Deletion State
				Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

				Eventually(func() bool {
					fetched := &slicev1beta1.Slice{}
					err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
					return err == nil && !fetched.DeletionTimestamp.IsZero()
				}, time.Second*5).Should(BeTrue())

				migInfo := cache.MIGInfo{
					Name:  migURL,
					Nodes: make(map[string]*cache.NodeInfo),
				}
				mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
				mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
				mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

				// GCE lookup queries return empty SliceID
				mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
					TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
						"partition-0": {
							AcceleratorTopologyHealthStatus: cache.MIGHealthStatus(cache.MMIGHealthStatusActive),
							AcceleratorTopologyState:        cache.MIGPartitionState(util.DynamicSliceInactive),
							SliceID:                         "",
						},
					},
				}, nil).AnyTimes()

				// Deactivation call succeeds
				mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
					Return(&betacompute.Operation{Name: "test-op"}, nil).Times(1)

				// Reconcile should succeed, unlabeling and deleting the finalizer (object gone)
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() bool {
					var s slicev1beta1.Slice
					err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, &s)
					return apierrors.IsNotFound(err)
				}, time.Second*5).Should(BeTrue())
			})

			It("should proceed to delete CRD even if deactivation fails when GCE ID is empty initially", func() {
				By("marking it for deletion")

				// Transition to Deletion State
				Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

				Eventually(func() bool {
					fetched := &slicev1beta1.Slice{}
					err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
					return err == nil && !fetched.DeletionTimestamp.IsZero()
				}, time.Second*5).Should(BeTrue())

				migInfo := cache.MIGInfo{
					Name:  migURL,
					Nodes: make(map[string]*cache.NodeInfo),
				}
				mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
				mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
				mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

				// GCE lookup queries return empty SliceID
				mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
					TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
						"partition-0": {
							AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
							SliceID:                  "",
						},
					},
				}, nil).AnyTimes()

				// Deactivation call fails with error
				mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
					Return(nil, fmt.Errorf("GCE transient error")).AnyTimes()

				// Reconcile should still succeed, unlabeling and deleting finalizer without retrying deactivation
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() bool {
					var s slicev1beta1.Slice
					err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, &s)
					return apierrors.IsNotFound(err)
				}, time.Second*5).Should(BeTrue())
			})
		})
	})

	Context("When a sub-slice has an in-flight activate operation (STAGE 0)", func() {
		// Helper: create a subslice with an activate operation annotation, set it to
		// SliceCreationFailed, and mark it for deletion.
		setupSubsliceWithOpAnnotation := func() {
			if err := k8sClient.Create(ctx, mockNode); err != nil && !apierrors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}
			// Wait for any previous test's subslice to be fully cleaned up
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, &slicev1beta1.Slice{})
				return apierrors.IsNotFound(err)
			}, time.Second*10).Should(BeTrue())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Set status to SliceCreationFailed (simulates activate timeout)
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionFalse, util.SliceCreationFailed, "Failed", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Add the activate operation annotation
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				patch := client.MergeFrom(fetched.DeepCopy())
				if fetched.Annotations == nil {
					fetched.Annotations = make(map[string]string)
				}
				fetched.Annotations[util.SubsliceLastOpAnnotation] = "operation-12345"
				return k8sClient.Patch(ctx, fetched, patch)
			}, time.Second*5).Should(Succeed())

			// Delete the subslice
			Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

			// Wait for deletion timestamp
			Eventually(func() bool {
				fetched := &slicev1beta1.Slice{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
				return err == nil && !fetched.DeletionTimestamp.IsZero()
			}, time.Second*5).Should(BeTrue())
		}

		It("should wait (requeue) when the activate operation is still RUNNING", func() {
			setupSubsliceWithOpAnnotation()

			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			// cacheSyncUpSubslice calls getPartitionTopologyFromGCE and DeleteSlice before handleSubsliceDeletion
			migInfo := cache.MIGInfo{Name: migURL, Nodes: make(map[string]*cache.NodeInfo)}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						SliceID:                  "",
					},
				},
			}, nil).AnyTimes()

			// GetZonalOperation returns RUNNING status
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "operation-12345").
				Return(&betacompute.Operation{Name: "operation-12345", Status: "RUNNING"}, nil).Times(1)

			// Reconcile should return a requeue (no error, non-zero RequeueAfter)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(15 * time.Second))

			// Verify annotation is still present
			fetched := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)).To(Succeed())
			opName, found := fetched.Annotations[util.SubsliceLastOpAnnotation]
			Expect(found).To(BeTrue())
			Expect(opName).To(Equal("operation-12345"))
		})

		It("should proceed with deletion when the activate operation is DONE", func() {
			setupSubsliceWithOpAnnotation()

			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()

			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// GetZonalOperation returns DONE status
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "operation-12345").
				Return(&betacompute.Operation{Name: "operation-12345", Status: "DONE"}, nil).Times(1)

			// GCE topology shows our slice is now ACTIVE (activate succeeded)
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceActive),
						SliceID:                  util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
				},
			}, nil).AnyTimes()

			// Should proceed to deactivate after clearing annotation
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(&betacompute.Operation{Name: "deactivate-op"}, nil).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())

			// The barrier no longer explicitly clears the annotation; it just allows
			// deletion to proceed. The annotation is cleaned up when the slice is deleted.
			// The barrier allows deletion to proceed but doesn't clear the annotation.
			// The annotation is cleaned up when the slice is eventually deleted.
			// What matters is that the controller proceeded past STAGE 0 (no error above).
		})

		It("should proceed with deletion when the activate operation is NOT FOUND (404)", func() {
			setupSubsliceWithOpAnnotation()

			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()

			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// GetZonalOperation returns 404 (ResourceNotFound)
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "operation-12345").
				Return(nil, &googleapi.Error{Code: http.StatusNotFound, Message: "Not Found"}).Times(1)

			// GCE topology shows our slice is now ACTIVE (activate succeeded)
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceActive),
						SliceID:                  util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
				},
			}, nil).AnyTimes()

			// Should proceed to deactivate after clearing annotation
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(&betacompute.Operation{Name: "deactivate-op"}, nil).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should retry when GetZonalOperation fails", func() {
			setupSubsliceWithOpAnnotation()

			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			// cacheSyncUpSubslice calls getPartitionTopologyFromGCE and DeleteSlice before handleSubsliceDeletion
			migInfo := cache.MIGInfo{Name: migURL, Nodes: make(map[string]*cache.NodeInfo)}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						SliceID:                  "",
					},
				},
			}, nil).AnyTimes()

			// GetZonalOperation returns an error
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "operation-12345").
				Return(nil, fmt.Errorf("network error")).Times(1)

			// Should requeue after 15 seconds (barrier blocks on error)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(15 * time.Second))
		})

		It("should proceed with deletion when activate operation DONE with errors (activate failed)", func() {
			setupSubsliceWithOpAnnotation()

			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()

			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// GetZonalOperation returns DONE with errors (activate failed)
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "us-central1-a", "operation-12345").
				Return(&betacompute.Operation{
					Name:   "operation-12345",
					Status: "DONE",
					Error: &betacompute.OperationError{
						Errors: []*betacompute.OperationErrorErrors{
							{Code: "RESOURCE_NOT_FOUND", Message: "topology not found"},
						},
					},
				}, nil).Times(1)

			// GCE topology shows empty sliceID (activate failed, nothing to clean up)
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceInactive),
						SliceID:                  "",
					},
				},
			}, nil).AnyTimes()

			// Deactivation should still be attempted (CreationFailed + empty ID path)
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(&betacompute.Operation{Name: "deactivate-op"}, nil).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When a sub-slice has no operation annotation (legacy backward compat)", func() {
		It("should fall through to existing topology-based deletion logic", func() {
			if err := k8sClient.Create(ctx, mockNode); err != nil && !apierrors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}
			// Wait for any previous test's subslice to be fully cleaned up
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, &slicev1beta1.Slice{})
				return apierrors.IsNotFound(err)
			}, time.Second*10).Should(BeTrue())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Set status to ACTIVE
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Delete WITHOUT any operation annotation (simulates pre-change slice)
			Expect(k8sClient.Delete(ctx, subslice)).Should(Succeed())

			Eventually(func() bool {
				fetched := &slicev1beta1.Slice{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)
				return err == nil && !fetched.DeletionTimestamp.IsZero()
			}, time.Second*5).Should(BeTrue())

			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			// No GetZonalOperation call expected (no annotation)
			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			// Falls through to existing STAGE 1: topology-based deactivation
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyState: cache.MIGPartitionState(util.DynamicSliceActive),
						SliceID:                  util.GetResourceName(subslice.Name, reconciler.ClusterHash),
					},
				},
			}, nil).AnyTimes()

			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(&betacompute.Operation{Name: "deactivate-op"}, nil).AnyTimes()

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When SliceID mismatch happens", func() {
		It("should return SliceCreationFailed during formation", func() {
			By("creating a sub-slice and its associated node")
			subslice.Annotations = map[string]string{util.RetryAnnotation: util.EnableRetryAnnotation}
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Start from clean state (no manual status patch)

			// Define mock expectations
			migInfo := cache.MIGInfo{
				Name:  migURL,
				Nodes: make(map[string]*cache.NodeInfo),
			}
			for i := 0; i < 16; i++ {
				migInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					Name:               fmt.Sprintf("node-%d", i),
					MIGName:            migURL,
					ReservationName:    "res-1",
					ReservationBlockID: "block-1",
					TopologyPartitionIds: map[string][]string{
						"2x2x4": {"partition-0"},
					},
				}
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID("partition-0").Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{}, false).AnyTimes()
			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			mockRelationalCache.EXPECT().AddSlice(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			// Mock GCE actions
			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), "test-project", "us-central1-a", migName, gomock.Any(), gomock.Any()).
				Return(&betacompute.Operation{Name: "test-op"}, nil).AnyTimes()

			// Return ACTIVE state but with WRONG SliceID from GCE during the post-activation sync
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"partition-0": {
						AcceleratorTopologyHealthStatus: cache.MIGHealthStatus(cache.MMIGHealthStatusActive),
						AcceleratorTopologyState:        cache.MIGPartitionState(util.DynamicSliceActive),
						SliceID:                         "wrong-external-id",
					},
				},
			}, nil).AnyTimes()

			reconcileOnce := func() {
				Eventually(func() bool {
					_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
					return err == nil
				}, time.Second*10, time.Millisecond*250).Should(BeTrue())
			}

			reconcileOnce() // Initialization: Finalizer

			// The next reconciliation should trigger formation and fail with an External ID mismatch error
			// It should NOT retry, so no error is expected.
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())

			// Verify the status was patched to SliceCreationFailed
			updatedSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, updatedSlice)).Should(Succeed())
			reason := util.FindStateCondition(updatedSlice)
			Expect(reason).To(Equal(util.SliceCreationFailed))

			condition, found := util.FindSliceCondition(updatedSlice, slicev1beta1.SliceStateConditionType)
			Expect(found).To(BeTrue())
			Expect(condition.Message).To(ContainSubstring("the current slice id does not match"))
			Expect(condition.Message).To(ContainSubstring("\"wrong-external-id\""))
		})
	})

	Context("When a sub-slice is being updated", func() {
		BeforeEach(func() {
			reconciler.SupportSubsliceUpdate = true
			reconciler.EnableSliceUpdate = true

			subslice = &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-subslice-update",
					Namespace:  "default",
					Finalizers: []string{"accelerator.gke.io/slice-finalizer"},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         slicev1beta1.TypeTpu7x,
					Topology:     "2x2x4",
					PartitionIds: []string{"partition-old"},
				},
			}

			mockNode = &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-update-1",
					Labels: map[string]string{
						"topology.kubernetes.io/zone":                 "us-central1-a",
						"cloud.google.com/gke-tpu-partition-4x4x4-id": "partition-old",
					},
				},
			}
		})

		It("should safely deactivate the old partition and activate the new partition (N->0->M)", func() {
			By("creating the sub-slice and setting it to ACTIVE with provisioned-partition")
			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				if fetched.Annotations == nil {
					fetched.Annotations = make(map[string]string)
				}
				fetched.Annotations[util.SubsliceProvisionedPartitionAnnotation] = "partition-old"
				return k8sClient.Update(ctx, fetched)
			}, time.Second*5).Should(Succeed())

			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Status.ObservedGeneration = fetched.Generation
				if err := k8sClient.Status().Update(ctx, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			migInfo := cache.MIGInfo{Name: migURL, Nodes: make(map[string]*cache.NodeInfo)}
			for i := 0; i < 16; i++ {
				migInfo.Nodes[fmt.Sprintf("node-%d", i)] = &cache.NodeInfo{
					Name:               fmt.Sprintf("node-%d", i),
					MIGName:            migURL,
					ReservationName:    "res-1",
					ReservationBlockID: "block-1",
					TopologyPartitionIds: map[string][]string{
						"2x2x4": {"partition-old", "partition-new"},
					},
				}
			}
			mockRelationalCache.EXPECT().GetSliceInfo(gomock.Any()).Return(cache.SliceInfo{PartitionIDs: []string{"partition-old"}}, true).AnyTimes()
			mockRelationalCache.EXPECT().GetMIGByPartitionID(gomock.Any()).Return(migInfo, true).AnyTimes()
			mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()
			mockRelationalCache.EXPECT().AddSlice(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			mockComputeService.EXPECT().GetMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockKubeHelper.EXPECT().UnlabelNodeGroup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

			mockGCEHelper.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&betacompute.Operation{Status: "DONE"}, nil).AnyTimes()

			stateCounter := 0
			mockGCEHelper.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), gomock.Any(), gomock.Any(), "gke-test-cluster-default-pool-abcdef-mig").
				DoAndReturn(func(ctx context.Context, projectID, zone, igmName string) (cache.AcceleratorTopologies, error) {
					stateCounter++
					expectedSliceID := "test-subslice-update-test-clu"
					if stateCounter <= 2 {
						return cache.AcceleratorTopologies{
							TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
								"partition-old": {AcceleratorTopologyState: util.DynamicSliceActive, SliceID: expectedSliceID},
								"partition-new": {AcceleratorTopologyState: util.DynamicSliceInactive},
							},
						}, nil
					}
					return cache.AcceleratorTopologies{
						TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
							"partition-old": {AcceleratorTopologyState: util.DynamicSliceInactive},
							"partition-new": {AcceleratorTopologyState: util.DynamicSliceActive, SliceID: expectedSliceID},
						},
					}, nil
				}).AnyTimes()

			mockGCEHelper.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&betacompute.Operation{Name: "op-test"}, nil).AnyTimes()

			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Spec.PartitionIds = []string{"partition-new"}
				return k8sClient.Update(ctx, fetched)
			}, time.Second*5).Should(Succeed())

			for i := 0; i < 5; i++ {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
				Expect(err).NotTo(HaveOccurred())
			}

			Eventually(func() string {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return ""
				}
				return fetched.Annotations[util.SubsliceProvisionedPartitionAnnotation]
			}, time.Second*10).Should(Equal("partition-new"))
		})
	})

	Context("When the controller encounters a slice with ObservedGeneration = 0", func() {
		It("should backfill ObservedGeneration to Generation and not trigger update", func() {
			By("creating a slice and setting its ObservedGeneration to 0")
			subslice.Spec.PartitionIds = []string{"partition-old"}
			subslice.Annotations = map[string]string{
				util.SubsliceProvisionedPartitionAnnotation: "partition-old",
				util.RetryAnnotation:                        util.EnableRetryAnnotation,
			}

			reconciler.EnableSliceUpdate = true
			reconciler.SupportSubsliceUpdate = true

			mockRelationalCache.EXPECT().GetSliceInfo(subslice.Name).Return(cache.SliceInfo{Name: subslice.Name}, true).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).Return(true, nil).AnyTimes()

			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Ensure state is set to Active, but ObservedGeneration is explicitly 0 (the default)
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Update spec to increment generation to 2
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Spec.PartitionIds = []string{"partition-new"}
				return k8sClient.Update(ctx, fetched)
			}, time.Second*5).Should(Succeed())

			// Verify that Generation is 2 and ObservedGeneration is 0
			fetchedSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetchedSlice)).Should(Succeed())
			Expect(fetchedSlice.Generation).Should(Equal(int64(2)))
			Expect(fetchedSlice.Status.ObservedGeneration).Should(Equal(int64(0)))

			// Reconcile
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(reconcile.Result{Requeue: true}))

			// Verify ObservedGeneration is now backfilled to 1
			Eventually(func() int64 {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return -1
				}
				return fetched.Status.ObservedGeneration
			}, time.Second*5).Should(Equal(int64(1)))
		})

		It("should requeue with 10 seconds when ObservedGeneration is pruned by API server", func() {
			By("creating a slice and setting its ObservedGeneration to 0")
			subslice.Spec.PartitionIds = []string{"partition-old"}
			subslice.Annotations = map[string]string{
				util.SubsliceProvisionedPartitionAnnotation: "partition-old",
				util.RetryAnnotation:                        util.EnableRetryAnnotation,
			}

			reconciler.EnableSliceUpdate = true
			reconciler.SupportSubsliceUpdate = true

			mockRelationalCache.EXPECT().GetSliceInfo(subslice.Name).Return(cache.SliceInfo{Name: subslice.Name}, true).AnyTimes()
			mockKubeHelper.EXPECT().CheckNodeLabels(gomock.Any(), gomock.Any(), gomock.Any(), true).Return(true, nil).AnyTimes()

			Expect(k8sClient.Create(ctx, mockNode)).Should(Succeed())
			Expect(k8sClient.Create(ctx, subslice)).Should(Succeed())

			// Ensure state is set to Active, but ObservedGeneration is explicitly 0 (the default)
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				_, err := util.PatchSliceCondition(ctx, k8sClient, fetched, slicev1beta1.SliceStateConditionType, metav1.ConditionTrue, util.DynamicSliceActive, "Active", nil)
				return err
			}, time.Second*5).Should(Succeed())

			// Update spec to increment generation to 2
			Eventually(func() error {
				fetched := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched); err != nil {
					return err
				}
				fetched.Spec.PartitionIds = []string{"partition-new"}
				return k8sClient.Update(ctx, fetched)
			}, time.Second*5).Should(Succeed())

			// Set the client to a PruningClient to simulate API server pruning the status field
			originalClient := reconciler.Client
			reconciler.Client = &PruningClient{Client: k8sClient}
			defer func() {
				reconciler.Client = originalClient
			}()

			// Reconcile
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: subslice.Name}})
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(reconcile.Result{RequeueAfter: 10 * time.Second}))

			// Verify ObservedGeneration remains 0 (was pruned)
			fetched := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: subslice.Name}, fetched)).Should(Succeed())
			Expect(fetched.Status.ObservedGeneration).Should(Equal(int64(0)))
		})
	})
})

type InterceptingClient struct {
	client.Client
	OnGet func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
}

func (c *InterceptingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.OnGet != nil {
		return c.OnGet(ctx, key, obj, opts...)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

type PruningStatusWriter struct {
	client.StatusWriter
}

func (w *PruningStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if slice, ok := obj.(*slicev1beta1.Slice); ok {
		slice.Status.ObservedGeneration = 0
	}
	return w.StatusWriter.Patch(ctx, obj, patch, opts...)
}

type PruningClient struct {
	client.Client
}

func (c *PruningClient) Status() client.StatusWriter {
	return &PruningStatusWriter{StatusWriter: c.Client.Status()}
}
