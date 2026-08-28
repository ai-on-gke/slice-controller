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

package health

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	slicev1beta1 "slice-controller/api/v1beta1"
	internalcache "slice-controller/internal/cache"
	internalmetrics "slice-controller/internal/metrics"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	clusterHash                = "test-hash"
	testClusterName            = "test-cluster"
	partitionStatusMetricType  = "kubernetes.io/master/slice_controller/partition_status"
	partitionFailureMetricType = "kubernetes.io/master/slice_controller/slice_partition_failure_total"
)

// --- Handwritten Mocks (Requested by User) ---

type mockStatusWriter struct {
	client.SubResourceWriter
	UpdateFunc func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error
	PatchFunc  func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error
}

func (m *mockStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if m.UpdateFunc != nil {
		return m.UpdateFunc(ctx, obj, opts...)
	}
	panic("mockStatusWriter.Update called but UpdateFunc not set")
}

func (m *mockStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if m.PatchFunc != nil {
		return m.PatchFunc(ctx, obj, patch, opts...)
	}
	panic("mockStatusWriter.Patch called but PatchFunc not set")
}

type mockK8sClient struct {
	client.Client
	GetFunc    func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
	ListFunc   func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
	StatusFunc func() client.StatusWriter
	PatchFunc  func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error
}

func (m *mockK8sClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, key, obj, opts...)
	}
	panic("mockK8sClient.Get called but GetFunc not set")
}

func (m *mockK8sClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if m.ListFunc != nil {
		return m.ListFunc(ctx, list, opts...)
	}
	panic("mockK8sClient.List called but ListFunc not set")
}

func (m *mockK8sClient) Status() client.StatusWriter {
	if m.StatusFunc != nil {
		return m.StatusFunc()
	}
	panic("mockK8sClient.Status called but StatusFunc not set")
}

func (m *mockK8sClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if m.PatchFunc != nil {
		return m.PatchFunc(ctx, obj, patch, opts...)
	}
	panic("mockK8sClient.Patch called but PatchFunc not set")
}

// --- Hybrid Test Setup ---

// reconcilerMocks holds mixed mocks: handwritten for K8s client, gomock for others.
type reconcilerMocks struct {
	ctrl            *gomock.Controller
	client          *mockK8sClient
	statusWriter    *mockStatusWriter
	kubeHelper      *mocks.MockClient
	relationalCache *mocks.MockReconcilerRelationalCacheInterface
	healthCache     *mocks.MockReconcilerHealthCacheInterface
}

func newTestReconciler(t *testing.T) (*HealthReconciler, *reconcilerMocks) {
	ctrl := gomock.NewController(t)

	// Handwritten mocks
	statusWriter := &mockStatusWriter{}
	k8sClient := &mockK8sClient{
		StatusFunc: func() client.StatusWriter {
			return statusWriter
		},
		GetFunc: func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return apierrors.NewNotFound(schema.GroupResource{}, key.Name)
		},
		PatchFunc: func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return nil
		},
	}

	// Gomock mocks
	kubeClient := mocks.NewMockClient(ctrl)
	relationalCache := mocks.NewMockReconcilerRelationalCacheInterface(ctrl)
	relationalCache.EXPECT().GetBlockID(gomock.Any()).Return("test-block", true).AnyTimes()
	healthCache := mocks.NewMockReconcilerHealthCacheInterface(ctrl)

	// Initialize a real workqueue for the test (or a mock if strictly unit testing, but real is easier here)
	rateLimiter := workqueue.NewTypedItemExponentialFailureRateLimiter[WorkItem](
		workqueueBaseDelay,
		workqueueMaxDelay,
	)
	wq := workqueue.NewTypedRateLimitingQueue(rateLimiter)

	reconciler := &HealthReconciler{
		client:           k8sClient,
		KubeClient:       kubeClient,
		relationalCache:  relationalCache,
		healthCache:      healthCache,
		workqueue:        wq,
		enableSubSlicing: true,
	}

	m := &reconcilerMocks{
		ctrl:            ctrl,
		client:          k8sClient,
		statusWriter:    statusWriter,
		kubeHelper:      kubeClient,
		relationalCache: relationalCache,
		healthCache:     healthCache,
	}

	return reconciler, m
}

// --- Tests ---

func TestProcessWorkItem(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("should return non-retryable for unknown type", func(t *testing.T) {
		t.Parallel()
		r, _ := newTestReconciler(t)

		item := WorkItem{ResourceType: "INVALID_TYPE", ResourceURL: "url-1"}
		err := r.processWorkItem(ctx, item)

		require.Error(t, err)
		assert.True(t, IsTerminal(err), "Expected error to be terminal/non-retryable")
	})
}

func TestReconcileMIG(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	migURL := "projects/test-project/zones/test-zone/instanceGroupManagers/mig-1"

	const numNodesInMig = 16
	topologyStateLabel := "cloud.google.com/gke-tpu-partition-4x4x4-state"
	migInfo := internalcache.MIGInfo{
		Nodes: make(map[string]*internalcache.NodeInfo),
	}
	// Populate the map
	for i := 0; i < numNodesInMig; i++ {
		name := fmt.Sprintf("node-%d", i)
		migInfo.Nodes[name] = &internalcache.NodeInfo{
			Name:                 name,
			NodePoolName:         "pool-1",
			TopologyPartitionIds: map[string][]string{"4x4x4": {"p1"}},
		}
	}

	t.Run("should apply Healthy state", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.clusterHash = clusterHash

		health := internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{CurrentStatus: internalcache.MIGHealthStatusHealthy}
		toposHealth := map[string]internalcache.AcceleratorTopologyInfo{"p1": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusHealthy}}

		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(health, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(toposHealth, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("p1").Return("slice-1", true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo("slice-1").Return(internalcache.SliceInfo{Name: "slice-1", Topology: "4x4x4"}, true).AnyTimes()

		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{topologyStateLabel: healthStatusHealthy},
			gomock.Any(),
		).Return(nil).Times(1)
		err := r.reconcileMIG(ctx, migURL)
		require.NoError(t, err)
	})

	t.Run("should apply Degraded state", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.clusterHash = clusterHash

		health := internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{CurrentStatus: internalcache.MIGHealthStatusDegraded}
		toposHealth := map[string]internalcache.AcceleratorTopologyInfo{"p1": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusDegraded}}

		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(health, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(toposHealth, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("p1").Return("slice-1", true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo("slice-1").Return(internalcache.SliceInfo{Name: "slice-1", Topology: "4x4x4"}, true).AnyTimes()

		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{topologyStateLabel: healthStatusDegraded},
			gomock.Any(),
		).Return(nil).Times(1)

		err := r.reconcileMIG(ctx, migURL)
		require.NoError(t, err)
	})

	t.Run("should apply UNKNOWN state", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.clusterHash = clusterHash

		health := internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{CurrentStatus: internalcache.MIGHealthStatusUnknown}
		toposHealth := map[string]internalcache.AcceleratorTopologyInfo{"p1": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusUnknown}}

		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(health, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(toposHealth, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("p1").Return("slice-1", true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo("slice-1").Return(internalcache.SliceInfo{Name: "slice-1", Topology: "4x4x4"}, true).AnyTimes()

		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{topologyStateLabel: healthStatusUnknown},
			gomock.Any(),
		).Return(nil).Times(1)

		err := r.reconcileMIG(ctx, migURL)
		require.NoError(t, err)
	})

	t.Run("should apply Unhealthy state if MIG missing in health cache", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)

		// 1. Relational Cache Hit (We find the nodes)
		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfo, true)

		// 2. Health Cache Miss
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(
			internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{},
			false,
		)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(nil, false)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("p1").Return("slice-1", true).AnyTimes()

		// 3. Expect Label call with Incomplete status (Updated Logic)
		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{topologyStateLabel: healthStatusIncomplete},
			gomock.Any(),
		).Return(nil).Times(1)

		err := r.reconcileMIG(ctx, migURL)
		require.NoError(t, err)
	})

	t.Run("should return retryable if KubeClient fails", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.clusterHash = clusterHash

		apiErr := errors.New("API server is down")
		health := internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{CurrentStatus: internalcache.MIGHealthStatusHealthy}
		toposHealth := map[string]internalcache.AcceleratorTopologyInfo{"p1": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusHealthy}}

		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(health, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(toposHealth, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("p1").Return("slice-1", true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo("slice-1").Return(internalcache.SliceInfo{Name: "slice-1", Topology: "4x4x4"}, true).AnyTimes()

		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(apiErr)

		err := r.reconcileMIG(ctx, migURL)
		require.Error(t, err)
		assert.False(t, IsTerminal(err), "Expected error to be retryable (NOT terminal)")
	})

	t.Run("should apply multi-level labels for subslice correctly", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)

		// Create a node that belongs to multiple topology levels (subslice hierarchy)
		nodeName := "subslice-node-1"
		partitionIDs := map[string][]string{
			util.Topology2x2x1: {"uid-2x2x1"},
			util.Topology2x2x2: {"uid-2x2x2"},
			util.Topology4x4x4: {"uid-4x4x4"},
		}

		migInfoSub := internalcache.MIGInfo{
			Nodes: map[string]*internalcache.NodeInfo{
				nodeName: {
					Name:                 nodeName,
					TopologyPartitionIds: partitionIDs,
				},
			},
		}
		// Fill to 16 nodes to avoid forcing INCOMPLETE status
		for i := 1; i < 16; i++ {
			name := fmt.Sprintf("node-%d", i)
			migInfoSub.Nodes[name] = &internalcache.NodeInfo{Name: name, TopologyPartitionIds: partitionIDs}
		}

		toposHealth := map[string]internalcache.AcceleratorTopologyInfo{
			"uid-2x2x1": {AcceleratorTopology: "2x2x1", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusHealthy},
			"uid-2x2x2": {AcceleratorTopology: "2x2x2", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusDegraded},
			"uid-4x4x4": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusHealthy},
		}

		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfoSub, true)
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{CurrentStatus: internalcache.MIGHealthStatusHealthy}, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(toposHealth, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID(gomock.Any()).Return("slice-1", true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo("slice-1").Return(internalcache.SliceInfo{Name: "slice-1", Topology: "4x4x4"}, true).AnyTimes()

		// Expect separate calls for each sub-partition level
		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{"cloud.google.com/gke-tpu-partition-2x2x1-state": healthStatusHealthy},
			gomock.Any(),
		).Return(nil)
		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{"cloud.google.com/gke-tpu-partition-2x2x2-state": healthStatusDegraded},
			gomock.Any(),
		).Return(nil)

		// Expect one MIG-level call for 4x4x4
		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-state": healthStatusHealthy},
			gomock.Any(),
		).Return(nil)

		err := r.reconcileMIG(ctx, migURL)
		require.NoError(t, err)
	})

	t.Run("should handle mixed health states across different sub-topologies", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)

		// Two nodes belonging to different 2x2x1 partitions but the same 4x4x4 partition
		node1Name := "node-1"
		node2Name := "node-2"
		rootUID := "uid-root"

		migInfoMixed := internalcache.MIGInfo{
			Nodes: map[string]*internalcache.NodeInfo{
				node1Name: {
					Name: node1Name,
					TopologyPartitionIds: map[string][]string{
						util.Topology2x2x1: {"uid-p1"},
						util.Topology4x4x4: {rootUID},
					},
				},
				node2Name: {
					Name: node2Name,
					TopologyPartitionIds: map[string][]string{
						util.Topology2x2x1: {"uid-p2"},
						util.Topology4x4x4: {rootUID},
					},
				},
			},
		}
		// Fill to 16 nodes total (including node-1 and node-2)
		for i := 3; i <= 16; i++ {
			name := fmt.Sprintf("node-%d", i)
			migInfoMixed.Nodes[name] = &internalcache.NodeInfo{
				Name:                 name,
				TopologyPartitionIds: map[string][]string{util.Topology4x4x4: {rootUID}},
			}
		}

		toposHealth := map[string]internalcache.AcceleratorTopologyInfo{
			"uid-p1": {AcceleratorTopology: "2x2x1", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusHealthy},
			"uid-p2": {AcceleratorTopology: "2x2x1", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusUnhealthy},
			rootUID:  {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusDegraded},
		}

		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfoMixed, true)
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{CurrentStatus: internalcache.MIGHealthStatusHealthy}, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(toposHealth, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID(gomock.Any()).Return("slice-1", true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo("slice-1").Return(internalcache.SliceInfo{Name: "slice-1", Topology: "4x4x4"}, true).AnyTimes()

		// Expect separate calls for each partition UID
		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{"cloud.google.com/gke-tpu-partition-2x2x1-state": healthStatusHealthy},
			[]string{node1Name},
		).Return(nil)

		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{"cloud.google.com/gke-tpu-partition-2x2x1-state": healthStatusUnhealthy},
			[]string{node2Name},
		).Return(nil)

		// Expect one MIG-level call for 4x4x4 (from MIG healthStatus, which is HEALTHY in this test)
		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(
			gomock.Any(),
			map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-state": healthStatusHealthy},
			gomock.Any(),
		).Return(nil)

		err := r.reconcileMIG(ctx, migURL)
		require.NoError(t, err)
	})

	t.Run("should still reconcile MIG when subslicing is disabled", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.enableSubSlicing = false

		m.relationalCache.EXPECT().GetMIG(migURL).Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth(migURL).Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{CurrentStatus: internalcache.MIGHealthStatusHealthy}, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus(migURL).Return(map[string]internalcache.AcceleratorTopologyInfo{"p1": {AcceleratorTopology: "4x4x4"}}, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("p1").Return("slice-1", true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo("slice-1").Return(internalcache.SliceInfo{Name: "slice-1", Topology: "4x4x4"}, true).AnyTimes()

		m.kubeHelper.EXPECT().LabelNodeGroupByNodeNames(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

		err := r.reconcileMIG(ctx, migURL)
		require.NoError(t, err)
	})
}

func TestReconcileMMIG(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Setup consistent naming to pass extraction logic
	const (
		clusterHash = "test-hash"
		sliceName   = "test-slice-" + clusterHash
		mmigName    = sliceName
	)
	mmigURL := fmt.Sprintf("projects/p/regions/r/multiMigs/%s", mmigName)
	conflictErr := apierrors.NewConflict(schema.GroupResource{}, sliceName, errors.New("conflict"))
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{}, sliceName)

	t.Run("should update slice status with retry on conflict", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)

		// 2. Set Hash
		r.clusterHash = clusterHash

		health := internalcache.TimestampedHealth[internalcache.MMIGHealthStatus]{CurrentStatus: internalcache.MMIGHealthStatusFailed}
		sliceInfo := internalcache.SliceInfo{Name: sliceName}
		slice := &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: sliceName}}

		m.healthCache.EXPECT().GetMMIGHealth(mmigURL).Return(health, true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo(sliceName).Return(sliceInfo, true).AnyTimes()

		getCalls := 0
		m.client.GetFunc = func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			getCalls++
			if key.Name != sliceName {
				return fmt.Errorf("unexpected key name: %s", key.Name)
			}
			slice.DeepCopyInto(obj.(*slicev1beta1.Slice))
			return nil
		}

		patchCalls := 0
		m.statusWriter.PatchFunc = func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patchCalls++
			if patchCalls == 1 {
				return conflictErr
			}
			return nil
		}

		err := r.reconcileMMIG(ctx, mmigURL)

		require.NoError(t, err)
		assert.Equal(t, 2, getCalls, "client.Get should be called twice")
		assert.Equal(t, 2, patchCalls, "client.Status().Patch should be called twice")
	})

	t.Run("should fail after max retries", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.clusterHash = clusterHash

		health := internalcache.TimestampedHealth[internalcache.MMIGHealthStatus]{CurrentStatus: internalcache.MMIGHealthStatusFailed}
		sliceInfo := internalcache.SliceInfo{Name: sliceName}

		m.healthCache.EXPECT().GetMMIGHealth(mmigURL).Return(health, true).AnyTimes()
		m.relationalCache.EXPECT().GetSliceInfo(sliceName).Return(sliceInfo, true).AnyTimes()

		m.client.GetFunc = func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			(&slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: sliceName}}).DeepCopyInto(obj.(*slicev1beta1.Slice))
			return nil
		}

		m.statusWriter.PatchFunc = func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return conflictErr
		}

		err := r.reconcileMMIG(ctx, mmigURL)

		require.Error(t, err)
		assert.True(t, apierrors.IsConflict(err))
	})

	t.Run("should return non-retryable error if slice not found in API", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.clusterHash = clusterHash

		health := internalcache.TimestampedHealth[internalcache.MMIGHealthStatus]{CurrentStatus: internalcache.MMIGHealthStatusFailed}
		sliceInfo := internalcache.SliceInfo{Name: sliceName}

		m.healthCache.EXPECT().GetMMIGHealth(mmigURL).Return(health, true)
		m.relationalCache.EXPECT().GetSliceInfo(sliceName).Return(sliceInfo, true)

		m.client.GetFunc = func(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return notFoundErr
		}

		err := r.reconcileMMIG(ctx, mmigURL)

		require.Error(t, err)
		assert.True(t, IsTerminal(err))
	})

	t.Run("should NOT skip reconciliation if disabled (MMIG is for super-slices)", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)
		r.enableSubSlicing = false

		health := internalcache.TimestampedHealth[internalcache.MMIGHealthStatus]{CurrentStatus: internalcache.MMIGHealthStatusActive}
		sliceInfo := internalcache.SliceInfo{Name: sliceName, Topology: "4x4x4"}

		m.healthCache.EXPECT().GetMMIGHealth(mmigURL).Return(health, true)
		m.relationalCache.EXPECT().GetSliceInfo(sliceName).Return(sliceInfo, true)

		m.client.GetFunc = func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			(&slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: sliceName}}).DeepCopyInto(obj.(*slicev1beta1.Slice))
			return nil
		}
		m.statusWriter.PatchFunc = func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return nil
		}

		err := r.reconcileMMIG(ctx, mmigURL)
		require.NoError(t, err)
	})
}

func TestEnqueueAllMIGs(t *testing.T) {
	t.Parallel()
	r, m := newTestReconciler(t)

	// 1. Setup Mock Data
	keys := []string{"mig-1", "mig-2", "mig-3"}

	// 2. Expectation: The Reconciler should ask the Relational Cache for all MIG names
	m.relationalCache.EXPECT().GetAllMIGNames().Return(keys)

	// 3. Execute
	r.enqueueAllMIGs()

	// 4. Verify
	assert.Equal(t, 3, r.workqueue.Len(), "Queue should contain 3 items")

	// Drain the queue to verify contents
	foundKeys := make(map[string]bool)
	for r.workqueue.Len() > 0 {
		item, _ := r.workqueue.Get()

		// Validation
		assert.Equal(t, MIG, item.ResourceType)
		foundKeys[item.ResourceURL] = true

		// Mark as done to clean up
		r.workqueue.Done(item)
	}

	// Ensure all expected keys were found
	assert.True(t, foundKeys["mig-1"])
	assert.True(t, foundKeys["mig-2"])
	assert.True(t, foundKeys["mig-3"])
}

func TestReportAllMetrics(t *testing.T) {
	ctx := context.Background()
	projectNumber := "12345"
	location := testRegion
	clusterName := testClusterName

	t.Run("should report slice and partition metrics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, m := newTestReconciler(t)
		r.metricsExporter = exporter
		r.enableSubSlicing = false

		// Mock slices
		slice := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name: "slice-1",
				UID:  "uid-1",
			},
			Spec: slicev1beta1.SliceSpec{
				Type:         "v4",
				Topology:     "4x4x4",
				PartitionIds: []string{"pid-1"},
			},
			Status: slicev1beta1.SliceStatus{
				Conditions: []metav1.Condition{
					{
						Type:   slicev1beta1.SliceStateConditionType,
						Status: metav1.ConditionTrue,
						Reason: "ACTIVE",
					},
				},
			},
		}
		m.client.ListFunc = func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
			sliceList := list.(*slicev1beta1.SliceList)
			sliceList.Items = []slicev1beta1.Slice{*slice}
			return nil
		}

		// Mock MIGs
		m.relationalCache.EXPECT().GetAllMIGNames().Return([]string{"mig-1"})
		migInfo := internalcache.MIGInfo{
			Name: "mig-1",
			Nodes: map[string]*internalcache.NodeInfo{
				"node-1": {
					Name:               "node-1",
					NodePoolName:       "pool-1",
					ReservationBlockID: "block-1",
					TopologyPartitionIds: map[string][]string{
						util.Topology4x4x4: {"pid-1"},
					},
				},
			},
		}
		m.relationalCache.EXPECT().GetMIG("mig-1").Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth("mig-1").Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{
			CurrentStatus: internalcache.MIGHealthStatusHealthy,
		}, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("pid-1").Return("slice-1", true)

		r.reportSliceMetrics(ctx)
		r.reportPartitionMetrics(ctx)
	})
}

func TestReportPartitionMetrics_MissingSlice(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	projectNumber := "12345"
	location := testRegion
	clusterName := testClusterName

	t.Run("should report partition metrics with node accelerator type when slice is missing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, m := newTestReconciler(t)
		r.metricsExporter = exporter
		r.enableSubSlicing = false

		// Mock empty slice list
		m.client.ListFunc = func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
			return nil
		}

		// Mock MIGs
		m.relationalCache.EXPECT().GetAllMIGNames().Return([]string{"mig-1"})
		migInfo := internalcache.MIGInfo{
			Name: "mig-1",
			Nodes: map[string]*internalcache.NodeInfo{
				"node-1": {
					Name:               "node-1",
					NodePoolName:       "pool-1",
					ReservationBlockID: "block-1",
					AcceleratorType:    "v4-fallback",
					TopologyPartitionIds: map[string][]string{
						"4x4x4": {"pid-1"},
					},
				},
			},
		}
		m.relationalCache.EXPECT().GetMIG("mig-1").Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth("mig-1").Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{
			CurrentStatus: internalcache.MIGHealthStatusHealthy,
		}, true)
		m.relationalCache.EXPECT().GetSliceNameByPartitionID("pid-1").Return("slice-1", true)

		r.reportPartitionMetrics(ctx)
	})
}

func TestReportPartitionMetrics_SubSlicing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	projectNumber := "12345"
	location := testRegion
	clusterName := testClusterName

	t.Run("should report partition metrics with partition specific status in sub-slicing mode", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, m := newTestReconciler(t)
		r.metricsExporter = exporter
		r.enableSubSlicing = true

		// Mock empty slice list
		m.client.ListFunc = func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
			return nil
		}

		// Mock MIGs
		m.relationalCache.EXPECT().GetAllMIGNames().Return([]string{"mig-1"})
		migInfo := internalcache.MIGInfo{
			Name: "mig-1",
			Nodes: map[string]*internalcache.NodeInfo{
				"node-1": {
					Name:               "node-1",
					NodePoolName:       "pool-1",
					ReservationBlockID: "block-1",
					AcceleratorType:    "v4",
					TopologyPartitionIds: map[string][]string{
						util.Topology2x2x1: {"pid-1"},
					},
				},
			},
		}
		m.relationalCache.EXPECT().GetMIG("mig-1").Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth("mig-1").Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{
			CurrentStatus: internalcache.MIGHealthStatusHealthy,
		}, true)

		// Mock partition-specific health in sub-slicing mode
		toposHealth := map[string]internalcache.AcceleratorTopologyInfo{
			"pid-1": {
				AcceleratorTopology:             util.Topology2x2x1,
				AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusUnhealthy,
			},
		}
		m.healthCache.EXPECT().GetMIGPartitionsStatus("mig-1").Return(toposHealth, true)

		m.relationalCache.EXPECT().GetSliceNameByPartitionID("pid-1").Return("slice-1", true)

		r.reportPartitionMetrics(ctx)
	})
}

func TestCheckAndRecordPartitionFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	projectNumber := "12345"
	location := testRegion
	clusterName := testClusterName

	t.Run("should record unhealthy partition and not MISSED", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, m := newTestReconciler(t)
		r.metricsExporter = exporter
		r.enableSubSlicing = false

		migInfo := internalcache.MIGInfo{
			Name: "mig-1",
			Nodes: map[string]*internalcache.NodeInfo{
				"node-1": {NodePoolName: "pool-1"},
			},
		}
		m.relationalCache.EXPECT().GetMIGByPartitionID("pid-1").Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth("mig-1").Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{
			CurrentStatus: internalcache.MIGHealthStatusUnhealthy,
		}, true)

		r.checkAndRecordPartitionFailures(ctx, []string{"pid-1"}, "slice-1", "test-ns", "uid-1")
		exporter.ExportCumulativeFailures()
	})

	t.Run("should record UNIDENTIFIED_CAUSE if all partitions are healthy", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, m := newTestReconciler(t)
		r.metricsExporter = exporter
		r.enableSubSlicing = false

		migInfo := internalcache.MIGInfo{
			Name: "mig-1",
			Nodes: map[string]*internalcache.NodeInfo{
				"node-1": {NodePoolName: "pool-1"},
			},
		}
		m.relationalCache.EXPECT().GetMIGByPartitionID("pid-1").Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGHealth("mig-1").Return(internalcache.TimestampedHealth[internalcache.MIGHealthStatus]{
			CurrentStatus: internalcache.MIGHealthStatusHealthy,
		}, true)

		r.checkAndRecordPartitionFailures(ctx, []string{"pid-1"}, "slice-1", "test-ns", "uid-1")
		exporter.ExportCumulativeFailures()
	})

	t.Run("should record UNKNOWN if MIG not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, m := newTestReconciler(t)
		r.metricsExporter = exporter
		r.enableSubSlicing = false

		m.relationalCache.EXPECT().GetMIGByPartitionID("pid-1").Return(internalcache.MIGInfo{}, false)

		r.checkAndRecordPartitionFailures(ctx, []string{"pid-1"}, "slice-1", "test-ns", "uid-1")
		exporter.ExportCumulativeFailures()
	})

	t.Run("should record nothing if partitionIDs is empty", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, _ := newTestReconciler(t)
		r.metricsExporter = exporter

		r.checkAndRecordPartitionFailures(ctx, []string{}, "slice-1", "test-ns", "uid-1")
		exporter.ExportCumulativeFailures()
	})

	t.Run("should use partition health if sub-slicing is enabled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		exporter := internalmetrics.NewMetricsExporter(projectNumber, location, clusterName)
		r, m := newTestReconciler(t)
		r.metricsExporter = exporter
		r.enableSubSlicing = true

		migInfo := internalcache.MIGInfo{
			Name: "mig-1",
			Nodes: map[string]*internalcache.NodeInfo{
				"node-1": {NodePoolName: "pool-1"},
			},
		}
		m.relationalCache.EXPECT().GetMIGByPartitionID("pid-1").Return(migInfo, true)
		m.healthCache.EXPECT().GetMIGPartitionsStatus("mig-1").Return(map[string]internalcache.AcceleratorTopologyInfo{
			"pid-1": {AcceleratorTopologyHealthStatus: internalcache.MIGHealthStatusDegraded},
		}, true)

		r.checkAndRecordPartitionFailures(ctx, []string{"pid-1"}, "slice-1", "test-ns", "uid-1")
		exporter.ExportCumulativeFailures()
	})
}

func TestUpdatePartitionSliceWithRetry_Guard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	partitionID := "pid-1"
	sliceName := "slice-1"

	t.Run("should skip status update to INACTIVE if slice recently transitioned to ACTIVATING (< 2 min ago)", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)

		m.relationalCache.EXPECT().GetSliceNameByPartitionID(partitionID).Return(sliceName, true)

		// Create slice currently in ACTIVATING state, transitioned 10 seconds ago
		slice := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name: sliceName,
			},
			Status: slicev1beta1.SliceStatus{
				Conditions: []metav1.Condition{
					{
						Type:               slicev1beta1.SliceStateConditionType,
						Status:             metav1.ConditionFalse,
						Reason:             string(util.DynamicSliceActivating),
						LastTransitionTime: metav1.Time{Time: time.Now().Add(-10 * time.Second)},
					},
				},
			},
		}

		m.client.GetFunc = func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			assert.Equal(t, sliceName, key.Name)
			slice.DeepCopyInto(obj.(*slicev1beta1.Slice))
			return nil
		}

		// We expect NO patch/update calls to the Kubernetes status client because the guard returns nil early!
		m.statusWriter.PatchFunc = func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			t.Fatalf("Status Patch should not be called when guard is active")
			return nil
		}

		partitionInfo := internalcache.AcceleratorTopologyInfo{
			AcceleratorTopologyState: internalcache.MIGPartitionStateInactive,
		}

		err := r.updatePartitionSliceWithRetry(ctx, partitionID, partitionInfo)
		require.NoError(t, err)
	})

	t.Run("should proceed with status update to INACTIVE if slice transitioned to ACTIVATING a long time ago (> 2 min ago)", func(t *testing.T) {
		t.Parallel()
		r, m := newTestReconciler(t)

		m.relationalCache.EXPECT().GetSliceNameByPartitionID(partitionID).Return(sliceName, true)

		// Create slice currently in ACTIVATING state, transitioned 3 minutes ago
		slice := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name: sliceName,
			},
			Status: slicev1beta1.SliceStatus{
				Conditions: []metav1.Condition{
					{
						Type:               slicev1beta1.SliceStateConditionType,
						Status:             metav1.ConditionFalse,
						Reason:             string(util.DynamicSliceActivating),
						LastTransitionTime: metav1.Time{Time: time.Now().Add(-3 * time.Minute)},
					},
				},
			},
		}

		m.client.GetFunc = func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			assert.Equal(t, sliceName, key.Name)
			slice.DeepCopyInto(obj.(*slicev1beta1.Slice))
			return nil
		}

		// We expect status Patch to be called since the 2 minute guard window has expired!
		patchCalled := false
		m.statusWriter.PatchFunc = func(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patchCalled = true
			return nil
		}

		partitionInfo := internalcache.AcceleratorTopologyInfo{
			AcceleratorTopologyState: internalcache.MIGPartitionStateInactive,
		}

		err := r.updatePartitionSliceWithRetry(ctx, partitionID, partitionInfo)
		require.NoError(t, err)
		assert.True(t, patchCalled, "Expected status Patch to be called")
	})
}
