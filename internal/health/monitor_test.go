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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"slice-controller/internal/cache"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
)

// Constants used in tests
const (
	testProject     = "test-project"
	testRegion      = "us-central1"
	testZone        = "us-central1-a"
	testClusterHash = "test-cluster-hash" // Long enough to trigger truncation
	testSliceName   = "slice-1"
	testPartitionID = "partition-1"
)

// --- Test Harness ---

type monitorTestHarness struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	ctrl                  *gomock.Controller
	monitor               *Monitor
	mockRC                *mocks.MockMonitorRelationalCacheInterface
	mockHC                *mocks.MockMonitorHealthCacheInterface
	mockGCE               *mocks.MockGCEClient
	fakeClient            client.Client
	sliceNamesByPartition map[string]string
	sliceInfoByName       map[string]cache.SliceInfo
}

func setupMonitorTest(t *testing.T, enableLinkedRunner bool) *monitorTestHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ctrl := gomock.NewController(t)

	mockRC := mocks.NewMockMonitorRelationalCacheInterface(ctrl)
	mockHC := mocks.NewMockMonitorHealthCacheInterface(ctrl)
	mockGCE := mocks.NewMockGCEClient(ctrl)
	fakeClient := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()

	monitor, err := NewMonitor(
		fakeClient,
		mockRC,
		mockHC,
		nil,
		testProject,
		testZone,
		100, // High QPS limit for tests
		25,  // Worker count
		testClusterHash,
		true, // enableHighGCEConcurrency
		enableLinkedRunner,
		true, // enableSubSlicing
	)
	require.NoError(t, err)
	require.NotNil(t, monitor)

	monitor.GCEClient = mockGCE
	// Speed up polling for tests
	monitor.pollInterval = 10 * time.Millisecond

	h := &monitorTestHarness{
		ctx:                   ctx,
		cancel:                cancel,
		ctrl:                  ctrl,
		monitor:               monitor,
		mockRC:                mockRC,
		mockHC:                mockHC,
		mockGCE:               mockGCE,
		fakeClient:            fakeClient,
		sliceNamesByPartition: make(map[string]string),
		sliceInfoByName:       make(map[string]cache.SliceInfo),
	}

	mockRC.EXPECT().GetSliceNameByPartitionID(gomock.Any()).DoAndReturn(func(partitionID string) (string, bool) {
		name, exists := h.sliceNamesByPartition[partitionID]
		return name, exists
	}).AnyTimes()

	mockRC.EXPECT().GetSliceInfo(gomock.Any()).DoAndReturn(func(sliceName string) (cache.SliceInfo, bool) {
		info, found := h.sliceInfoByName[sliceName]
		return info, found
	}).AnyTimes()

	return h
}

func TestMonitor_Scheduler_DisabledSubSlicing(t *testing.T) {
	h := setupMonitorTest(t, false)
	h.monitor.enableSubSlicing = false

	mig1URL := migURL("mig-1")
	sliceName := testSliceName

	h.mockRC.EXPECT().GetAllMIGNames().Return([]string{mig1URL}).AnyTimes()
	h.mockRC.EXPECT().GetAllSliceNames().Return([]string{sliceName}).AnyTimes()
	h.sliceInfoByName[sliceName] = cache.SliceInfo{Name: sliceName, Topology: "4x4x4"}

	h.mockHC.EXPECT().GetAllMIGKeys().Return(map[string]struct{}{}).AnyTimes()
	h.mockHC.EXPECT().GetAllMMIGKeys().Return(map[string]struct{}{}).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.monitor.runScheduler(h.ctx)
	}()

	// Wait for at least one scheduler cycle
	time.Sleep(h.monitor.pollInterval * 5)
	h.cancel()
	wg.Wait()

	// Both MIG and MMIG should be enqueued (MMIG is for superslices)
	require.GreaterOrEqual(t, h.monitor.healthMonitorQueue.Len(), 2, "Queue should have at least 2 items")

	var foundSlice bool
	// Drain queue to ensure MMIG items are present
	for h.monitor.healthMonitorQueue.Len() > 0 {
		item, _ := h.monitor.healthMonitorQueue.Get()
		if strings.Contains(item.ResourceURL, mmigURLSubstring) {
			foundSlice = true
		}
		h.monitor.healthMonitorQueue.Done(item)
	}

	assert.True(t, foundSlice, "Expected MMIG URL to be enqueued even when subslicing health monitoring is disabled (as it is a super-slice)")
}

func TestMonitor_SyncHealth_MIG_EnabledWhenSubSlicingDisabled(t *testing.T) {
	h := setupMonitorTest(t, false)
	h.monitor.enableSubSlicing = false
	testURL := migURL("mig-enabled")

	h.mockRC.EXPECT().GetMIG(testURL).Return(makeValidMigInfo(testURL), true)
	h.mockRC.EXPECT().GetSliceNameByPartitionID("partition-1").Return(testSliceName, true).AnyTimes()
	// Expect GCE call even if subslicing is disabled
	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-enabled").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				"partition-1": {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "2x2x2",
				},
				"4x4x4": {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "4x4x4",
				},
			},
		}, nil)

	// h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, gomock.Any()) // Removed as it's no longer called when enableSubSlicing is false
	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusHealthy, "")

	err := h.monitor.SyncHealth(h.ctx, testURL)
	assert.NoError(t, err)
}

func TestMonitor_SyncHealth_DisabledSubSlicing(t *testing.T) {
	h := setupMonitorTest(t, false)
	h.monitor.enableSubSlicing = false
	sliceName := testSliceName
	mmigName := hashedMMIGName(sliceName)
	testURL := mmigURL(mmigName)

	h.sliceInfoByName[sliceName] = cache.SliceInfo{Name: sliceName, Topology: "4x4x4"}
	h.mockGCE.EXPECT().GetMMIGHealthStatus(gomock.Any(), testProject, testRegion, mmigName).
		Return(cache.SuperSliceHealthStatus{
			TopologyHealths: []*cache.SuperSliceTopologyHealth{
				{AcceleratorTopology: util.Topology4x4x4, HealthStatus: cache.MMIGHealthStatusActive},
			},
		}, nil)
	h.mockHC.EXPECT().SetMMIGHealth(testURL, cache.MMIGHealthStatusActive, "")

	// Should proceed with sync even if flag is false (as it is a super-slice)
	err := h.monitor.SyncHealth(h.ctx, testURL)
	assert.NoError(t, err)
}

// --- Helper Functions ---

func migURL(name string) string {
	return fmt.Sprintf("projects/%s/zones/%s/instanceGroupManagers/%s", testProject, testZone, name)
}

func mmigURL(name string) string {
	return fmt.Sprintf("projects/%s/regions/%s/multiMigs/%s", testProject, testRegion, name)
}

// hashedMMIGName simulates util.GetResourceName behavior, including 8-char truncation.
func hashedMMIGName(sliceName string) string {
	shortHash := testClusterHash
	if len(testClusterHash) > 8 {
		shortHash = testClusterHash[:8]
	}
	return fmt.Sprintf("%s-%s", sliceName, shortHash)
}

func makeValidMigInfo(migURL string) cache.MIGInfo {
	dummyNodes := make(map[string]*cache.NodeInfo, 16)
	for i := 0; i < 16; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		dummyNodes[nodeName] = &cache.NodeInfo{Name: nodeName}
	}
	return cache.MIGInfo{
		Name:  migURL,
		Nodes: dummyNodes,
	}
}

func TestMonitor_SyncHealth_MIG_IncompleteDynamicNodeCount(t *testing.T) {
	h := setupMonitorTest(t, false)
	h.monitor.enableSubSlicing = false
	testURL := migURL("mig-4x4x4")

	// Create 15 nodes (incomplete for tpu7x which expects 16)
	dummyNodes := make(map[string]*cache.NodeInfo, 15)
	for i := 0; i < 15; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		dummyNodes[nodeName] = &cache.NodeInfo{
			Name:            nodeName,
			AcceleratorType: "tpu7x",

			TopologyPartitionIds: map[string][]string{
				"4x4x4": {"partition-1"},
			},
		}
	}
	migInfo := cache.MIGInfo{
		Name:  testURL,
		Nodes: dummyNodes,
	}

	h.mockRC.EXPECT().GetMIG(testURL).Return(migInfo, true)

	// We expect it to fail BEFORE GCE call because it's incomplete.
	// So no GCE mock needed.

	h.mockHC.EXPECT().CleanupMIGHealth(testURL)

	err := h.monitor.SyncHealth(h.ctx, testURL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mig does not have full nodes")
	assert.Contains(t, err.Error(), "expected: 16")
	assert.Contains(t, err.Error(), "actual: 15")
}

// --- Tests ---

func TestMonitor_Scheduler_EnqueuesJobs(t *testing.T) {
	h := setupMonitorTest(t, false)
	mig1URL := migURL("mig-1")
	sliceName := testSliceName
	// Expect the URL to use the truncated hash (e.g., slice-1-test-clu)
	expectedSliceURL := mmigURL(hashedMMIGName(sliceName))

	h.mockRC.EXPECT().GetAllMIGNames().Return([]string{mig1URL}).AnyTimes()
	h.mockRC.EXPECT().GetAllSliceNames().Return([]string{sliceName}).AnyTimes()
	h.sliceInfoByName[sliceName] = cache.SliceInfo{Name: sliceName, Topology: "4x4x4"}

	h.mockHC.EXPECT().GetAllMIGKeys().Return(map[string]struct{}{}).AnyTimes()
	h.mockHC.EXPECT().GetAllMMIGKeys().Return(map[string]struct{}{}).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.monitor.runScheduler(h.ctx)
	}()

	// Wait for at least one scheduler cycle
	time.Sleep(h.monitor.pollInterval * 5)
	h.cancel()
	wg.Wait()

	require.GreaterOrEqual(t, h.monitor.healthMonitorQueue.Len(), 2, "Queue should have at least 2 items")

	var foundMig, foundSlice bool
	var seenURLs []string
	// Drain queue to find our items
	for h.monitor.healthMonitorQueue.Len() > 0 {
		item, _ := h.monitor.healthMonitorQueue.Get()
		seenURLs = append(seenURLs, item.ResourceURL)
		if item.ResourceURL == mig1URL {
			foundMig = true
		}
		if item.ResourceURL == expectedSliceURL {
			foundSlice = true
		}
		h.monitor.healthMonitorQueue.Done(item)
	}

	assert.True(t, foundMig, "Expected MIG URL %s to be enqueued. Seen: %v", mig1URL, seenURLs)
	assert.True(t, foundSlice, "Expected MMIG URL %s to be enqueued. Seen: %v", expectedSliceURL, seenURLs)
}

func TestMonitor_Scheduler_RespectsMaxQueueLength(t *testing.T) {
	h := setupMonitorTest(t, false)
	mig1URL := migURL("mig-1")

	h.mockRC.EXPECT().GetAllMIGNames().Return([]string{mig1URL}).AnyTimes()
	h.mockRC.EXPECT().GetAllSliceNames().Return(nil).AnyTimes()
	h.mockHC.EXPECT().GetAllMIGKeys().Return(map[string]struct{}{}).AnyTimes()
	h.mockHC.EXPECT().GetAllMMIGKeys().Return(map[string]struct{}{}).AnyTimes()

	// Fill queue to the limit
	for i := 0; i < maxQueueLengthBeforeSkip+1; i++ {
		h.monitor.healthMonitorQueue.Add(Job{ResourceURL: fmt.Sprintf("fake-job-%d", i)})
	}

	initialLen := h.monitor.healthMonitorQueue.Len()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.monitor.runScheduler(h.ctx)
	}()

	time.Sleep(h.monitor.pollInterval * 2)
	h.cancel()
	wg.Wait()

	// Length should not increase as scheduler should skip when full
	assert.Equal(t, initialLen, h.monitor.healthMonitorQueue.Len())
}

func TestMonitor_Worker_MIG_Success(t *testing.T) {
	h := setupMonitorTest(t, false)
	testURL := migURL("mig-success")
	partitionID := testPartitionID
	sliceName := testSliceName

	h.mockRC.EXPECT().GetMIG(testURL).Return(makeValidMigInfo(testURL), true)
	h.mockRC.EXPECT().GetSliceNameByPartitionID(partitionID).Return(sliceName, true).AnyTimes()

	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-success").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				partitionID: {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "2x2x2",
					SliceID:                         hashedMMIGName(sliceName),
					AcceleratorTopologyState:        cache.MIGPartitionStateActive,
				},
				"4x4x4": {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "4x4x4",
				},
			},
		}, nil)

	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusHealthy, "")
	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, map[string]cache.AcceleratorTopologyInfo{
		partitionID: {
			AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
			AcceleratorTopology:             "2x2x2",
			SliceID:                         hashedMMIGName(sliceName),
			AcceleratorTopologyState:        cache.MIGPartitionStateActive,
		},
	})

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.Len())
}

func TestMonitor_Worker_MIG_SliceIDMismatch(t *testing.T) {
	h := setupMonitorTest(t, false)
	testURL := migURL("mig-mismatch")
	partitionID := testPartitionID
	sliceName := testSliceName

	h.mockRC.EXPECT().GetMIG(testURL).Return(makeValidMigInfo(testURL), true)
	h.mockRC.EXPECT().GetSliceNameByPartitionID(partitionID).Return(sliceName, true).AnyTimes()

	// GCE returns a different SliceID
	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-mismatch").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				partitionID: {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "2x2x2",
					SliceID:                         "wrong-owner-id",
					AcceleratorTopologyState:        cache.MIGPartitionStateActive,
				},
				"4x4x4": {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "4x4x4",
				},
			},
		}, nil)

	// Health monitor should preserve the actual status from GCE even on mismatch
	// Note: 4x4x4 is filtered out in syncMigHealth as it's handled by MIG-level health.
	expectedTopos := map[string]cache.AcceleratorTopologyInfo{
		partitionID: {
			AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
			AcceleratorTopology:             "2x2x2",
			SliceID:                         "wrong-owner-id",
			AcceleratorTopologyState:        cache.MIGPartitionStateActive,
		},
	}

	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusHealthy, "")
	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, expectedTopos)

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
}

func TestMonitor_Worker_MIG_RetryableError(t *testing.T) {
	h := setupMonitorTest(t, false)
	testURL := migURL("mig-retry")

	h.mockRC.EXPECT().GetMIG(testURL).Return(makeValidMigInfo(testURL), true)
	retryErr := &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "transient 503"}

	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-retry").
		Return(cache.AcceleratorTopologies{}, retryErr)

	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusUnknown, retryErr.Error())
	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, map[string]cache.AcceleratorTopologyInfo{})

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 1, h.monitor.healthMonitorQueue.NumRequeues(job))
}

func TestMonitor_Worker_MIG_PermanentError_NotFullNodes(t *testing.T) {
	h := setupMonitorTest(t, false)
	testURL := migURL("mig-partial")
	topology := "2x2x2"
	partitionID := testPartitionID
	sliceName := testSliceName

	// Create a MIG with fewer than required nodes (15 nodes)
	partialMig := makeValidMigInfo(testURL)
	delete(partialMig.Nodes, "node-0")
	assert.Equal(t, 15, len(partialMig.Nodes))

	h.mockRC.EXPECT().GetMIG(testURL).Return(partialMig, true)
	h.mockRC.EXPECT().GetSliceNameByPartitionID(partitionID).Return(sliceName, true).AnyTimes()

	// Now we expect GCE call even with partial nodes
	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-partial").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				partitionID: {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             topology,
					SliceID:                         hashedMMIGName(sliceName),
				},
				"4x4x4": {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "4x4x4",
				},
			},
		}, nil)

	// Expect SetMIGHealth with INCOMPLETE status
	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusIncomplete, gomock.Any())
	// Expect SetMIGPartitionsStatus
	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, map[string]cache.AcceleratorTopologyInfo{
		partitionID: {
			AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
			AcceleratorTopology:             topology,
			SliceID:                         hashedMMIGName(sliceName),
		},
	})

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.NumRequeues(job))
}

func TestMonitor_Worker_MIG_Incomplete_NoSubSlicing(t *testing.T) {
	h := setupMonitorTest(t, false)
	h.monitor.enableSubSlicing = false
	testURL := migURL("mig-incomplete")

	// Create a MIG with fewer than required nodes (15 nodes)
	partialMig := makeValidMigInfo(testURL)
	delete(partialMig.Nodes, "node-0")
	assert.Equal(t, 15, len(partialMig.Nodes))

	h.mockRC.EXPECT().GetMIG(testURL).Return(partialMig, true)

	// In the non-sub-slicing case, an incomplete MIG triggers CleanupMIGHealth
	h.mockHC.EXPECT().CleanupMIGHealth(testURL)

	// We do NOT expect GCE call or SetMIGHealth because it returns TerminalError immediately.

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.NumRequeues(job))
}

func TestMonitor_Worker_MMIG_Success(t *testing.T) {
	h := setupMonitorTest(t, false)
	sliceName := "slice-success"
	mmigName := hashedMMIGName(sliceName)
	testURL := mmigURL(mmigName)

	// RC is queried with the BARE slice name (extracted from MMIG name)
	h.sliceInfoByName[sliceName] = cache.SliceInfo{Name: sliceName}

	h.mockGCE.EXPECT().GetMMIGHealthStatus(gomock.Any(), testProject, testRegion, mmigName).
		Return(cache.SuperSliceHealthStatus{
			TopologyHealths: []*cache.SuperSliceTopologyHealth{
				{AcceleratorTopology: util.Topology4x4x4, HealthStatus: cache.MMIGHealthStatusActive},
			},
		}, nil)

	h.mockHC.EXPECT().SetMMIGHealth(testURL, cache.MMIGHealthStatusActive, "")

	h.monitor.healthMonitorQueue.Add(Job{ResourceURL: testURL})
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
}

func TestMonitor_Worker_MMIG_FailedStatus(t *testing.T) {
	h := setupMonitorTest(t, false)
	sliceName := "slice-failed"
	mmigName := hashedMMIGName(sliceName)
	testURL := mmigURL(mmigName)

	h.sliceInfoByName[sliceName] = cache.SliceInfo{Name: sliceName}

	h.mockGCE.EXPECT().GetMMIGHealthStatus(gomock.Any(), testProject, testRegion, mmigName).
		Return(cache.SuperSliceHealthStatus{
			TopologyHealths: []*cache.SuperSliceTopologyHealth{
				{
					AcceleratorTopology:    util.Topology4x4x4,
					HealthStatus:           cache.MMIGHealthStatusFailed,
					LastCheckErrorMessages: []string{"node-1 broken"},
				},
			},
		}, nil)

	h.mockHC.EXPECT().SetMMIGHealth(testURL, cache.MMIGHealthStatusFailed, gomock.Any())

	h.monitor.healthMonitorQueue.Add(Job{ResourceURL: testURL})
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
}

func TestMonitor_Worker_MMIG_RetryableError(t *testing.T) {
	h := setupMonitorTest(t, false)
	sliceName := "slice-retry"
	mmigName := hashedMMIGName(sliceName)
	testURL := mmigURL(mmigName)

	h.sliceInfoByName[sliceName] = cache.SliceInfo{Name: sliceName}
	retryErr := &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "transient 503"}

	h.mockGCE.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cache.SuperSliceHealthStatus{}, retryErr)

	h.mockHC.EXPECT().SetMMIGHealth(testURL, cache.MMIGHealthStatusProbeFailed, retryErr.Error())

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 1, h.monitor.healthMonitorQueue.NumRequeues(job))
}

func TestMonitor_Worker_MMIG_CacheMiss(t *testing.T) {
	h := setupMonitorTest(t, false)
	sliceName := "slice-stale"
	testURL := mmigURL(hashedMMIGName(sliceName))

	// Cache miss means not found in map, so no need to register anything.
	h.mockHC.EXPECT().CleanupMMIGHealth(testURL)

	h.monitor.healthMonitorQueue.Add(Job{ResourceURL: testURL})
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.NumRequeues(Job{ResourceURL: testURL}))
}

func TestMonitor_Worker_MMIG_EmptyTopologyResponse(t *testing.T) {
	h := setupMonitorTest(t, false)
	sliceName := "slice-empty"
	mmigName := hashedMMIGName(sliceName)
	testURL := mmigURL(mmigName)

	h.sliceInfoByName[sliceName] = cache.SliceInfo{Name: sliceName}
	// GCE returns success but empty topology list
	h.mockGCE.EXPECT().GetMMIGHealthStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cache.SuperSliceHealthStatus{TopologyHealths: []*cache.SuperSliceTopologyHealth{}}, nil)

	h.mockHC.EXPECT().SetMMIGHealth(testURL, cache.MMIGHealthStatusUnknown, gomock.Eq("no topology health information found"))

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.NumRequeues(job))
}

func TestMonitor_RunHealthCacheGC(t *testing.T) {
	h := setupMonitorTest(t, false)
	validMigURL := migURL("mig-1")
	staleMigURL := migURL("mig-stale")

	sliceName := testSliceName
	validMmigURL := mmigURL(hashedMMIGName(sliceName))
	staleMmigURL := mmigURL(hashedMMIGName("slice-stale"))

	h.mockHC.EXPECT().GetAllMIGKeys().Return(map[string]struct{}{
		validMigURL: {},
		staleMigURL: {},
	})
	h.mockHC.EXPECT().GetAllMMIGKeys().Return(map[string]struct{}{
		validMmigURL: {},
		staleMmigURL: {},
	})

	h.mockHC.EXPECT().CleanupMIGHealth(staleMigURL).Times(1)
	h.mockHC.EXPECT().CleanupMIGPartitionsStatus(staleMigURL).Times(1)
	h.mockHC.EXPECT().CleanupMMIGHealth(staleMmigURL).Times(1)

	h.mockHC.EXPECT().CleanupMIGHealth(validMigURL).Times(0)
	h.mockHC.EXPECT().CleanupMIGPartitionsStatus(validMigURL).Times(0)
	h.mockHC.EXPECT().CleanupMMIGHealth(validMmigURL).Times(0)

	// GC uses the bare slice name, Monitor internally hashes it to check against cache
	h.monitor.runHealthCacheGC(h.ctx, []string{validMigURL}, []string{sliceName})
}

func TestMonitor_RunHealthCacheGC_DisabledSubSlicing(t *testing.T) {
	h := setupMonitorTest(t, false)
	h.monitor.enableSubSlicing = false

	validMigURL := migURL("mig-1")
	staleMigURL := migURL("mig-stale")

	sliceName := testSliceName
	validMmigURL := mmigURL(hashedMMIGName(sliceName))
	staleMmigURL := mmigURL(hashedMMIGName("slice-stale"))

	h.mockHC.EXPECT().GetAllMIGKeys().Return(map[string]struct{}{
		validMigURL: {},
		staleMigURL: {},
	})
	h.mockHC.EXPECT().GetAllMMIGKeys().Return(map[string]struct{}{
		validMmigURL: {},
		staleMmigURL: {},
	})

	h.mockHC.EXPECT().CleanupMIGHealth(staleMigURL).Times(1)
	// CleanupMIGPartitionsStatus should NOT be called when enableSubSlicing is false
	h.mockHC.EXPECT().CleanupMIGPartitionsStatus(staleMigURL).Times(0)
	h.mockHC.EXPECT().CleanupMMIGHealth(staleMmigURL).Times(1)

	h.mockHC.EXPECT().CleanupMIGHealth(validMigURL).Times(0)
	h.mockHC.EXPECT().CleanupMIGPartitionsStatus(validMigURL).Times(0)
	h.mockHC.EXPECT().CleanupMMIGHealth(validMmigURL).Times(0)

	h.monitor.runHealthCacheGC(h.ctx, []string{validMigURL}, []string{sliceName})
}

// --- Linked Runner Tests ---

// makeControlNode creates a mock control node for Linked Runner tests.
func makeControlNode(name string, runnerCount int, malformedAnnotation bool) *corev1.Node {
	annotations := make(map[string]string)
	if malformedAnnotation {
		annotations[util.LinkedRunnerAnnotationPrefix+"/0"] = "this-is-not-json"
	} else if runnerCount > 0 {
		for i := range runnerCount {
			runnerID := 1000 + i
			annotationKey := fmt.Sprintf("%s/%d", util.LinkedRunnerAnnotationPrefix, runnerID)

			topoMap := map[string]string{
				util.Topology4x4x4: "uid-4x4x4-0",
				util.Topology2x4x4: fmt.Sprintf("uid-2x4x4-%d", i/8), // 2 groups of 8
				util.Topology2x2x4: fmt.Sprintf("uid-2x2x4-%d", i/4), // 4 groups of 4
				util.Topology2x2x2: fmt.Sprintf("uid-2x2x2-%d", i/2), // 8 groups of 2
				util.Topology2x2x1: fmt.Sprintf("uid-2x2x1-%d", i),   // 16 unique
			}
			jsonTopology, _ := json.Marshal(topoMap)

			runnerInfo := fmt.Sprintf(`{"IP_ADDRESS":"10.0.0.%d","NAME":"runner-%d","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":%q}`, i, i, string(jsonTopology))
			annotations[annotationKey] = runnerInfo
		}
	}

	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
			Annotations: annotations,
		},
	}
}

// makeLinkedRunnerMigInfo creates a MIGInfo for a Linked Runner MIG, containing one control node.
func makeLinkedRunnerMigInfo(migURL, nodeName string) cache.MIGInfo {
	parts := strings.Split(migURL, "/")
	migName := parts[len(parts)-1]

	topologyPartitionIds := make(map[string][]string)
	topologyPartitionIds[util.Topology4x4x4] = []string{"uid-4x4x4-0"}
	for i := range 2 {
		topologyPartitionIds[util.Topology2x4x4] = append(topologyPartitionIds["2x4x4"], fmt.Sprintf("uid-2x4x4-%d", i))
	}
	for i := range 4 {
		topologyPartitionIds[util.Topology2x2x4] = append(topologyPartitionIds["2x2x4"], fmt.Sprintf("uid-2x2x4-%d", i))
	}
	for i := range 8 {
		topologyPartitionIds[util.Topology2x2x2] = append(topologyPartitionIds["2x2x2"], fmt.Sprintf("uid-2x2x2-%d", i))
	}
	for i := range 16 {
		topologyPartitionIds[util.Topology2x2x1] = append(topologyPartitionIds["2x2x1"], fmt.Sprintf("uid-2x2x1-%d", i))
	}

	return cache.MIGInfo{
		Name: migName,
		Nodes: map[string]*cache.NodeInfo{
			nodeName: {
				Name:                 nodeName,
				MIGName:              migName,
				TopologyPartitionIds: topologyPartitionIds,
			},
		},
	}
}

func TestMonitor_Worker_MIG_LinkedRunner_Success(t *testing.T) {
	h := setupMonitorTest(t, true)
	testURL := migURL("mig-linkedvm-success")
	controlNodeName := "control-node-1"
	topology := util.Topology4x4x4

	controlNode := makeControlNode(controlNodeName, 16, false)

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	h.monitor.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(controlNode).Build()

	migInfo := makeLinkedRunnerMigInfo(testURL, controlNodeName)
	h.mockRC.EXPECT().GetMIG(testURL).Return(migInfo, true)

	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-linkedvm-success").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				topology: {AcceleratorTopologyHealthStatus: "HEALTHY", AcceleratorTopology: topology},
			},
		}, nil)

	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, gomock.Any())
	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusHealthy, "")

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.Len())
}

func TestMonitor_Worker_MIG_LinkedRunner_NotEnoughRunners(t *testing.T) {
	h := setupMonitorTest(t, true)
	testURL := migURL("mig-linkedvm-notenough")
	controlNodeName := "control-node-2"

	controlNode := makeControlNode(controlNodeName, 15, false)

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	h.monitor.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(controlNode).Build()

	migInfo := makeLinkedRunnerMigInfo(testURL, controlNodeName)
	h.mockRC.EXPECT().GetMIG(testURL).Return(migInfo, true)
	h.mockRC.EXPECT().GetSliceNameByPartitionID("uid-2x2x2-0").Return(testSliceName, true).AnyTimes()

	// In the sub-slicing case, an incomplete Linked Runner MIG should continue monitoring.
	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-linkedvm-notenough").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				"uid-2x2x2-0": {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "2x2x2",
					SliceID:                         hashedMMIGName(testSliceName),
				},
			},
		}, nil)

	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, gomock.Any())
	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusIncomplete, gomock.Any())

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.NumRequeues(job), "Should have processed successfully")
}

func TestMonitor_Worker_MIG_Subslice16x16_NotSkipped(t *testing.T) {
	h := setupMonitorTest(t, false)
	testURL := migURL("mig-16x16-subslice")
	partitionID := "partition-16x16"
	sliceName := "subslice-16x16"

	h.mockRC.EXPECT().GetMIG(testURL).Return(makeValidMigInfo(testURL), true)
	h.sliceNamesByPartition[partitionID] = sliceName
	h.sliceInfoByName[sliceName] = cache.SliceInfo{
		Name:         sliceName,
		PartitionIDs: []string{partitionID},
		Topology:     "16x16",
	}

	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-16x16-subslice").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				partitionID: {
					AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
					AcceleratorTopology:             "16x16",
					SliceID:                         hashedMMIGName(sliceName),
					AcceleratorTopologyState:        cache.MIGPartitionStateActive,
				},
			},
		}, nil)

	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusHealthy, "")
	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, map[string]cache.AcceleratorTopologyInfo{
		partitionID: {
			AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy,
			AcceleratorTopology:             "16x16",
			SliceID:                         hashedMMIGName(sliceName),
			AcceleratorTopologyState:        cache.MIGPartitionStateActive,
		},
	})

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.Len())
}

func TestMonitor_Worker_MIG_LinkedRunner_MalformedAnnotation(t *testing.T) {
	h := setupMonitorTest(t, true)
	testURL := migURL("mig-linkedvm-malformed")
	controlNodeName := "control-node-3"

	controlNode := makeControlNode(controlNodeName, 0, true)

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	h.monitor.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(controlNode).Build()

	migInfo := makeLinkedRunnerMigInfo(testURL, controlNodeName)
	h.mockRC.EXPECT().GetMIG(testURL).Return(migInfo, true)
	h.mockHC.EXPECT().CleanupMIGHealth(testURL)

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.NumRequeues(job), "Should be a terminal error, not requeued")
}

func TestMonitor_Worker_MIG_LinkedRunner_Fallback_Success(t *testing.T) {
	h := setupMonitorTest(t, true)
	testURL := migURL("mig-not-linkedvm-success")
	topology := util.Topology4x4x4

	migInfo := makeValidMigInfo(testURL)
	h.mockRC.EXPECT().GetMIG(testURL).Return(migInfo, true)

	h.mockGCE.EXPECT().GetAvailableAcceleratorTopologies(gomock.Any(), testProject, testZone, "mig-not-linkedvm-success").
		Return(cache.AcceleratorTopologies{
			TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
				topology: {AcceleratorTopologyHealthStatus: "HEALTHY", AcceleratorTopology: topology},
			},
		}, nil)
	h.mockHC.EXPECT().SetMIGPartitionsStatus(testURL, gomock.Any())
	h.mockHC.EXPECT().SetMIGHealth(testURL, cache.MIGHealthStatusHealthy, "")

	job := Job{ResourceURL: testURL}
	h.monitor.healthMonitorQueue.Add(job)
	processed := h.monitor.processNextWorkItem(h.ctx)

	assert.True(t, processed)
	assert.Equal(t, 0, h.monitor.healthMonitorQueue.Len())
}
