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
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
)

// --- HANDWRITTEN STUBS ---
// These replace complex gomocks for standard K8s interfaces.

type mockManager struct {
	manager.Manager
	cache ctrlcache.Cache
}

func (m *mockManager) GetCache() ctrlcache.Cache { return m.cache }

type mockCache struct {
	ctrlcache.Cache
	informer ctrlcache.Informer
	err      error
}

func (c *mockCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...ctrlcache.InformerGetOption) (ctrlcache.Informer, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.informer, nil
}

type mockInformer struct {
	ctrlcache.Informer
	handlers  cache.ResourceEventHandlerFuncs
	addErr    error
	hasSynced bool
}

func (i *mockInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	if i.addErr != nil {
		return nil, i.addErr
	}
	// Capture the handler so we can call it in tests
	i.handlers = handler.(cache.ResourceEventHandlerFuncs)
	return nil, nil
}

func (i *mockInformer) HasSynced() bool { return i.hasSynced }

// --- TEST SETUP ---

type testFixture struct {
	mockRC  *mocks.MockNodeInformerRelationalCacheInterface
	mockMgr *mockManager
	mockC   *mockCache
	mockInf *mockInformer
	ctx     context.Context
	ctrl    *gomock.Controller
}

func setupTest(t *testing.T) *testFixture {
	ctrl := gomock.NewController(t)

	// 1. Init handwritten stubs
	mockInf := &mockInformer{hasSynced: true} // Default to synced
	mockC := &mockCache{informer: mockInf}
	mockMgr := &mockManager{cache: mockC}

	// 2. Init gomock for OUR interface
	mockRC := mocks.NewMockNodeInformerRelationalCacheInterface(ctrl)

	return &testFixture{
		mockRC:  mockRC,
		mockMgr: mockMgr,
		mockC:   mockC,
		mockInf: mockInf,
		ctx:     log.IntoContext(context.Background(), logr.Discard()),
		ctrl:    ctrl,
	}
}

// makeTestNode is a helper to build nodes for testing
func makeTestNode(name, mode, accelerator, mig string, partitions map[string]string) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
		},
	}
	if mode != "" {
		node.Labels[ProvisioningModeLabel] = mode
	}
	if accelerator != "" {
		node.Labels[util.TpuAcceleratorLabel] = accelerator
	}
	if mig != "" {
		node.Annotations[util.MIGAnnotationKey] = mig
	}
	node.Labels[ReservationNameLabel] = "test-reservation"
	node.Labels[ReservationBlockIDLabel] = "test-block"
	for k, v := range partitions {
		node.Labels[k] = v
	}
	return node
}

func makeValidNode(name string) *corev1.Node {
	return makeTestNode(name, ProvisionOnly, TpuV7XAcceleratorLabelValue, "mig-url-123", map[string]string{
		util.TopologyLabel2x2x1: "uid-1",
		util.TopologyLabel2x2x2: "uid-2",
		util.TopologyLabel2x2x4: "uid-3",
		util.TopologyLabel2x4x4: "uid-4",
		util.TopologyLabel4x4x4: "uid-5",
	})
}

// --- Tests ---

func TestNewNodeInformer(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()

		ni, err := NewNodeInformer(f.mockMgr, f.mockRC, false)
		assert.NoError(t, err)
		assert.NotNil(t, ni)
		// Verify handler capture worked
		assert.NotNil(t, f.mockInf.handlers.AddFunc)
	})

	t.Run("Nil Manager", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()
		_, err := NewNodeInformer(nil, f.mockRC, false)
		assert.ErrorContains(t, err, "manager cannot be nil")
	})

	t.Run("Nil Relational Cache", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()
		_, err := NewNodeInformer(f.mockMgr, nil, false)
		assert.ErrorContains(t, err, "relational cache cannot be nil")
	})

	t.Run("GetInformer fails", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()
		// Configure stub to fail
		f.mockC.err = errors.New("informer error")

		_, err := NewNodeInformer(f.mockMgr, f.mockRC, false)
		assert.ErrorContains(t, err, "failed to get Node informer")
	})

	t.Run("AddEventHandler fails", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()
		// Configure stub to fail
		f.mockInf.addErr = errors.New("handler error")

		_, err := NewNodeInformer(f.mockMgr, f.mockRC, false)
		assert.ErrorContains(t, err, "failed to add event handlers")
	})
}

func TestNodeInformer_Start(t *testing.T) {
	t.Run("Cache Sync Timeout", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()

		ni, _ := NewNodeInformer(f.mockMgr, f.mockRC, false)
		// Configure stub to never sync
		f.mockInf.hasSynced = false

		ctx, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
		defer cancel()

		err := ni.Start(ctx)
		assert.ErrorContains(t, err, "timed out waiting for Node informer cache to sync")
	})

	t.Run("Sync Success and Context Cancel", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()

		ni, _ := NewNodeInformer(f.mockMgr, f.mockRC, false)

		ctx, cancel := context.WithCancel(f.ctx)
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := ni.Start(ctx)
		assert.NoError(t, err)
	})
}

func TestNodeInformer_Handlers(t *testing.T) {
	// Helper to get initialized informer and handlers
	getInformer := func(t *testing.T, f *testFixture) (*NodeInformer, cache.ResourceEventHandlerFuncs) {
		ni, err := NewNodeInformer(f.mockMgr, f.mockRC, false)
		require.NoError(t, err)
		ni.ctx = f.ctx
		return ni, f.mockInf.handlers
	}

	t.Run("Add: Happy Path", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()
		ni, handlers := getInformer(t, f)
		node := makeValidNode("node-1")

		// Gomock expectation
		f.mockRC.EXPECT().UpdateNodeInfo("mig-url-123", "node-1", "test-reservation", "test-block", gomock.Any(), gomock.Any(), false, gomock.Any()).Return(nil)

		handlers.AddFunc(node)

		hash, exists := ni.processedNodes.Load("node-1")
		assert.True(t, exists)
		expectedHash, err := ni.computeNodeHash(node)
		assert.NoError(t, err)
		assert.Equal(t, expectedHash, hash)
	})

	t.Run("Update: Relevant Change (Happy Path)", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()
		ni, handlers := getInformer(t, f)
		oldNode := makeValidNode("node-1")
		newNode := oldNode.DeepCopy()
		newNode.Labels[util.TopologyLabel2x2x1] = "new-uid"

		oldHash, err := ni.computeNodeHash(oldNode)
		assert.NoError(t, err)
		ni.processedNodes.Store("node-1", oldHash)

		// Gomock expectation
		f.mockRC.EXPECT().UpdateNodeInfo("mig-url-123", "node-1", "test-reservation", "test-block", gomock.Any(), gomock.Any(), false, gomock.Any()).Return(nil)

		handlers.UpdateFunc(oldNode, newNode)

		newHash, _ := ni.processedNodes.Load("node-1")
		expectedHash, err := ni.computeNodeHash(newNode)
		assert.NoError(t, err)
		assert.Equal(t, expectedHash, newHash)
	})

	t.Run("Delete: Happy Path", func(t *testing.T) {
		f := setupTest(t)
		defer f.ctrl.Finish()
		ni, handlers := getInformer(t, f)
		node := makeValidNode("node-1")

		ni.processedNodes.Store("node-1", "some-hash")

		// Gomock expectation
		f.mockRC.EXPECT().DeleteNodeAndCleanUpMIG("node-1").Return(true, nil)

		handlers.DeleteFunc(node)

		_, exists := ni.processedNodes.Load("node-1")
		assert.False(t, exists)
	})
}

func TestShouldProcessNode(t *testing.T) {
	testCases := []struct {
		name               string
		node               *corev1.Node
		enableLinkedRunner bool
		shouldProcess      bool
	}{
		{
			name:          "valid regular node",
			node:          makeValidNode("regular-node"),
			shouldProcess: true,
		},
		{
			name: "valid Linked Runner control node",
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: "lvm-control-node",
				Labels: map[string]string{
					util.LinkedRunnerNodeTypeLabel: "control-node",
				},
				Annotations: map[string]string{
					util.LinkedRunnerAnnotationPrefix + "runner-1": "{}",
				},
			}},
			enableLinkedRunner: true,
			shouldProcess:      true,
		},
		{
			name: "Linked Runner control node without runner annotations",
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: "lvm-control-node-no-runners",
				Labels: map[string]string{
					util.LinkedRunnerNodeTypeLabel: "control-node",
				},
			}},
			enableLinkedRunner: true,
			shouldProcess:      false,
		},
		{
			name:               "Linked Runner control node but feature disabled",
			node:               &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "lvm-control-node"}},
			enableLinkedRunner: false,
			shouldProcess:      false,
		},
		{
			name:          "regular node missing mode label",
			node:          makeTestNode("missing-label", "", TpuV7XAcceleratorLabelValue, "mig", nil),
			shouldProcess: false,
		},
		{
			name:          "regular node wrong accelerator label",
			node:          makeTestNode("wrong-label", ProvisionOnly, "wrong-accelerator", "mig", nil),
			shouldProcess: false,
		},
		{
			name:          "nil node",
			node:          nil,
			shouldProcess: false,
		},
	}
	f := setupTest(t)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ni, err := NewNodeInformer(f.mockMgr, f.mockRC, tc.enableLinkedRunner)
			assert.NoError(t, err)
			assert.Equal(t, tc.shouldProcess, ni.shouldProcessNode(tc.node))
		})
	}
}

func TestComputeNodeHash(t *testing.T) {
	testCases := []struct {
		name               string
		node               *corev1.Node
		expected           string
		expectErr          bool
		expectedErr        string
		enableLinkedRunner bool
	}{
		{
			name:     "successful hash for regular node",
			node:     makeValidNode("node-1"),
			expected: "mig-url-123,uid-1,uid-2,uid-3,uid-4,uid-5",
		},
		{
			name: "successful hash for Linked Runner control node",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						util.ChildPoolIgUrlAnnotationKey:                "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig",
						util.LinkedRunnerAnnotationPrefix + "/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x2\":\"uid-a\",\"4x4x4\":\"uid-c\"}"}`,
						util.LinkedRunnerAnnotationPrefix + "/runner-2": `{"IP_ADDRESS":"10.0.0.2","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x2\":\"uid-b\",\"4x4x4\":\"uid-c\"}"}`,
					},
				},
			},
			expected:           "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig,uid-a,uid-b,uid-c",
			enableLinkedRunner: true,
		},
		{
			name:        "node is nil",
			node:        nil,
			expectErr:   true,
			expectedErr: "node, labels or annotations cannot be nil",
		},
		{
			name: "MIG annotation missing",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{},
					Annotations: map[string]string{},
				},
			},
			expectErr:   true,
			expectedErr: "MIG annotation 'node.gke.io/created-by-mig' not found or empty on node node-1",
		},
	}

	f := setupTest(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ni, err := NewNodeInformer(f.mockMgr, f.mockRC, tc.enableLinkedRunner)
			assert.NoError(t, err)

			hash, err := ni.computeNodeHash(tc.node)

			if tc.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, hash)
			}
		})
	}
}

func TestIsNodeReady(t *testing.T) {
	testCases := []struct {
		name               string
		node               *corev1.Node
		expectReady        bool
		expectErr          bool
		expectedErr        string
		enableLinkedRunner bool
	}{
		{
			name:        "valid regular node",
			node:        makeValidNode("node-1"),
			expectReady: true,
		},
		{
			name: "valid Linked Runner control node",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						util.ChildPoolIgUrlAnnotationKey:         "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig",
						"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x1\":\"uid-a\",\"2x2x2\":\"uid-b\",\"2x2x4\":\"uid-c\",\"2x4x4\":\"uid-d\",\"4x4x4\":\"uid-e\"}"}`,
					},
				},
			},
			expectReady:        true,
			enableLinkedRunner: true,
		},
		{
			name: "Linked Runner control node with runner not running",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						util.ChildPoolIgUrlAnnotationKey:         "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig",
						"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"PROVISIONING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x1\":\"uid-a\"}"}`,
					},
				},
			},
			expectReady:        false,
			expectErr:          true,
			expectedErr:        "no runner instances on Linked Runner control node control-node-1 are RUNNING",
			enableLinkedRunner: true,
		},
		{
			name: "Linked Runner control node with runner missing partition IDs",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						util.ChildPoolIgUrlAnnotationKey:         "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig",
						"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":""}`,
					},
				},
			},
			expectReady:        false,
			expectErr:          true,
			expectedErr:        "runner instance runner-1 on Linked Runner control node control-node-1 is RUNNING but is missing PartitionIDs",
			enableLinkedRunner: true,
		},
		{
			name: "Linked Runner control node missing a topology label",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						util.ChildPoolIgUrlAnnotationKey:         "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig",
						"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x1\":\"uid-a\"}"}`,
					},
				},
			},
			expectReady:        false,
			expectErr:          true,
			expectedErr:        "missing or is missing required topology: 2x2x2",
			enableLinkedRunner: true,
		},
		{
			name: "Linked Runner control node with no runner infos",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						util.ChildPoolIgUrlAnnotationKey: "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig",
						"some-other-annotation":          "value",
					},
				},
			},
			expectReady:        false,
			expectErr:          false,
			enableLinkedRunner: true,
		},
		{
			name:        "node is nil",
			node:        nil,
			expectReady: false,
			expectErr:   true,
			expectedErr: "node, annotations or labels cannot be nil",
		},
		{
			name: "MIG annotation missing",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Labels:      map[string]string{},
					Annotations: map[string]string{},
				},
			},
			expectReady: false,
			expectErr:   true,
			expectedErr: "MIG annotation 'node.gke.io/created-by-mig' not found or empty on node node-1",
		},
	}

	f := setupTest(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ni, err := NewNodeInformer(f.mockMgr, f.mockRC, tc.enableLinkedRunner)
			assert.NoError(t, err)

			ready, err := ni.isNodeReady(tc.node)

			if tc.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expectReady, ready)
		})
	}
}
