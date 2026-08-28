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

package kubehelper

import (
	"context"
	"strings"
	"testing"

	"fmt"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/cache"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	// Define constants to match the hardcoded topology labels in GetNodesByPartitionId
	tpuTopology1x      = "2x2x1" // Expects 1 node
	tpuTopology2x      = "2x2x2" // Expects 2 nodes
	testNodeGroupLabel = "test-nodegroup"
	testValue          = "test-value"
)

// mockClient is a mock implementation of client.Client for testing.
type mockClient struct {
	client.Client
	getErr     error
	patchErr   []error // Simulate a sequence of patch errors
	patchCalls int
	node       *corev1.Node
}

func (m *mockClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if m.getErr != nil {
		return m.getErr
	}
	if m.node != nil {
		node := obj.(*corev1.Node)
		m.node.DeepCopyInto(node)
	}
	return m.Client.Get(ctx, key, obj, opts...)
}

func (m *mockClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	var err error
	if m.patchCalls < len(m.patchErr) {
		err = m.patchErr[m.patchCalls]
	}
	m.patchCalls++
	if err != nil {
		return err
	}

	// Simulate the patch on the mockClient's node
	if m.node != nil {
		// Apply the modification logic to the mock node
		modifyFunc := func(n *corev1.Node) {
			if n.Labels == nil {
				n.Labels = make(map[string]string)
			}
			n.Labels["test-label"] = testValue
		}
		modifyFunc(m.node)

		// Simulate resource version update
		rv := m.node.ResourceVersion
		newRv := ""
		if rv == "" {
			newRv = "1"
		} else {
			i := 0
			_, err := fmt.Sscanf(rv, "%d", &i)
			if err != nil {
				panic(fmt.Sprintf("failed to parse resource version: %v", err))
			}
			newRv = fmt.Sprintf("%d", i+1)
		}
		m.node.ResourceVersion = newRv
	}
	return m.Client.Patch(ctx, obj, patch, opts...)
}

func TestNewClient(t *testing.T) {
	fakeClient := fakeclient.NewClientBuilder().Build()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockCache := mocks.NewMockSliceRelationalCache(ctrl)
	client := NewClient(fakeClient, mockCache, false)
	if client == nil {
		t.Fatal("NewClient() returned nil")
	}
	kubeHelper, ok := client.(*kubeHelper)
	if !ok {
		t.Fatal("NewClient() did not return a *kubeHelper")
	}
	if kubeHelper.client != fakeClient {
		t.Errorf("NewClient() did not set client correctly: got %v, want %v", kubeHelper.client, fakeClient)
	}
	if kubeHelper.cache != mockCache {
		t.Errorf("NewClient() did not set cache correctly: got %v, want %v", kubeHelper.cache, mockCache)
	}
}

func TestGetNodesByPartitionIdFallback(t *testing.T) {
	testCases := []struct {
		name               string
		partitionId        string
		existingNodes      []runtime.Object
		enableLinkedRunner bool
		controlNodes       []corev1.Node // Only for linked runner tests
		expectedNodeNames  []string
		expectErr          bool
		expectedErr        string
	}{
		{
			name:        "found nodes for 2x2x1 topology",
			partitionId: "p1",
			existingNodes: []runtime.Object{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: map[string]string{util.TopologyLabel2x2x1: "p1"}}},
			},
			enableLinkedRunner: false,
			expectedNodeNames:  []string{"node-1"},
			expectErr:          false,
		},
		{
			name:               "linked runner - valid control node found",
			partitionId:        "p1",
			existingNodes:      []runtime.Object{},
			enableLinkedRunner: true,
			controlNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "control-node-1",
						Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
						Annotations: map[string]string{
							"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x1\":\"p1\"}"}`,
						},
					},
				},
			},
			expectedNodeNames: []string{"control-node-1"},
			expectErr:         false,
		},
		{
			name:               "linked runner - partition not found in control nodes",
			partitionId:        "p2",
			existingNodes:      []runtime.Object{},
			enableLinkedRunner: true,
			controlNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "control-node-1",
						Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
						Annotations: map[string]string{
							"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x1\":\"p1\"}"}`,
						},
					},
				},
			},
			expectedNodeNames: nil,
			expectErr:         false,
		},
		{
			name:               "linked runner - incorrect node count for topology",
			partitionId:        "p1",
			existingNodes:      []runtime.Object{},
			enableLinkedRunner: true,
			controlNodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "control-node-1",
						Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"},
						Annotations: map[string]string{
							"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x2\":\"p1\"}"}`,
						},
					},
				},
			},
			expectedNodeNames: []string{"control-node-1"},
			expectErr:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("Failed to add corev1 to scheme: %v", err)
			}

			controlNodesObjects := make([]runtime.Object, len(tc.controlNodes))
			for i := range tc.controlNodes {
				controlNodesObjects[i] = &tc.controlNodes[i]
			}
			fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existingNodes...).WithRuntimeObjects(controlNodesObjects...).Build()
			kubeHelper := &kubeHelper{client: fakeClient, enableLinkedRunner: tc.enableLinkedRunner}

			nodes, err := kubeHelper.getNodesByPartitionIdFallback(context.Background(), tc.partitionId)

			if (err != nil) != tc.expectErr {
				t.Fatalf("getNodesByPartitionIdFallback() error = %v, expectErr %v", err, tc.expectErr)
			}

			if err != nil && tc.expectedErr != "" && !strings.Contains(err.Error(), tc.expectedErr) {
				t.Errorf("getNodesByPartitionIdFallback() error substring mismatch, got: %v, want: %v", err.Error(), tc.expectedErr)
			}

			if tc.expectedNodeNames == nil {
				if nodes != nil {
					t.Errorf("getNodesByPartitionIdFallback() expected nil nodes but got %v", nodes)
				}
			} else {
				var gotNodeNames []string
				for _, node := range nodes {
					gotNodeNames = append(gotNodeNames, node.Name)
				}

				if diff := cmp.Diff(tc.expectedNodeNames, gotNodeNames); diff != "" {
					t.Errorf("getNodesByPartitionIdFallback() node names mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestLabelNodeGroup(t *testing.T) {
	testCases := []struct {
		name                    string
		nodes                   []runtime.Object
		labels                  util.NodeLabels
		partitionID             string
		enableLinkedRunner      bool
		expectedErr             string
		expectedLabeled         []string
		mockGetMIGByPartitionID func(partitionID string) (cache.MIGInfo, bool)
	}{
		{
			name: "label nodes",
			nodes: []runtime.Object{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2"}},
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node3"}},
			},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{"new-label": "new-value"},
			},
			partitionID:        "p1",
			enableLinkedRunner: false,
			expectedErr:        "",
			expectedLabeled:    []string{"node1", "node3"},
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
							"node2": {Name: "node2", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p2"}}},
							"node3": {Name: "node3", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name: "label nodes in a linked VM environment",
			nodes: []runtime.Object{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-node-1", Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node"}}},
			},
			labels: util.NodeLabels{
				LinkedRunnerLabels: map[string]string{"cloud.google.com/gke-tpu-slice-test-slice": "true"},
			},
			partitionID:        "p1",
			enableLinkedRunner: true,
			expectedErr:        "",
			expectedLabeled:    []string{"control-node-1"},
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "linked-vm-mig",
						Nodes: map[string]*cache.NodeInfo{
							"control-node-1": {Name: "control-node-1", MIGName: "linked-vm-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name: "No nodes found for partition",
			nodes: []runtime.Object{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{"new-label": "new-value"},
			},
			partitionID:        "non-existent",
			enableLinkedRunner: false,
			expectedErr:        "",
			expectedLabeled:    []string{},
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				return cache.MIGInfo{}, false
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockCache := mocks.NewMockSliceRelationalCache(ctrl)

			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.nodes...).Build()
			mockCache.EXPECT().GetMIGByPartitionID(tc.partitionID).DoAndReturn(tc.mockGetMIGByPartitionID).AnyTimes()

			c := NewClient(fakeClient, mockCache, tc.enableLinkedRunner)

			err := c.LabelNodeGroup(context.Background(), tc.labels, tc.partitionID)

			if (err != nil) != (tc.expectedErr != "") {
				t.Errorf("TaintNodeGroup() error = %v, wantErr %v", err, tc.expectedErr)
			}
			if tc.expectedErr != "" && err != nil && !strings.Contains(err.Error(), tc.expectedErr) {
				t.Errorf("TaintNodeGroup() error = %v, wantErr %v", err, tc.expectedErr)
			}

			wantLabeledSet := make(map[string]bool)
			for _, name := range tc.expectedLabeled {
				wantLabeledSet[name] = true
			}

			for _, obj := range tc.nodes {
				originalNode, ok := obj.(*corev1.Node)
				if !ok {
					continue
				}

				currentNode := &corev1.Node{}
				err := fakeClient.Get(context.Background(), types.NamespacedName{Name: originalNode.Name}, currentNode)
				if err != nil {
					t.Fatalf("Failed to get node %s: %v", originalNode.Name, err)
				}

				var wantLabels map[string]string
				if originalNode.Labels != nil {
					wantLabels = make(map[string]string)
					for k, v := range originalNode.Labels {
						wantLabels[k] = v
					}
				}

				if wantLabeledSet[originalNode.Name] {
					if wantLabels == nil {
						wantLabels = make(map[string]string)
					}
					if tc.enableLinkedRunner && util.IsLinkedRunnerControlNode(originalNode) {
						for k, v := range tc.labels.LinkedRunnerLabels {
							wantLabels[k] = v
						}
					} else {
						for k, v := range tc.labels.RegularLabels {
							wantLabels[k] = v
						}
					}
				}

				if diff := cmp.Diff(wantLabels, currentNode.Labels); diff != "" {
					if wantLabeledSet[originalNode.Name] {
						t.Errorf("Node %s mismatch (should have been labeled) (-want +got):\n%s", originalNode.Name, diff)
					} else {
						t.Errorf("Node %s mismatch (should NOT have changed) (-want +got):\n%s", originalNode.Name, diff)
					}
				}
			}
		})
	}
}

func TestUnlabelNodeGroup(t *testing.T) {
	testCases := []struct {
		name                    string
		nodes                   []runtime.Object
		labelKeys               util.NodeLabelKeys
		partitionID             string
		enableLinkedRunner      bool
		expectedErr             string
		expectedUnlabeled       []string
		mockGetMIGByPartitionID func(partitionID string) (cache.MIGInfo, bool)
	}{
		{
			name: "unlabel nodes",
			nodes: []runtime.Object{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"label1": "val1", "label2": "val2"}}},
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"label1": "val1", "label2": "val2"}}},
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"label1": "val1", "label2": "val2"}}},
			},
			labelKeys: util.NodeLabelKeys{
				RegularLabels: []string{"label1", "label2"},
			},
			partitionID:        "p1",
			enableLinkedRunner: false,
			expectedErr:        "",
			expectedUnlabeled:  []string{"node1", "node3"},
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
							"node2": {Name: "node2", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p2"}}},
							"node3": {Name: "node3", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name: "unlabel nodes in a linked VM environment",
			nodes: []runtime.Object{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-node-1", Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node", "linked-label": "true"}}},
			},
			labelKeys: util.NodeLabelKeys{
				LinkedRunnerLabels: []string{"linked-label"},
			},
			partitionID:        "p1",
			enableLinkedRunner: true,
			expectedErr:        "",
			expectedUnlabeled:  []string{"control-node-1"},
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "linked-vm-mig",
						Nodes: map[string]*cache.NodeInfo{
							"control-node-1": {Name: "control-node-1", MIGName: "linked-vm-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockCache := mocks.NewMockSliceRelationalCache(ctrl)

			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.nodes...).Build()
			mockCache.EXPECT().GetMIGByPartitionID(tc.partitionID).DoAndReturn(tc.mockGetMIGByPartitionID).AnyTimes()

			c := NewClient(fakeClient, mockCache, tc.enableLinkedRunner)

			err := c.UnlabelNodeGroup(context.Background(), tc.labelKeys, tc.partitionID)

			if (err != nil) != (tc.expectedErr != "") {
				t.Errorf("TaintNodeGroup() error = %v, wantErr %v", err, tc.expectedErr)
			}
			if tc.expectedErr != "" && err != nil && !strings.Contains(err.Error(), tc.expectedErr) {
				t.Errorf("TaintNodeGroup() error = %v, wantErr %v", err, tc.expectedErr)
			}

			wantUnlabeledSet := make(map[string]bool)
			for _, name := range tc.expectedUnlabeled {
				wantUnlabeledSet[name] = true
			}

			for _, obj := range tc.nodes {
				originalNode, ok := obj.(*corev1.Node)
				if !ok {
					continue
				}

				currentNode := &corev1.Node{}
				err := fakeClient.Get(context.Background(), types.NamespacedName{Name: originalNode.Name}, currentNode)
				if err != nil {
					t.Fatalf("Failed to get node %s: %v", originalNode.Name, err)
				}

				wantLabels := make(map[string]string)
				for k, v := range originalNode.Labels {
					wantLabels[k] = v
				}

				if wantUnlabeledSet[originalNode.Name] {
					if tc.enableLinkedRunner && util.IsLinkedRunnerControlNode(originalNode) {
						for _, key := range tc.labelKeys.LinkedRunnerLabels {
							delete(wantLabels, key)
						}
					} else {
						for _, key := range tc.labelKeys.RegularLabels {
							delete(wantLabels, key)
						}
					}
				}
				if len(wantLabels) == 0 {
					wantLabels = nil
				}

				if diff := cmp.Diff(wantLabels, currentNode.Labels); diff != "" {
					if wantUnlabeledSet[originalNode.Name] {
						t.Errorf("Node %s mismatch (should have been unlabeled) (-want +got):\n%s", originalNode.Name, diff)
					} else {
						t.Errorf("Node %s mismatch (should NOT have changed) (-want +got):\n%s", originalNode.Name, diff)
					}
				}
			}
		})
	}
}

func TestPatchNodeWithRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node", ResourceVersion: "1"},
	}

	modifyFunc := func(n *corev1.Node) {
		if n.Labels == nil {
			n.Labels = make(map[string]string)
		}
		n.Labels["test-label"] = "test-value"
	}

	tests := []struct {
		name          string
		getErr        error
		patchErr      []error
		expectErr     bool
		expectedCalls int
	}{
		{
			name:          "successful patch",
			expectErr:     false,
			expectedCalls: 1,
		},
		{
			name:          "retry on conflict",
			patchErr:      []error{errors.NewConflict(corev1.Resource("nodes"), "test-node", nil)},
			expectErr:     false,
			expectedCalls: 2,
		},
		{
			name: "retry on multiple conflicts",
			patchErr: []error{
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
			},
			expectErr:     false,
			expectedCalls: 3,
		},
		{
			name: "fail after exceeding retry limit",
			patchErr: []error{
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
				errors.NewConflict(corev1.Resource("nodes"), "test-node", nil),
			},
			expectErr:     true,
			expectedCalls: 5,
		},
		{
			name:          "fail on get error",
			getErr:        errors.NewNotFound(corev1.Resource("nodes"), "test-node"),
			expectErr:     true,
			expectedCalls: 0,
		},
		{
			name:          "fail on non-conflict patch error",
			patchErr:      []error{errors.NewBadRequest("test error")},
			expectErr:     true,
			expectedCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(node.DeepCopy()).Build()
			mockClient := &mockClient{
				Client:   fakeClient,
				getErr:   tt.getErr,
				patchErr: tt.patchErr,
				node:     node.DeepCopy(),
			}
			c := &kubeHelper{client: mockClient}

			err := c.patchNodeWithRetry(context.Background(), node, modifyFunc)

			if (err != nil) != tt.expectErr {
				t.Errorf("patchNodeWithRetry() error = %v, wantErr %v", err, tt.expectErr)
			}

			if mockClient.patchCalls != tt.expectedCalls {
				t.Errorf("patchNodeWithRetry() expected %d patch calls, got %d", tt.expectedCalls, mockClient.patchCalls)
			}

			if err == nil {
				updatedNode := &corev1.Node{}
				err := fakeClient.Get(context.Background(), types.NamespacedName{Name: node.Name}, updatedNode)
				if err != nil {
					t.Fatalf("Failed to get updated node: %v", err)
				}
				if updatedNode.Labels["test-label"] != testValue {
					t.Errorf("Node label not updated as expected")
				}
			}
		})
	}
}

func TestCheckNodeLabels(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := slicev1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add slicev1beta1 to scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}

	testCases := []struct {
		name                    string
		partitionIds            []string
		labels                  util.NodeLabels
		nodes                   []client.Object
		shouldExist             bool
		wantResult              bool
		wantErr                 bool
		mockGetMIGByPartitionID func(partitionID string) (cache.MIGInfo, bool)
		enableLinkedRunner      bool
	}{
		{
			name:         "labels exist, shouldExist is true",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x1"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x1", tpuTopology1x: "p1"},
					},
				},
			},
			shouldExist: true,
			wantResult:  true,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "labels exist but value mismatch, shouldExist is true",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x1"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "wrong-topology", tpuTopology1x: "p1"},
					},
				},
			},
			shouldExist: true,
			wantResult:  false,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "some labels missing, shouldExist is true",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x1"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", tpuTopology1x: "p1"},
					},
				},
			},
			shouldExist: true,
			wantResult:  false,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "labels do not exist, shouldExist is false",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x1"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{"key1": "val1", tpuTopology1x: "p1"},
					},
				},
			},
			shouldExist: false,
			wantResult:  true,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "some labels exist, shouldExist is false",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x1"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", tpuTopology1x: "p1"},
					},
				},
			},
			shouldExist: false,
			wantResult:  false,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "slice label belongs to another slice, shouldExist is false",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x1"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{util.SliceLabelKey: "another-slice", util.TopologyLabelKey: "2x2x1", tpuTopology1x: "p1"},
					},
				},
			},
			shouldExist: false,
			wantResult:  true,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "LinkedRunner control node label exists, shouldExist is false",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				LinkedRunnerLabels: map[string]string{"cloud.google.com/gke-tpu-slice-test-slice": "true"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "control-node-1",
						Labels: map[string]string{util.LinkedRunnerNodeTypeLabel: "control-node", "cloud.google.com/gke-tpu-slice-test-slice": "true", tpuTopology1x: "p1"},
					},
				},
			},
			shouldExist:        false,
			wantResult:         false,
			enableLinkedRunner: true,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "linked-vm-mig",
						Nodes: map[string]*cache.NodeInfo{
							"control-node-1": {Name: "control-node-1", MIGName: "linked-vm-mig", TopologyPartitionIds: map[string][]string{tpuTopology1x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "no nodes match selector",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{"check1": "v1"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{"key1": "val1", "check1": "v1", tpuTopology1x: "p2"},
					},
				},
			},
			shouldExist: true,
			wantResult:  true,
			wantErr:     false,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "multiple nodes, all match",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x2"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x2", tpuTopology2x: "p1"},
					},
				},
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node2",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x2", tpuTopology2x: "p1"},
					},
				},
			},
			shouldExist: true,
			wantResult:  true,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
							"node2": {Name: "node2", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
		{
			name:         "multiple nodes, one mismatch",
			partitionIds: []string{"p1"},
			labels: util.NodeLabels{
				RegularLabels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x2"},
			},
			nodes: []client.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node1",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", util.TopologyLabelKey: "2x2x2", tpuTopology2x: "p1"},
					},
				},
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node2",
						Labels: map[string]string{util.SliceLabelKey: "test-slice", tpuTopology2x: "p1"},
					},
				},
			},
			shouldExist: true,
			wantResult:  false,
			mockGetMIGByPartitionID: func(partitionID string) (cache.MIGInfo, bool) {
				if partitionID == "p1" {
					return cache.MIGInfo{
						Name: "test-mig",
						Nodes: map[string]*cache.NodeInfo{
							"node1": {Name: "node1", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
							"node2": {Name: "node2", MIGName: "test-mig", TopologyPartitionIds: map[string][]string{tpuTopology2x: {"p1"}}},
						},
					}, true
				}
				return cache.MIGInfo{}, false
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockCache := mocks.NewMockSliceRelationalCache(ctrl)

			scheme := runtime.NewScheme()
			if err := slicev1beta1.AddToScheme(scheme); err != nil {
				t.Fatalf("Failed to add slicev1beta1 to scheme: %v", err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("Failed to add corev1 to scheme: %v", err)
			}

			fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(tc.nodes...).Build()
			mockCache.EXPECT().GetMIGByPartitionID(gomock.Any()).DoAndReturn(tc.mockGetMIGByPartitionID).AnyTimes()
			c := &kubeHelper{client: fakeClient, enableLinkedRunner: tc.enableLinkedRunner, cache: mockCache}
			ctx := context.Background()

			gotResult, err := c.CheckNodeLabels(ctx, tc.partitionIds, tc.labels, tc.shouldExist)

			if (err != nil) != tc.wantErr {
				t.Errorf("CheckNodeLabels() error = %v, wantErr %v", err, tc.wantErr)
			}
			if gotResult != tc.wantResult {
				t.Errorf("CheckNodeLabels() gotResult = %v, want %v", gotResult, tc.wantResult)
			}
		})
	}
}

func TestLabelNodeGroupByNodeNames(t *testing.T) {
	nodes := []runtime.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"pre-existing": "true"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{testNodeGroupLabel: "pool2"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-c", Labels: nil}}, // Test adding to nil map
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}
	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(nodes...).Build()

	c := NewClient(fakeClient, nil, false)
	labels := map[string]string{"new-label": "new-value"}
	nodeNames := []string{"node-a", "node-c"}

	err := c.LabelNodeGroupByNodeNames(context.Background(), labels, nodeNames)
	if err != nil {
		t.Fatalf("LabelNodeGroupByNodeNames() error = %v, wantErr nil", err)
	}

	// Check node-a
	expectedLabelsA := map[string]string{
		"pre-existing": "true",
		"new-label":    "new-value",
	}
	updatedNodeA := &corev1.Node{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, updatedNodeA); err != nil {
		t.Fatalf("Failed to get updated node-a: %v", err)
	}
	if diff := cmp.Diff(expectedLabelsA, updatedNodeA.Labels); diff != "" {
		t.Errorf("LabelNodeGroupByNodeNames() mismatch for node-a (-want +got):\n%s", diff)
	}

	// Check node-c
	expectedLabelsC := map[string]string{
		"new-label": "new-value",
	}
	updatedNodeC := &corev1.Node{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-c"}, updatedNodeC); err != nil {
		t.Fatalf("Failed to get updated node-c: %v", err)
	}
	if diff := cmp.Diff(expectedLabelsC, updatedNodeC.Labels); diff != "" {
		t.Errorf("LabelNodeGroupByNodeNames() mismatch for node-c (-want +got):\n%s", diff)
	}

	// Check that node-b was not labeled
	nodeB := &corev1.Node{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-b"}, nodeB); err != nil {
		t.Fatalf("Failed to get node-b: %v", err)
	}
	if _, ok := nodeB.Labels["new-label"]; ok {
		t.Errorf("Node 'node-b' should not have been labeled, but has new label")
	}
}

func TestLabelNodeGroupByNodeNames_NodeNotFound(t *testing.T) {
	nodes := []runtime.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"pre-existing": "true"}}},
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}
	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(nodes...).Build()

	c := NewClient(fakeClient, nil, false)
	labels := map[string]string{"new-label": "new-value"}
	nodeNames := []string{"node-a", "node-MISSING"}

	err := c.LabelNodeGroupByNodeNames(context.Background(), labels, nodeNames)
	if err == nil {
		t.Fatalf("LabelNodeGroupByNodeNames() error = nil, wantErr")
	}

	// Check that the error is about the missing node
	if !strings.Contains(err.Error(), "node-MISSING") {
		t.Errorf("Error message does not contain the missing node name. Got: %v", err)
	}

	// Check that node-a was NOT labeled because the function failed early
	updatedNodeA := &corev1.Node{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, updatedNodeA); err != nil {
		t.Fatalf("Failed to get node-a: %v", err)
	}
	if _, ok := updatedNodeA.Labels["new-label"]; ok {
		t.Errorf("Node 'node-a' should not have been labeled, but has new label")
	}
}

func TestUnlabelNodeGroupByNames(t *testing.T) {
	nodes := []runtime.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"remove-me": "value", "keep-me": "value-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"remove-me": "value", "keep-me": "value-b"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-c", Labels: map[string]string{"remove-me": "value", "keep-me": "value-c"}}},
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}
	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(nodes...).Build()

	c := NewClient(fakeClient, nil, false)

	labelKeys := []string{"remove-me"}
	nodeNames := []string{"node-a", "node-c"}

	err := c.UnlabelNodeGroupByNames(context.Background(), labelKeys, nodeNames)
	if err != nil {
		t.Fatalf("UnlabelNodeGroupByNames() error = %v, wantErr nil", err)
	}

	// Check node-a (should be unlabelled)
	expectedLabelsA := map[string]string{
		"keep-me": "value-a",
	}
	updatedNodeA := &corev1.Node{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, updatedNodeA); err != nil {
		t.Fatalf("Failed to get updated node-a: %v", err)
	}
	if diff := cmp.Diff(expectedLabelsA, updatedNodeA.Labels); diff != "" {
		t.Errorf("UnlabelNodeGroupByNames() mismatch for node-a (-want +got):\n%s", diff)
	}

	// Check node-c (should be unlabelled)
	expectedLabelsC := map[string]string{
		"keep-me": "value-c",
	}
	updatedNodeC := &corev1.Node{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-c"}, updatedNodeC); err != nil {
		t.Fatalf("Failed to get updated node-c: %v", err)
	}
	if diff := cmp.Diff(expectedLabelsC, updatedNodeC.Labels); diff != "" {
		t.Errorf("UnlabelNodeGroupByNames() mismatch for node-c (-want +got):\n%s", diff)
	}

	// Check that node-b was NOT unlabelled
	expectedLabelsB := map[string]string{
		"remove-me": "value",
		"keep-me":   "value-b",
	}
	nodeB := &corev1.Node{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-b"}, nodeB); err != nil {
		t.Fatalf("Failed to get node-b: %v", err)
	}
	if diff := cmp.Diff(expectedLabelsB, nodeB.Labels); diff != "" {
		t.Errorf("Node 'node-b' should not have been unlabelled, but its labels changed (-want +got):\n%s", diff)
	}
}

func TestLabelNodeGroupByNodeNames_WithExistingLabels(t *testing.T) {
	// Setup initial nodes
	nodes := []runtime.Object{
		// Node 1: No labels, needs update
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{}}},
		// Node 2: Already has the label (Tests your optimization path)
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"target-label": "target-val"}}},
		// Node 3: Has a different label, needs appending
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"other": "existing"}}},
		// Node 4: Not targeted, should not be touched
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node4", Labels: map[string]string{}}},
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}
	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(nodes...).Build()
	c := NewClient(fakeClient, nil, false)

	// Define the operation
	labelsToAdd := map[string]string{"target-label": "target-val"}
	targetNodes := []string{"node1", "node2", "node3"}

	// Execute
	err := c.LabelNodeGroupByNodeNames(context.Background(), labelsToAdd, targetNodes)
	if err != nil {
		t.Fatalf("LabelNodeGroupByNodeNames() error = %v, wantErr nil", err)
	}

	// Verification
	tests := []struct {
		nodeName string
		want     map[string]string
	}{
		{
			nodeName: "node1",
			want:     map[string]string{"target-label": "target-val"},
		},
		{
			nodeName: "node2",
			want:     map[string]string{"target-label": "target-val"},
		},
		{
			nodeName: "node3",
			want:     map[string]string{"other": "existing", "target-label": "target-val"},
		},
		{
			nodeName: "node4", // Should remain empty
			want:     nil,
		},
	}

	for _, tt := range tests {
		gotNode := &corev1.Node{}
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.nodeName}, gotNode)
		if err != nil {
			t.Errorf("Failed to get node %s: %v", tt.nodeName, err)
			continue
		}
		if diff := cmp.Diff(tt.want, gotNode.Labels); diff != "" {
			t.Errorf("Node %s labels mismatch (-want +got):\n%s", tt.nodeName, diff)
		}
	}
}

func TestUnlabelNodeGroupByNames_WithExistingLabels(t *testing.T) {
	// Setup initial nodes
	nodes := []runtime.Object{
		// Node 1: Has label, needs removal
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"remove-me": "val", "keep-me": "val"}}},
		// Node 2: Label already missing (Tests your optimization path)
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"keep-me": "val"}}},
		// Node 3: Nil labels map (Tests nil safety optimization)
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: nil}},
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}
	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(nodes...).Build()
	c := NewClient(fakeClient, nil, false)

	// Define the operation
	labelsToRemove := []string{"remove-me"}
	targetNodes := []string{"node1", "node2", "node3"}

	// Execute
	err := c.UnlabelNodeGroupByNames(context.Background(), labelsToRemove, targetNodes)
	if err != nil {
		t.Fatalf("UnlabelNodeGroupByNames() error = %v, wantErr nil", err)
	}

	// Verification
	tests := []struct {
		nodeName string
		want     map[string]string
	}{
		{
			nodeName: "node1",
			want:     map[string]string{"keep-me": "val"}, // "remove-me" should be gone
		},
		{
			nodeName: "node2",
			want:     map[string]string{"keep-me": "val"}, // Unchanged
		},
		{
			nodeName: "node3",
			want:     nil, // Unchanged (still nil)
		},
	}

	for _, tt := range tests {
		gotNode := &corev1.Node{}
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.nodeName}, gotNode)
		if err != nil {
			t.Errorf("Failed to get node %s: %v", tt.nodeName, err)
			continue
		}
		if diff := cmp.Diff(tt.want, gotNode.Labels); diff != "" {
			t.Errorf("Node %s labels mismatch (-want +got):\n%s", tt.nodeName, diff)
		}
	}
}
