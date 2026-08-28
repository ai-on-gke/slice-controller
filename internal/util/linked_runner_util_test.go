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

package util

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsLinkedRunnerControlNode(t *testing.T) {
	testCases := []struct {
		name string
		node *corev1.Node
		want bool
	}{
		{
			name: "is a control node",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
				},
			},
			want: true,
		},
		{
			name: "is not a control node",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "runner-node"},
				},
			},
			want: false,
		},
		{
			name: "label does not exist",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			want: false,
		},
		{
			name: "nil labels",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{},
			},
			want: false,
		},
		{
			name: "nil node",
			node: nil,
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsLinkedRunnerControlNode(tc.node)
			if got != tc.want {
				t.Errorf("IsLinkedRunnerControlNode() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExtractRunnerPartitionsFromControlNode(t *testing.T) {
	testCases := []struct {
		name          string
		node          *corev1.Node
		want          map[string]map[string]string
		wantErr       bool
		expectedError string
	}{
		{
			name: "successful extraction",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"READY","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x1\":\"uid-1\", \"2x2x2\":\"uid-2\"}"}`,
						"linked-runner.runner-instance/runner-2": `{"IP_ADDRESS":"10.0.0.2","STATUS":"READY","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x2\":\"uid-2\"}"}`,
					},
				},
			},
			want: map[string]map[string]string{
				"10.0.0.1": {
					"2x2x1": "uid-1",
					"2x2x2": "uid-2",
				},
				"10.0.0.2": {
					"2x2x2": "uid-2",
				},
			},
			wantErr: false,
		},
		{
			name: "malformed childVMInfo annotation",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						"linked-runner.runner-instance/runner-1": `malformed-json`,
					},
				},
			},
			want:          nil,
			wantErr:       true,
			expectedError: "failed to unmarshal annotation linked-runner.runner-instance/runner-1 on node control-node-1: invalid character 'm' looking for beginning of value",
		},
		{
			name: "malformed topologyIDs json",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"READY","TPU_ACCELERATOR_PARTITION_ID":"malformed-json"}`,
					},
				},
			},
			want:          nil,
			wantErr:       true,
			expectedError: "failed to unmarshal PartitionIDs string malformed-json for runner IP 10.0.0.1 on node control-node-1: invalid character 'm' looking for beginning of value",
		},
		{
			name: "no linked-runner annotations",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						"some-other-annotation": "some-value",
					},
				},
			},
			want:    map[string]map[string]string{},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExtractRunnerPartitionsFromControlNode(tc.node)
			if (err != nil) != tc.wantErr {
				t.Errorf("ExtractRunnerPartitionsFromControlNode() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if tc.wantErr && err.Error() != tc.expectedError {
				t.Errorf("ExtractRunnerPartitionsFromControlNode() error = %q, want %q", err.Error(), tc.expectedError)
			}
			if !cmp.Equal(got, tc.want) {
				t.Errorf("ExtractRunnerPartitionsFromControlNode() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExtractRunnerInfo(t *testing.T) {
	testCases := []struct {
		name          string
		node          *corev1.Node
		want          map[string]RunnerNodeInfo
		wantErr       bool
		expectedError string
	}{
		{
			name: "successful extraction",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						"linked-runner.runner-instance/runner-1": `{"IP_ADDRESS":"10.0.0.1","STATUS":"RUNNING","TPU_ACCELERATOR_PARTITION_ID":"{\"2x2x1\":\"uid-1\"}"} `,
						"linked-runner.runner-instance/runner-2": `{"IP_ADDRESS":"10.0.0.2","STATUS":"PROVISIONING","TPU_ACCELERATOR_PARTITION_ID":""}`,
					},
				},
			},
			want: map[string]RunnerNodeInfo{
				"runner-1": {IPAddress: "10.0.0.1", Status: "RUNNING", PartitionIDs: "{\"2x2x1\":\"uid-1\"}"},
				"runner-2": {IPAddress: "10.0.0.2", Status: "PROVISIONING", PartitionIDs: ""},
			},
			wantErr: false,
		},
		{
			name: "not a control node",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "not-control-node",
				},
			},
			wantErr:       true,
			expectedError: "node not-control-node is not a Linked VM control node",
		},
		{
			name: "malformed annotation json",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						"linked-runner.runner-instance/runner-1": `malformed`,
					},
				},
			},
			wantErr:       true,
			expectedError: "failed to unmarshal annotation linked-runner.runner-instance/runner-1 on node control-node-1: invalid character 'm' looking for beginning of value",
		},
		{
			name: "no runner annotations",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{
						"other-annotation": "value",
					},
				},
			},
			want:    map[string]RunnerNodeInfo{},
			wantErr: false,
		},
		{
			name: "nil annotations",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "control-node-1",
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
				},
			},
			want:    map[string]RunnerNodeInfo{},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExtractRunnerInfo(tc.node)
			if (err != nil) != tc.wantErr {
				t.Errorf("ExtractRunnerInfo() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if tc.wantErr && err.Error() != tc.expectedError {
				t.Errorf("ExtractRunnerInfo() error = %q, want %q", err.Error(), tc.expectedError)
			}
			if !cmp.Equal(got, tc.want) {
				t.Errorf("ExtractRunnerInfo() = %v, want %v, diff: %s", got, tc.want, cmp.Diff(got, tc.want))
			}
		})
	}
}
