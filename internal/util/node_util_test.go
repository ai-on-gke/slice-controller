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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetMIGAnnotationKey(t *testing.T) {
	testCases := []struct {
		name               string
		node               *corev1.Node
		want               string
		enableLinkedRunner bool
	}{
		{
			name: "is a control node, linked VMs enabled",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
				},
			},
			want:               ChildPoolIgUrlAnnotationKey,
			enableLinkedRunner: true,
		},
		{
			name: "is a control node, linked VMs disabled",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
				},
			},
			want:               MIGAnnotationKey,
			enableLinkedRunner: false,
		},
		{
			name: "is not a control node",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{LinkedRunnerNodeTypeLabel: "runner-node"},
				},
			},
			want:               MIGAnnotationKey,
			enableLinkedRunner: true,
		},
		{
			name: "label does not exist",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			want:               MIGAnnotationKey,
			enableLinkedRunner: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := GetMIGAnnotationKey(tc.node, tc.enableLinkedRunner)
			if got != tc.want {
				t.Errorf("GetMIGAnnotationKey() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetMIGAnnotationFromNode(t *testing.T) {
	testCases := []struct {
		name               string
		node               *corev1.Node
		want               string
		wantErr            bool
		enableLinkedRunner bool
	}{
		{
			name: "successful extraction for regular node",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Annotations: map[string]string{MIGAnnotationKey: "my-mig"},
				},
			},
			want:               "my-mig",
			wantErr:            false,
			enableLinkedRunner: true,
		},
		{
			name: "successful extraction for linked VM control node, linked VMs enabled",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "control-node-1",
					Labels:      map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{ChildPoolIgUrlAnnotationKey: "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig"},
				},
			},
			want:               "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers/runner-mig",
			wantErr:            false,
			enableLinkedRunner: true,
		},
		{
			name: "successful extraction for linked VM control node, linked VMs disabled",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "control-node-1",
					Labels:      map[string]string{LinkedRunnerNodeTypeLabel: "control-node"},
					Annotations: map[string]string{MIGAnnotationKey: "my-regular-mig"},
				},
			},
			want:               "my-regular-mig",
			wantErr:            false,
			enableLinkedRunner: false,
		},
		{
			name: "annotation not found",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Annotations: map[string]string{},
				},
			},
			want:               "",
			wantErr:            true,
			enableLinkedRunner: true,
		},
		{
			name: "nil annotations",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
				},
			},
			want:               "",
			wantErr:            true,
			enableLinkedRunner: true,
		},
		{
			name:               "nil node",
			node:               nil,
			want:               "",
			wantErr:            true,
			enableLinkedRunner: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetMIGAnnotationFromNode(tc.node, tc.enableLinkedRunner)
			if (err != nil) != tc.wantErr {
				t.Errorf("GetMIGAnnotationFromNode() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("GetMIGAnnotationFromNode() = %v, want %v", got, tc.want)
			}
		})
	}
}
