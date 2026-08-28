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
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestNodeChangePredicate(t *testing.T) {
	g := NewWithT(t)
	p := NodeChangePredicate{}

	makeNode := func(labels map[string]string, conditions []corev1.NodeCondition) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
			},
			Status: corev1.NodeStatus{
				Conditions: conditions,
			},
		}
	}

	baseLabels := map[string]string{
		"tpus.gke.io/topology-2x2-id": "1",
		tpuAcceleratorLabel:           "v5e",
	}

	tpuNode := makeNode(baseLabels, nil)
	nonTpuNode := makeNode(map[string]string{"other-label": "value"}, nil)
	pod := &corev1.Pod{}

	createTests := []struct {
		name     string
		event    event.CreateEvent
		expected bool
	}{
		{
			name:     "should accept a valid TPU node",
			event:    event.CreateEvent{Object: tpuNode},
			expected: true,
		},
		{
			name:     "should reject a non-TPU node",
			event:    event.CreateEvent{Object: nonTpuNode},
			expected: false,
		},
		{
			name:     "should reject a non-Node object",
			event:    event.CreateEvent{Object: pod},
			expected: false,
		},
	}

	t.Run("CreateEvent", func(t *testing.T) {
		for _, tc := range createTests {
			t.Run(tc.name, func(t *testing.T) {
				g.Expect(p.Create(tc.event)).To(Equal(tc.expected))
			})
		}
	})

	deleteTests := []struct {
		name     string
		event    event.DeleteEvent
		expected bool
	}{
		{
			name:     "should accept a valid TPU node",
			event:    event.DeleteEvent{Object: tpuNode},
			expected: true,
		},
		{
			name:     "should reject a non-TPU node",
			event:    event.DeleteEvent{Object: nonTpuNode},
			expected: false,
		},
	}

	t.Run("DeleteEvent", func(t *testing.T) {
		for _, tc := range deleteTests {
			t.Run(tc.name, func(t *testing.T) {
				g.Expect(p.Delete(tc.event)).To(Equal(tc.expected))
			})
		}
	})

	updateTests := []struct {
		name     string
		event    event.UpdateEvent
		expected bool
	}{
		{
			name: "should accept when NodeReady condition changes",
			event: event.UpdateEvent{
				ObjectOld: makeNode(baseLabels, []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}),
				ObjectNew: makeNode(baseLabels, []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}),
			},
			expected: true,
		},
		{
			name: "should accept when tpuClaimedBySliceLabel changes",
			event: event.UpdateEvent{
				ObjectOld: makeNode(map[string]string{
					"tpus.gke.io/topology-2x2-id": "1",
					tpuAcceleratorLabel:           "v5e",
					tpuClaimedBySliceLabel:        "slice-a",
				}, nil),
				ObjectNew: makeNode(map[string]string{
					"tpus.gke.io/topology-2x2-id": "1",
					tpuAcceleratorLabel:           "v5e",
					tpuClaimedBySliceLabel:        "slice-b",
				}, nil),
			},
			expected: true,
		},
		{
			name: "should accept when topology label changes",
			event: event.UpdateEvent{
				ObjectOld: makeNode(baseLabels, nil),
				ObjectNew: makeNode(map[string]string{
					"tpus.gke.io/topology-2x2-id": "2",
					tpuAcceleratorLabel:           "v5e",
				}, nil),
			},
			expected: true,
		},
		{
			name: "should reject when a non-TPU node is updated",
			event: event.UpdateEvent{
				ObjectOld: makeNode(map[string]string{"other": "a"}, nil),
				ObjectNew: makeNode(map[string]string{"other": "b"}, nil),
			},
			expected: false,
		},
		{
			name: "should reject for unrelated label changes",
			event: event.UpdateEvent{
				ObjectOld: makeNode(map[string]string{
					"tpus.gke.io/topology-2x2-id": "1",
					tpuAcceleratorLabel:           "v5e",
					"unrelated":                   "old",
				}, nil),
				ObjectNew: makeNode(map[string]string{
					"tpus.gke.io/topology-2x2-id": "1",
					tpuAcceleratorLabel:           "v5e",
					"unrelated":                   "new",
				}, nil),
			},
			expected: false,
		},
		{
			name: "should reject for update event with non-node objects",
			event: event.UpdateEvent{
				ObjectOld: &corev1.Pod{},
				ObjectNew: &corev1.Pod{},
			},
			expected: false,
		},
	}

	t.Run("UpdateEvent", func(t *testing.T) {
		for _, tc := range updateTests {
			t.Run(tc.name, func(t *testing.T) {
				g.Expect(p.Update(tc.event)).To(Equal(tc.expected))
			})
		}
	})

	// Test Generic Predicate
	genericTests := []struct {
		name     string
		obj      client.Object
		expected bool
	}{
		{
			name:     "should accept a valid TPU node",
			obj:      tpuNode,
			expected: true,
		},
		{
			name:     "should reject a non-TPU node",
			obj:      nonTpuNode,
			expected: false,
		},
		{
			name:     "should reject a non-Node object",
			obj:      pod,
			expected: false,
		},
	}

	t.Run("GenericEvent", func(t *testing.T) {
		for _, tc := range genericTests {
			t.Run(tc.name, func(t *testing.T) {
				e := event.GenericEvent{Object: tc.obj}
				g.Expect(p.Generic(e)).To(Equal(tc.expected))
			})
		}
	})
}
