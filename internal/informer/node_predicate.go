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
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// NodeChangePredicate filters events for the SliceReconciler.
type NodeChangePredicate struct {
	predicate.Funcs
}

// hasTopologyLabel checks if a node has at least one TPU topology label.
func hasTopologyLabel(node *corev1.Node) bool {
	if node == nil || node.Labels == nil {
		return false
	}
	for label := range node.Labels {
		if strings.HasPrefix(label, tpuTopologyLabelPrefix) {
			return true
		}
	}
	return false
}

// getTopologyLabels extracts all TPU topology labels from a node.
func getTopologyLabels(node *corev1.Node) map[string]string {
	topoLabels := make(map[string]string)
	if node == nil || node.Labels == nil {
		return topoLabels
	}
	for label, value := range node.Labels {
		if strings.HasPrefix(label, tpuTopologyLabelPrefix) {
			topoLabels[label] = value
		}
	}
	return topoLabels
}

// Create handles new nodes. We always want to process node additions.
func (NodeChangePredicate) Create(e event.CreateEvent) bool {
	node, ok := e.Object.(*corev1.Node)
	if !ok {
		return false
	}
	// Do not trigger if it's a non-TPU node or has no topology labels.
	if !hasTopologyLabel(node) || getLabel(node, tpuAcceleratorLabel) == "" {
		return false
	}

	return true
}

// Delete handles node deletions. We always want to process node removals.
func (NodeChangePredicate) Delete(e event.DeleteEvent) bool {
	node, ok := e.Object.(*corev1.Node)
	if !ok {
		return false
	}
	// Do not trigger if it's a non-TPU node or has no topology labels.
	if !hasTopologyLabel(node) || getLabel(node, tpuAcceleratorLabel) == "" {
		return false
	}
	return true
}

// Update is the core logic. It triggers reconciliation only on specific changes.
func (NodeChangePredicate) Update(e event.UpdateEvent) bool {
	oldNode, ok := e.ObjectOld.(*corev1.Node)
	if !ok {
		return false
	}
	newNode, ok := e.ObjectNew.(*corev1.Node)
	if !ok {
		return false
	}

	// 1) Trigger if the 'NodeReady' condition changes.
	if hasConditionChanged(oldNode, newNode, corev1.NodeReady) {
		return true
	}

	// 2) Trigger if the tpuClaimedBySliceLabel value has changed.
	if getLabel(oldNode, tpuClaimedBySliceLabel) != getLabel(newNode, tpuClaimedBySliceLabel) {
		return true
	}

	// 3) Trigger if any of the topology labels have changed.
	if !reflect.DeepEqual(getTopologyLabels(oldNode), getTopologyLabels(newNode)) {
		return true
	}

	// 4) Do not trigger if it's a non-TPU node or has no topology labels.
	if !hasTopologyLabel(newNode) || getLabel(newNode, tpuAcceleratorLabel) == "" {
		return false
	}

	// Ignore all other updates.
	return false
}

// Generic handles generic events.
func (NodeChangePredicate) Generic(e event.GenericEvent) bool {
	node, ok := e.Object.(*corev1.Node)
	if !ok {
		return false
	}
	// Do not trigger if it's a non-TPU node or has no topology labels.
	if !hasTopologyLabel(node) || getLabel(node, tpuAcceleratorLabel) == "" {
		return false
	}
	return true
}

// getLabel returns the value of a label from a node.
func getLabel(node *corev1.Node, label string) string {
	if node == nil || node.Labels == nil {
		return ""
	}
	return node.Labels[label]
}

// findNodeCondition is a helper to find a specific condition in a slice.
func findNodeCondition(conditions []corev1.NodeCondition, condType corev1.NodeConditionType) *corev1.NodeCondition {
	for i, cond := range conditions {
		if cond.Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// hasConditionChanged checks if a specific condition has changed between two nodes.
func hasConditionChanged(old, newNode *corev1.Node, condType corev1.NodeConditionType) bool {
	oldCond := findNodeCondition(old.Status.Conditions, condType)
	newCond := findNodeCondition(newNode.Status.Conditions, condType)

	// If the condition is present in one node but not the other, it has changed.
	if (oldCond == nil && newCond != nil) || (oldCond != nil && newCond == nil) {
		return true
	}

	// If both conditions are present, compare their statuses.
	if oldCond != nil && newCond != nil && oldCond.Status != newCond.Status {
		return true
	}

	return false
}
