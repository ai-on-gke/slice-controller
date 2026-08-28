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
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

const (
	MIGAnnotationKey = "node.gke.io/created-by-mig"
)

// GetMIGAnnotationKey returns the appropriate MIG annotation key for a given node.
func GetMIGAnnotationKey(node *corev1.Node, enableLinkedRunner bool) string {
	if enableLinkedRunner && IsLinkedRunnerControlNode(node) {
		return ChildPoolIgUrlAnnotationKey
	}
	return MIGAnnotationKey
}

// GetMIGAnnotationFromNode extracts the MIG annotation value from a node's annotations.
func GetMIGAnnotationFromNode(node *corev1.Node, enableLinkedRunner bool) (string, error) {
	if node == nil || node.Annotations == nil {
		return "", fmt.Errorf("node or its annotations are nil")
	}

	key := GetMIGAnnotationKey(node, enableLinkedRunner)
	migAnnotation, ok := node.Annotations[key]
	if !ok || migAnnotation == "" {
		return "", fmt.Errorf("MIG annotation '%s' not found or empty on node %s", key, node.Name)
	}
	return migAnnotation, nil
}
