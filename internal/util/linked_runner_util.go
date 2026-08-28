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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"slice-controller/internal/cache"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	LinkedRunnerNodeTypeLabel    = "cloud.google.com/goog-gke-linked-runner-node-type"
	LinkedRunnerAnnotationPrefix = "linked-runner.runner-instance/"
	ChildPoolIgUrlAnnotationKey  = "linked-runner.child-pool-ig-url"
)

// RunnerNodeInfo holds the unmarshalled data from the linked-vm control node annotation.
type RunnerNodeInfo struct {
	IPAddress    string `json:"IP_ADDRESS"`
	Status       string `json:"STATUS"`
	PartitionIDs string `json:"TPU_ACCELERATOR_PARTITION_ID"`
}

// IsLinkedRunnerControlNode checks if a node is a Linked Runner Control Node.
func IsLinkedRunnerControlNode(node *corev1.Node) bool {
	if node == nil || node.Labels == nil {
		return false
	}
	return node.Labels[LinkedRunnerNodeTypeLabel] == "control-node"
}

// ExtractRunnerPartitionsFromControlNode extracts a mapping of IP address to topology and their corresponding partition IDs
// from a linked runner control node's annotations.
func ExtractRunnerPartitionsFromControlNode(node *corev1.Node) (map[string]map[string]string, error) {
	partitions := make(map[string]map[string]string)

	runnerInfos, err := ExtractRunnerInfo(node)
	if err != nil {
		return nil, err
	}

	for _, runnerNodeInfo := range runnerInfos {
		if runnerNodeInfo.PartitionIDs == "" || runnerNodeInfo.IPAddress == "" {
			continue
		}

		var partitionIDs map[string]string
		if err := json.Unmarshal([]byte(runnerNodeInfo.PartitionIDs), &partitionIDs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal PartitionIDs string %s for runner IP %s on node %s: %w", runnerNodeInfo.PartitionIDs, runnerNodeInfo.IPAddress, node.Name, err)
		}

		if partitionIDs == nil {
			continue
		}

		runnerNodePartitions := make(map[string]string)
		for topology, partitionID := range partitionIDs {
			if _, ok := SupportedTopologiesInfo[topology]; ok {
				runnerNodePartitions[topology] = partitionID
			}
		}

		if len(runnerNodePartitions) > 0 {
			partitions[runnerNodeInfo.IPAddress] = runnerNodePartitions
		}
	}

	return partitions, nil
}

// ExtractRunnerInfo extracts a mapping of runner instance name to its info from a control node's annotations.
func ExtractRunnerInfo(node *corev1.Node) (map[string]RunnerNodeInfo, error) {
	if !IsLinkedRunnerControlNode(node) {
		return nil, fmt.Errorf("node %s is not a Linked VM control node", node.Name)
	}

	infos := make(map[string]RunnerNodeInfo)
	if node.Annotations == nil {
		return infos, nil
	}

	for key, value := range node.Annotations {
		if !strings.HasPrefix(key, LinkedRunnerAnnotationPrefix) {
			continue
		}

		var runnerNodeInfo RunnerNodeInfo
		if err := json.Unmarshal([]byte(value), &runnerNodeInfo); err != nil {
			return nil, fmt.Errorf("failed to unmarshal annotation %s on node %s: %w", key, node.Name, err)
		}

		instanceName := strings.TrimPrefix(key, LinkedRunnerAnnotationPrefix)
		infos[instanceName] = runnerNodeInfo
	}
	return infos, nil
}

// IsLinkedRunnerMIG checks if the MIG corresponds to a Linked Runner control node.
// For Linked Runners, there should only be one VM corresponding to the MIG.
// It returns true and the control node if found.
func IsLinkedRunnerMIG(ctx context.Context, migInfo cache.MIGInfo, c client.Client) (bool, *corev1.Node, error) {
	if len(migInfo.Nodes) != 1 {
		return false, nil, nil
	}

	var nodeName string
	for name := range migInfo.Nodes {
		nodeName = name
		break
	}

	node := &corev1.Node{}
	if err := c.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	if IsLinkedRunnerControlNode(node) {
		return true, node, nil
	}

	return false, nil, nil
}
