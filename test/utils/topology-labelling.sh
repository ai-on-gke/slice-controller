#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if [ -z "$1" ]; then
  echo "Usage: $0 <nodepool-name>"
  exit 1
fi

NODEPOOL_NAME=$1

# Get all nodes in the specified nodepool, sorted by name.
nodes_output=$(kubectl get nodes -l cloud.google.com/gke-nodepool=$NODEPOOL_NAME -o=jsonpath='{.items[*].metadata.name}')
if [ -z "$nodes_output" ]; then
  echo "Error: No nodes found in nodepool $NODEPOOL_NAME."
  exit 1
fi

# Create a sorted array of node names.
mapfile -t node_array < <(echo "$nodes_output" | tr " " "\n" | sort)

num_nodes=${#node_array[@]}
echo "Found $num_nodes nodes in nodepool $NODEPOOL_NAME. Starting labeling..."

# Loop through each node and apply all labels in a single command.
for i in "${!node_array[@]}"; do
  node_name=${node_array[$i]}

  if [ -z "$node_name" ]; then
      continue
  fi

  # Calculate topology IDs with nodepool prefix
  subBlock="${NODEPOOL_NAME}"
  block="block-A"
  id_16x16="${NODEPOOL_NAME}-16x16-0"
  id_8x16="${NODEPOOL_NAME}-8x16-$((i / 32))"
  id_8x8="${NODEPOOL_NAME}-8x8-$((i / 16))"
  id_4x8="${NODEPOOL_NAME}-4x8-$((i / 8))"
  id_4x4="${NODEPOOL_NAME}-4x4-$((i / 4))"
  id_2x4="${NODEPOOL_NAME}-2x4-$((i / 2))"
  id_2x2="${NODEPOOL_NAME}-2x2-$i"

  # Construct the full set of labels for the node.
  labels="tpus.gke.io/topology-16x16-id=${id_16x16} \
tpus.gke.io/topology-8x16-id=${id_8x16} \
tpus.gke.io/topology-8x8-id=${id_8x8} \
tpus.gke.io/topology-4x8-id=${id_4x8} \
tpus.gke.io/topology-4x4-id=${id_4x4} \
tpus.gke.io/topology-2x4-id=${id_2x4} \
tpus.gke.io/topology-2x2-id=${id_2x2} \
tpus.gke.io/accelerator=v6e \
tpus.gke.io/sub-block=$subBlock \
tpus.gke.io/sub-block=$block"

  # Apply all labels at once.
  if ! kubectl label node "$node_name" $labels --overwrite; then
    echo "Failed to label node $node_name"
  fi
done

echo "Labeling complete."
