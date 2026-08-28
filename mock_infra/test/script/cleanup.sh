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


# Safety check to prevent running against prod clusters
CURRENT_CONTEXT=$(kubectl config current-context 2>/dev/null || echo "unknown")
if [[ "$CURRENT_CONTEXT" != *"kind-"* ]] && [[ "$CURRENT_CONTEXT" != "kind" ]]; then
    echo "ERROR: Current kubectl context is '$CURRENT_CONTEXT'. This script must only be run against a kind cluster."
    exit 1
fi

# --- Configuration ---
# User-defined list of node pool names to clean up.
POOL_NAMES=(
    "np-t1" "np-t2" "np-t3"
)

# The specific label to map (needed here to reconstruct the list)
TARGET_LABEL="cloud.google.com/gke-tpu-partition-4x4x4-id"

TOPOLOGY_LABELS=(
    "cloud.google.com/gke-tpu-partition-2x2x1-id"
    "cloud.google.com/gke-tpu-partition-2x2x2-id"
    "cloud.google.com/gke-tpu-partition-2x2x4-id"
    "cloud.google.com/gke-tpu-partition-2x4x4-id"
    "$TARGET_LABEL"
    "cloud.google.com/gke-tpu-slice"
    "cloud.google.com/gke-tpu-topology"
)

# The Annotation Key to remove
ANNOTATION_KEY="node.gke.io/created-by-mig"
# ---------------------

echo "Starting CLEANUP for pools: ${POOL_NAMES[@]}"
echo "=================================================="

for POOL_NAME in "${POOL_NAMES[@]}"; do

    echo "Cleaning Node Pool: $POOL_NAME"
    # 1. Get all nodes for this specific pool
    NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="$POOL_NAME" -o jsonpath='{.items[*].metadata.name}')

    # Check if we found any nodes
    if [ -z "$NODES" ]; then
        echo "  -> WARNING: No nodes found for pool '$POOL_NAME'. Skipping..."
        echo "--------------------------------------------------"
        continue
    fi

    # 2. Remove annotations and labels from all found nodes
    for NODE in $NODES; do
        # Remove MIG annotation (note the minus '-' at the end)
        kubectl annotate node "$NODE" "${ANNOTATION_KEY}-" --overwrite > /dev/null 2>&1

        # Remove topology labels
        for LABEL in "${TOPOLOGY_LABELS[@]}"; do
             # Remove label (note the minus '-' at the end)
             kubectl label node "$NODE" "$LABEL-" --overwrite > /dev/null 2>&1
        done
    done

    echo "  -> Successfully removed annotations and labels from nodes in '$POOL_NAME'"
    echo "--------------------------------------------------"
done

echo "Cleanup Complete."
