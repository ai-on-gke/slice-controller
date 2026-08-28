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
# User-defined list of node pool names.
# Example: POOL_NAMES=("np-t1" "np-t2" "my-custom-pool")
POOL_NAMES=(
    # Add your node pool names here
    "np-t2"
)

# Get the directory where the script is located
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
YAML_PATH="$SCRIPT_DIR/../yaml/create-slice.yaml"
API_ENDPOINT="http://localhost:8080/inject/mig"
BASE_MIG_VAL="projects/test-project/zones/test-zone/instanceGroupManagers/"

TARGET_LABEL="cloud.google.com/gke-tpu-partition-4x4x4-id"

TOPOLOGY_LABELS=(
    "cloud.google.com/gke-tpu-partition-2x2x1-id"
    "cloud.google.com/gke-tpu-partition-2x2x2-id"
    "cloud.google.com/gke-tpu-partition-2x2x4-id"
    "cloud.google.com/gke-tpu-partition-2x4x4-id"
    "$TARGET_LABEL"
)

# ---------------------
SLICE_NAME="test-slice"

# --- Helper Functions ---

inject_status() {
    local status="$1"
    local message="$2"
    echo ""
    echo "Injecting $status status for MMIG..."
    echo "=================================================="
    local payload
    payload=$(printf '{"sliceName": "%s", "acceleratorTopologyState": "%s", "acceleratorTopology": "%s", "errorMessage": "%s"}' "$SLICE_NAME" "$status" "$ACCELERATOR_TOPOLOGY" "$message")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/mmig-health"
    echo ""
}

wait_for_slice_status() {
    local expected_status="$1"
    echo "Waiting for slice '$SLICE_NAME' to be in $expected_status state..."
    while true; do
        STATUS=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
        if [[ "$STATUS" == "$expected_status" ]]; then
            echo "-> Slice is $expected_status. Proceeding..."
            break
        fi
        echo "   Current slice status: $STATUS. Waiting 3 seconds..."
        sleep 3
    done
}

# --- Main Script ---

echo "Checking if Slice CRD (slices.accelerator.gke.io) is installed..."
if ! kubectl get crd slices.accelerator.gke.io > /dev/null 2>&1; then
    echo "Error: The Slice CRD is not installed in the cluster."
    echo "Please ensure the Slice Controller and its CRDs are properly installed before running this test."
    exit 1
fi
echo "-> Slice CRD is installed."
echo "=================================================="
echo ""

echo "Clearing mock server state..."
curl -s -X POST http://localhost:8080/clear
CURL_EXIT_CODE=$?

if [ $CURL_EXIT_CODE -ne 0 ]; then
    echo "Error: Could not connect to the mock GCE server (curl exit code: $CURL_EXIT_CODE)."
    if [ $CURL_EXIT_CODE -eq 7 ]; then
        echo "  Reason: Failed to connect. The server is likely not running or the port is blocked."
       else
        echo "  An unexpected curl error occurred. Please check the mock server logs."
    fi
    echo "Please ensure port-forwarding is running in a separate terminal:"
    echo "  kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080"
    exit 1
fi
echo "=================================================="
echo ""

echo "Running cleanup script before test..."
echo "=================================================="
"$SCRIPT_DIR/cleanup.sh"
echo ""

echo "Checking for pre-existing '$SLICE_NAME' slice..."
if kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; then
    echo "-> Found existing slice '$SLICE_NAME'. Deleting it before starting the test..."
    kubectl delete slice "$SLICE_NAME"

    echo "   Waiting for pre-existing slice to be deleted..."
    start_time=$SECONDS
    while kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; do
        elapsed_time=$((SECONDS - start_time))
        if [ "$elapsed_time" -ge 30 ]; then
            echo "  -> ERROR: Timeout waiting for pre-existing slice '$SLICE_NAME' to be deleted."
            exit 1
        fi
        echo "   Slice still exists. Waiting 3 seconds... ($elapsed_time/30s)"
        sleep 3
    done
    echo "-> Pre-existing slice deleted."
else
    echo "-> No pre-existing slice found. Continuing."
fi
echo "=================================================="

echo "Starting setup and injection for pools: ${POOL_NAMES[*]}"
echo "=================================================="

for i in "${!POOL_NAMES[@]}"; do
    IDX=$((i + 1)) # 1-based index
    POOL_NAME="${POOL_NAMES[$i]}"

    MIG_NAME="${POOL_NAME}-grp"
    FULL_MIG_ANNOTATION="${BASE_MIG_VAL}${MIG_NAME}" # e.g. projects/test-project/zones/test-zone/instanceGroupManagers/np-t1-grp

    echo "[Index ${IDX}] Processing Node Pool: $POOL_NAME"

    # 1. Get all nodes for this specific pool
    NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="$POOL_NAME" -o jsonpath='{.items[*].metadata.name}')

    # Check if we found any nodes
    if [ -z "$NODES" ]; then
        echo "  -> WARNING: No nodes found for pool '$POOL_NAME'. Skipping..."
        echo "--------------------------------------------------"
        continue
    fi

    # Convert NODES string to an array to count them
    NODE_ARRAY=($NODES)
    NUM_NODES=${#NODE_ARRAY[@]}

    if [ "$NUM_NODES" -ne 16 ]; then
        echo "  -> ERROR: Expected 16 nodes for pool '$POOL_NAME', but found $NUM_NODES. Aborting."
        echo "--------------------------------------------------"
        exit 1
    fi

    for NODE in $NODES; do
        # Apply MIG annotation
        kubectl annotate node "$NODE" "node.gke.io/created-by-mig=$FULL_MIG_ANNOTATION" --overwrite > /dev/null 2>&1

        # Apply topology labels
        for LABEL in "${TOPOLOGY_LABELS[@]}"; do
             LABEL_VAL=""

             if [ "$LABEL" == "$TARGET_LABEL" ]; then
                 # 1:1 Mapping: Set value to the unique POOL_NAME
                 LABEL_VAL="$POOL_NAME"
             else
                 # Random generation for all other labels
                 LABEL_VAL=$(openssl rand -hex 4)
             fi

             kubectl label node "$NODE" "$LABEL=$LABEL_VAL" --overwrite > /dev/null 2>&1
        done
    done
    echo "  -> Successfully annotated and labeled nodes in pool '$POOL_NAME'"

    # 3. Perform the Injection
    echo "  -> Injecting: MIG='$MIG_NAME', Slice='$SLICE_NAME'"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MIG_NAME")

    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT")

    if [ "$HTTP_STATUS" -eq 200 ] || [ "$HTTP_STATUS" -eq 201 ]; then
         echo "  -> Injection Success [$HTTP_STATUS]"
    else
         echo "  -> Injection Failed [$HTTP_STATUS]"
    fi
    echo "--------------------------------------------------"
done

ACCELERATOR_TOPOLOGY="4x4x8"

echo ""
read -p "About to inject INCOMPLETE status to the MMIG. Do you want to proceed? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Operation cancelled by user."
    exit 1
fi

inject_status "INCOMPLETE" "for creating slice"

read -p "About to create the slice. Do you want to proceed? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Operation cancelled by user."
    exit 1
fi

if ! OUTPUT=$(kubectl apply -f "$YAML_PATH" 2>&1); then
    echo "Error: Failed to create the slice from $YAML_PATH. Aborting."
    echo "--- kubectl output ---"
    echo "$OUTPUT"
    echo "----------------------"
    exit 1
fi
echo "$OUTPUT"

echo ""
sleep 2 # Give controller a moment to pick up the new slice

wait_for_slice_status "INCOMPLETE"

echo ""
echo "Current slices in the cluster:"
kubectl get slice -A

echo ""
read -p "About to inject ACTIVATING status to the MMIG. Do you want to proceed? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Operation cancelled by user."
    exit 1
fi

inject_status "ACTIVATING" "healthy"

wait_for_slice_status "ACTIVATING"

echo ""
echo "Current slices in the cluster:"
kubectl get slice -A

echo ""
read -p "About to inject ACTIVE status to the MMIG. Do you want to proceed? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Operation cancelled by user."
    exit 1
fi

inject_status "ACTIVE" "healthy"

wait_for_slice_status "ACTIVE"

echo ""
echo "Current slices in the cluster:"
kubectl get slice -A

echo ""
read -p "Slice is ACTIVE. Do you want to inject INCOMPLETE status to the MMIG before deletion? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    inject_status "INCOMPLETE" "injected incomplete before deletion"
else
    echo "Skipping injection."
fi

echo
read -p "Do you want to delete the slice? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Test finished."
    exit 0
fi

echo ""
echo "Deleting the slice..."
kubectl delete -f "$YAML_PATH" &

echo ""
echo "Continuously checking for slice deletion..."
while kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; do
    echo "Slice '$SLICE_NAME' still exists. Waiting 3 seconds..."
    kubectl get slice -A
    sleep 3
done

echo "-> Slice '$SLICE_NAME' has been deleted."
echo ""
echo "Current slices in the cluster:"
kubectl get slice -A
