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
POOL_NAMES=(
    # Add your node pool names here
    "np-t1" "np-t2"
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
SLICE_CONTROLLER_NAMESPACE="slice-controller-system"

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

inject_status "INCOMPLETE" "healthy"

echo "Creating the slice..."
if ! OUTPUT=$(kubectl apply -f "$YAML_PATH" 2>&1); then
    echo "Error: Failed to create the slice from $YAML_PATH. Aborting."
    echo "--- kubectl output ---"
    echo "$OUTPUT"
    echo "----------------------"
    exit 1
fi
echo "$OUTPUT"

sleep 2 # Give controller a moment to pick up the new slice
wait_for_slice_status "INCOMPLETE"

inject_status "ACTIVATING" "healthy"
wait_for_slice_status "ACTIVATING"

echo ""
echo "--- Pod Restart Test ---"
echo "=================================================="

echo "Finding slice controller pod..."
POD_NAME=$(kubectl get pods -n "$SLICE_CONTROLLER_NAMESPACE" -l control-plane=controller-manager -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
    echo "Error: Could not find slice controller pod in namespace '$SLICE_CONTROLLER_NAMESPACE'. Aborting."
    exit 1
fi

read -p "About to kill the slice controller pod. Do you want to inject a healthy 'ACTIVE' status while it's down? (y/n) " -n 1 -r
echo ""

echo "Found pod: $POD_NAME. Deleting it now to trigger a restart..."
START_TIME=$(date +%s)
kubectl delete pod "$POD_NAME" -n "$SLICE_CONTROLLER_NAMESPACE"
kubectl wait --for=delete pod "$POD_NAME" -n "$SLICE_CONTROLLER_NAMESPACE" --timeout=60s

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Injecting 'ACTIVE' status while controller is restarting..."
    inject_status "ACTIVE" "healthy"
    EXPECTED_FINAL_STATUS="ACTIVE"
else
    echo "Proceeding with restart without injecting new status."
    EXPECTED_FINAL_STATUS="ACTIVATING"
fi

echo "Waiting for the new slice controller pod to be running..."
kubectl wait --for=condition=Ready pod -l control-plane=controller-manager -n "$SLICE_CONTROLLER_NAMESPACE" --timeout=180s
END_TIME=$(date +%s)

NEW_POD_NAME=$(kubectl get pods -n "$SLICE_CONTROLLER_NAMESPACE" -l control-plane=controller-manager -o jsonpath='{.items[0].metadata.name}')
echo "-> New pod '$NEW_POD_NAME' is running."
RESTART_DURATION=$((END_TIME - START_TIME))
echo "-> Pod restart took $RESTART_DURATION seconds."

echo ""
echo "Verifying slice status after controller restart..."
sleep 5 # Give controller a moment to reconcile

CURRENT_STATUS=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")

echo "Current slice status is: $CURRENT_STATUS"
if [[ "$CURRENT_STATUS" == "$EXPECTED_FINAL_STATUS" ]]; then
    echo "-> SUCCESS: Slice status is '$CURRENT_STATUS' as expected after pod restart."
else
    echo "-> FAILURE: Slice status is '$CURRENT_STATUS', but expected '$EXPECTED_FINAL_STATUS'."
    exit 1
fi

echo ""
echo "Test finished. You can now manually inspect or clean up."
echo "To clean up, run: ./cleanup.sh"
