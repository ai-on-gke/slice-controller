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
POOL_NAMES=( # Add your node pool names here
    "np-t1"
)

# Get the directory where the script is located
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../../.."
API_ENDPOINT="http://localhost:8080/inject/mig"
TOPO_INFO_ENDPOINT="http://localhost:8080/inject/topology-info"

# Generate random hashes for pools
HASH1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Generated Hashes for Happy Path:"
echo "  np-t1 -> $HASH1"
echo "  np-t2 -> $HASH2"

# ---------------------
SLICE_NAME="test-slice"

# Helper to generate slice YAML
generate_slice_yaml() {
    local name="$1"
    local topology="$2"
    local p1="$3"
    local p2="$4"
    cat <<EOF > "/tmp/${name}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $name
spec:
  type: "tpu7x"
  topology: "$topology"
  partitionIds:
    - "$p1"
    - "$p2"
EOF
    echo "/tmp/${name}.yaml"
}

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

# echo "Running cleanup script before test..."
# echo "=================================================="
# "$SCRIPT_DIR/cleanup.sh"
# echo ""

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

echo "Re-labeling nodes with random hashes..."
echo "Re-labeling np-t1 nodes with hash $HASH1..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH1" --overwrite
done

echo "Re-labeling np-t2 nodes with hash $HASH2..."
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH2" --overwrite
done

echo "Injecting MIG info for np-t1-grp and np-t2-grp..."
echo "=================================================="
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    echo "  -> Injecting: MIG='$MIG_NAME', Slice='$SLICE_NAME'"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MIG_NAME")
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT")
    if [ "$HTTP_STATUS" -eq 200 ] || [ "$HTTP_STATUS" -eq 201 ]; then
         echo "  -> Injection Success [$HTTP_STATUS]"
    else
         echo "  -> Injection Failed [$HTTP_STATUS]"
    fi
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done
echo "--------------------------------------------------"

ACCELERATOR_TOPOLOGY="4x4x8"
DYNAMIC_YAML_PATH=$(generate_slice_yaml "$SLICE_NAME" "$ACCELERATOR_TOPOLOGY" "$HASH1" "$HASH2")

echo "About to create the slice..."
if ! OUTPUT=$(kubectl apply -f "$DYNAMIC_YAML_PATH" 2>&1); then
    echo "Error: Failed to create the slice from $DYNAMIC_YAML_PATH. Aborting."
    echo "--- kubectl output ---"
    echo "$OUTPUT"
    echo "----------------------"
    exit 1
fi
echo "$OUTPUT"

echo ""
sleep 2 # Give controller a moment to pick up the new slice

wait_for_slice_status "ACTIVE"

echo ""
echo "Current slices in the cluster:"
kubectl get slice -A

# Verification of ACTIVE state
echo "-> Slice is ACTIVE. Proceeding with happy path verification..."

echo "Verification success! Output of kubectl describe slice:"
kubectl describe slice "$SLICE_NAME"

echo ""
echo "Deleting the slice now..."
echo "Deleting the slice now..."
kubectl delete -f "$DYNAMIC_YAML_PATH" &

echo ""
echo "Continuously checking for slice deletion..."
start_time=$SECONDS
while kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; do
    elapsed_time=$((SECONDS - start_time))
    if [ "$elapsed_time" -ge 30 ]; then
        echo "  -> ERROR: Timeout waiting for slice '$SLICE_NAME' to be deleted."
        exit 1
    fi
    echo "Slice '$SLICE_NAME' still exists. Waiting 3 seconds... ($elapsed_time/30s)"
    sleep 3
done

echo "-> Slice '$SLICE_NAME' has been deleted."
echo ""

echo "========================================================="
echo "Starting test case 2: ACTIVE_DEGRADED slice (2 cubes: 1 healthy, 1 degraded)"
echo "========================================================="
echo ""

SLICE_NAME_2="test-slice-degraded"
DYNAMIC_YAML_PATH_2=$(generate_slice_yaml "$SLICE_NAME_2" "4x4x8" "$HASH1" "$HASH2")

# 1. Label nodes in np-t2 as DEGRADED (test if controller reads node labels or if mock GCE uses it? We inject to mock GCE as well)
echo "Labeling np-t2 nodes as DEGRADED..."
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-state=DEGRADED" --overwrite
done

# 2. Inject MIG statuses for Test 2 (1 healthy, 1 degraded)
echo "Injecting MIG statuses for Test 2..."

# Register np-t1-grp as healthy (clear any old status if needed, or just inject)
payload1=$(printf '{"sliceName": "%s", "migName": "%s", "status": "HEALTHY"}' "$SLICE_NAME_2" "np-t1-grp")
curl -s -X POST -H "Content-Type: application/json" -d "$payload1" http://localhost:8080/inject/mig
topo_payload1=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_2" "np-t1-grp" "np-t1-grp")
curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload1" "$TOPO_INFO_ENDPOINT"

# Register np-t2-grp as DEGRADED
payload2=$(printf '{"sliceName": "%s", "migName": "%s", "status": "DEGRADED"}' "$SLICE_NAME_2" "np-t2-grp")
curl -s -X POST -H "Content-Type: application/json" -d "$payload2" http://localhost:8080/inject/mig
topo_payload2=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "DEGRADED", "instancesHealth": "DEGRADED"}}}' "$SLICE_NAME_2" "np-t2-grp" "np-t2-grp")
curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload2" "$TOPO_INFO_ENDPOINT"

# 3. Apply Slice 2
echo "Applying Slice 2 configuration..."
kubectl apply -f "$DYNAMIC_YAML_PATH_2"

# 4. Verify Slice 2 state
echo "Verifying Slice 2 state..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice 2 to become ACTIVE_DEGRADED."
        kubectl describe slice "$SLICE_NAME_2"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_2" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE_DEGRADED" ]; then
        echo "-> Slice 2 is ACTIVE_DEGRADED!"
        break
    fi
    echo "   Slice 2 status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done


# 6. Cleanup Slice 2
echo "Deleting Slice 2..."
kubectl delete slice "$SLICE_NAME_2"
start_time=$SECONDS
while kubectl get slice "$SLICE_NAME_2" -n default > /dev/null 2>&1; do
    elapsed_time=$((SECONDS - start_time))
    if [ "$elapsed_time" -ge 30 ]; then
        echo "  -> ERROR: Timeout waiting for slice '$SLICE_NAME_2' to be deleted."
        exit 1
    fi
    echo "Slice '$SLICE_NAME_2' still exists. Waiting 3 seconds... ($elapsed_time/30s)"
    sleep 3
done
echo "-> Slice '$SLICE_NAME_2' has been deleted."

echo ""
echo "========================================================="
echo "Starting test case 3: INCOMPLETE state test (Deletion)"
echo "========================================================="
echo ""

SLICE_NAME_3="test-slice-incomplete"

echo "Ensuring Slice 3 does not exist..."
kubectl delete slice "$SLICE_NAME_3" --ignore-not-found
start_time=$SECONDS
while kubectl get slice "$SLICE_NAME_3" -n default > /dev/null 2>&1; do
    elapsed_time=$((SECONDS - start_time))
    if [ "$elapsed_time" -ge 30 ]; then
        echo "  -> ERROR: Timeout waiting for slice '$SLICE_NAME_3' to be deleted."
        exit 1
    fi
    echo "Slice '$SLICE_NAME_3' still exists. Waiting 3 seconds... ($elapsed_time/30s)"
    sleep 3
done

echo "Clearing mock server state for Case 3..."
curl -s -X POST http://localhost:8080/clear

echo "Re-labeling nodes with hashes for Case 3..."
echo "Re-labeling np-t1 nodes with hash $HASH1..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH1" --overwrite
done

echo "Re-labeling np-t2 nodes with hash $HASH2..."
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH2" --overwrite
done

DYNAMIC_YAML_PATH_3=$(generate_slice_yaml "$SLICE_NAME_3" "4x4x8" "$HASH1" "$HASH2")

# 1. Register MIGs for Slice 3
echo "Injecting MIG info for np-t1-grp and np-t2-grp..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_3" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_3" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

# 2. Apply Slice 3
echo "Applying Slice 3 configuration..."
kubectl apply -f "$DYNAMIC_YAML_PATH_3"

# 3. Wait for it to be ACTIVE
echo "Waiting for slice '$SLICE_NAME_3' to be in ACTIVE state..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice 3 to become ACTIVE."
        kubectl describe slice "$SLICE_NAME_3"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_3" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice 3 is ACTIVE!"
        break
    fi
    echo "   Slice 3 status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done

# 4. Inject RUNNING for BOTH MIG detach ops (making it hang) BEFORE delete
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    OP_KEY="op-detachmigs-${MIG_NAME}"
    echo "Injecting RUNNING status for operation $OP_KEY..."
    payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME_3" "$OP_KEY")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status
done

echo "Slice is ACTIVE. Now deleting..."
kubectl delete -f "$DYNAMIC_YAML_PATH_3" &
DELETE_PID=$!

echo "Waiting for deletion to start and enter DEACTIVATING..."
sleep 5
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice 3 to become DEACTIVATING."
        kubectl describe slice "$SLICE_NAME_3"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_3" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "DEACTIVATING" ]; then
        echo "-> Slice 3 is DEACTIVATING (as expected due to hanging detach)!"
        break
    fi
    echo "   Slice 3 status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"

    sleep 3
done

# 5. Unblock ops by clearing op-status
echo "Clearing injected op status to let deletion proceed..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    OP_KEY="op-detachmigs-${MIG_NAME}"
    payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME_3" "$OP_KEY")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status
done

echo "Waiting for slice deletion to complete..."
start_time=$SECONDS
while kubectl get slice "$SLICE_NAME_3" -n default > /dev/null 2>&1; do
    elapsed_time=$((SECONDS - start_time))
    if [ "$elapsed_time" -ge 30 ]; then
        echo "  -> ERROR: Timeout waiting for slice '$SLICE_NAME_3' to be deleted."
        exit 1
    fi
    echo "Slice '$SLICE_NAME_3' still exists. Waiting 3 seconds... ($elapsed_time/30s)"
    sleep 3
done

echo "-> Success! Slice is deleted."

echo "All tests in happy path script passed!"