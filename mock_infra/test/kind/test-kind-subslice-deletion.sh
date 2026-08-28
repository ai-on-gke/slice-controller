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
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../../.."
API_ENDPOINT="http://localhost:8080/inject/mig"
TOPO_INFO_ENDPOINT="http://localhost:8080/inject/topology-info"

# --- Helper Functions ---

wait_for_slice_status() {
    local slice_name="$1"
    local expected_status="$2"
    local timeout="${3:-60}"
    echo "Waiting for slice '$slice_name' to be in $expected_status state (timeout: ${timeout}s)..."
    local elapsed=0
    while true; do
        elapsed=$((elapsed + 3))
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "  -> ERROR: Timeout waiting for slice '$slice_name' to become $expected_status."
            kubectl describe slice "$slice_name"
            return 1
        fi
        STATUS=$(kubectl get slice "$slice_name" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
        if [ "$STATUS" == "$expected_status" ]; then
            echo "  -> Slice is $expected_status!"
            return 0
        fi
        echo "   Current status: $STATUS. Waiting 3 seconds... ($elapsed/${timeout}s)"
        sleep 3
    done
}

wait_for_slice_deleted() {
    local slice_name="$1"
    local timeout="${2:-60}"
    echo "Waiting for slice '$slice_name' to be fully deleted (timeout: ${timeout}s)..."
    local elapsed=0
    while kubectl get slice "$slice_name" -n default > /dev/null 2>&1; do
        elapsed=$((elapsed + 3))
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "  -> ERROR: Timeout waiting for slice '$slice_name' to be deleted."
            kubectl describe slice "$slice_name" || true
            return 1
        fi
        echo "   Slice still exists. Waiting 3 seconds... ($elapsed/${timeout}s)"
        sleep 3
    done
    echo "  -> Slice '$slice_name' has been deleted."
    return 0
}

cleanup_slice() {
    local slice_name="$1"
    echo "Cleaning up slice '$slice_name'..."
    kubectl delete slice "$slice_name" --ignore-not-found
    wait_for_slice_deleted "$slice_name" 30 || true
}

# Generate a subslice YAML (1 partitionId, small topology = sub-slice)
generate_subslice_yaml() {
    local name="$1"
    local topology="$2"
    local partition="$3"
    cat <<EOF > "/tmp/${name}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $name
spec:
  type: "tpu7x"
  topology: "$topology"
  partitionIds:
    - "$partition"
EOF
    echo "/tmp/${name}.yaml"
}

# Inject MIG and topology info for a subslice
setup_subslice_mock() {
    local slice_name="$1"
    local mig_name="$2"
    local partition_id="$3"

    echo "Injecting MIG '$mig_name' for slice '$slice_name' with partitionId '$partition_id'..."
    payload=$(printf '{"sliceName": "%s", "migName": "%s", "partitionId": "%s"}' "$slice_name" "$mig_name" "$partition_id")
    curl -s -o /dev/null -w "  -> MIG inject: HTTP %{http_code}\n" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"

    echo "Injecting topology info for MIG '$mig_name', topology '$partition_id'..."
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopologyState": {"currentState": "INACTIVE"}}}}' "$slice_name" "$mig_name" "$partition_id")
    curl -s -o /dev/null -w "  -> Topo inject: HTTP %{http_code}\n" -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
}

# --- Main Script ---

echo "Checking if Slice CRD is installed..."
if ! kubectl get crd slices.accelerator.gke.io > /dev/null 2>&1; then
    echo "Error: The Slice CRD is not installed."
    exit 1
fi
echo "-> Slice CRD is installed."

echo ""
echo "========================================================="
echo "  SUBSLICE DELETION E2E TESTS"
echo "========================================================="
echo ""

# --- Test Case 1: Subslice Happy Path (Create -> Activate -> Delete) ---
echo "========================================================="
echo "Test Case 1: Subslice Happy Path (Create -> Delete)"
echo "========================================================="
echo ""

SLICE_NAME_1="subslice-happy"
HASH1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Clearing mock server state..."
curl -s -X POST http://localhost:8080/clear

echo "Ensuring no pre-existing slice..."
cleanup_slice "$SLICE_NAME_1"

# Label one node pool's nodes with the partition ID
echo "Re-labeling np-t1 nodes with partition hash $HASH1..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x1-id=$HASH1" --overwrite > /dev/null
done

# Set up mock GCE state for subslice
setup_subslice_mock "$SLICE_NAME_1" "np-t1-grp" "$HASH1"

# Generate and apply the subslice YAML
DYNAMIC_YAML_1=$(generate_subslice_yaml "$SLICE_NAME_1" "2x2x1" "$HASH1")
echo "Creating subslice with topology 2x2x1..."
kubectl apply -f "$DYNAMIC_YAML_1"
sleep 2

# Wait for it to become ACTIVE
if ! wait_for_slice_status "$SLICE_NAME_1" "ACTIVE" 60; then
    echo "FAILURE: Subslice did not become ACTIVE."
    kubectl logs -n slice-controller-system deployment/slice-controller-controller-manager --tail=50 || true
    exit 1
fi

echo ""
echo "Subslice is ACTIVE. Verifying describe output..."
kubectl describe slice "$SLICE_NAME_1"

echo ""
echo "Deleting the subslice..."
kubectl delete -f "$DYNAMIC_YAML_1" &
DELETE_PID=$!

if ! wait_for_slice_deleted "$SLICE_NAME_1" 60; then
    echo "FAILURE: Subslice was not deleted within timeout."
    kill $DELETE_PID 2>/dev/null || true
    exit 1
fi
wait $DELETE_PID 2>/dev/null

echo ""
echo "✅ Test Case 1 PASSED: Subslice happy path (create + delete) works."
echo ""

# --- Test Case 2: Subslice Deletion with In-Flight Activate (STAGE 0 Zombie Prevention) ---
echo "========================================================="
echo "Test Case 2: Subslice Deletion with In-Flight Activate"
echo "  (STAGE 0 zombie prevention - LIFO race condition fix)"
echo "========================================================="
echo ""

SLICE_NAME_2="subslice-zombie"
HASH2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Clearing mock server state..."
curl -s -X POST http://localhost:8080/clear

echo "Ensuring no pre-existing slice..."
cleanup_slice "$SLICE_NAME_2"

# Label nodes
echo "Re-labeling np-t1 nodes with partition hash $HASH2..."
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x1-id=$HASH2" --overwrite > /dev/null
done

# Set up mock GCE state
setup_subslice_mock "$SLICE_NAME_2" "np-t1-grp" "$HASH2"

# Generate and apply the subslice YAML
DYNAMIC_YAML_2=$(generate_subslice_yaml "$SLICE_NAME_2" "2x2x1" "$HASH2")
echo "Creating subslice with topology 2x2x1..."
kubectl apply -f "$DYNAMIC_YAML_2"
sleep 2

# Wait for it to become ACTIVE
if ! wait_for_slice_status "$SLICE_NAME_2" "ACTIVE" 60; then
    echo "FAILURE: Subslice did not become ACTIVE."
    exit 1
fi

echo ""
echo "Subslice is ACTIVE."

# Now simulate the zombie scenario:
# 1. Manually inject the last-operation-id annotation to simulate an in-flight activate
#    The annotation value is just the operation name (shared with update flow's LIFO barrier).
# 2. Inject the operation as RUNNING in the mock GCE
# 3. Delete the slice and verify it waits (STAGE 0)

OP_NAME="op-configure-np-t1-grp-${HASH2}"

echo "Injecting in-flight activate operation annotation..."
echo "  annotation value: $OP_NAME"
kubectl annotate slice "$SLICE_NAME_2" \
    "accelerator.gke.io/last-operation-id=${OP_NAME}" --overwrite

# Inject the operation as RUNNING in the mock GCE so STAGE 0 sees it as in-flight
echo "Injecting RUNNING status for operation $OP_NAME..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME_2" "$OP_NAME")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"

echo ""
echo "Deleting slice in background (should be blocked by STAGE 0)..."
kubectl delete -f "$DYNAMIC_YAML_2" &
DELETE_PID=$!

# Wait a bit - the slice should still exist because STAGE 0 is waiting
echo "Waiting 20s to verify deletion is blocked by STAGE 0..."
sleep 20

if ! kubectl get slice "$SLICE_NAME_2" -n default > /dev/null 2>&1; then
    echo "FAILURE: Slice was deleted immediately without waiting for in-flight activate!"
    echo "  The STAGE 0 zombie prevention did not work."
    exit 1
fi

echo "  -> Good: Slice still exists (STAGE 0 is blocking deletion)."

# Verify the annotation is still present (controller is checking via barrier)
ANNO_VAL=$(kubectl get slice "$SLICE_NAME_2" -o jsonpath='{.metadata.annotations.accelerator\.gke\.io/last-operation-id}' 2>/dev/null || echo "")
if [ -z "$ANNO_VAL" ]; then
    echo "  Note: Annotation was already cleared (operation completed between checks)."
else
    echo "  -> Annotation still present: $ANNO_VAL"
fi

# Now unblock by setting the operation to DONE
echo ""
echo "Unblocking: Setting operation to DONE..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME_2" "$OP_NAME")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"

# Wait for deletion to complete
if ! wait_for_slice_deleted "$SLICE_NAME_2" 90; then
    echo "FAILURE: Slice was not deleted after unblocking the in-flight activate operation."
    kill $DELETE_PID 2>/dev/null || true
    exit 1
fi
wait $DELETE_PID 2>/dev/null

echo ""
echo "✅ Test Case 2 PASSED: STAGE 0 zombie prevention works correctly."
echo "   Deletion waited for in-flight activate, then proceeded."
echo ""

# --- Test Case 3: Subslice Deletion Without Annotation (Backward Compatibility) ---
echo "========================================================="
echo "Test Case 3: Subslice Deletion Without Activate Annotation"
echo "  (Backward compatibility - no annotation = immediate delete)"
echo "========================================================="
echo ""

SLICE_NAME_3="subslice-noannot"
HASH3=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Clearing mock server state..."
curl -s -X POST http://localhost:8080/clear

echo "Ensuring no pre-existing slice..."
cleanup_slice "$SLICE_NAME_3"

# Label nodes
echo "Re-labeling np-t1 nodes with partition hash $HASH3..."
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x1-id=$HASH3" --overwrite > /dev/null
done

# Set up mock GCE state
setup_subslice_mock "$SLICE_NAME_3" "np-t1-grp" "$HASH3"

# Generate and apply the subslice YAML
DYNAMIC_YAML_3=$(generate_subslice_yaml "$SLICE_NAME_3" "2x2x1" "$HASH3")
echo "Creating subslice with topology 2x2x1..."
kubectl apply -f "$DYNAMIC_YAML_3"
sleep 2

# Wait for it to become ACTIVE
if ! wait_for_slice_status "$SLICE_NAME_3" "ACTIVE" 60; then
    echo "FAILURE: Subslice did not become ACTIVE."
    exit 1
fi

echo ""
echo "Subslice is ACTIVE."

# Explicitly remove any operation annotation (for test clarity)
kubectl annotate slice "$SLICE_NAME_3" "accelerator.gke.io/last-operation-id-" --overwrite 2>/dev/null || true

echo "Deleting slice (should proceed immediately - no in-flight activate)..."
kubectl delete -f "$DYNAMIC_YAML_3" &
DELETE_PID=$!

if ! wait_for_slice_deleted "$SLICE_NAME_3" 60; then
    echo "FAILURE: Subslice without annotation was not deleted within timeout."
    kill $DELETE_PID 2>/dev/null || true
    exit 1
fi
wait $DELETE_PID 2>/dev/null

echo ""
echo "✅ Test Case 3 PASSED: Backward compatibility - deletion without annotation works."
echo ""

echo "========================================================="
echo "✅ All subslice deletion E2E tests passed!"
echo "========================================================="
