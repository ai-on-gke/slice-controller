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
SLICE_NAME="test-slice-attach"

NODE_POOL_COUNT=${NODE_POOL_COUNT:-4}
unset HASHES
HASHES=()
echo "Generated Hashes for Failure Test ($NODE_POOL_COUNT Cubes):"
for ((i=1; i<=NODE_POOL_COUNT; i++)); do
    H=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
    HASHES+=("$H")
    eval "HASH$i=$H"
    echo "  np-t$i -> $H"
done

# Helper to generate slice YAML
generate_slice_yaml() {
    local name="$1"
    local topology="$2"
    local yaml_file="/tmp/${name}-failure.yaml"
    cat <<EOF > "$yaml_file"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $name
  annotations:
    "slice.gke.io/retry-on-failure": "true"
spec:
  type: "tpu7x"
  topology: "$topology"
  partitionIds:
EOF
    for h in "${HASHES[@]}"; do
        echo "    - \"$h\"" >> "$yaml_file"
    done

    echo "$yaml_file"
}


# --- Main Script ---

echo "Checking if Slice CRD is installed..."
if ! kubectl get crd slices.accelerator.gke.io > /dev/null 2>&1; then
    echo "Error: The Slice CRD is not installed."
    exit 1
fi

echo "=================================================="
echo "Running Failure Test: Invalid Partition Count for 4x4x8"
echo "=================================================="

echo "Cleaning any leftover slices from previous runs..."
kubectl delete slice --all --ignore-not-found
while [ -n "$(kubectl get slice --all-namespaces 2>/dev/null | grep -v NAME)" ]; do
    echo "Waiting for slices to be deleted..."
    sleep 2
done
echo "Clearing mock server state..."
curl -s -X POST http://localhost:8080/clear

cat <<EOF | kubectl apply -f - 2>&1 | grep -q "indicates 2 partitions, but found 1"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: invalid-slice-4x4x8
spec:
  type: "tpu7x"
  topology: "4x4x8"
  partitionIds:
  - "p1"
EOF

if [ $? -eq 0 ]; then
    echo "Success: Slice creation rejected as expected."
else
    echo "ERROR: Slice creation should have been rejected."
    kubectl delete slice invalid-slice-4x4x8 --ignore-not-found
    exit 1
fi

echo "=================================================="
echo "Running Failure Test: Partial Failure during Attach (2 Cubes)..."
echo "=================================================="

echo "Cleaning any leftover slices from previous runs..."
kubectl delete slice --all --ignore-not-found
while [ -n "$(kubectl get slice --all-namespaces 2>/dev/null | grep -v NAME)" ]; do
    echo "Waiting for slices to be deleted..."
    sleep 2
done

echo "Clearing mock server state..."
curl -s -X POST http://localhost:8080/clear

echo "Explicitly setting up $NODE_POOL_COUNT Node Pools with labels and MIG annotations..."
BASE_MIG="https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers"
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    start_idx=$(((pool - 1) * 16 + 1))
    end_idx=$((pool * 16))
    curr_hash="HASH$pool"
    hash_val=${!curr_hash}
    for ((i=start_idx; i<=end_idx; i++)); do
        kubectl label node "kwok-node-$i" "cloud.google.com/gke-nodepool=np-t$pool" "cloud.google.com/gke-tpu-partition-4x4x4-id=$hash_val" "accelerator.gke.io/slice=$SLICE_NAME" --overwrite
        kubectl annotate node "kwok-node-$i" "node.gke.io/created-by-mig=$BASE_MIG/np-t$pool-grp" --overwrite
    done
done

echo "Giving the slice controller informer 5s to fully sync node labels/annotations..."
sleep 5

echo "Injecting MIG info for $NODE_POOL_COUNT pools..."
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    MIG_NAME="np-t$pool-grp"
    echo "  -> Injecting: MIG='$MIG_NAME', Slice='$SLICE_NAME'"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x8x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done
echo "--------------------------------------------------"

OP_KEY_ATTACH_1="op-attachmigs-np-t1-grp"
OP_KEY_ATTACH_LAST="op-attachmigs-np-t$NODE_POOL_COUNT-grp"

echo "Injecting RUNNING status for T1 ($OP_KEY_ATTACH_1) & T4 ($OP_KEY_ATTACH_LAST) simultaneously..."
payload1=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME" "$OP_KEY_ATTACH_1")
payload4=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME" "$OP_KEY_ATTACH_LAST")
curl -s -X POST -H "Content-Type: application/json" -d "$payload1" "http://localhost:8080/inject/op-status"
curl -s -X POST -H "Content-Type: application/json" -d "$payload4" "http://localhost:8080/inject/op-status"

ACCELERATOR_TOPOLOGY="4x8x8"
DYNAMIC_YAML_PATH=$(generate_slice_yaml "$SLICE_NAME" "$ACCELERATOR_TOPOLOGY")

echo "About to create the slice..."
kubectl apply -f "$DYNAMIC_YAML_PATH"

echo "Waiting for dual block timeout (65s)..."
sleep 65

echo "Capturing Slice CR for dual attach timeout..."
kubectl get slice "$SLICE_NAME" -o yaml

echo "Unblocking T1..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME" "$OP_KEY_ATTACH_1")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"

echo "Waiting for intermediate transition (65s)..."
sleep 65

echo "Capturing Slice CR after unblocking T1..."
kubectl get slice "$SLICE_NAME" -o yaml

echo "Unblocking T4..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME" "$OP_KEY_ATTACH_LAST")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"

echo "Waiting for slice to become ACTIVE..."
timeout=60
while [ $timeout -gt 0 ]; do
    status=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$status" = "ACTIVE" ]; then
        echo "Success: Slice is now ACTIVE!"
        break
    fi
    echo "  Slice status is $status. Waiting 3 seconds... ($((60 - timeout))s)"
    sleep 3
    timeout=$((timeout - 3))
done
if [ $timeout -le 0 ]; then
    echo "Error: Slice did not become ACTIVE after recovery."
    exit 1
fi

# Cleanup first test case

echo "Cleaning up first test case..."
kubectl delete -f "$DYNAMIC_YAML_PATH"
while kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; do
    sleep 3
done

# --- Test Case 2: Failure during Detach ---
echo "=================================================="
echo "Running Failure Test 2: Failure during Detach"
echo "=================================================="

SLICE_NAME_DETACH="test-slice-detach"
DYNAMIC_YAML_PATH_DETACH=$(generate_slice_yaml "$SLICE_NAME_DETACH" "$ACCELERATOR_TOPOLOGY")

for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    start_idx=$(((pool - 1) * 16 + 1))
    end_idx=$((pool * 16))
    for ((i=start_idx; i<=end_idx; i++)); do
        kubectl label node "kwok-node-$i" "accelerator.gke.io/slice=$SLICE_NAME_DETACH" --overwrite
    done
done

for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    MIG_NAME="np-t$pool-grp"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_DETACH" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x8x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_DETACH" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done



echo "Creating slice for detach test..."
kubectl apply -f "$DYNAMIC_YAML_PATH_DETACH"

echo "Waiting for slice to become Active..."
for i in {1..20}; do
    STATUS=$(kubectl get slice "$SLICE_NAME_DETACH" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
    if [[ "$STATUS" == "True" ]]; then
        echo "Slice is Active!"
        break
    fi
    sleep 5
done

if [[ "$STATUS" != "True" ]]; then
    echo "FAILURE: Slice did not become Active for detach test."
    kubectl get slice "$SLICE_NAME_DETACH" -o yaml
    exit 1
fi

OP_KEY_DETACH_1="op-detachmigs-np-t1-grp"
OP_KEY_DETACH_LAST="op-detachmigs-np-t4-grp"

echo "Injecting RUNNING status for T1 & T4 during detach simultaneously..."
payload1=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME_DETACH" "$OP_KEY_DETACH_1")
payload4=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME_DETACH" "$OP_KEY_DETACH_LAST")
curl -s -X POST -H "Content-Type: application/json" -d "$payload1" "http://localhost:8080/inject/op-status"
curl -s -X POST -H "Content-Type: application/json" -d "$payload4" "http://localhost:8080/inject/op-status"

echo "Deleting slice in background..."
kubectl delete -f "$DYNAMIC_YAML_PATH_DETACH" &
DELETE_PID=$!

echo "Waiting for dual detach block timeout (65s)..."
sleep 65

echo "Capturing Slice CR for initial dual detach timeout..."
kubectl get slice "$SLICE_NAME_DETACH" -o yaml

echo "Unblocking T1..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME_DETACH" "$OP_KEY_DETACH_1")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"

echo "Waiting for intermediate detach transition (65s)..."
sleep 65

echo "Capturing Slice CR after unblocking T1..."
kubectl get slice "$SLICE_NAME_DETACH" -o yaml

echo "Unblocking T4..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME_DETACH" "$OP_KEY_DETACH_LAST")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"


echo "Waiting for slice deletion to perfectly complete..."
wait $DELETE_PID

echo "Sequential detach verification completed perfectly!"

echo "=================================================="
echo "Running Failure Test 3: Transient API Failure Recovery"
echo "=================================================="

ACCELERATOR_TOPOLOGY="4x8x8"
kubectl delete slice --all --force --ignore-not-found
sleep 2
SLICE_NAME_RECOVERY="test-slice-recovery"
DYNAMIC_YAML_PATH_RECOVERY=$(generate_slice_yaml "$SLICE_NAME_RECOVERY" "$ACCELERATOR_TOPOLOGY")
# Add retry annotation
sed -i 's|metadata:|metadata:\n  annotations:\n    slice.gke.io/retry-on-failure: "true"|' "$DYNAMIC_YAML_PATH_RECOVERY"

echo "Clearing mock server state for Recovery Test..."
curl -s -X POST http://localhost:8080/clear

echo "Injecting MIG info for recovery test..."
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    MIG_NAME="np-t$pool-grp"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_RECOVERY" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x8x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_RECOVERY" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

echo "Explicitly setting up $NODE_POOL_COUNT Node Pools with labels and MIG annotations..."
BASE_MIG="https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers"
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    start_idx=$(((pool - 1) * 16 + 1))
    end_idx=$((pool * 16))
    curr_hash="HASH$pool"
    hash_val=${!curr_hash}
    for ((i=start_idx; i<=end_idx; i++)); do
        kubectl label node "kwok-node-$i" "cloud.google.com/gke-nodepool=np-t$pool" "cloud.google.com/gke-tpu-partition-4x4x4-id=$hash_val" "accelerator.gke.io/slice=$SLICE_NAME_RECOVERY" --overwrite
        kubectl annotate node "kwok-node-$i" "node.gke.io/created-by-mig=$BASE_MIG/np-t$pool-grp" --overwrite
    done
done

echo "Giving the slice controller informer 5s to fully sync node labels/annotations..."
sleep 5

# 1. Inject HTTP 500 for CreateWorkloadPolicy
echo "Injecting HTTP 500 error for CreateWorkloadPolicy..."
payload=$(printf '{"sliceName": "%s", "key": "CreateWorkloadPolicy", "code": 500, "message": "Injected Internal Error"}' "$SLICE_NAME_RECOVERY")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/http-error

# 2. Apply Slice
echo "Applying Slice configuration..."
kubectl apply -f "$DYNAMIC_YAML_PATH_RECOVERY"

# 3. Wait and verify it is failing (not becoming Active)
echo "Waiting to verify Slice is failing (should not become Active)..."
sleep 10
STATUS=$(kubectl get slice "$SLICE_NAME_RECOVERY" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
if [[ "$STATUS" == "True" ]]; then
    echo "FAILURE: Slice became Active despite injected error!"
    exit 1
else
    echo "Slice is not Active (as expected)."
fi

echo "Checking retryAttempt for transient error..."
if kubectl get slice "$SLICE_NAME_RECOVERY" -n default -o yaml | grep -q "accelerator.gke.io/retry-attempt"; then
    echo "FAILURE: retryAttempt increased for transient error!"
    exit 1
else
    echo "SUCCESS: retryAttempt did not increase for transient error."
fi

# 4. Clear injection
echo "Clearing injected HTTP error to let creation proceed..."
payload=$(printf '{"sliceName": "%s", "key": "CreateWorkloadPolicy"}' "$SLICE_NAME_RECOVERY")
curl -s -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/http-error

# 5. Verify it becomes Active now
echo "Waiting for slice to become Active after clearing error..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        kubectl describe slice "$SLICE_NAME_RECOVERY"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_RECOVERY" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "True" ]; then
        echo "-> Slice is ACTIVE after recovery!"
        break
    fi
    echo "   Slice status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done

# 6. Cleanup
echo "Deleting Slice..."
kubectl delete -f "$DYNAMIC_YAML_PATH_RECOVERY"
while kubectl get slice "$SLICE_NAME_RECOVERY" -n default > /dev/null 2>&1; do
    sleep 3
done
echo "-> Success! Recovery test completed."

echo "=================================================="
echo "Running Failure Test 4: Operation Failure Retry"
echo "=================================================="

SLICE_NAME_OP_FAIL="test-slice-op-fail"
DYNAMIC_YAML_PATH_OP_FAIL=$(generate_slice_yaml "$SLICE_NAME_OP_FAIL" "$ACCELERATOR_TOPOLOGY")
# Add retry annotation
sed -i 's|metadata:|metadata:\n  annotations:\n    slice.gke.io/retry-on-failure: "true"|' "$DYNAMIC_YAML_PATH_OP_FAIL"

echo "Clearing mock server state for Op Failure Test..."
curl -s -X POST http://localhost:8080/clear

echo "Injecting MIG info for op failure test..."
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    MIG_NAME="np-t$pool-grp"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_OP_FAIL" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
done

echo "Explicitly setting up $NODE_POOL_COUNT Node Pools with labels and MIG annotations..."
BASE_MIG="https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers"
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    start_idx=$(((pool - 1) * 16 + 1))
    end_idx=$((pool * 16))
    curr_hash="HASH$pool"
    hash_val=${!curr_hash}
    for ((i=start_idx; i<=end_idx; i++)); do
        kubectl label node "kwok-node-$i" "cloud.google.com/gke-nodepool=np-t$pool" "cloud.google.com/gke-tpu-partition-4x4x4-id=$hash_val" "accelerator.gke.io/slice=$SLICE_NAME_OP_FAIL" --overwrite
        kubectl annotate node "kwok-node-$i" "node.gke.io/created-by-mig=$BASE_MIG/np-t$pool-grp" --overwrite
    done
done

echo "Giving the slice controller informer 5s to fully sync node labels/annotations..."
sleep 5

OP_KEY_ATTACH_1="op-attachmigs-np-t1-grp"

echo "Injecting RUNNING status for T1 ($OP_KEY_ATTACH_1) to cause timeout..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME_OP_FAIL" "$OP_KEY_ATTACH_1")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"

echo "Applying Slice configuration..."
kubectl apply -f "$DYNAMIC_YAML_PATH_OP_FAIL"

echo "Waiting for operation timeout (65s)..."
sleep 65

echo "Checking retryAttempt for operation failure..."
# Wait a bit more for controller to update status after timeout
sleep 5
if kubectl get slice "$SLICE_NAME_OP_FAIL" -n default -o yaml | grep -q 'accelerator.gke.io/retry-attempt:'; then
    echo "FAILURE: retryAttempt increased for operation timeout!"
    kubectl get slice "$SLICE_NAME_OP_FAIL" -o yaml
    exit 1
else
    echo "SUCCESS: retryAttempt did not increase for operation timeout."
fi

# Cleanup
echo "Clearing injected operation status..."
payload=$(printf '{"sliceName": "%s", "key": "%s"}' "$SLICE_NAME_OP_FAIL" "$OP_KEY_ATTACH_1")
curl -s -X DELETE -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/op-status"

echo "Deleting Slice..."
kubectl delete -f "$DYNAMIC_YAML_PATH_OP_FAIL"
while kubectl get slice "$SLICE_NAME_OP_FAIL" -n default > /dev/null 2>&1; do
    sleep 3
done
echo "-> Success! Operation failure test completed."

echo "=================================================="
echo "Running Failure Test 7: GetOperation API Error Retry"
echo "=================================================="

SLICE_NAME_GET_OP_ERR="test-slice-get-op-err"
DYNAMIC_YAML_PATH_GET_OP_ERR=$(generate_slice_yaml "$SLICE_NAME_GET_OP_ERR" "$ACCELERATOR_TOPOLOGY")
# Add retry annotation
sed -i 's|metadata:|metadata:\n  annotations:\n    slice.gke.io/retry-on-failure: "true"|' "$DYNAMIC_YAML_PATH_GET_OP_ERR"

echo "Clearing mock server state for GetOperation Error Test..."
curl -s -X POST http://localhost:8080/clear

echo "Injecting MIG info for get operation error test..."
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    MIG_NAME="np-t$pool-grp"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_GET_OP_ERR" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
done

echo "Explicitly setting up $NODE_POOL_COUNT Node Pools with labels and MIG annotations..."
BASE_MIG="https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-c/instanceGroupManagers"
for ((pool=1; pool<=NODE_POOL_COUNT; pool++)); do
    start_idx=$(((pool - 1) * 16 + 1))
    end_idx=$((pool * 16))
    curr_hash="HASH$pool"
    hash_val=${!curr_hash}
    for ((i=start_idx; i<=end_idx; i++)); do
        kubectl label node "kwok-node-$i" "cloud.google.com/gke-nodepool=np-t$pool" "cloud.google.com/gke-tpu-partition-4x4x4-id=$hash_val" "accelerator.gke.io/slice=$SLICE_NAME_GET_OP_ERR" --overwrite
        kubectl annotate node "kwok-node-$i" "node.gke.io/created-by-mig=$BASE_MIG/np-t$pool-grp" --overwrite
    done
done

echo "Giving the slice controller informer 5s to fully sync node labels/annotations..."
sleep 5

echo "Injecting HTTP 500 error for GetOperation..."
PAYLOAD=$(printf '{"sliceName": "%s", "key": "GetOperation", "code": 500, "message": "injected get operation error"}' "$SLICE_NAME_GET_OP_ERR")
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "http://localhost:8080/inject/http-error"

echo "Applying Slice configuration..."
kubectl apply -f "$DYNAMIC_YAML_PATH_GET_OP_ERR"

echo "Waiting for get operation error to be processed (15s)..."
sleep 15

echo "Checking retryAttempt for GetOperation API call error..."
if kubectl get slice "$SLICE_NAME_GET_OP_ERR" -n default -o yaml | grep -q 'accelerator.gke.io/retry-attempt:'; then
    echo "FAILURE: retryAttempt increased for GetOperation API call error!"
    kubectl get slice "$SLICE_NAME_GET_OP_ERR" -o yaml
    exit 1
else
    echo "SUCCESS: retryAttempt did not increase for GetOperation API call error."
fi

# Cleanup
echo "Clearing injected HTTP error..."
PAYLOAD=$(printf '{"sliceName": "%s", "key": "GetOperation"}' "$SLICE_NAME_GET_OP_ERR")
curl -s -X DELETE -H "Content-Type: application/json" -d "$PAYLOAD" "http://localhost:8080/inject/http-error"

echo "Deleting Slice..."
kubectl delete -f "$DYNAMIC_YAML_PATH_GET_OP_ERR"
while kubectl get slice "$SLICE_NAME_GET_OP_ERR" -n default > /dev/null 2>&1; do
    sleep 3
done
echo "-> Success! GetOperation API call error test completed."
