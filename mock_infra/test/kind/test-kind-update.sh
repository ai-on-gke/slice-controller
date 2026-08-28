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

# Get the directory where the script is located
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../../.."

SLICE_NAME="empty-slice"

generate_empty_slice_yaml() {
    local name="$1"
    cat <<EOF > "/tmp/${name}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $name
spec:
  type: "tpu7x"
  topology: "4x4x8"
  partitionIds: []
EOF
    echo "/tmp/${name}.yaml"
}

ORIG_CRD="$PROJECT_ROOT/config/crd/bases/accelerator.gke.io_slices.yaml"
PRUNED_CRD="/tmp/pruned-slices-crd.yaml"
CASE0_SLICE_NAME="observed-gen-test-slice"

# Start port-forward to mock GCE service
echo "Starting port-forward to mock GCE service in the background..."
kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080 > /tmp/port-forward.log 2>&1 &
PORT_FORWARD_PID=$!

# Global cleanup function registered on EXIT
cleanup() {
    echo "Running global cleanup..."
    if [ -n "${PORT_FORWARD_PID}" ]; then
        echo "Stopping port-forward..."
        kill ${PORT_FORWARD_PID} || true
    fi

    # Restore original CRD status schema if Case 0 was running/modified it
    if [ -f "$ORIG_CRD" ] && [ -f "$PRUNED_CRD" ]; then
        echo "Restoring original CRD status schema..."
        kubectl apply -f "$ORIG_CRD" || true
        rm -f "$PRUNED_CRD" || true
    fi

    # Clean up test slice from Case 0
    if [ -n "$CASE0_SLICE_NAME" ]; then
        echo "Cleaning up Case 0 test slice..."
        kubectl delete slice "$CASE0_SLICE_NAME" --ignore-not-found --wait=false || true
    fi
}
trap cleanup EXIT

wait_for_labeled_node_count() {
    local slice_name="$1"
    local expected_count="$2"
    local elapsed=0
    while true; do
        local count=$(kubectl get nodes -l cloud.google.com/gke-tpu-slice="$slice_name" -o name 2>/dev/null | wc -l)
        if [ "$count" -eq "$expected_count" ]; then
            echo "Success: Found $expected_count labeled nodes!"
            return 0
        fi
        if [ "$elapsed" -ge 15 ]; then
            echo "ERROR: Expected $expected_count labeled nodes, got $count after 15s"
            return 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
}

wait_for_slice_status() {
    local slice_name="$1"
    local expected_status="$2"
    local elapsed=0
    while true; do
        local status=$(kubectl get slice "$slice_name" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
        if [ "$status" == "$expected_status" ]; then
            echo "-> Slice '$slice_name' is $expected_status!"
            return 0
        fi
        if [ "$elapsed" -ge 60 ]; then
            echo "ERROR: Timeout waiting for Slice '$slice_name' status to become $expected_status. Got status: $status"
            return 1
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
}


# Wait for port-forward to be ready
echo "Waiting for port-forward to be ready..."
for i in {1..15}; do
    if curl -s http://localhost:8080/version > /dev/null 2>&1; then
        echo "-> Port-forward is ready!"
        break
    fi
    if [ "$i" -eq 15 ]; then
        echo "ERROR: Timeout waiting for port-forward to be ready."
        kill ${PORT_FORWARD_PID}
        exit 1
    fi
    sleep 1
done

echo "Clearing mock server state..."
curl -s -X POST http://localhost:8080/clear

echo "Cleaning any leftover slices..."
kubectl delete slice --all --ignore-not-found --wait=false 2>/dev/null || true
SLICES=$(kubectl get slice -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
for S in $SLICES; do
    echo "  -> Force removing finalizer for leftover $S"
    kubectl patch slice "$S" --type=json -p='[{"op": "remove", "path": "/metadata/finalizers"}]' 2>/dev/null || true
done
while [ -n "$(kubectl get slice --all-namespaces 2>/dev/null | grep -v NAME)" ]; do
    echo "Waiting for slices to be deleted..."
    sleep 2
done

echo "=== Case 0: ObservedGeneration Pruning and Backfill ==="

CASE0_SLICE_NAME="observed-gen-test-slice"
API_ENDPOINT="http://localhost:8080/inject/mig"
TOPO_INFO_ENDPOINT="http://localhost:8080/inject/topology-info"

# 1. Back up the original CRD
echo "Backing up original CRD..."
ORIG_CRD="$PROJECT_ROOT/config/crd/bases/accelerator.gke.io_slices.yaml"
PRUNED_CRD="/tmp/pruned-slices-crd.yaml"
cp "$ORIG_CRD" "$PRUNED_CRD"

# 2. Prune observedGeneration from the CRD copy
echo "Pruning observedGeneration status property from the CRD..."
sed -i '/^              observedGeneration:/,/^[[:space:]]\{16\}type: integer/d' "$PRUNED_CRD"

# Verify observedGeneration is removed
if grep -q "ObservedGeneration is the most recent" "$PRUNED_CRD"; then
    echo "ERROR: Failed to prune observedGeneration from CRD copy"
    exit 1
fi
echo "-> observedGeneration successfully pruned from CRD copy."

# 3. Apply the pruned CRD
echo "Applying pruned CRD..."
kubectl apply -f "$PRUNED_CRD"

# Sleep to allow the API server to update CRD definition and controller to sync
sleep 5

# Verify the CRD in the cluster does not contain observedGeneration under status properties
CRD_OBS_GEN=$(kubectl get crd slices.accelerator.gke.io -o jsonpath='{.spec.versions[*].schema.openAPIV3Schema.properties.status.properties.observedGeneration}' 2>/dev/null)
if [ -n "$CRD_OBS_GEN" ]; then
    echo "ERROR: CRD slices.accelerator.gke.io in cluster still has observedGeneration in status properties"
    exit 1
fi
echo "-> Verified CRD in cluster does NOT have observedGeneration."

# Clear mock server state
echo "Clearing mock server state for Case 0..."
curl -s -X POST http://localhost:8080/clear

# Re-label nodes with random hashes
HASH0_1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH0_2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Re-labeling nodes..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH0_1" --overwrite
done
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH0_2" --overwrite
done

# Inject MIG info
echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$CASE0_SLICE_NAME" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

# 4. Create slice
cat <<EOF > "/tmp/${CASE0_SLICE_NAME}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $CASE0_SLICE_NAME
spec:
  type: "tpu7x"
  topology: "4x4x8"
  partitionIds:
    - "$HASH0_1"
    - "$HASH0_2"
EOF

echo "Creating slice..."
kubectl apply -f "/tmp/${CASE0_SLICE_NAME}.yaml"

# 5. Wait for slice state to be SliceNotCreated
echo "Waiting for slice '$CASE0_SLICE_NAME' to be in SliceNotCreated state..."
VERIFY_ELAPSED=0
while true; do
    STATUS=$(kubectl get slice "$CASE0_SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [[ "$STATUS" == "SliceNotCreated" ]]; then
        echo "-> Slice is SliceNotCreated. Proceeding..."
        break
    fi
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "ERROR: Timeout waiting for slice status to become SliceNotCreated. Got status: $STATUS"
        exit 1
    fi
    sleep 3
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
done

# 6. Verify controller log contains the pruned message
echo "Waiting for log pattern 'observedGeneration was pruned — CRD schema not yet updated'..."
VERIFY_ELAPSED=0
while true; do
    if kubectl logs -n slice-controller-system deployment/slice-controller-controller-manager | grep -q "observedGeneration was pruned — CRD schema not yet updated"; then
        echo "-> Found log pattern!"
        break
    fi
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "ERROR: Timeout waiting for log pattern"
        exit 1
    fi
    sleep 3
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
done

# Verify status.observedGeneration is absent on the resource
OBS_GEN=$(kubectl get slice "$CASE0_SLICE_NAME" -o jsonpath='{.status.observedGeneration}' 2>/dev/null)
if [ -z "$OBS_GEN" ]; then
    echo "Success: status.observedGeneration is absent (pruned by API server)."
else
    echo "ERROR: Expected status.observedGeneration to be absent, got: $OBS_GEN"
    exit 1
fi

# 7. Apply the original CRD to restore the field
echo "Restoring original CRD..."
kubectl apply -f "$ORIG_CRD"

# Sleep to allow the API server to update CRD definition and controller to sync
sleep 5

# Verify the CRD in the cluster contains observedGeneration under status properties
CRD_OBS_GEN=$(kubectl get crd slices.accelerator.gke.io -o jsonpath='{.spec.versions[*].schema.openAPIV3Schema.properties.status.properties.observedGeneration}' 2>/dev/null)
if [ -z "$CRD_OBS_GEN" ]; then
    echo "ERROR: CRD slices.accelerator.gke.io in cluster does not have observedGeneration in status properties after restore"
    exit 1
fi
echo "-> Verified CRD in cluster has observedGeneration."

# Force a reconciliation by annotating the slice
kubectl annotate slice "$CASE0_SLICE_NAME" "reconcile-trigger=force" --overwrite

# 8. Wait for controller log to contain backfilled message
echo "Waiting for log pattern 'observedGeneration backfilled'..."
VERIFY_ELAPSED=0
while true; do
    if kubectl logs -n slice-controller-system deployment/slice-controller-controller-manager | grep -q "observedGeneration backfilled"; then
        echo "-> Found log pattern!"
        break
    fi
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "ERROR: Timeout waiting for log pattern"
        exit 1
    fi
    sleep 3
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
done

# 9. Wait for slice to become ACTIVE
echo "Waiting for slice '$CASE0_SLICE_NAME' to be in ACTIVE state..."
VERIFY_ELAPSED=0
while true; do
    STATUS=$(kubectl get slice "$CASE0_SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [[ "$STATUS" == "ACTIVE" ]]; then
        echo "-> Slice is ACTIVE. Proceeding..."
        break
    fi
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "ERROR: Timeout waiting for slice status to become ACTIVE. Got status: $STATUS"
        exit 1
    fi
    sleep 3
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
done

# Verify status.observedGeneration is 1
OBS_GEN=$(kubectl get slice "$CASE0_SLICE_NAME" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "0")
if [ "$OBS_GEN" -eq 1 ]; then
    echo "Success: status.observedGeneration is backfilled to 1."
else
    echo "ERROR: Expected status.observedGeneration to be 1, got: $OBS_GEN"
    exit 1
fi

# 10. Clean up and verify deletion
echo "Deleting slice..."
kubectl delete -f "/tmp/${CASE0_SLICE_NAME}.yaml"

echo "Waiting for slice deletion..."
DELETE_ELAPSED=0
while kubectl get slice "$CASE0_SLICE_NAME" >/dev/null 2>&1; do
    if [ "$DELETE_ELAPSED" -ge 30 ]; then
        echo "ERROR: Slice deletion timed out!"
        exit 1
    fi
    sleep 2
    DELETE_ELAPSED=$((DELETE_ELAPSED + 2))
done
echo "Success: Slice deleted successfully."

echo "✅ Case 0: ObservedGeneration Pruning and Backfill test passed successfully!"

echo ""
echo "========================================================="
echo "Starting test case 4: Sequential WP/MMIG & Attachment Failure"
echo "========================================================="
echo ""

SLICE_NAME_4="update-seq-failure"

cat <<EOF > "/tmp/${SLICE_NAME_4}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_4
  annotations:
    slice.gke.io/retry-on-failure: "true"
spec:
  type: "tpu7x"
  topology: "4x4x8"
  partitionIds: []
EOF

HASH1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Re-labeling nodes for Case 4..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH1" --overwrite
done
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH2" --overwrite
done

echo "Sleeping 10s to allow informers to sync node labels..."
sleep 10

echo "Creating empty slice for Case 4..."
kubectl create -f "/tmp/${SLICE_NAME_4}.yaml"

echo "Verifying slice partitionIds is empty..."
kubectl get slice "$SLICE_NAME_4" -o jsonpath='{.spec.partitionIds}'

sleep 30

echo "Injecting FAIL status for create-mmig..."
payload=$(printf '{"sliceName": "%s", "key": "CreateMMIG", "code": 500, "message": "Injected Error"}' "$SLICE_NAME_4")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/http-error

echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_4" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

echo "Triggering 0->N update..."
kubectl patch slice "$SLICE_NAME_4" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH1\",\"$HASH2\"]}}"

sleep 20

STATUS=$(kubectl get slice "$SLICE_NAME_4" -n default -o jsonpath="{.status.conditions[?(@.type==\"Ready\")].reason}" 2>/dev/null || echo "NotFound")
OBS_GEN=$(kubectl get slice "$SLICE_NAME_4" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")

if [[ "$STATUS" == "SliceCreationFailed" ]] && [[ "$OBS_GEN" == "1" ]]; then
    echo "Success: State is SliceCreationFailed and observedGeneration remains 1."
else
    echo "ERROR: Expected SliceCreationFailed with observedGeneration 1, got State: $STATUS, Gen: $OBS_GEN"
    echo "Capturing logs..."
    kubectl logs -n slice-controller-system deployment/slice-controller-controller-manager > /tmp/controller.log 2>&1 || echo "Failed to get controller logs"
    kubectl logs -n slice-controller-system deployment/mock-gce > /tmp/mock-gce.log 2>&1 || echo "Failed to get mock-gce logs"
    echo "Logs captured to /tmp/controller.log and /tmp/mock-gce.log"
    exit 1
fi

echo "Injecting DONE for create-mmig and FAIL for np-t1-grp attachment..."
payload1=$(printf '{"sliceName": "%s", "key": "CreateMMIG"}' "$SLICE_NAME_4")
curl -s -X DELETE -H "Content-Type: application/json" -d "$payload1" http://localhost:8080/inject/http-error
payload2=$(printf '{"sliceName": "%s", "key": "%s", "status": "FAIL"}' "$SLICE_NAME_4" "op-attachmigs-np-t1-grp")
curl -s -X POST -H "Content-Type: application/json" -d "$payload2" http://localhost:8080/inject/op-status
RAND1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
kubectl annotate slice "$SLICE_NAME_4" accelerator.gke.io/trigger-reconcile="$RAND1" --overwrite

sleep 30

STATUS=$(kubectl get slice "$SLICE_NAME_4" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
OBS_GEN=$(kubectl get slice "$SLICE_NAME_4" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")

if [[ "$STATUS" == "SliceCreationFailed" ]] && [[ "$OBS_GEN" == "1" ]]; then
    echo "Success: State is SliceCreationFailed and observedGeneration remains 1."
else
    echo "ERROR: Expected SliceCreationFailed with observedGeneration 1, got State: $STATUS, Gen: $OBS_GEN"
    exit 1
fi

echo "Unblocking np-t1-grp attachment..."
payload3=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME_4" "op-attachmigs-np-t1-grp")
curl -s -X POST -H "Content-Type: application/json" -d "$payload3" http://localhost:8080/inject/op-status

echo "Waiting for slice to become ACTIVE..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        kubectl describe slice "$SLICE_NAME_4"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_4" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice is ACTIVE!"
        break
    fi
    echo "   Slice status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done

echo "Deleting slice..."
kubectl delete slice "$SLICE_NAME_4"

echo "Waiting for slice to be deleted..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to be deleted."
        kubectl describe slice "$SLICE_NAME_4"
        exit 1
    fi
    if ! kubectl get slice "$SLICE_NAME_4" 2>/dev/null; then
        echo "-> Slice deleted successfully!"
        break
    fi
    echo "   Slice still exists. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done

echo ""
echo "========================================================="
echo "Starting test case 1: 0->N update"
echo "========================================================="
echo ""

DYNAMIC_YAML_PATH=$(generate_empty_slice_yaml "$SLICE_NAME")
echo "Creating empty slice..."
if ! OUTPUT=$(kubectl apply -f "$DYNAMIC_YAML_PATH" 2>&1); then
    echo "Error: Failed to create the slice from $DYNAMIC_YAML_PATH. Aborting."
    echo "$OUTPUT"
    exit 1
fi
echo "$OUTPUT"

echo "Waiting for slice to settle..."
sleep 5

echo "Verifying slice status..."
STATUS=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")

if [[ "$STATUS" == "SliceNotCreated" ]]; then
    echo "Success: Slice is in SliceNotCreated state."
else
    echo "ERROR: Expected SliceNotCreated but got: $STATUS"
    kubectl describe slice "$SLICE_NAME"
    kubectl delete -f "$DYNAMIC_YAML_PATH"
    exit 1
fi

echo "Proceeding with 0->N update test..."

# Generate random hashes for pools
HASH1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Generated Hashes for Update:"
echo "  np-t1 -> $HASH1"
echo "  np-t2 -> $HASH2"

echo "Re-labeling nodes with random hashes..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
if [ -z "$NP1_NODES" ]; then
    echo "ERROR: No nodes found for np-t1"
    exit 1
fi
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH1" --overwrite
done

NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
if [ -z "$NP2_NODES" ]; then
    echo "ERROR: No nodes found for np-t2"
    exit 1
fi
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH2" --overwrite
done

echo "Injecting MIG info for np-t1-grp and np-t2-grp..."
API_ENDPOINT="http://localhost:8080/inject/mig"
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    echo "  -> Injecting: MIG='$MIG_NAME', Slice='$SLICE_NAME'"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MIG_NAME")
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT")
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
    if [ "$HTTP_STATUS" -eq 200 ] || [ "$HTTP_STATUS" -eq 201 ]; then
         echo "  -> Injection Success [$HTTP_STATUS]"
    else
         echo "  -> Injection Failed [$HTTP_STATUS]"
    fi
done

echo "Patching slice to add partition IDs..."
kubectl patch slice "$SLICE_NAME" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH1\",\"$HASH2\"]}}"

echo "Waiting for slice to become ACTIVE..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        kubectl describe slice "$SLICE_NAME"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice is ACTIVE!"
        break
    fi
    echo "   Slice status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done

echo "Deleting slice..."
kubectl delete -f "$DYNAMIC_YAML_PATH"

echo "Verifying slice is deleted..."
if kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; then
    echo "ERROR: Slice '$SLICE_NAME' still exists after deletion."
    exit 1
else
    echo "Success: Slice '$SLICE_NAME' is gone."
fi

echo "Proceeding with test case 2: Update during Deletion..."

# 1. Create a slice and make it active
SLICE_NAME_2="update-during-delete"
DYNAMIC_YAML_PATH_2=$(generate_empty_slice_yaml "$SLICE_NAME_2")

echo "Creating empty slice for Case 2..."
kubectl apply -f "$DYNAMIC_YAML_PATH_2"

sleep 2

# Patch to make it active so we can test deletion
HASH3=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH4=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Generated Hashes for Case 2:"
echo "  np-t1 -> $HASH3"
echo "  np-t2 -> $HASH4"

echo "Re-labeling nodes..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH3" --overwrite
done
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH4" --overwrite
done

echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_2" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

echo "Patching slice to make it active..."
kubectl patch slice "$SLICE_NAME_2" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH3\",\"$HASH4\"]}}"

echo "Waiting for slice to become ACTIVE..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        kubectl describe slice "$SLICE_NAME_2"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_2" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice is ACTIVE!"
        break
    fi
    echo "   Slice status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done

# Now start deletion but block it!
echo "Injecting RUNNING status for detach ops to block deletion..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    OP_KEY="op-detachmigs-${MIG_NAME}"
    payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME_2" "$OP_KEY")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status
done

echo "Deleting slice in background..."
kubectl delete slice "$SLICE_NAME_2" &
DELETE_PID=$!

echo "Waiting for slice to enter DEACTIVATING..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become DEACTIVATING."
        kubectl describe slice "$SLICE_NAME_2"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_2" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "DEACTIVATING" ]; then
        echo "-> Slice is DEACTIVATING!"
        break
    fi
    echo "   Slice status is $STATUS. Waiting 3 seconds... ($VERIFY_ELAPSED/60s)"
    sleep 3
done

# Now try to update it while deleting!
echo "Attempting to update partitions while deleting (should be ignored)..."
kubectl patch slice "$SLICE_NAME_2" --type=merge -p "{\"spec\":{\"partitionIds\":[]}}"

sleep 5
# Verify it is STILL DEACTIVATING and not trying to update
STATUS=$(kubectl get slice "$SLICE_NAME_2" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
if [[ "$STATUS" == "DEACTIVATING" ]]; then
    echo "Success: Update was ignored, slice is still DEACTIVATING."
else
    echo "ERROR: Expected DEACTIVATING but got: $STATUS"
    exit 1
fi

# Unblock deletion
echo "Unblocking deletion..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    OP_KEY="op-detachmigs-${MIG_NAME}"
    payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME_2" "$OP_KEY")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status
done

# Wait for deletion
wait $DELETE_PID

echo "Verifying slice is deleted..."
if kubectl get slice "$SLICE_NAME_2" -n default > /dev/null 2>&1; then
    echo "ERROR: Slice '$SLICE_NAME_2' still exists after deletion."
    exit 1
else
    echo "Success: Slice '$SLICE_NAME_2' is gone."
fi

echo "Verified slice deletion complete for Case 2."

echo ""
echo "========================================================="
echo "Starting test case 3: Delete during Update"
echo "========================================================="
echo ""

SLICE_NAME_3="delete-during-update"
DYNAMIC_YAML_PATH_3=$(generate_empty_slice_yaml "$SLICE_NAME_3")

echo "Creating empty slice for Case 3..."
kubectl apply -f "$DYNAMIC_YAML_PATH_3"

sleep 2

echo "Injecting RUNNING status for attach ops to block update..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    OP_KEY="op-attachmigs-${MIG_NAME}"
    payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME_3" "$OP_KEY")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status
done

echo "Triggering update (0->N)..."
kubectl patch slice "$SLICE_NAME_3" --type=merge -p "{\"spec\":{\"partitionIds\":[\"p1\",\"p2\"]}}"

sleep 5

echo "Sending DELETE command while update is blocked..."
kubectl delete slice "$SLICE_NAME_3" &
DELETE_PID_3=$!

sleep 5

# Unblock attach ops to let it fail or finish and see what happens
echo "Unblocking attach ops..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    OP_KEY="op-attachmigs-${MIG_NAME}"
    payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "DONE"}' "$SLICE_NAME_3" "$OP_KEY")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status
done

wait $DELETE_PID_3

echo "Verifying slice is deleted..."
if kubectl get slice "$SLICE_NAME_3" -n default > /dev/null 2>&1; then
    echo "ERROR: Slice '$SLICE_NAME_3' still exists after deletion."
    exit 1
else
    echo "Success: Slice '$SLICE_NAME_3' is gone."
fi

echo "Verified slice deletion complete for Case 3."

echo ""
echo "========================================================="
echo "Starting test case 5: N->0 update (Happy Path)"
echo "========================================================="
echo ""

SLICE_NAME_5="update-n-to-0"
DYNAMIC_YAML_PATH_5=$(generate_empty_slice_yaml "$SLICE_NAME_5")

echo "Creating empty slice for Case 5..."
kubectl apply -f "$DYNAMIC_YAML_PATH_5"
sleep 5

HASH5=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH6=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH5" --overwrite
done
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH6" --overwrite
done

for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_5" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

kubectl patch slice "$SLICE_NAME_5" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH5\",\"$HASH6\"]}}"

echo "Waiting for slice to become ACTIVE..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_5" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice is ACTIVE!"
        break
    fi
    sleep 3
done

echo "Triggering N->0 update..."
kubectl patch slice "$SLICE_NAME_5" --type=merge -p "{\"spec\":{\"partitionIds\":[]}}"

echo "Waiting for slice to become SliceNotCreated..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become SliceNotCreated."
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_5" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "SliceNotCreated" ]; then
        echo "-> Slice is SliceNotCreated!"
        break
    fi
    sleep 3
done

echo "Success: N->0 update completed!"

echo "Deleting slice for cleanup..."
kubectl delete slice "$SLICE_NAME_5"

echo "Verifying slice is deleted..."
if kubectl get slice "$SLICE_NAME_5" -n default > /dev/null 2>&1; then
    echo "ERROR: Slice '$SLICE_NAME_5' still exists after deletion."
    exit 1
else
    echo "Success: Slice '$SLICE_NAME_5' is gone."
fi

echo ""
echo "========================================================="
echo "Starting test case 6: N->0 update (Failure during Detach)"
echo "========================================================="
echo ""

SLICE_NAME_6="update-n-to-0-fail"
DYNAMIC_YAML_PATH_6=$(generate_empty_slice_yaml "$SLICE_NAME_6")

echo "Creating empty slice for Case 6..."
kubectl apply -f "$DYNAMIC_YAML_PATH_6"
sleep 5

HASH7=$(head -c 512 /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH8=$(head -c 512 /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH7" --overwrite
done
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH8" --overwrite
done

for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_6" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

kubectl patch slice "$SLICE_NAME_6" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH7\",\"$HASH8\"]}}"

echo "Waiting for slice to become ACTIVE..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_6" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice is ACTIVE!"
        break
    fi
    sleep 3
done

echo "Injecting FAIL status for detach ops..."
payload1=$(printf '{"sliceName": "%s", "key": "op-detachmigs-np-t1-grp", "status": "FAIL"}' "$SLICE_NAME_6")
payload2=$(printf '{"sliceName": "%s", "key": "op-detachmigs-np-t2-grp", "status": "FAIL"}' "$SLICE_NAME_6")
curl -s -X POST -H "Content-Type: application/json" -d "$payload1" http://localhost:8080/inject/op-status
curl -s -X POST -H "Content-Type: application/json" -d "$payload2" http://localhost:8080/inject/op-status

BEFORE_GEN=$(kubectl get slice "$SLICE_NAME_6" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")

echo "Triggering N->0 update..."
kubectl patch slice "$SLICE_NAME_6" --type=merge -p "{\"spec\":{\"partitionIds\":[]}}"

sleep 20

STATUS=$(kubectl get slice "$SLICE_NAME_6" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
OBS_GEN=$(kubectl get slice "$SLICE_NAME_6" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")

if [[ "$STATUS" == "ACTIVE" ]] && [[ "$OBS_GEN" == "$BEFORE_GEN" ]]; then
    echo "Success: State is ACTIVE and observedGeneration remains $BEFORE_GEN."
else
    echo "ERROR: Expected ACTIVE with observedGeneration $BEFORE_GEN, got State: $STATUS, Gen: $OBS_GEN"
    exit 1
fi

echo "Clearing injected op statuses to unblock detach..."
payload_clear=$(printf '{"sliceName": "%s", "clearOpStatuses": true}' "$SLICE_NAME_6")
curl -s -X POST -H "Content-Type: application/json" -d "$payload_clear" http://localhost:8080/inject/clear

echo "Deleting slice..."
kubectl delete slice "$SLICE_NAME_6"

echo "Verifying slice is deleted..."
if kubectl get slice "$SLICE_NAME_6" -n default > /dev/null 2>&1; then
    echo "ERROR: Slice '$SLICE_NAME_6' still exists after deletion."
    exit 1
else
    echo "Success: Slice '$SLICE_NAME_6' is gone."
fi

echo ""
echo "========================================================="
echo "Starting test case 7: 0-N-0-N update with different partitions"
echo "========================================================="
echo ""

SLICE_NAME_7="update-0-n-0-n"
DYNAMIC_YAML_PATH_7=$(generate_empty_slice_yaml "$SLICE_NAME_7")

echo "Creating empty slice for Case 7..."
kubectl apply -f "$DYNAMIC_YAML_PATH_7"
sleep 5

# Step 1: 0 -> N (Partitions P1, P2)
HASH9=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH10=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH9" --overwrite
done
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH10" --overwrite
done

for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_7" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

echo "Verifying no nodes have slice label before 0-N update..."
wait_for_labeled_node_count "$SLICE_NAME_7" 0 || exit 1

kubectl patch slice "$SLICE_NAME_7" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH9\",\"$HASH10\"]}}"

echo "Waiting for slice to become ACTIVE (first N)..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_7" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice is ACTIVE!"
        break
    fi
    sleep 3
done

echo "Verifying nodes have slice label after 0-N update..."
wait_for_labeled_node_count "$SLICE_NAME_7" 32 || exit 1

echo "Verifying MMIG members (Step 1)..."
MEMBERS=$(curl -s http://localhost:8080/projects/123456789012/regions/us-central2/multiMigs/${SLICE_NAME_7}-testclus/multiMigMembers)
if [[ "$MEMBERS" == *"np-t1-grp"* ]] && [[ "$MEMBERS" == *"np-t2-grp"* ]]; then
    echo "Success: Both MIGs are attached in Step 1."
else
    echo "ERROR: Expected both MIGs attached, got: $MEMBERS"
    exit 1
fi

# Step 2: N -> 0
echo "Manually labeling nodes before N-0 update to ensure they are there..."
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-slice=$SLICE_NAME_7" --overwrite
done
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-slice=$SLICE_NAME_7" --overwrite
done

echo "Triggering N->0 update..."
kubectl patch slice "$SLICE_NAME_7" --type=merge -p "{\"spec\":{\"partitionIds\":[]}}"

echo "Waiting for slice to become SliceNotCreated..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become SliceNotCreated."
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_7" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "SliceNotCreated" ]; then
        echo "-> Slice is SliceNotCreated!"
        break
    fi
    sleep 3
done

echo "Verifying nodes have slice label removed after N-0 update..."
wait_for_labeled_node_count "$SLICE_NAME_7" 0 || exit 1

echo "Verifying MMIG members (Step 2)..."
MEMBERS=$(curl -s http://localhost:8080/projects/123456789012/regions/us-central2/multiMigs/${SLICE_NAME_7}-testclus/multiMigMembers)
if [[ "$MEMBERS" != *"np-t1-grp"* ]] && [[ "$MEMBERS" != *"np-t2-grp"* ]]; then
    echo "Success: MMIG is empty or gone in Step 2."
else
    echo "ERROR: Expected MMIG to be empty or gone, but got response: $MEMBERS"
    exit 1
fi

# Step 3: 0 -> N (Partitions P3, P4)
HASH11=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH12=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH11" --overwrite
done
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x4-id=$HASH12" --overwrite
done

for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_7" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

kubectl patch slice "$SLICE_NAME_7" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH11\",\"$HASH12\"]}}"

echo "Waiting for slice to become ACTIVE (second N)..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for Slice to become ACTIVE."
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_7" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> Slice is ACTIVE!"
        break
    fi
    sleep 3
done

echo "Verifying MMIG members (Step 3)..."
MEMBERS=$(curl -s http://localhost:8080/projects/123456789012/regions/us-central2/multiMigs/${SLICE_NAME_7}-testclus/multiMigMembers)
if [[ "$MEMBERS" == *"np-t1-grp"* ]] && [[ "$MEMBERS" == *"np-t2-grp"* ]]; then
    echo "Success: Both MIGs are attached in Step 3."
else
    echo "ERROR: Expected both MIGs attached, got: $MEMBERS"
    exit 1
fi

echo "Success: 0-N-0-N update completed successfully!"

echo "Deleting slice for cleanup..."
timeout 60s kubectl delete slice "$SLICE_NAME_7" || {
    echo "ERROR: Timeout deleting slice $SLICE_NAME_7"
    echo "Capturing logs..."
    kubectl logs -n slice-controller-system deployment/slice-controller-controller-manager > /tmp/controller-case7.log 2>&1 || echo "Failed to get controller logs"
    kubectl logs -n slice-controller-system deployment/mock-gce > /tmp/mock-gce-case7.log 2>&1 || echo "Failed to get mock-gce logs"
    exit 1
}

echo "Verifying slice is deleted..."
if kubectl get slice "$SLICE_NAME_7" -n default > /dev/null 2>&1; then
    echo "ERROR: Slice '$SLICE_NAME_7' still exists after deletion."
    exit 1
else
    echo "Success: Slice '$SLICE_NAME_7' is gone."
fi

echo "=== Case 8: 0->N Update Failure Without Retry ==="

SLICE_NAME_8="update-fail-no-retry"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_8}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_8
spec:
  type: "tpu7x"
  topology: "4x4x8"
  partitionIds: []
INNER_EOF

HASH8_1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH8_2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x8-id=$HASH8_1" --overwrite
done
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x8-id=$HASH8_2" --overwrite
done

sleep 10

echo "Creating empty slice for Case 8..."
kubectl create -f "/tmp/${SLICE_NAME_8}.yaml"

echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_8" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

echo "Injecting FAIL status for create-mmig..."
payload=$(printf '{"sliceName": "%s", "key": "CreateMMIG", "code": 500, "message": "Injected Error"}' "$SLICE_NAME_8")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/http-error

echo "Triggering 0->N update..."
kubectl patch slice "$SLICE_NAME_8" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH8_1\",\"$HASH8_2\"]}}"

echo "Waiting for slice to fail and observe generation..."
for i in {1..120}; do
    STATUS=$(kubectl get slice "$SLICE_NAME_8" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    OBS_GEN=$(kubectl get slice "$SLICE_NAME_8" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")
    GEN=$(kubectl get slice "$SLICE_NAME_8" -n default -o jsonpath='{.metadata.generation}' 2>/dev/null || echo "NotFound")

    if [ "$STATUS" == "SliceCreationFailed" ] && [ "$OBS_GEN" == "$GEN" ]; then
        echo "-> Slice failed correctly and observedGeneration advanced to $GEN!"
        break
    fi
    sleep 3
done

if [ "$STATUS" != "SliceCreationFailed" ] || [ "$OBS_GEN" != "$GEN" ]; then
    echo "ERROR: Slice did not correctly update observedGeneration on failure without retry."
    echo "STATUS: $STATUS, OBS_GEN: $OBS_GEN, GEN: $GEN"
    exit 1
fi
echo "Success: Case 8 completed!"

echo "Removing injected error..."
curl -s -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/http-error

echo "Deleting slice for cleanup..."
kubectl delete slice "$SLICE_NAME_8"

echo "=== Case 9: 0->N Update Attach Failure Without Retry ==="

SLICE_NAME_9="update-attach-fail-no-retry"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_9}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_9
spec:
  type: "tpu7x"
  topology: "4x4x8"
  partitionIds: []
INNER_EOF

HASH9_1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH9_2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x8-id=$HASH9_1" --overwrite
done
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-4x4x8-id=$HASH9_2" --overwrite
done

sleep 10

echo "Creating empty slice for Case 9..."
kubectl create -f "/tmp/${SLICE_NAME_9}.yaml"

echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_9" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

echo "Injecting FAIL status for attach-migs..."
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "FAIL"}' "$SLICE_NAME_9" "op-attachmigs-np-t1-grp")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status

echo "Triggering 0->N update..."
kubectl patch slice "$SLICE_NAME_9" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH9_1\",\"$HASH9_2\"]}}"

echo "Waiting for slice to fail and observe generation..."
for i in {1..120}; do
    STATUS=$(kubectl get slice "$SLICE_NAME_9" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    OBS_GEN=$(kubectl get slice "$SLICE_NAME_9" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")
    GEN=$(kubectl get slice "$SLICE_NAME_9" -n default -o jsonpath='{.metadata.generation}' 2>/dev/null || echo "NotFound")

    if [ "$STATUS" == "SliceCreationFailed" ] && [ "$OBS_GEN" == "$GEN" ]; then
        echo "-> Slice failed correctly and observedGeneration advanced to $GEN!"
        break
    fi
    sleep 3
done

if [ "$STATUS" != "SliceCreationFailed" ] || [ "$OBS_GEN" != "$GEN" ]; then
    echo "ERROR: Slice did not correctly update observedGeneration on attach failure without retry."
    echo "STATUS: $STATUS, OBS_GEN: $OBS_GEN, GEN: $GEN"
    exit 1
fi
echo "Success: Case 9 completed!"

echo "Removing injected error..."
curl -s -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status

echo "Deleting slice for cleanup..."
kubectl delete slice "$SLICE_NAME_9"


echo "=== Case 10: 0->N Update Preflight Failure ==="

SLICE_NAME_10="update-preflight-fail"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_10}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_10
spec:
  type: "tpu7x"
  topology: "4x4x8"
  partitionIds: []
INNER_EOF

# We intentionally do NOT label nodes for Case 10, causing Preflight to fail (MIG info not found)

echo "Creating empty slice for Case 10..."
kubectl create -f "/tmp/${SLICE_NAME_10}.yaml"

echo "Triggering 0->N update..."
kubectl patch slice "$SLICE_NAME_10" --type=merge -p "{\"spec\":{\"partitionIds\":[\"invalid1\",\"invalid2\"]}}"

echo "Waiting for slice to fail preflight and observe generation..."
for i in {1..120}; do
    STATUS=$(kubectl get slice "$SLICE_NAME_10" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    OBS_GEN=$(kubectl get slice "$SLICE_NAME_10" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")
    GEN=$(kubectl get slice "$SLICE_NAME_10" -n default -o jsonpath='{.metadata.generation}' 2>/dev/null || echo "NotFound")

    if [ "$STATUS" == "SliceCreationFailed" ] && [ "$OBS_GEN" == "$GEN" ]; then
        echo "-> Slice failed preflight correctly and observedGeneration advanced to $GEN!"
        break
    fi
    sleep 3
done

if [ "$STATUS" != "SliceCreationFailed" ] || [ "$OBS_GEN" != "$GEN" ]; then
    echo "ERROR: Slice did not correctly update observedGeneration on preflight failure."
    echo "STATUS: $STATUS, OBS_GEN: $OBS_GEN, GEN: $GEN"
    exit 1
fi
echo "Success: Case 10 completed!"

echo "Deleting slice for cleanup..."
kubectl delete slice "$SLICE_NAME_10"


echo ""
echo "========================================================="
echo "=== Case 11: SubSlice Update Happy Path (N->0->M) ==="
echo "========================================================="
echo ""

SLICE_NAME_11="subslice-update-happy"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_11}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_11
spec:
  type: "tpu7x"
  topology: "2x2x4"
  partitionIds: []
INNER_EOF

HASH_OLD=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH_NEW=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Re-labeling nodes for Case 11..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH_OLD" --overwrite
done
NP2_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t2" -o jsonpath='{.items[*].metadata.name}')
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH_NEW" --overwrite
done

echo "Sleeping 10s to allow informers to sync..."
sleep 10

echo "Creating empty slice for Case 11..."
kubectl create -f "/tmp/${SLICE_NAME_11}.yaml"

echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_11" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
    topo_payload=$(printf '{"sliceName": "%s", "igmName": "%s", "topologies": {"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME" "$MIG_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$topo_payload" "$TOPO_INFO_ENDPOINT"
done

echo "Injecting MIG topology states..."
payload_old=$(printf '{"sliceName": "%s", "igmName": "np-t1-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t1-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_11" "$HASH_OLD")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload_old" http://localhost:8080/inject/topology-info

payload_new=$(printf '{"sliceName": "%s", "igmName": "np-t2-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t2-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_11" "$HASH_NEW")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload_new" http://localhost:8080/inject/topology-info

echo "Triggering 0->N update..."
kubectl patch slice "$SLICE_NAME_11" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH_OLD\"]}}"

echo "Waiting for slice to become ACTIVE on HASH_OLD..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 90 ]; then
        echo "  -> ERROR: Timeout waiting for SubSlice to become ACTIVE."
        kubectl describe slice "$SLICE_NAME_11"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_11" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> SubSlice is ACTIVE on HASH_OLD!"
        break
    fi
    sleep 3
done

ANNOTATION=$(kubectl get slice "$SLICE_NAME_11" -n default -o jsonpath='{.metadata.annotations.accelerator\.gke\.io/provisioned-partition}')
if [ "$ANNOTATION" != "$HASH_OLD" ]; then
    echo "ERROR: provisioned-partition annotation should be $HASH_OLD, got $ANNOTATION"
    exit 1
fi
echo "Success: Stamped with provisioned partition $HASH_OLD"

echo "Triggering N->M update..."
kubectl patch slice "$SLICE_NAME_11" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH_NEW\"]}}"

echo "Waiting for slice to transition N->0->M and become ACTIVE on HASH_NEW..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 120 ]; then
        echo "  -> ERROR: Timeout waiting for SubSlice to complete update N->0->M and become ACTIVE."
        kubectl describe slice "$SLICE_NAME_11"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_11" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    ANNOTATION=$(kubectl get slice "$SLICE_NAME_11" -n default -o jsonpath='{.metadata.annotations.accelerator\.gke\.io/provisioned-partition}')
    if [ "$STATUS" == "ACTIVE" ] && [ "$ANNOTATION" == "$HASH_NEW" ]; then
        echo "-> SubSlice successfully updated N->0->M and is ACTIVE on HASH_NEW!"
        break
    fi
    sleep 3
done

echo "Success: Case 11 completed!"
kubectl delete slice "$SLICE_NAME_11"

echo ""
echo "========================================================="
echo "=== Case 12: SubSlice Update Deform Phase Failure Handoff ==="
echo "========================================================="
echo ""

SLICE_NAME_12="subslice-deform-fail"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_12}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_12
spec:
  type: "tpu7x"
  topology: "2x2x4"
  partitionIds: []
INNER_EOF

HASH12_OLD=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH12_NEW=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Re-labeling nodes for Case 12..."
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH12_OLD" --overwrite
done
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH12_NEW" --overwrite
done

echo "Sleeping 10s..."
sleep 10

echo "Creating empty slice for Case 12..."
kubectl create -f "/tmp/${SLICE_NAME_12}.yaml"

echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_12" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
done

payload12_old=$(printf '{"sliceName": "%s", "igmName": "np-t1-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t1-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_12" "$HASH12_OLD")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload12_old" http://localhost:8080/inject/topology-info

payload12_new=$(printf '{"sliceName": "%s", "igmName": "np-t2-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t2-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_12" "$HASH12_NEW")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload12_new" http://localhost:8080/inject/topology-info

kubectl patch slice "$SLICE_NAME_12" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH12_OLD\"]}}"
wait_for_slice_status "$SLICE_NAME_12" "ACTIVE"

echo "Injecting HTTP 500 Error for Deactivation..."
payload_fail=$(printf '{"sliceName": "%s", "key": "ConfigureAcceleratorTopologies", "code": 500, "message": "Injected Deform Failure"}' "$SLICE_NAME_12")
curl -s -X POST -H "Content-Type: application/json" -d "$payload_fail" http://localhost:8080/inject/http-error

kubectl patch slice "$SLICE_NAME_12" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH12_NEW\"]}}"

echo "Waiting for slice to hit failure state..."
sleep 20

STATUS=$(kubectl get slice "$SLICE_NAME_12" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
OBS_GEN=$(kubectl get slice "$SLICE_NAME_12" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")

if [ "$STATUS" == "SliceCreationFailed" ] && [ "$OBS_GEN" == "3" ]; then
    echo "Success: Case 12 form failure correctly advanced ObservedGeneration to 3!"
else
    echo "ERROR: Expected SliceCreationFailed with ObservedGen 3, got State: $STATUS, Gen: $OBS_GEN"
    exit 1
fi

curl -s -X DELETE -H "Content-Type: application/json" -d "$payload_fail" http://localhost:8080/inject/http-error
kubectl delete slice "$SLICE_NAME_12"


echo ""
echo "========================================================="
echo "=== Case 13: SubSlice Update Form Phase Failure Handoff ==="
echo "========================================================="
echo ""

SLICE_NAME_13="subslice-form-fail"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_13}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_13
spec:
  type: "tpu7x"
  topology: "2x2x4"
  partitionIds: []
INNER_EOF

HASH13_OLD=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
HASH13_NEW=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Re-labeling nodes for Case 13..."
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH13_OLD" --overwrite
done
for node in $NP2_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH13_NEW" --overwrite
done

echo "Sleeping 10s..."
sleep 10

echo "Creating empty slice for Case 13..."
kubectl create -f "/tmp/${SLICE_NAME_13}.yaml"

echo "Injecting MIG info..."
for MIG_NAME in "np-t1-grp" "np-t2-grp"; do
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME_13" "$MIG_NAME")
    curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig
done

payload13_old=$(printf '{"sliceName": "%s", "igmName": "np-t1-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t1-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_13" "$HASH13_OLD")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload13_old" http://localhost:8080/inject/topology-info

payload13_new=$(printf '{"sliceName": "%s", "igmName": "np-t2-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t2-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_13" "$HASH13_NEW")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload13_new" http://localhost:8080/inject/topology-info

kubectl patch slice "$SLICE_NAME_13" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH13_OLD\"]}}"
wait_for_slice_status "$SLICE_NAME_13" "ACTIVE"

echo "Injecting HTTP 500 Error for Activation..."
payload_fail13=$(printf '{"sliceName": "%s", "key": "ConfigureAcceleratorTopologies", "code": 500, "message": "Injected Form Failure"}' "$SLICE_NAME_13")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload_fail13" http://localhost:8080/inject/http-error

kubectl patch slice "$SLICE_NAME_13" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH13_NEW\"]}}"

echo "Waiting for slice to fail formation on HASH_NEW..."
for i in {1..60}; do
    STATUS=$(kubectl get slice "$SLICE_NAME_13" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    OBS_GEN=$(kubectl get slice "$SLICE_NAME_13" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "NotFound")

    if [ "$STATUS" == "SliceCreationFailed" ] && [ "$OBS_GEN" == "3" ]; then
        echo "Success: Case 13 form failure correctly advanced ObservedGeneration to 3 (baton handoff)!"
        break
    fi
    sleep 3
done

if [ "$STATUS" != "SliceCreationFailed" ] || [ "$OBS_GEN" != "3" ]; then
    echo "ERROR: Expected SliceCreationFailed status with ObservedGen 3, got State: $STATUS, Gen: $OBS_GEN"
    exit 1
fi

curl -s -X DELETE -H "Content-Type: application/json" -d "$payload_fail13" http://localhost:8080/inject/http-error
kubectl delete slice "$SLICE_NAME_13"


echo ""
echo "========================================================="
echo "=== Case 14: SubSlice N->0 Update (Remove Partition) ==="
echo "========================================================="
echo ""

SLICE_NAME_14="subslice-n-to-0"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_14}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_14
spec:
  type: "tpu7x"
  topology: "2x2x4"
  partitionIds: []
INNER_EOF

HASH14=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Re-labeling nodes for Case 14..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH14" --overwrite
done

echo "Sleeping 10s to allow informers to sync..."
sleep 10

echo "Creating empty slice for Case 14..."
kubectl create -f "/tmp/${SLICE_NAME_14}.yaml"

echo "Injecting MIG info..."
payload=$(printf '{"sliceName": "%s", "migName": "np-t1-grp"}' "$SLICE_NAME_14")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig

echo "Injecting MIG topology state..."
payload14=$(printf '{"sliceName": "%s", "igmName": "np-t1-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t1-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_14" "$HASH14")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload14" http://localhost:8080/inject/topology-info

echo "Triggering 0->N to form initial subslice..."
kubectl patch slice "$SLICE_NAME_14" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH14\"]}}"
wait_for_slice_status "$SLICE_NAME_14" "ACTIVE"

ANNOTATION=$(kubectl get slice "$SLICE_NAME_14" -n default -o jsonpath='{.metadata.annotations.accelerator\.gke\.io/provisioned-partition}')
if [ "$ANNOTATION" != "$HASH14" ]; then
    echo "ERROR: provisioned-partition annotation should be $HASH14, got $ANNOTATION"
    exit 1
fi
echo "Success: SubSlice is ACTIVE with provisioned partition $HASH14"

echo "Triggering N->0 update (removing partition)..."
kubectl patch slice "$SLICE_NAME_14" --type=merge -p '{"spec":{"partitionIds":[]}}'

echo "Waiting for slice to transition to SliceNotCreated..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 120 ]; then
        echo "  -> ERROR: Timeout waiting for SubSlice to transition to SliceNotCreated."
        kubectl describe slice "$SLICE_NAME_14"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_14" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "SliceNotCreated" ]; then
        echo "-> SubSlice successfully transitioned to SliceNotCreated!"
        break
    fi
    sleep 3
done

echo "Verifying provisioned-partition annotation is cleared..."
ANNOTATION=$(kubectl get slice "$SLICE_NAME_14" -n default -o jsonpath='{.metadata.annotations.accelerator\.gke\.io/provisioned-partition}' 2>/dev/null || echo "")
if [ -n "$ANNOTATION" ]; then
    echo "ERROR: provisioned-partition annotation should be empty after N->0, got $ANNOTATION"
    exit 1
fi

echo "Verifying NodeLabeled condition is False..."
NODE_LABEL_STATUS=$(kubectl get slice "$SLICE_NAME_14" -n default -o jsonpath='{.status.conditions[?(@.type=="NodeLabeled")].status}' 2>/dev/null || echo "")
if [ "$NODE_LABEL_STATUS" != "False" ]; then
    echo "ERROR: NodeLabeled condition should be False after N->0, got $NODE_LABEL_STATUS"
    exit 1
fi

echo "Success: Case 14 completed!"
kubectl delete slice "$SLICE_NAME_14"


echo ""
echo "========================================================="
echo "=== Case 15: SubSlice 0->N Update (Add Partition)     ==="
echo "========================================================="
echo ""

SLICE_NAME_15="subslice-0-to-n"

cat <<INNER_EOF > "/tmp/${SLICE_NAME_15}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME_15
spec:
  type: "tpu7x"
  topology: "2x2x4"
  partitionIds: []
INNER_EOF

HASH15=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

echo "Re-labeling nodes for Case 15..."
NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
for node in $NP1_NODES; do
    kubectl label node "$node" "cloud.google.com/gke-tpu-partition-2x2x4-id=$HASH15" --overwrite
done

echo "Sleeping 10s to allow informers to sync..."
sleep 10

echo "Creating empty slice for Case 15 (starts with 0 partitions)..."
kubectl create -f "/tmp/${SLICE_NAME_15}.yaml"

echo "Injecting MIG info..."
payload=$(printf '{"sliceName": "%s", "migName": "np-t1-grp"}' "$SLICE_NAME_15")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig

echo "Injecting MIG topology state..."
payload15=$(printf '{"sliceName": "%s", "igmName": "np-t1-grp", "topologies": {"%s": {"acceleratorTopology": "2x2x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY", "acceleratorTopologyState": {"currentState": "INACTIVE"}}, "np-t1-grp": {"acceleratorTopology": "4x4x4", "acceleratorTopologyHealth": "HEALTHY", "instancesHealth": "ALL_HEALTHY"}}}' "$SLICE_NAME_15" "$HASH15")
curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload15" http://localhost:8080/inject/topology-info

echo "Waiting for slice to stabilize in SliceNotCreated state..."
sleep 5

echo "Verifying initial state is SliceNotCreated..."
STATUS=$(kubectl get slice "$SLICE_NAME_15" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
OBS_GEN=$(kubectl get slice "$SLICE_NAME_15" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "0")
echo "Initial state: status=$STATUS, observedGeneration=$OBS_GEN"

echo "Triggering 0->N update (adding partition)..."
kubectl patch slice "$SLICE_NAME_15" --type=merge -p "{\"spec\":{\"partitionIds\":[\"$HASH15\"]}}"

echo "Waiting for slice to become ACTIVE..."
VERIFY_ELAPSED=0
while true; do
    VERIFY_ELAPSED=$((VERIFY_ELAPSED + 3))
    if [ "$VERIFY_ELAPSED" -ge 90 ]; then
        echo "  -> ERROR: Timeout waiting for SubSlice to become ACTIVE on 0->N update."
        kubectl describe slice "$SLICE_NAME_15"
        exit 1
    fi
    STATUS=$(kubectl get slice "$SLICE_NAME_15" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
    if [ "$STATUS" == "ACTIVE" ]; then
        echo "-> SubSlice is ACTIVE!"
        break
    fi
    sleep 3
done

echo "Verifying provisioned-partition annotation is set..."
ANNOTATION=$(kubectl get slice "$SLICE_NAME_15" -n default -o jsonpath='{.metadata.annotations.accelerator\.gke\.io/provisioned-partition}')
if [ "$ANNOTATION" != "$HASH15" ]; then
    echo "ERROR: provisioned-partition annotation should be $HASH15, got $ANNOTATION"
    exit 1
fi

echo "Verifying observedGeneration advanced..."
OBS_GEN=$(kubectl get slice "$SLICE_NAME_15" -n default -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo "0")
GEN=$(kubectl get slice "$SLICE_NAME_15" -n default -o jsonpath='{.metadata.generation}' 2>/dev/null || echo "0")
if [ "$OBS_GEN" != "$GEN" ]; then
    echo "ERROR: observedGeneration ($OBS_GEN) should match generation ($GEN)"
    exit 1
fi

echo "Success: Case 15 completed!"
kubectl delete slice "$SLICE_NAME_15"

echo ""
echo "========================================================="
echo "Starting test case: Rapid Sub-slice Updates (Preemption/Re-assignment spam)"
echo "========================================================="
echo ""

generate_empty_subslice_yaml() {
    local name="$1"
    cat <<EOF > "/tmp/${name}.yaml"
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $name
spec:
  type: "tpu7x"
  topology: "2x2x2"
  partitionIds: []
EOF
    echo "/tmp/${name}.yaml"
}

echo "Clearing mock server state before rapid test..."
curl -s -X POST http://localhost:8080/clear

SLICE_NAME="rapid-subslice"
DYNAMIC_YAML_PATH=$(generate_empty_subslice_yaml "$SLICE_NAME")
echo "Creating empty sub-slice..."
if ! OUTPUT=$(kubectl apply -f "$DYNAMIC_YAML_PATH" 2>&1); then
    echo "Error: Failed to create the slice from $DYNAMIC_YAML_PATH. Aborting."
    echo "$OUTPUT"
    exit 1
fi

sleep 3
STATUS=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
if [[ "$STATUS" == "SliceNotCreated" ]]; then
    echo "Success: Initial state is SliceNotCreated."
else
    echo "ERROR: Expected SliceNotCreated but got: $STATUS"
    exit 1
fi

# Generate 4 random hashes
H1=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
H2=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
H3=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)
H4=$(head /dev/urandom | tr -dc 'a-f0-9' | fold -w 8 | head -n 1)

CHOICES=("[]" "[\"$H1\"]" "[\"$H2\"]" "[\"$H3\"]" "[\"$H4\"]")
NUM_CHOICES=${#CHOICES[@]}

NP1_NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="np-t1" -o jsonpath='{.items[*].metadata.name}')
if [ -z "$NP1_NODES" ]; then
    echo "ERROR: No nodes found for np-t1"
    exit 1
fi

# We only need np-t1-grp for a 2x2x2 sub-slice (which requires 2 nodes).
echo "Injecting MIG info for np-t1-grp..."
payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "np-t1-grp")
curl -s -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig > /dev/null

echo "Injecting topology info for np-t1-grp..."
cat <<EOF > /tmp/topo-inject.json
{
  "sliceName": "$SLICE_NAME",
  "igmName": "np-t1-grp",
  "topologies": {
    "$H1": {
      "acceleratorTopology": "2x2x2",
      "acceleratorTopologyHealth": "HEALTHY",
      "instancesHealth": "ALL_HEALTHY",
      "acceleratorTopologyState": {
        "currentState": "ACTIVE"
      }
    },
    "$H2": {
      "acceleratorTopology": "2x2x2",
      "acceleratorTopologyHealth": "HEALTHY",
      "instancesHealth": "ALL_HEALTHY",
      "acceleratorTopologyState": {
        "currentState": "ACTIVE"
      }
    },
    "$H3": {
      "acceleratorTopology": "2x2x2",
      "acceleratorTopologyHealth": "HEALTHY",
      "instancesHealth": "ALL_HEALTHY",
      "acceleratorTopologyState": {
        "currentState": "ACTIVE"
      }
    },
    "$H4": {
      "acceleratorTopology": "2x2x2",
      "acceleratorTopologyHealth": "HEALTHY",
      "instancesHealth": "ALL_HEALTHY",
      "acceleratorTopologyState": {
        "currentState": "ACTIVE"
      }
    }
  }
}
EOF
curl -s -X POST -H "Content-Type: application/json" -d @/tmp/topo-inject.json http://localhost:8080/inject/topology-info > /dev/null

echo "Starting rapid update loop (20 iterations)..."
LAST_IDX=0
for i in {1..20}; do
    IDX=$((RANDOM % NUM_CHOICES))
    LAST_IDX=$IDX
    PATCH_STR=${CHOICES[$IDX]}
    echo "  Iteration $i: Patching with partitionIds: $PATCH_STR"

    if [ "$PATCH_STR" == "[]" ]; then
        kubectl label node $NP1_NODES "cloud.google.com/gke-tpu-partition-2x2x2-id-" --overwrite > /dev/null 2>&1 || true
        kubectl patch slice "$SLICE_NAME" --type=merge -p "{\"spec\":{\"partitionIds\":$PATCH_STR}}" > /dev/null
    else
        HASH=$(echo "$PATCH_STR" | tr -d '[]"')
        kubectl label node $NP1_NODES "cloud.google.com/gke-tpu-partition-2x2x2-id=$HASH" --overwrite > /dev/null
        kubectl patch slice "$SLICE_NAME" --type=merge -p "{\"spec\":{\"partitionIds\":$PATCH_STR}}" > /dev/null
    fi

    sleep 0.25
done

# The very last state we landed on
FINAL_PATCH=${CHOICES[$LAST_IDX]}
echo ""
echo "Finished rapid loop. Final patched state is: partitionIds=$FINAL_PATCH"

if [ "$FINAL_PATCH" == "[]" ]; then
    echo "Waiting for slice to settle into SliceNotCreated..."
    wait_for_slice_status "$SLICE_NAME" "SliceNotCreated" || exit 1
else
    # It's a hash. It is already labeled.
    HASH=$(echo "$FINAL_PATCH" | tr -d '[]"')
    echo "Waiting for slice to settle into ACTIVE..."
    wait_for_slice_status "$SLICE_NAME" "ACTIVE" || exit 1

    # Verify the final partition
    CURRENT_PARTITION=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath="{.metadata.annotations['accelerator\.gke\.io/provisioned-partition']}" 2>/dev/null)
    if [ "$CURRENT_PARTITION" != "$HASH" ]; then
        echo "ERROR: Expected partition ID to be '$HASH' but got '$CURRENT_PARTITION'"
        exit 1
    fi
    echo "-> Verified: Slice is ACTIVE with the correct partition ID '$HASH'."
fi


echo "Deleting slice..."
kubectl delete -f "$DYNAMIC_YAML_PATH"

echo "Verifying slice is deleted..."
if kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; then
    echo "ERROR: Slice '$SLICE_NAME' still exists after deletion."
    exit 1
else
    echo "Success: Slice '$SLICE_NAME' is gone."
fi

echo "✅ Rapid Sub-slice Updates test passed successfully!"

echo "All tests passed!"
