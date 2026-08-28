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
    "np-t1"
    "np-t2"
    "np-t3"
)
TARGET_COUNT=1

# Get the directory where the script is located
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GENERATED_YAML_DIR="$SCRIPT_DIR/generated"
YAML_PATH="$GENERATED_YAML_DIR/create-slice.yaml"
API_ENDPOINT="http://localhost:8080/inject/mig"
BASE_MIG_VAL="projects/test-project/zones/test-zone/instanceGroupManagers/"

PARTITION_ID_LABEL="cloud.google.com/gke-tpu-partition-4x4x4-id"
PARTITION_STATE_LABEL="cloud.google.com/gke-tpu-partition-4x4x4-state"
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

inject_mig_health() {
    local mig_name="$1"
    local health_status="$2"
    echo ""
    echo "Injecting $health_status health for MIG '$mig_name'..."
    echo "=================================================="
    local payload
    payload=$(printf '{"migName": "%s", "healthStatus": "%s"}' "$mig_name" "$health_status")
    curl -s -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/mig-health"
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
        sleep 2
    done
}

wait_for_slice_creation() {
    echo "Waiting for slice '$SLICE_NAME' to be created..."
    local start_time=$SECONDS
    while true; do
        if kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; then
            echo "-> Slice '$SLICE_NAME' has been created."
            break
        fi

        local elapsed_time=$((SECONDS - start_time))
        if [ "$elapsed_time" -ge 30 ]; then
            echo "  -> ERROR: Timeout waiting for slice '$SLICE_NAME' to be created."
            exit 1
        fi
        echo "   Slice not yet found. Waiting 3 seconds... ($elapsed_time/30s)"
        sleep 3
    done
}

find_healthy_partitions() {
    echo "Searching for $TARGET_COUNT healthy partitions..."
    HEALTHY_PARTITIONS=()
    for pool_name in "${POOL_NAMES[@]}"; do
        local partition_id="$pool_name"

        # Check if any node in this partition is UNHEALTHY or DEGRADED
        local unhealthy_nodes
        unhealthy_nodes=$(kubectl get nodes -l "$PARTITION_ID_LABEL=$partition_id" -o json | jq -r ".items[] | select(.metadata.labels[\"$PARTITION_STATE_LABEL\"] == \"UNHEALTHY\" or .metadata.labels[\"$PARTITION_STATE_LABEL\"] == \"DEGRADED\") | .metadata.name" | tr '\n' ' ')

        if [ -z "$unhealthy_nodes" ]; then
            echo "  -> Partition '$partition_id' in pool '$pool_name' is HEALTHY."
            HEALTHY_PARTITIONS+=("$partition_id")
            if [ "${#HEALTHY_PARTITIONS[@]}" -eq "$TARGET_COUNT" ]; then
                break
            fi
        else
            echo "  -> Partition '$partition_id' in pool '$pool_name' is UNHEALTHY (nodes: $unhealthy_nodes). Skipping."
        fi
    done

    if [ "${#HEALTHY_PARTITIONS[@]}" -lt "$TARGET_COUNT" ]; then
        echo "Error: Found only ${#HEALTHY_PARTITIONS[@]} healthy partitions, but require $TARGET_COUNT. Aborting."
        exit 1
    fi

    echo "-> Found sufficient healthy partitions: ${HEALTHY_PARTITIONS[*]}"
    echo "=================================================="
}

generate_slice_yaml() {
    if [ "${#HEALTHY_PARTITIONS[@]}" -eq 0 ]; then
        echo "Error: Cannot generate YAML, no healthy partitions were found."
        exit 1
    fi

    mkdir -p "$GENERATED_YAML_DIR"

    echo "Generating slice YAML at $YAML_PATH..."
    # This will overwrite the file if it exists
    cat > "$YAML_PATH" <<EOF
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: $SLICE_NAME
spec:
  type: "tpu7x"
  topology: "4x4x4"
  partitionIds:
EOF

    for partition_id in "${HEALTHY_PARTITIONS[@]}"; do
        echo "    - \"$partition_id\"" >> "$YAML_PATH"
    done
    echo "-> YAML generated successfully."
    echo "=================================================="
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

    echo "[Index ${IDX}] Setting up Node Pool: $POOL_NAME"

    NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="$POOL_NAME" -o jsonpath='{.items[*].metadata.name}')

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

    # for NODE in $NODES; do
    #     # Apply MIG annotation
    #     kubectl annotate node "$NODE" "node.gke.io/created-by-mig=$FULL_MIG_ANNOTATION" --overwrite > /dev/null 2>&1

    #     # Apply topology labels
    #     for LABEL in "${TOPOLOGY_LABELS[@]}"; do
    #          kubectl label node "$NODE" "$PARTITION_ID_LABEL=$POOL_NAME" --overwrite > /dev/null 2>&1
    #          kubectl label node "$NODE" "$PARTITION_STATE_LABEL=HEALTHY" --overwrite > /dev/null 2>&1
    #     done
    # done
    echo "  -> Successfully annotated nodes and set initial state to HEALTHY in pool '$POOL_NAME'"
    echo "--------------------------------------------------"
done

ACCELERATOR_TOPOLOGY="4x4x4"

echo ""
echo "--- Starting Initial Slice Creation ---"
echo "=================================================="
find_healthy_partitions

echo "Injecting MIG associations for healthy partitions..."
for partition_id in "${HEALTHY_PARTITIONS[@]}"; do
    MIG_NAME="${partition_id}-grp"
    echo "  -> Injecting: MIG='$MIG_NAME', Slice='$SLICE_NAME'"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MIG_NAME")

    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT")

    if [ "$HTTP_STATUS" -eq 200 ] || [ "$HTTP_STATUS" -eq 201 ]; then
         echo "  -> Injection Success [$HTTP_STATUS]"
    else
         echo "  -> Injection Failed [$HTTP_STATUS]"
    fi
done
echo "=================================================="

generate_slice_yaml

echo "Creating the slice and injecting INCOMPLETE status to the MMIG..."

inject_status "INCOMPLETE" "for creating slice"

if ! OUTPUT=$(kubectl apply -f "$YAML_PATH" 2>&1); then
    echo "Error: Failed to create the slice from $YAML_PATH. Aborting."
    echo "--- kubectl output ---"
    echo "$OUTPUT"
    echo "----------------------"
    exit 1
fi
echo "$OUTPUT"

wait_for_slice_creation
wait_for_slice_status "INCOMPLETE"

echo ""
echo "Current slices in the cluster:"
kubectl get slice -A

echo "Injecting ACTIVATING status to the MMIG..."

inject_status "ACTIVATING" "healthy"

wait_for_slice_status "ACTIVATING"

echo "Injecting ACTIVE status to the MMIG..."

inject_status "ACTIVE" "healthy"

wait_for_slice_status "ACTIVE"

echo ""
echo "-> Slice is ACTIVE."
echo "Current slices in the cluster:"
kubectl get slice -A

echo ""
echo "--- Simulating Node Failure ---"
echo "=================================================="

# Select a random MIG from the healthy partitions used to create the slice
RANDOM_PARTITION_ID=${HEALTHY_PARTITIONS[$((RANDOM % ${#HEALTHY_PARTITIONS[@]}))]}
MIG_TO_FAIL="${RANDOM_PARTITION_ID}-grp"

echo "Starting MTTR timer..."
MTTR_START_TIME=$SECONDS

echo "Injecting UNHEALTHY status for MIG '$MIG_TO_FAIL'..."

inject_mig_health "$MIG_TO_FAIL" "UNHEALTHY"

echo "Waiting for node state to update to UNHEALTHY for partition '$RANDOM_PARTITION_ID'..."
start_time=$SECONDS
while true; do
    NODE_STATE=$(kubectl get nodes -l "$PARTITION_ID_LABEL=$RANDOM_PARTITION_ID" -o jsonpath="{.items[0].metadata.labels.$PARTITION_STATE_LABEL}" 2>/dev/null || echo "NotFound")
    if [[ "$NODE_STATE" == "UNHEALTHY" ]]; then
        elapsed_time=$((SECONDS - start_time))
        echo "-> Node state for partition '$RANDOM_PARTITION_ID' is now UNHEALTHY. (Took ${elapsed_time}s)"
        break
    fi
    elapsed_time=$((SECONDS - start_time))
    if [ "$elapsed_time" -ge 60 ]; then
        echo "  -> ERROR: Timeout waiting for node state to become UNHEALTHY."
        exit 1
    fi
    echo "   Current node state: $NODE_STATE. Waiting 3 seconds... ($elapsed_time/60s)"
    sleep 3
done

echo "Injecting FAILED status into MMIG due to unhealthy node..."
inject_status "FAILED" "Node became unhealthy"

wait_for_slice_status "FAILED"
echo "-> Slice status correctly reflects FAILED state."
echo "=================================================="


echo ""
echo "--- Starting Slice Deletion and Recovery ---"
echo "=================================================="

echo "Deleting the failed slice to trigger recovery..."

kubectl delete -f "$YAML_PATH" &
DELETION_PID=$!

echo "Waiting for slice '$SLICE_NAME' to be deleted..."
while kubectl get slice "$SLICE_NAME" -n default > /dev/null 2>&1; do
    echo "   Slice still exists. Waiting 3 seconds..."
    sleep 3
done
wait $DELETION_PID

echo "-> Slice '$SLICE_NAME' has been deleted."
echo ""

echo "--- Starting Slice Re-creation (Recovery) ---"
echo "=================================================="
find_healthy_partitions
generate_slice_yaml

echo "Clearing mock server state before re-creation..."
curl -s -X POST http://localhost:8080/clear
echo "Re-injecting MIG associations for recovery..."
for partition_id in "${HEALTHY_PARTITIONS[@]}"; do
    MIG_NAME="${partition_id}-grp"
    echo "  -> Re-injecting: MIG='$MIG_NAME', Slice='$SLICE_NAME'"
    payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MIG_NAME")
    curl -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT"
done
echo "=================================================="

echo "Applying new slice definition to recover the slice..."
if ! OUTPUT=$(kubectl apply -f "$YAML_PATH" 2>&1); then
    echo "Error: Failed to re-create the slice from $YAML_PATH. Aborting."
    echo "--- kubectl output ---"
    echo "$OUTPUT"
    echo "----------------------"
    exit 1
fi
echo "$OUTPUT"

wait_for_slice_creation
echo "Injecting INCOMPLETE status for recovered slice..."
inject_status "INCOMPLETE" "for re-creating slice"
wait_for_slice_status "INCOMPLETE"

echo "Injecting ACTIVATING status for recovered slice..."
inject_status "ACTIVATING" "healthy"
wait_for_slice_status "ACTIVATING"

echo "Injecting ACTIVE status for recovered slice..."
inject_status "ACTIVE" "healthy"
wait_for_slice_status "ACTIVE"

echo "-> Recovered slice is ACTIVE. Stopping MTTR timer."
MTTR_END_TIME=$SECONDS

MTTR_DURATION=$((MTTR_END_TIME - MTTR_START_TIME))

echo "=================================================="
echo "MTTR Test Complete for $TARGET_COUNT cube(s): $MTTR_DURATION seconds"
echo "=================================================="
