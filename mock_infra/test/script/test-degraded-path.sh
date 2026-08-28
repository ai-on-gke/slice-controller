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
POOL_NAMES=("np-t1" "np-t2") # Patch both node pools to match happy-path setup

# Get the directory where the script is located
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
YAML_PATH="$SCRIPT_DIR/../yaml/create-slice.yaml"
API_ENDPOINT_BASE="http://localhost:8080/inject"
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

# Checks for panics in the slice-controller log and exits if found.
check_for_panic() {
	# Get the running slice-controller pod
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers --field-selector=status.phase=Running | grep '^slice-controller' | awk '{print $1}' | head -n 1)

	if [ -z "$CONTROLLER_POD" ]; then
		# If no running pod is found, we can't check logs. This is okay.
		return
	fi

	# Check recent logs for "panic". Use -C to get context lines.
	# The command will have a success exit code (0) only if grep finds a match.
	if panic_logs=$(kubectl logs "$CONTROLLER_POD" -n slice-controller-system --tail=100 | grep -i -C 20 "panic"); then
		echo ""
		echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
		echo "    PANIC DETECTED IN SLICE CONTROLLER LOGS!"
		echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
		echo "--- Panic logs from pod: $CONTROLLER_POD ---"
		echo "$panic_logs"
		echo "--------------------------------------------------"
		echo "❌ ERROR: Stopping test script due to controller panic."
		exit 1
	fi
}

# Waits for the slice to reach an expected status (reason for the 'Ready' condition)
wait_for_slice_status() {
	local expected_status="$1"
	local timeout_seconds=${2:-60} # Default timeout of 60 seconds
	echo "Waiting for slice '$SLICE_NAME' to be in '$expected_status' state..."
	local end_time=$((SECONDS + timeout_seconds))

	while [ $SECONDS -lt $end_time ]; do
		# On each check, also look for panics in the controller log.
		check_for_panic
		# Use -A for cluster-scoped resources. The jsonpath needs .items[0] because -A can return a list.
        STATUS=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || echo "NotFound")
		# Case-insensitive comparison by converting both strings to lowercase
		if [[ "${STATUS,,}" == "${expected_status,,}" ]]; then
			echo "-> Slice is '$expected_status'. Verification successful."
			return 0
		fi
		echo "   Current slice status: '$STATUS'. Waiting..."
		sleep 3
	done

	echo "-> ERROR: Timed out waiting for slice status to become '$expected_status'. Final status: '$STATUS'."
	return 1
}

# A wrapper for curl to ensure the script exits if the mock server is unreachable.
curl_wrapper() {
	local http_code
	http_code=$(curl -s -o /dev/null -w "%{http_code}" "$@")
	local exit_code=$?

	if [ $exit_code -ne 0 ]; then
		echo ""
		echo "❌ ERROR: curl command failed with exit code $exit_code. The mock server might be down."
		echo "   Please ensure port-forwarding is active: 'kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080'"
		exit 1
	elif [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
		echo ""
		echo "❌ ERROR: Mock server returned a non-successful HTTP status: $http_code"
		echo "   Command: curl $*"
		exit 1
	fi
	# If successful, we don't print anything to keep the output clean.
}

# Resets the environment between test cases
reset_between_tests() {
	echo ""
	echo "=================================================="
	echo "              RESETTING ENVIRONMENT"
	echo "=================================================="

	# 1. Clear the mock server state completely
	echo "Clearing mock server state..."
	curl_wrapper -X POST http://localhost:8080/clear

	# 1. Delete the slice if it exists and verify its removal
	if kubectl get slice "$SLICE_NAME" -A >/dev/null 2>&1; then
		echo "Deleting existing slice '$SLICE_NAME'..."
		kubectl delete slice "$SLICE_NAME" --wait=false # No namespace needed for delete

		echo "Waiting for slice to be fully deleted..."
		local end_time=$((SECONDS + 30))
		while [ $SECONDS -lt $end_time ]; do
			if ! kubectl get slice "$SLICE_NAME" -A &>/dev/null; then
				echo "-> Slice successfully deleted."
				break
			fi
			sleep 2
		done
		if kubectl get slice "$SLICE_NAME" -A &>/dev/null; then
			echo "-> ERROR: Timed out waiting for slice '$SLICE_NAME' to be deleted."
			exit 1
		fi
	fi

	echo "Environment reset for next test."
	echo "=================================================="
	echo ""
}

# Checks if the nodes are already configured correctly.
# Returns 0 if correctly configured, 1 otherwise.
check_if_nodes_are_configured() {
	echo "Checking if nodes are already configured correctly..."
	for pool_name in "${POOL_NAMES[@]}"; do
		local mig_name="${pool_name}-grp"
		local expected_annotation_value="${BASE_MIG_VAL}${mig_name}"
		local expected_label_value="$pool_name"

		local nodes
		nodes=$(kubectl get nodes -l cloud.google.com/gke-nodepool="$pool_name" -o jsonpath='{.items[*].metadata.name}')

		if [ -z "$nodes" ]; then
			echo "  -> WARNING: No nodes found for pool '$pool_name'. Cannot verify configuration."
			continue
		fi

		for node in $nodes; do
			# Check annotation
			local current_annotation
			current_annotation=$(kubectl get node "$node" -o jsonpath='{.metadata.annotations.node\.gke\.io/created-by-mig}' 2>/dev/null)
			if [[ "$current_annotation" != "$expected_annotation_value" ]]; then
				echo "  -> Node '$node' has incorrect annotation. Expected '$expected_annotation_value', got '$current_annotation'."
				return 1 # Not configured
			fi

			# Check target label
			local current_label
			current_label=$(kubectl get node "$node" -o jsonpath="{.metadata.labels['$TARGET_LABEL']}" 2>/dev/null)
			if [[ "$current_label" != "$expected_label_value" ]]; then
				echo "  -> Node '$node' has incorrect label. Expected '$expected_label_value', got '$current_label'."
				return 1 # Not configured
			fi
		done
	done

	echo "-> All nodes appear to be correctly configured."
	return 0 # Configured
}

# Injects MIGs into the mock server state.
inject_migs_into_mock_server() {
	for pool_name in "${POOL_NAMES[@]}"; do
		local mig_name="${pool_name}-grp"
		echo "  -> Injecting MIG '$mig_name' for slice '$SLICE_NAME'..."
		local payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$mig_name")
		curl_wrapper -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT_BASE/mig"
	done
}
# Performs a one-time setup of the testing environment
one_time_setup() {
	echo ""
	echo "=================================================="
	echo "            PERFORMING ONE-TIME SETUP"
	echo "=================================================="

	# 1. Perform a full reset to clean up any leftover resources from previous runs.
	echo "Performing initial environment reset..."
	reset_between_tests

	# 2. Check if nodes are already set up. If not, clean and re-apply.
	if ! check_if_nodes_are_configured; then
		echo "Node configuration is incorrect or incomplete. Performing full node setup."
		# Run the main cleanup script to ensure a clean slate for nodes
		echo "Running node cleanup script..."
		"$SCRIPT_DIR/cleanup.sh" >/dev/null

		# Initialize node pools, apply labels/annotations
		echo "Setting up node pools: ${POOL_NAMES[*]}"
		for pool_name in "${POOL_NAMES[@]}"; do
			echo "--------------------------------------------------"
			echo "Processing Node Pool: $pool_name"
			local mig_name="${pool_name}-grp"
			local full_mig_annotation="${BASE_MIG_VAL}${mig_name}"
			local nodes
			nodes=$(kubectl get nodes -l cloud.google.com/gke-nodepool="$pool_name" -o jsonpath='{.items[*].metadata.name}')

			if [ -z "$nodes" ]; then
				echo "  -> WARNING: No nodes found for pool '$pool_name'. Skipping..."
				continue
			fi

			for node in $nodes; do
				kubectl annotate node "$node" "node.gke.io/created-by-mig=$full_mig_annotation" --overwrite >/dev/null
				for label in "${TOPOLOGY_LABELS[@]}"; do
					local label_val=""
					if [ "$label" == "$TARGET_LABEL" ]; then
						label_val="$pool_name"
					else
						label_val=$(openssl rand -hex 4)
					fi
					kubectl label node "$node" "$label=$label_val" --overwrite >/dev/null
				done
			done
			echo "  -> Successfully annotated and labeled nodes in pool '$pool_name'"
		done
	else
		echo "Reusing existing node configuration."
	fi

	echo "One-time setup complete."
	echo "=================================================="
	echo ""
}

# --- Main Script ---

echo "Checking prerequisites..."
if ! kubectl get crd slices.accelerator.gke.io >/dev/null 2>&1; then
	echo "Error: The Slice CRD is not installed in the cluster."
	exit 1
fi

if ! curl -s -f http://localhost:8080/clear >/dev/null; then
	echo "Error: Could not connect to the mock GCE server."
	echo "Please ensure port-forwarding is running in a separate terminal:"
	echo "  kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080"
	exit 1
fi
echo "-> Prerequisites met."

one_time_setup

# --- TEST CASE 1: One of two MIGs is in a degraded state ---
echo ""
echo "**************************************************"
echo "  TEST 1: One MIG is in a degraded state"
echo "**************************************************"

reset_between_tests

# Inject the MIGs needed for this test case after resetting the environment.
inject_migs_into_mock_server

echo "Injecting 'INCOMPLETE' state for MMIG..."
PAYLOAD=$(printf '{"sliceName": "%s", "acceleratorTopologyState": "INCOMPLETE", "acceleratorTopology": "4x4x8", "errorMessage": "initializing"}' "$SLICE_NAME")
curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/mmig-health"
echo ""

echo "Applying slice manifest..."
kubectl apply -f "$YAML_PATH"

wait_for_slice_status "INCOMPLETE" 60
echo ""

echo "Injecting 'ACTIVE' state for MMIG..."
PAYLOAD=$(printf '{"sliceName": "%s", "acceleratorTopologyState": "ACTIVE", "acceleratorTopology": "4x4x8", "errorMessage": "healthy"}' "$SLICE_NAME")
curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/mmig-health"
echo ""

wait_for_slice_status "ACTIVE" 60
echo ""

DEGRADED_MIG_NAME="${POOL_NAMES[0]}-grp"
echo "Injecting 'DEGRADED' state for MIG '$DEGRADED_MIG_NAME'..."
PAYLOAD=$(printf '{"sliceName": "%s", "key": "%s", "status": "DEGRADED"}' "$SLICE_NAME" "$DEGRADED_MIG_NAME")
curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/topo-health"
echo ""

wait_for_slice_status "UNKNOWN" 60

# --- TEST CASE 2: MMIG is ACTIVATING and a MIG detach is in progress ---
echo ""
echo "**************************************************"
echo "  TEST 2: MMIG is ACTIVATING, MIG detach is RUNNING"
echo "**************************************************"

reset_between_tests

# Inject the MIGs needed for this test case.
inject_migs_into_mock_server

echo "Injecting 'INCOMPLETE' state for MMIG..."
PAYLOAD=$(printf '{"sliceName": "%s", "acceleratorTopologyState": "INCOMPLETE"}' "$SLICE_NAME")
curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/mmig-health"
echo ""

echo "Applying slice manifest..."
kubectl apply -f "$YAML_PATH"

wait_for_slice_status "INCOMPLETE" 60
echo ""

echo "Injecting 'ACTIVATING' state for MMIG..."
PAYLOAD=$(printf '{"sliceName": "%s", "acceleratorTopologyState": "ACTIVATING"}' "$SLICE_NAME")
curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/mmig-health"
echo ""

wait_for_slice_status "ACTIVATING" 60
echo ""

DETACH_MIG_NAME="${POOL_NAMES[0]}-grp"
echo "Simulating detach of MIG '$DETACH_MIG_NAME' by removing it from MMIG attachments..."
PAYLOAD=$(printf '{"sliceName": "%s", "mmigName": "%s", "migName": "%s"}' "$SLICE_NAME" "$SLICE_NAME" "$DETACH_MIG_NAME")
curl_wrapper -X DELETE -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/mmig-attachment"
echo ""

wait_for_slice_status "ACTIVATING" 60

echo ""
echo "**************************************************"
echo "            ALL DEGRADED TESTS PASSED"
echo "**************************************************"

# Final cleanup
echo ""
read -p "All tests are complete. Do you want to run final cleanup? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
	reset_between_tests
	"$SCRIPT_DIR/cleanup.sh" >/dev/null
	echo "Final cleanup complete."
fi

exit 0