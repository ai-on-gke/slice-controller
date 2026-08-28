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
POOL_NAMES=("np-t2") # Patch both node pools to match happy-path setup

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
ACCELERATOR_TOPOLOGY="4x4x8"

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

# Verifies the presence or absence of a specific condition type in the slice's status.
# Usage: verify_slice_condition <ConditionType> <"present"|"absent"> [ExpectedReason] [ExpectedStatus] [ExpectedMessageSubstring]
verify_slice_condition() {
	local condition_type="$1"
	local expected_presence="$2" # "present" or "absent"
	local expected_reason="$3"
	local expected_status="$4"
	local expected_message_sub="$5"

	echo "Verifying condition: Type='$condition_type', Presence='$expected_presence', Reason='$expected_reason', Status='$expected_status'..."

	# Use jq to parse the conditions reliably
	local conditions_json
	conditions_json=$(kubectl get slice "$SLICE_NAME" -n default -o jsonpath='{.status.conditions}')
	if [ -z "$conditions_json" ]; then
		conditions_json="[]" # Handle case with no conditions
	fi

	local condition_found_json
	condition_found_json=$(echo "$conditions_json" | jq -c ".[] | select(.type == \"$condition_type\")")

	if [ -n "$condition_found_json" ]; then
		# --- Condition was found ---
		if [[ "$expected_presence" == "absent" ]]; then
			echo "-> ERROR: Condition '$condition_type' was found, but it should be absent."
			kubectl describe slice "$SLICE_NAME" -A
			exit 1
		fi

		# If present, check reason, status, and message if provided
		local actual_reason
		actual_reason=$(echo "$condition_found_json" | jq -r '.reason')
		local actual_status
		actual_status=$(echo "$condition_found_json" | jq -r '.status')
		local actual_message
		actual_message=$(echo "$condition_found_json" | jq -r '.message')

		if [[ -n "$expected_reason" && "$actual_reason" != "$expected_reason" ]]; then
			echo "-> ERROR: Condition '$condition_type' has Reason '$actual_reason', but expected '$expected_reason'."
			kubectl describe slice "$SLICE_NAME" -A
			exit 1
		fi
		if [[ -n "$expected_status" && "$actual_status" != "$expected_status" ]]; then
			echo "-> ERROR: Condition '$condition_type' has Status '$actual_status', but expected '$expected_status'."
			kubectl describe slice "$SLICE_NAME" -A
			exit 1
		fi
		if [[ -n "$expected_message_sub" ]] && ! echo "$actual_message" | grep -qF "$expected_message_sub"; then
			echo "-> ERROR: Condition '$condition_type' message does not contain the expected substring."
			echo "   Expected to find: '$expected_message_sub'"
			echo "   Actual message: '$actual_message'"
			kubectl describe slice "$SLICE_NAME" -A
			exit 1
		fi
		echo "-> Verification successful. Condition '$condition_type' is present with the expected properties."
	else
		# --- Condition was not found ---
		if [[ "$expected_presence" == "absent" ]]; then
			echo "-> Verification successful. Condition '$condition_type' is absent as expected."
		else
			echo "-> ERROR: Condition '$condition_type' was NOT found, but it should be present."
			kubectl describe slice "$SLICE_NAME" -A
			exit 1
		fi
	fi
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

	# 1. Delete the slice if it exists and verify its removal
	if kubectl get slice "$SLICE_NAME" -A >/dev/null 2>&1; then
		echo "Deleting existing slice '$SLICE_NAME'..."
		kubectl delete slice "$SLICE_NAME" --wait=false # No namespace needed for delete

		echo "Waiting for slice to be fully deleted..."
		local end_time=$((SECONDS + 30))
		while [ $SECONDS -lt $end_time ]; do
			if ! kubectl get slice "$SLICE_NAME" -A &>/dev/null; then
				echo "-> Slice successfully deleted."
				# 2. Verify no slice resources are present in any namespace
				echo "Verifying no slice resources exist..."
				kubectl get slice -A
				# 3. Clear the mock server state completely
				echo "Clearing mock server state..."
				curl_wrapper -X POST http://localhost:8080/clear
				echo "Environment reset for next test."
				echo "=================================================="
				echo ""
				return 0
			fi
			sleep 2
		done
		echo "-> ERROR: Timed out waiting for slice '$SLICE_NAME' to be deleted."
		exit 1
	fi
	# If the slice didn't exist in the first place, just clear the server.
	echo "Clearing mock server state..."
	curl_wrapper -X POST http://localhost:8080/clear
	echo "Environment reset for next test."
	echo "=================================================="
	echo ""
}

# Injects the initial MIGs that the controller will discover.
inject_initial_migs() {
	echo "Injecting initial MIGs for discovery..."
	for pool_name in "${POOL_NAMES[@]}"; do
		local mig_name="${pool_name}-grp"
		echo "  -> Injecting MIG '$mig_name' for slice '$SLICE_NAME'..."
		local payload
		payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$mig_name")
		curl_wrapper -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT_BASE/mig"
	done
	echo ""
}

# Injects a health status for the MMIG.
inject_mmig_health_status() {
	local status="$1"
	local message="$2"
	echo "Injecting MMIG health status: '$status'..."
	local payload
	payload=$(printf '{"sliceName": "%s", "acceleratorTopologyState": "%s", "acceleratorTopology": "%s", "errorMessage": "%s"}' \
		"$SLICE_NAME" "$status" "$ACCELERATOR_TOPOLOGY" "$message")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$payload" "$API_ENDPOINT_BASE/mmig-health"
}

# Performs a one-time setup of the testing environment
one_time_setup() {
	echo ""
	echo "=================================================="
	echo "            PERFORMING ONE-TIME SETUP"
	echo "=================================================="

	# 1. Perform a full reset to clean up any leftover resources from previous runs.
	# This function handles slice deletion, waiting, and mock server clearing.
	echo "Performing initial environment reset..."
	reset_between_tests

	# 2. Run the main cleanup script to ensure a clean slate for nodes
	echo "Running node cleanup script..."
	"$SCRIPT_DIR/cleanup.sh" >/dev/null

	# 3. Initialize node pools, apply labels/annotations, and inject MIGs
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
			# Apply MIG annotation
			kubectl annotate node "$node" "node.gke.io/created-by-mig=$full_mig_annotation" --overwrite >/dev/null

			# Apply topology labels
			for label in "${TOPOLOGY_LABELS[@]}"; do
				local label_val=""
				if [ "$label" == "$TARGET_LABEL" ]; then
					# 1:1 Mapping: Set value to the unique POOL_NAME
					label_val="$pool_name"
				else
					# Random generation for all other labels
					label_val=$(openssl rand -hex 4)
				fi
				kubectl label node "$node" "$label=$label_val" --overwrite >/dev/null
			done
		done
		echo "  -> Successfully annotated and labeled nodes in pool '$pool_name'"
	done

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

# Ask user if they want to run all tests
RUN_ALL_TESTS=false
read -p "Do you want to run all tests without individual prompts? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
	RUN_ALL_TESTS=true
	echo "-> Running all tests..."
else
	echo "-> Will prompt before each test..."
fi

# Perform the initial, one-time setup of nodes
one_time_setup

# --- TEST CASE 1: CreateWorkloadPolicy returns an HTTP error ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 1 (CreateWorkloadPolicy returns HTTP 500)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "  TEST 1: CreateWorkloadPolicy returns HTTP 500"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	SPECIAL_ERROR_MSG="INJECTED_500_ERROR_FOR_TEST_$(openssl rand -hex 2)"
	echo "Injecting HTTP 500 error for CreateWorkloadPolicy..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "CreateWorkloadPolicy", "code": 500, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting a few seconds for the controller to process and log the error..."
	sleep 5

	echo "Fetching logs from slice-controller containing the error message..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "$SPECIAL_ERROR_MSG"
else
	echo "Skipping TEST 1."
fi

# --- TEST CASE 2: CreateWorkloadPolicy operation times out ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 2 (CreateWorkloadPolicy times out)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "      TEST 2: CreateWorkloadPolicy times out"
	echo "**************************************************"
	reset_between_tests

	inject_initial_migs

	SPECIAL_ERROR_MSG="INJECTED_TIMEOUT_FOR_TEST_$(openssl rand -hex 2)"
	echo "Injecting HTTP 500 error (simulating timeout) for CreateWorkloadPolicy..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "CreateWorkloadPolicy", "code": 500, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting a few seconds for the controller to process and log the error..."
	sleep 5

	echo "Fetching logs from slice-controller containing the error message..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "$SPECIAL_ERROR_MSG"
else
	echo "Skipping TEST 2."
fi

# --- TEST CASE 3: CreateWorkloadPolicy operation status stays RUNNING ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 3 (CreateWorkloadPolicy op stays RUNNING)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "  TEST 3: CreateWorkloadPolicy op stays RUNNING"
	echo "**************************************************"
	reset_between_tests

	inject_initial_migs

	# The operation name is constructed as 'op-createpolicy-<sliceName>-<clusterHash>'
	# The mock server allows matching on the prefix 'op-createpolicy-<sliceName>'
	OP_KEY="op-createpolicy-${SLICE_NAME}"
	echo "Injecting 'RUNNING' status for CreateWorkloadPolicy operation (key: $OP_KEY)..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME" "$OP_KEY")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/op-status"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	# With a stuck operation, the controller will retry and eventually time out.
	# The final status should become 'Unknown' after the timeout.
	# We will check the logs for the timeout message.
	echo "Waiting for controller to time out on the 'RUNNING' operation..."
	wait_for_slice_status "SliceCreationFailed" 120 # Wait up to 2 minutes

	echo "Fetching logs from slice-controller containing the timeout error..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep -E "timed out waiting for operation|context deadline exceeded" || echo "-> No timeout logs found."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "timed out waiting for operation"
else
	echo "Skipping TEST 3."
fi

# --- TEST CASE 4: CreateWorkloadPolicy returns an HTTP 400 error ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 4 (CreateWorkloadPolicy returns HTTP 400)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "  TEST 4: CreateWorkloadPolicy returns HTTP 400"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	SPECIAL_ERROR_MSG="INJECTED_400_BAD_REQUEST_FOR_TEST_$(openssl rand -hex 2)"
	echo "Injecting HTTP 400 error for CreateWorkloadPolicy..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "CreateWorkloadPolicy", "code": 400, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting a few seconds for the controller to process and log the error..."
	sleep 5

	echo "Fetching logs from slice-controller containing the error message..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "$SPECIAL_ERROR_MSG"
else
	echo "Skipping TEST 4."
fi

# --- TEST CASE 5: CreateWorkloadPolicy returns 409 Conflict (already exists) ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 5 (CreateWorkloadPolicy returns HTTP 409)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "  TEST 5: CreateWorkloadPolicy returns HTTP 409"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	# The controller will generate a policy name like 'test-slice-<hash>'.
	# The mock server's 409 logic is triggered by the existence of a policy
	# with the exact name. We will pre-create it.
	# The hash is deterministic based on cluster name, so we can hardcode it for the test.
	POLICY_NAME="test-slice"
	echo "Injecting pre-existing workload policy '$POLICY_NAME' to trigger a 409 Conflict..."
	PAYLOAD=$(printf '{"sliceName": "%s", "policyName": "%s"}' "$SLICE_NAME" "$POLICY_NAME")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/policy"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	# The controller should handle 409 gracefully and proceed.
	wait_for_slice_status "ACTIVE" 120
	verify_slice_condition "Ready" "present" "ACTIVE" "True"

	echo "Injecting INCOMPLETE status before deleting the slice..."
	inject_mmig_health_status "INCOMPLETE" "injected incomplete before deletion"
	echo "Deleting the slice..."
	kubectl delete -f "$YAML_PATH"
else
	echo "Skipping TEST 5."
fi

# --- TEST CASE 6: CreateMMIG returns an HTTP error ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 6 (CreateMMIG returns HTTP 500)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "        TEST 6: CreateMMIG returns HTTP 500"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	SPECIAL_ERROR_MSG="INJECTED_500_ERROR_FOR_MMIG_$(openssl rand -hex 2)"
	echo "Injecting HTTP 500 error for CreateMMIG..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "CreateMMIG", "code": 500, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting a few seconds for the controller to process and log the error..."
	sleep 5

	echo "Fetching logs from slice-controller containing the error message..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "$SPECIAL_ERROR_MSG"
else
	echo "Skipping TEST 6."
fi

# --- TEST CASE 7: CreateMMIG operation times out (simulated with 500) ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 7 (CreateMMIG times out)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "           TEST 7: CreateMMIG times out"
	echo "**************************************************"
	reset_between_tests

	inject_initial_migs

	SPECIAL_ERROR_MSG="INJECTED_TIMEOUT_FOR_MMIG_$(openssl rand -hex 2)"
	echo "Injecting HTTP 500 error (simulating timeout) for CreateMMIG..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "CreateMMIG", "code": 500, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting a few seconds for the controller to process and log the error..."
	sleep 5

	echo "Fetching logs from slice-controller containing the error message..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "$SPECIAL_ERROR_MSG"
else
	echo "Skipping TEST 7."
fi

# --- TEST CASE 8: CreateMMIG operation status stays RUNNING ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 8 (CreateMMIG op stays RUNNING)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "     TEST 8: CreateMMIG op stays RUNNING"
	echo "**************************************************"
	reset_between_tests

	inject_initial_migs

	OP_KEY="op-createmmig-${SLICE_NAME}"
	echo "Injecting 'RUNNING' status for CreateMMIG operation (key: $OP_KEY)..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME" "$OP_KEY")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/op-status"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting for controller to time out on the 'RUNNING' operation..."
	wait_for_slice_status "SliceCreationFailed" 120 # Wait up to 2 minutes

	echo "Fetching logs from slice-controller containing the timeout error..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep -E "timed out waiting for operation|context deadline exceeded" || echo "-> No timeout logs found."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "timed out waiting for operation"
else
	echo "Skipping TEST 8."
fi

# --- TEST CASE 9: CreateMMIG returns an HTTP 400 error ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 9 (CreateMMIG returns HTTP 400)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "        TEST 9: CreateMMIG returns HTTP 400"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	SPECIAL_ERROR_MSG="INJECTED_400_BAD_REQUEST_FOR_MMIG_$(openssl rand -hex 2)"
	echo "Injecting HTTP 400 error for CreateMMIG..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "CreateMMIG", "code": 400, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting a few seconds for the controller to process and log the error..."
	sleep 5

	echo "Fetching logs from slice-controller containing the error message..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "$SPECIAL_ERROR_MSG"
else
	echo "Skipping TEST 9."
fi

# --- TEST CASE 10: CreateMMIG returns 409 Conflict (already exists) ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 10 (CreateMMIG returns HTTP 409)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "      TEST 10: CreateMMIG returns HTTP 409"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	MMIG_NAME="test-slice"
	echo "Injecting pre-existing MMIG '$MMIG_NAME' to trigger a 409 Conflict..."
	PAYLOAD=$(printf '{"sliceName": "%s", "mmigName": "%s"}' "$SLICE_NAME" "$MMIG_NAME")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/mmig"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	# The controller should handle 409 gracefully and proceed.
	wait_for_slice_status "ACTIVE" 120
	verify_slice_condition "Ready" "present" "ACTIVE" "True"

	echo "Injecting INCOMPLETE status before deleting the slice..."
	inject_mmig_health_status "INCOMPLETE" "injected incomplete before deletion"
	echo "Deleting the slice..."
	kubectl delete -f "$YAML_PATH"
else
	echo "Skipping TEST 10."
fi

# --- TEST CASE 11: AttachMIGToMMIG returns an HTTP 500 error ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 11 (AttachMIGToMMIG returns HTTP 500)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "    TEST 11: AttachMIGToMMIG returns HTTP 500"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	SPECIAL_ERROR_MSG="INJECTED_500_ERROR_FOR_ATTACH_MIG_$(openssl rand -hex 2)"
	echo "Injecting HTTP 500 error for AttachMIGToMMIG..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "AttachMIGToMMIG", "code": 500, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting a few seconds for the controller to process and log the error..."
	sleep 5

	echo "Fetching logs from slice-controller containing the error message..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	wait_for_slice_status "SliceCreationFailed"
	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "$SPECIAL_ERROR_MSG"
else
	echo "Skipping TEST 11."
fi

# --- TEST CASE 12: AttachMIGToMMIG operation status stays RUNNING ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 12 (AttachMIGToMMIG op stays RUNNING)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "   TEST 12: AttachMIGToMMIG op stays RUNNING"
	echo "**************************************************"
	reset_between_tests

	inject_initial_migs

	# The operation key is based on the MIG name. We assume the first pool name.
	MIG_NAME="${POOL_NAMES[0]}-grp"
	OP_KEY="op-attachmigs-${MIG_NAME}"
	echo "Injecting 'RUNNING' status for AttachMIGToMMIG operation (key: $OP_KEY)..."
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "%s", "status": "RUNNING"}' "$SLICE_NAME" "$OP_KEY")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/op-status"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"

	echo "Waiting for controller to time out on the 'RUNNING' operation..."
	wait_for_slice_status "SliceCreationFailed" 120 # Wait up to 2 minutes

	echo "Fetching logs from slice-controller containing the timeout error..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system | grep -E "timed out waiting for operation|context deadline exceeded" || echo "-> No timeout logs found."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	verify_slice_condition "Ready" "present" "SliceCreationFailed" "False" "timed out waiting for operation"
else
	echo "Skipping TEST 12."
fi

# --- TEST CASE 13: GetMMIG returns an HTTP 500 error after slice is ACTIVE ---
echo ""
if [[ "$RUN_ALL_TESTS" != "true" ]]; then
	read -p "Run TEST 13 (GetMMIG returns HTTP 500 post-creation)? (y/n) " -n 1 -r
	echo ""
fi
if [[ "$RUN_ALL_TESTS" == "true" || $REPLY =~ ^[Yy]$ ]]; then
	echo "**************************************************"
	echo "  TEST 13: GetMMIG returns HTTP 500 post-creation"
	echo "**************************************************"

	reset_between_tests

	inject_initial_migs

	echo "Starting slice creation process..."
	inject_mmig_health_status "INCOMPLETE" "Simulating initial creation state"

	echo "Applying slice manifest..."
	kubectl apply -f "$YAML_PATH"
	wait_for_slice_status "INCOMPLETE"

	inject_mmig_health_status "ACTIVE" "Simulating slice is now healthy"
	wait_for_slice_status "ACTIVE"

	echo "Slice is ACTIVE. Now injecting failure into GetMMIG health probe..."
	SPECIAL_ERROR_MSG="INJECTED_500_FOR_GET_MMIG_$(openssl rand -hex 2)"
	PAYLOAD=$(printf '{"sliceName": "%s", "key": "GetMMIG", "code": 500, "message": "%s"}' "$SLICE_NAME" "$SPECIAL_ERROR_MSG")
	curl_wrapper -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$API_ENDPOINT_BASE/http-error"

	echo "Waiting 15 seconds for the controller's health probe to fail..."
	sleep 15

	echo "Fetching logs from slice-controller containing the health probe error..."
	CONTROLLER_POD=$(kubectl get pods -n slice-controller-system --no-headers | grep '^slice-controller' | awk '{print $1}' | head -n 1)
	if [ -n "$CONTROLLER_POD" ]; then
		echo "--- Logs from pod: $CONTROLLER_POD ---"
		# Check logs since the failure was injected
		kubectl logs "$CONTROLLER_POD" -n slice-controller-system --since=20s | grep "$SPECIAL_ERROR_MSG" || echo "-> No logs found with the special error message."
		echo "------------------------------------"
	else
		echo "-> WARNING: Could not find the slice-controller pod to fetch logs."
	fi

	echo "Verifying slice status has transitioned to a failed health probe state..."
	sleep 10
	# The Ready condition should now be False with the reason MMIGHealthProbeFailed
	verify_slice_condition "Ready" "present" "MMIGHealthProbeFailed" "False" "$SPECIAL_ERROR_MSG"

	echo "Injecting INCOMPLETE status before deleting the slice..."
	inject_mmig_health_status "INCOMPLETE" "injected incomplete before deletion"
	echo "Deleting the slice..."
	kubectl delete -f "$YAML_PATH"
else
	echo "Skipping TEST 13."
fi

echo ""
echo "**************************************************"
echo "            ALL FAILURE TESTS PASSED"
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
