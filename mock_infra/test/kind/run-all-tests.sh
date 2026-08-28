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


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../../.."

export KUBECONFIG="$PROJECT_ROOT/.kubeconfig-kind-slice-testing"
if [ ! -f "$KUBECONFIG" ]; then
    touch "$KUBECONFIG"
fi

# Safety check to prevent running against prod clusters
CURRENT_CONTEXT=$(kubectl config current-context 2>/dev/null || echo "unknown")
if [[ "$CURRENT_CONTEXT" != *"kind-"* ]] && [[ "$CURRENT_CONTEXT" != "kind" ]] && [[ "$CURRENT_CONTEXT" != "unknown" ]]; then
    echo "ERROR: Current kubectl context is '$CURRENT_CONTEXT'. This script must only be run against a kind cluster."
    exit 1
fi
set -eo pipefail

export KIND=${KIND:-kind}

echo "Deploying Kind cluster and mock GCE environment..."
"$PROJECT_ROOT/test/local_deployment_kind_mock_infra.sh"

echo ""
echo "Waiting for mock gce server and port-forward to be ready..."
kubectl rollout status deployment/mock-gce -n slice-controller-system --timeout=120s

# Re-establish port-forward here to ensure it's reliable
PF_PID=$(pgrep -f "port-forward -n slice-controller-system service/mock-gce-service 8080:8080" || echo "")
if [ -n "$PF_PID" ]; then
    kill $PF_PID || true
fi

kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080 > /dev/null 2>&1 &
PF_PID=$!
sleep 5 # give the port-forward a moment to establish

# Register cleanup for the port forward process
cleanup() {
    echo ""
    echo "Cleaning up Mock GCE port-forward..."
    if [ -n "$PF_PID" ]; then
        kill $PF_PID || true
        echo "Port-forward stopped."
    fi
}
trap cleanup EXIT

echo ""
echo "=================================================="
echo "    Running test-kind-update.sh"
echo "=================================================="
"$SCRIPT_DIR/test-kind-update.sh"



echo ""
echo "=================================================="
echo "    Running test-kind-happy-path.sh"
echo "=================================================="
"$SCRIPT_DIR/test-kind-happy-path.sh"

echo ""
echo "=================================================="
echo "    Running test-kind-failure.sh"
echo "=================================================="
"$SCRIPT_DIR/test-kind-failure.sh"

echo ""
echo "=================================================="
echo "    Running test-kind-subslice-deletion.sh"
echo "=================================================="
"$SCRIPT_DIR/test-kind-subslice-deletion.sh"

echo ""
echo "✅ All Mock GCE Kind tests passed successfully!"
