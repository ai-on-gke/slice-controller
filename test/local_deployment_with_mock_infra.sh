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


# Check if cert-manager is installed and running
if ! kubectl get deployment -n cert-manager cert-manager > /dev/null 2>&1 || ! kubectl rollout status deployment -n cert-manager cert-manager --timeout=180s > /dev/null 2>&1; then
  echo "cert-manager not found or not ready, installing..."
  kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.5/cert-manager.yaml
  # Wait for all deployments in cert-manager namespace to be ready
  kubectl rollout status deployment -n cert-manager cert-manager --timeout=180s
  kubectl rollout status deployment -n cert-manager cert-manager-webhook --timeout=180s
  kubectl rollout status deployment -n cert-manager cert-manager-cainjector --timeout=180s
  echo "cert-manager installation complete."
else
  echo "cert-manager is already installed and running."
fi

# Replace the registry for local depolyment
export REGISTRY=${REGISTRY:-"us-central1-docker.pkg.dev/your-project/slice-controller-test"}

# Clean any existing slices, otherwise the finalizer will pause the undeploy
kubectl delete -f ./test/yaml
kubectl delete -f ./mock_infra/test/yaml
echo "Slice's yaml deleted"

# Clean previous deployment
make undeploy
echo "Undeployment completed"

# Failed the process if error
set -e

# --- Build and Push Controller Image ---
echo "Building and pushing slice-controller image..."
controller_output=$(make container push)
echo "Slice-controller image generated"
# Fetch the latest image hash for the controller
controller_hash=$(echo "$controller_output" | grep -o 'sha256:[a-f0-9]*' | tail -n 1 | cut -d':' -f2)
CONTROLLER_IMG="${REGISTRY}/slice-controller@sha256:${controller_hash}"

# --- Deploy Slice Controller ---
echo "Deploying Slice Controller..."
make deploy IMG=${CONTROLLER_IMG}

# Configure the controller to use the mock GCE service endpoint
MOCKING_GCE_API_ENDPOINT="http://mock-gce-service:8080"
kubectl set env deployment/slice-controller-controller-manager -n slice-controller-system MOCKING_GCE_API_ENDPOINT=${MOCKING_GCE_API_ENDPOINT}

# --- Build and Push Mock GCE Image ---
echo "Building and pushing mock-gce image..."
mock_infra_output=$(make mock-infra-docker-build mock-infra-docker-push MOCK_INFRA_IMG=${REGISTRY}/mock-infra-server:latest)
echo "Mock GCE image generated"
mock_infra_hash=$(echo "$mock_infra_output" | grep -o 'sha256:[a-f0-9]*' | tail -n 1 | cut -d':' -f2)
MOCK_INFRA_IMG="${REGISTRY}/mock-infra-server@sha256:${mock_infra_hash}"

# --- Deploy Mock GCE Server ---
echo "Deploying Mock GCE server..."
# We deploy the mock server first, then set the environment variable on the controller.
# The mock server itself does not need the MOCKING_GCE_API_ENDPOINT variable.
make deploy-mock-gce MOCK_INFRA_IMG=${MOCK_INFRA_IMG} MOCK_GCE_PORT=8080

echo "Waiting for mock-gce pod to be available..."
kubectl wait --for=condition=ready pod -l app=mock-gce -n slice-controller-system --timeout=120s

echo "All deployments completed and configured."

echo ""
echo "Making test scripts executable..."
chmod +x ./mock_infra/test/script/*.sh
echo "-> Scripts in ./mock_infra/test/script/ are now executable."

echo ""
echo "Starting port-forward to the mock GCE service in the background..."

# Start port-forwarding in the background
kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080 > /dev/null 2>&1 &

# Get the process ID of the background job
PORT_FORWARD_PID=$!

echo "Port-forward is running with PID: ${PORT_FORWARD_PID}"
echo "You can now interact with the mock GCE API at http://localhost:8080"
echo "To stop it, run: kill ${PORT_FORWARD_PID}"