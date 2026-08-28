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
echo "Slice's yaml deleted"

# Clean previous deployment
make undeploy
echo "Undeployment completed"

# Failed the process if error
set -e

# Generate image hash
output=$(make container push)
echo "Image generated"

# Fetch the latest image hash
hash=$(echo "$output" | grep -o 'sha256:[a-f0-9]*' | tail -n 1 | cut -d':' -f2)

# Local deployment
make deploy IMG=${REGISTRY}/slice-controller@sha256:$hash
echo "Deployment completed"