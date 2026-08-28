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

# Commands
# Port Forward
kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080

# List all
curl http://localhost:8080/list

# List qps
curl http://localhost:8080/qps

# List slice
export SLICE_NAME=test-slice
curl http://localhost:8080/list?sliceName=${SLICE_NAME}

# Clear a slice injections
export SLICE_NAME=test-slice
payload=$(printf '{"sliceName": "%s"}' "$SLICE_NAME")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/clear

# Clear a slice injections and resources
export SLICE_NAME=test-slice
payload=$(printf '{"sliceName": "%s"}' "$SLICE_NAME")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/clear

# Clear everything
curl -X POST http://localhost:8080/clear

# Inject init slice resource
export SLICE_NAME=test-slice
payload=$(printf '{"sliceName": "%s"}' "$SLICE_NAME")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/slice

# Inject init mig resource
export SLICE_NAME=test-slice
export MIG_NAME=np-t1-grp
payload=$(printf '{"sliceName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MIG_NAME")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mig

# Inject init policy resource
export SLICE_NAME=test-slice
export POLICY_NAME=test-slice
payload=$(printf '{"sliceName": "%s", "policyName": "%s"}' "$SLICE_NAME" "$POLICY_NAME")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/policy

# Delete policy resource
export SLICE_NAME=test-slice
export POLICY_NAME=test-slice
payload=$(printf '{"sliceName": "%s", "policyName": "%s"}' "$SLICE_NAME" "$POLICY_NAME")
curl -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/policy

# Inject init mmig resource
export SLICE_NAME=test-slice
export MMIG_NAME=test-slice
payload=$(printf '{"sliceName": "%s", "mmigName": "%s"}' "$SLICE_NAME" "$MMIG_NAME")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mmig

# Get AvailableAcceleratorTopologies
export MIG_NAME=np-t1-grp1
curl http://localhost:8080/projects/test-project/zones/us-central1-a/instanceGroupManagers/$MIG_NAME/getAvailableAcceleratorTopologies

# Get MMIG
export MMIG_NAME=test-slice
curl http://localhost:8080/projects/test-project/regions/us-central1/multiMigs/$MMIG_NAME


# Inject slice MMIG status with topology and error message
export SLICE_NAME=test-slice
export MMIG_STATUS=INCOMPLETE
export ACCELERATOR_TOPOLOGY="4x4x8"
export ERROR_MESSAGE="Injected failure from test command"
payload=$(printf '{"sliceName": "%s", "acceleratorTopologyState": "%s", "acceleratorTopology": "%s", "errorMessage": "%s"}' "$SLICE_NAME" "$MMIG_STATUS" "$ACCELERATOR_TOPOLOGY" "$ERROR_MESSAGE")
curl -X POST -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/mmig-health"

# Clear slice MMIG status
payload=$(printf '{"sliceName": "%s"}' "$SLICE_NAME")
curl -X DELETE -H "Content-Type: application/json" -d "$payload" "http://localhost:8080/inject/mmig-health"


# Inject status for create policy operation
export SLICE_NAME=test-slice
export OP_NAME=op-createpolicy-test-slice
export OP_STATUS=RUNNING
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "%s"}' "$SLICE_NAME" "$OP_NAME" "$OP_STATUS")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status

# Clear status for operation
export SLICE_NAME=test-slice
export OP_NAME=op-createpolicy-test-slice
payload=$(printf '{"sliceName": "%s", "key": "%s"}' "$SLICE_NAME" "$OP_NAME")
curl -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status

# Inject status for attach mig operation
export SLICE_NAME=test-slice
export MIG_NAME=default-pool-grp
export OP_NAME=op-attachmigs-$MIG_NAME
# export OP_NAME=op-detachmigs-$MIG_NAME
export OP_STATUS=RUNNING
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "%s"}' "$SLICE_NAME" "$OP_NAME" "$OP_STATUS")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/op-status

# Inject http error
export SLICE_NAME=test-slice
export STEP_NAME=CreateWorkloadPolicy
# Other step names:
# CreateMMIG
# GetMMIG
# GetInstanceGroupManager
# AttachMIGToMMIG
# DetachMIGFromMMIG
# GetAvailableTopologies
# DeleteMMIG
# DeleteWorkloadPolicy
export ERROR_CODE=500
export ERROR_MESSAGE="Injected Internal Error"
payload=$(printf '{"sliceName": "%s", "key": "%s", "code": %d, "message": "%s"}' "$SLICE_NAME" "$STEP_NAME" "$ERROR_CODE" "$ERROR_MESSAGE")

curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/http-error

# Clear http error
export SLICE_NAME=test-slice
export STEP_NAME=CreateWorkloadPolicy
payload=$(printf '{"sliceName": "%s", "key": "%s"}' "$SLICE_NAME" "$STEP_NAME")
curl -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/http-error

# Inject timeout
export SLICE_NAME=test-slice
export STEP_NAME=CreateWorkloadPolicy
# Other step names:
# CreateMMIG
# GetMMIG
# GetInstanceGroupManager
# AttachMIGToMMIG
# DetachMIGFromMMIG
# GetAvailableTopologies
# DeleteMMIG
# DeleteWorkloadPolicy
export DURATION=5 # in seconds
payload=$(printf '{"sliceName": "%s", "key": "%s", "duration": %d}' "$SLICE_NAME" "$STEP_NAME" "$DURATION")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/timeout

# Clear timeout
export SLICE_NAME=test-slice
export STEP_NAME=CreateWorkloadPolicy
payload=$(printf '{"sliceName": "%s", "key": "%s"}' "$SLICE_NAME" "$STEP_NAME")
curl -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/timeout

# Inject Topology Health
export SLICE_NAME=test-slice
export MIG_NAME=default-pool-grp
export STATUS=DEGRADED # Can be HEALTHY, DEGRADED, or UNHEALTHY
payload=$(printf '{"sliceName": "%s", "key": "%s", "status": "%s"}' "$SLICE_NAME" "$MIG_NAME" "$STATUS")
curl -X POST -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/topo-health

# Clear Injected Topology Health
payload=$(printf '{"sliceName": "%s", "key": "%s"}' "$SLICE_NAME" "$MIG_NAME")
curl -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/topo-health

# Remove MIG attachment from MMIG
export SLICE_NAME=test-slice
export MMIG_NAME=test-slice
export MIG_NAME=np-t1-grp
payload=$(printf '{"sliceName": "%s", "mmigName": "%s", "migName": "%s"}' "$SLICE_NAME" "$MMIG_NAME" "$MIG_NAME")
curl -X DELETE -H "Content-Type: application/json" -d "$payload" http://localhost:8080/inject/mmig-attachment
