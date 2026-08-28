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


# --- Configuration ---
export KIND_CLUSTER_NAME="slice-testing"
export KIND_CONFIG_PATH="./mock_infra/test/kind/kind-nodes.yaml"
NAMESPACE="slice-controller-system"

NODE_POOL_COUNT=${NODE_POOL_COUNT:-4}
KIND_POOL_NAMES=()
for ((i=1; i<=NODE_POOL_COUNT; i++)); do
    KIND_POOL_NAMES+=("np-t$i")
done
BASE_MIG_VAL="projects/test-project/zones/test-zone/instanceGroupManagers/"
TARGET_LABEL="cloud.google.com/gke-tpu-partition-4x4x4-id"
PROVISIONING_MODE_LABEL="cloud.google.com/gke-accelerator-topology-mode=PROVISION_ONLY"
TPU_ACCELERATOR_LABEL="cloud.google.com/gke-tpu-accelerator=tpu7x"
RES_NAME_LABEL="cloud.google.com/reservation-name=mock-reservation"
RES_BLOCK_LABEL="cloud.google.com/gce-topology-block=mock-block-id"
TOPOLOGY_LABELS=(
    "cloud.google.com/gke-tpu-partition-2x2x1-id"
    "cloud.google.com/gke-tpu-partition-2x2x2-id"
    "cloud.google.com/gke-tpu-partition-2x2x4-id"
    "cloud.google.com/gke-tpu-partition-2x4x4-id"
    "$TARGET_LABEL"
)


# Define local-only image names and tags for kind
export CONTROLLER_IMG="slice-controller:kind-test"
export MOCK_INFRA_IMG="mock-infra-server:kind-test"

# --- Helper Functions ---

# Auto-detect Docker API version mismatch (e.g., in Louhi CI) and downgrade if necessary
SERVER_API_VERSION=$(docker version -f '{{.Server.APIVersion}}' 2>/dev/null || true)

if [ -z "$SERVER_API_VERSION" ]; then
    # If the above failed, it might be due to version mismatch.
    # Capture stderr and look for "Maximum supported API version is X.XX"
    DOCKER_ERR=$(docker version 2>&1 || true)
    if [[ "$DOCKER_ERR" =~ Maximum[[:space:]]supported[[:space:]]API[[:space:]]version[[:space:]]is[[:space:]]([0-9\.]+) ]]; then
        SERVER_API_VERSION="${BASH_REMATCH[1]}"
    fi
fi

if [ -n "$SERVER_API_VERSION" ]; then
    export DOCKER_API_VERSION="$SERVER_API_VERSION"
    echo "-> Set Docker API version to $SERVER_API_VERSION to match daemon."
fi


# Check if kind is installed
if ! command -v ${KIND:-kind} &> /dev/null; then
    echo "Error: '${KIND:-kind}' command not found. Please install kind: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
    exit 1
fi

# --- Cluster Setup ---

echo "Checking for kind cluster '$KIND_CLUSTER_NAME'..."
if ! ${KIND:-kind} get clusters | grep -q "^${KIND_CLUSTER_NAME}$"; then
    echo "-> Cluster not found. Creating new kind cluster with config: $KIND_CONFIG_PATH"
    ${KIND:-kind} create cluster --name "$KIND_CLUSTER_NAME" --config "$KIND_CONFIG_PATH" --wait 20m
    echo "-> Cluster '$KIND_CLUSTER_NAME' created."
else
    echo "-> Cluster '$KIND_CLUSTER_NAME' already exists. Ensuring kubeconfig is populated."
    if [ -n "$KUBECONFIG" ]; then
        ${KIND:-kind} export kubeconfig --name "$KIND_CLUSTER_NAME" --kubeconfig "$KUBECONFIG"
    else
        ${KIND:-kind} export kubeconfig --name "$KIND_CLUSTER_NAME"
    fi
    kubectl config use-context "kind-${KIND_CLUSTER_NAME}"
fi

echo "Wait for API Server to fully boot and resolve routing..."
MAX_API_RETRIES=15
for i in $(seq 1 $MAX_API_RETRIES); do
    SERVER_URL=$(kubectl config view -o jsonpath="{.clusters[?(@.name=='kind-${KIND_CLUSTER_NAME}')].cluster.server}" 2>/dev/null || true)
    if [[ "$SERVER_URL" =~ :([0-9]+)/*$ ]]; then
        PORT="${BASH_REMATCH[1]}"

        check_api() {
            if [ -z "$1" ] || [ "$1" = ":" ] || [ "$1" = "0.0.0.0:$PORT" ]; then return 1; fi
            kubectl --server="https://$1" --insecure-skip-tls-verify=true --request-timeout=2s get --raw /healthz >/dev/null 2>&1
        }

        GATEWAY_IP=$(command -v ip >/dev/null 2>&1 && ip -4 route show default | awk '{print $3}' | head -n1 || echo "")
        KIND_NET_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${KIND_CLUSTER_NAME}-control-plane 2>/dev/null || echo "")
        DOCKER_IP=$( [ -n "${DOCKER_HOST:-}" ] && echo "$DOCKER_HOST" | sed -E 's|.*://([^:]+).*|\1|' || echo "" )
        BRIDGE_IP=$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || echo "")
        HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "")

        echo "Attempt $i: Testing paths - Local: 127.0.0.1:$PORT | Gateway: $GATEWAY_IP:$PORT | Internal: $KIND_NET_IP:6443 | DockerHost: $DOCKER_IP:$PORT | Bridge: $BRIDGE_IP:$PORT | HostIP: $HOST_IP:$PORT"

        if check_api "127.0.0.1:$PORT"; then
            echo "-> API server reachable at 127.0.0.1:$PORT"
            kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://127.0.0.1:$PORT" --insecure-skip-tls-verify=true
            break
        elif check_api "$DOCKER_IP:$PORT"; then
            echo "-> API server reachable at DOCKER_HOST $DOCKER_IP:$PORT"
            kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://$DOCKER_IP:$PORT" --insecure-skip-tls-verify=true
            break
        elif check_api "$GATEWAY_IP:$PORT"; then
            echo "-> API server reachable at Gateway $GATEWAY_IP:$PORT"
            kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://$GATEWAY_IP:$PORT" --insecure-skip-tls-verify=true
            break
        elif check_api "$BRIDGE_IP:$PORT"; then
            echo "-> API server reachable at Bridge $BRIDGE_IP:$PORT"
            kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://$BRIDGE_IP:$PORT" --insecure-skip-tls-verify=true
            break
        elif check_api "$HOST_IP:$PORT"; then
            echo "-> API server reachable at Host IP $HOST_IP:$PORT"
            kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://$HOST_IP:$PORT" --insecure-skip-tls-verify=true
            break
        elif check_api "$KIND_NET_IP:6443"; then
             echo "-> API server reachable at Kind Internal IP $KIND_NET_IP:6443"
             kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://$KIND_NET_IP:6443" --insecure-skip-tls-verify=true
             break
        elif check_api "host.docker.internal:$PORT"; then
             echo "-> API server reachable at host.docker.internal:$PORT"
             kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://host.docker.internal:$PORT" --insecure-skip-tls-verify=true
             break
        else
            # Deep isolation detected. Try forcing local container into kind docker network.
            if [ -n "$KIND_NET_IP" ]; then
                MY_CONTAINER_ID=$(cat /proc/self/cgroup 2>/dev/null | grep -oE '[0-9a-f]{64}' | head -n1 || echo "")
                if [ -n "$MY_CONTAINER_ID" ]; then
                    docker network connect kind "$MY_CONTAINER_ID" 2>/dev/null || true
                    if check_api "$KIND_NET_IP:6443"; then
                        echo "-> API server reachable at Kind Internal IP $KIND_NET_IP:6443 after attaching network!"
                        kubectl config set-cluster "kind-${KIND_CLUSTER_NAME}" --server="https://$KIND_NET_IP:6443" --insecure-skip-tls-verify=true
                        break
                    fi
                fi
            fi
        fi
    else
        echo "Attempt $i: Could not parse PORT from SERVER_URL='$SERVER_URL'."
    fi

    if [ "$i" -eq "$MAX_API_RETRIES" ]; then
        echo "Error: API Server did not become reachable after trying all paths."
        # Debugging diagnostic dump
        echo "--- Network Debug Info ---"
        echo "docker ps:"
        docker ps || true
        echo "curl local:"
        curl -v -k -m 2 "https://127.0.0.1:$PORT/version" || true
        echo "curl internal:"
        curl -v -k -m 2 "https://$KIND_NET_IP:6443/version" || true
        exit 1
    fi
    sleep 3
done

kubectl cluster-info

echo ""
echo "Installing KWOK controller and fast-stage pipeline..."
kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/download/v0.6.0/kwok.yaml
kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/download/v0.6.0/stage-fast.yaml
echo "Waiting for kwok-controller to become available..."
kubectl wait --for=condition=Available deployment/kwok-controller -n kube-system --timeout=120s

echo ""
TOTAL_NODES=$((NODE_POOL_COUNT * 16))
echo "Synthesizing $TOTAL_NODES fake worker nodes via KWOK..."
for ((i=1; i<=TOTAL_NODES; i++)); do
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Node
metadata:
  annotations:
    node.alpha.kubernetes.io/ttl: "0"
    kwok.x-k8s.io/node: "fake"
  labels:
    beta.kubernetes.io/arch: amd64
    beta.kubernetes.io/os: linux
    kubernetes.io/arch: amd64
    kubernetes.io/os: linux
    kubernetes.io/role: agent
    node-role.kubernetes.io/agent: ""
    type: kwok
  name: kwok-node-${i}
spec:
  taints:
    - effect: NoSchedule
      key: kwok.x-k8s.io/node
      value: "fake"
EOF
done

echo ""
echo "Labeling worker nodes to simulate node pools..."
WORKER_NODES=($(kubectl get nodes -l type=kwok -o jsonpath='{.items[*].metadata.name}'))

for i in "${!WORKER_NODES[@]}"; do
    # Calculate the pool number: 0-15 -> 1, 16-31 -> 2, etc.
    POOL_NUM=$(( (i / 16) + 1 ))
    POOL_NAME="np-t${POOL_NUM}"

    # Apply the common labels required by shouldProcessNode and isNodeReady
    kubectl label node "${WORKER_NODES[$i]}" $PROVISIONING_MODE_LABEL $TPU_ACCELERATOR_LABEL $RES_NAME_LABEL $RES_BLOCK_LABEL --overwrite > /dev/null

    # Apply the dynamically calculated node pool label
    kubectl label node "${WORKER_NODES[$i]}" cloud.google.com/gke-nodepool=$POOL_NAME --overwrite > /dev/null
done
echo "-> Applied common mock labels to all worker nodes."
echo "-> Dynamically grouped nodes into pools of 16 (np-t1, np-t2, etc.)."

# --- Clean Previous Deployments ---
echo "Deleting all slices and clearing finalizers to force teardown..."
kubectl delete slice --all -n default --wait=false 2>/dev/null || true
SLICES=$(kubectl get slice -n default -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
for S in $SLICES; do
    echo "  -> Force removing finalizer for $S"
    kubectl patch slice "$S" -n default --type=json -p="[{\"op\": \"remove\", \"path\": \"/metadata/finalizers\"}]" 2>/dev/null || true
done
kubectl delete -f ./test/yaml --ignore-not-found
kubectl delete -f ./mock_infra/test/yaml --ignore-not-found
echo "Slice's yaml deleted"

make undeploy
echo "Undeployment completed. Waiting for Slice CRD to be completely gone..."
while kubectl get crd slices.accelerator.gke.io > /dev/null 2>&1; do
    kubectl patch crd slices.accelerator.gke.io -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
    sleep 2
done
echo "Slice CRD is gone."

declare -A LABEL_VALUES
echo ""
echo "Annotating and labeling nodes to simulate GCE environment..."
for POOL_NAME in "${KIND_POOL_NAMES[@]}"; do
    MIG_NAME="${POOL_NAME}-grp"
    FULL_MIG_ANNOTATION="${BASE_MIG_VAL}${MIG_NAME}"

    echo "Processing Node Pool: $POOL_NAME"
    NODES=$(kubectl get nodes -l cloud.google.com/gke-nodepool="$POOL_NAME" -o jsonpath='{.items[*].metadata.name}')

    if [ -z "$NODES" ]; then
        echo "  -> WARNING: No nodes found for pool '$POOL_NAME'. Skipping..."
        continue
    fi

    # Clear previous values for this pool to avoid leakage or re-declaration issues
    LABEL_VALUES=()
    for LABEL in "${TOPOLOGY_LABELS[@]}"; do
        if [ "$LABEL" == "$TARGET_LABEL" ]; then
            LABEL_VALUES["$LABEL"]="$POOL_NAME"
        else
            LABEL_VALUES["$LABEL"]=$(openssl rand -hex 4)
        fi
    done

    for NODE in $NODES; do
        kubectl annotate node "$NODE" "node.gke.io/created-by-mig=$FULL_MIG_ANNOTATION" --overwrite > /dev/null
        for LABEL in "${TOPOLOGY_LABELS[@]}"; do
             LABEL_VAL="${LABEL_VALUES[$LABEL]}"
             kubectl label node "$NODE" "$LABEL=$LABEL_VAL" --overwrite > /dev/null
        done
    done
    echo "  -> Successfully annotated and labeled nodes in pool '$POOL_NAME'"
done

echo "=================================================="
echo ""

# --- Install Cert-Manager ---
echo "Checking if cert-manager is installed and running..."
if ! kubectl get deployment -n cert-manager cert-manager > /dev/null 2>&1 || ! kubectl rollout status deployment -n cert-manager cert-manager --timeout=180s > /dev/null 2>&1; then
  echo "cert-manager not found or not ready, installing..."
  kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.5/cert-manager.yaml

  # Wait for all deployments in cert-manager namespace to be ready
  kubectl rollout status deployment -n cert-manager cert-manager --timeout=180s
  kubectl rollout status deployment -n cert-manager cert-manager-webhook --timeout=180s
  kubectl rollout status deployment -n cert-manager cert-manager-cainjector --timeout=180s
  echo "cert-manager deployments are ready. Waiting for webhook TLS to stabilize..."
  sleep 15
  echo "cert-manager installation complete."
else
  echo "cert-manager is already installed and running."
fi



# Fail the process if any subsequent command errors out
set -e

# --- Build and Load Controller Image ---
echo "Building slice-controller image locally for kind..."
make docker-build IMG=${CONTROLLER_IMG}

echo "Loading slice-controller image into kind cluster..."
${KIND:-kind} load docker-image ${CONTROLLER_IMG} --name "$KIND_CLUSTER_NAME"

# --- Deploy Slice Controller ---
echo "Deploying Slice Controller..."
make deploy IMG=${CONTROLLER_IMG}

echo "Waiting for Slice CRD to be Established..."
kubectl wait --for=condition=Established crd/slices.accelerator.gke.io --timeout=60s

# Configure the controller to use the mock GCE service endpoint
MOCKING_GCE_API_ENDPOINT="http://mock-gce-service.slice-controller-system.svc:8080"
kubectl set env deployment/slice-controller-controller-manager -n slice-controller-system MOCKING_GCE_API_ENDPOINT=${MOCKING_GCE_API_ENDPOINT}
kubectl patch deployment slice-controller-controller-manager -n slice-controller-system --type=json -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--enable-slice-update=true"}, {"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--support-subslice-update=true"}]'

# --- Build and Load Mock GCE Image ---
echo "Building mock-gce image locally for kind..."
make mock-infra-docker-build MOCK_INFRA_IMG=${MOCK_INFRA_IMG}

echo "Loading mock-gce image into kind cluster..."
${KIND:-kind} load docker-image ${MOCK_INFRA_IMG} --name "$KIND_CLUSTER_NAME"

# --- Deploy Mock GCE Server ---
echo "Deploying Mock GCE server..."
make deploy-mock-gce MOCK_INFRA_IMG=${MOCK_INFRA_IMG} MOCK_GCE_PORT=8080

echo "Waiting for mock-gce pod to be available..."
kubectl rollout status deployment/mock-gce -n slice-controller-system --timeout=120s

echo "Waiting for slice-controller deployment to roll out..."
kubectl rollout status deployment/slice-controller-controller-manager -n slice-controller-system --timeout=120s

echo "Allowing webhooks and certificate injection to warm up..."
sleep 20

echo "All deployments completed and configured."

# --- Final Output and Port Forwarding ---
echo ""
echo "Making test scripts executable..."
chmod +x ./mock_infra/test/script/*.sh
chmod +x ./mock_infra/test/kind/*.sh
echo "-> Scripts in ./mock_infra/test/script/ are now executable."
echo "-> Scripts in ./mock_infra/test/kind/ are now executable."

echo ""
echo "Starting port-forward to the mock GCE service in the background..."
kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080 > /dev/null 2>&1 &

PORT_FORWARD_PID=$!
echo "Port-forward is running with PID: ${PORT_FORWARD_PID}"
echo "You can now interact with the mock GCE API at http://localhost:8080"
echo "To stop it, run: kill ${PORT_FORWARD_PID}"
echo ""
echo "Kind cluster setup is complete. You can now run test scripts like 'test/test-kind-happy-path.sh'."
echo "When finished, stop the port-forward process with 'kill ${PORT_FORWARD_PID}' and run './mock_infra/test/script/cleanup.sh'."