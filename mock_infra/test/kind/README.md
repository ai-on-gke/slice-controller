### Steps to run

Run  `./test/local_deployment_kind_mock_infra.sh` and then `./mock_infra/test/kind/test-kind-happy-path.sh`

### 1. Troubleshooting & Managing Kubernetes Contexts

Use these to verify which cluster `kubectl` is currently talking to and fix connection timeouts.

```bash
# See which cluster you are currently targeting
kubectl config current-context

# List all available contexts
kubectl config get-contexts

# Switch context to your local kind cluster (replace with your cluster name)
kubectl config use-context kind-slice-testing

# Verify connectivity to the current cluster
kubectl cluster-info
kubectl get nodes
```

### 2. System Preparation for Large Local Clusters (16+ Nodes)

Running a large `kind` cluster requires preparing your host machine, clearing old data, and ensuring enough resources.

```bash
# Check how much total RAM Docker is allowed to use
docker info | grep -i "Total Memory"

# Clean up dangling containers, networks, and cached local images from failed runs
docker system prune -f

# (Linux Only) Increase inotify limits to prevent silent hangs when starting many nodes
sudo sysctl fs.inotify.max_user_watches=524288
sudo sysctl fs.inotify.max_user_instances=512
```