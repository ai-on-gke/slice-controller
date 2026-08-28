# How to Contribute to Slice Controller

Please note: This project is currently not accepting external contributions or pull requests. Instead please open an issue and project maintainers will be in touch.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License Agreement (CLA). You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of the project. Head over to <https://cla.developers.google.com/> to see your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one (even if it was for a different project), you probably don't need to do it again.

## Community Guidelines

This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Development Workflow

### Prerequisites

- **Go**: `v1.23.0+`
- **Docker**: `17.03+`
- **kubectl**: `v1.11.3+`
- **Kubernetes cluster**: `v1.26+` or local test cluster (e.g. [kind](https://kind.sigs.k8s.io/))
- **controller-gen** / **kustomize**: Managed automatically via Makefile tools directory

### Building and Testing

1. **Format and lint code**:
   ```bash
   make fmt
   make vet
   make lint
   ```

2. **Verify license headers**:
   Ensure all source files have appropriate Apache 2.0 copyright headers:
   ```bash
   make check-license
   ```
   To automatically add missing license headers:
   ```bash
   make addlicense
   ```

3. **Run unit tests**:
   ```bash
   make test
   ```

4. **Run presubmit checks**:
   Runs linting, unit tests, and license checks in a single pass:
   ```bash
   make presubmit
   ```

### Local Testing with Mock Infrastructure (Kind)

To test the full lifecycle of TPU Slice creation, activation, health monitoring, and teardown without physical ASIC hardware, you can use the integrated mock infrastructure:

```bash
# Set up a local kind cluster with mock GCE API server and Slice Controller
./test/local_deployment_kind_mock_infra.sh

# Run the test suite against the local mock environment
./mock_infra/test/kind/run-all-tests.sh
```
