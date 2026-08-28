# Mock GCE API Server

## Overview

This package implements a standalone mock Google Compute Engine (GCE) API server. It is designed for testing clients that interact with GCE, particularly for components like the `slice-controller` that manage GCE resources such as `MultiMig`, `InstanceGroupManager`, and `ResourcePolicy`.

The server simulates the GCE API and provides a set of "control" endpoints to inject failures, delays, and specific health statuses to test client retry logic and error handling.

## Quick Start: Deployment & Testing

### 1. Automated Testing (Kind)
The easiest way to run the full E2E test suite in a local Kind cluster:
```bash
make test-mock-gce-kind
```

### 2. Manual Deployment (Kind Cluster)
To build and deploy the controller and mock GCE server to a Kind cluster:
```bash
./test/local_deployment_kind_mock_infra.sh
```

### 3. Port Forwarding
To interact with the mock GCE control endpoints locally:
```bash
kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080
```

## Core Concept: Slices

The entire state of the mock server is namespaced by a **`sliceName`**.

Before a test can interact with resources for a specific slice, it **must** initialize that slice. All simulated resources (MMIGs, MIGs, Policies, Operations) and all failure injections (HTTP errors, timeouts, health) are stored in maps keyed by this `sliceName`.

This design allows for parallel tests to run against the same mock server without interfering with each other's state, as long as they use unique slice names.

---

## Control & Injection API

These are the primary endpoints you will use to set up your test scenarios.

### 1. Initialize a Slice

This is the first call you must make for any test. It creates the data structures in the server to track resources and injections for your slice.

* **Endpoint:** `POST /inject/slice`
* **Purpose:** Initializes a new, empty slice in the mock server.
* **Body:**
    ```json
    {
      "sliceName": "my-test-slice"
    }
    ```
* **Example:**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice"}' \
      http://localhost:8080/inject/slice
    ```

### 2. Inject an HTTP Error

* **Endpoint:** `POST /inject/http-error`
* **Purpose:** Forces a specific GCE API call (identified by a step key) to fail with a specific HTTP error code and message.
* **Body:**
    ```json
    {
      "sliceName": "my-test-slice",
      "key": "CreateWorkloadPolicy",
      "code": 500,
      "message": "Internal Server Error"
    }
    ```
* **Allowed `key` values:**
    * `CreateWorkloadPolicy`
    * `DeleteWorkloadPolicy`
    * `CreateMMIG`
    * `GetMMIG`
    * `DeleteMMIG`
    * `GetInstanceGroupManager`
    * `AttachMIGToMMIG`
    * `DetachMIGFromMMIG`
    * `GetAvailableTopologies`
* **Example (Inject):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "CreateWorkloadPolicy", "code": 500, "message": "Injected Error"}' \
      http://localhost:8080/inject/http-error
    ```
* **Example (Clear):**
    ```bash
    curl -X DELETE -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "CreateWorkloadPolicy"}' \
      http://localhost:8080/inject/http-error
    ```

### 3. Inject a Timeout (Delay)

* **Endpoint:** `POST /inject/timeout`
* **Purpose:** Forces a specific GCE API call to hang by injecting a `time.Sleep`. This is useful for testing client-side timeouts.
* **Body:**
    ```json
    {
      "sliceName": "my-test-slice",
      "key": "CreateWorkloadPolicy",
      "duration": 5
    }
    ```
* **Allowed `key` values:**
    * `CreateWorkloadPolicy`
    * `DeleteWorkloadPolicy`
    * `CreateMMIG`
    * `GetMMIG`
    * `DeleteMMIG`
    * `GetInstanceGroupManager`
    * `AttachMIGToMMIG`
    * `DetachMIGFromMMIG`
    * `GetAvailableTopologies`
* **Example (Inject):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "CreateWorkloadPolicy", "duration": 5}' \
      http://localhost:8080/inject/timeout
    ```
* **Example (Clear):**
    ```bash
    curl -X DELETE -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "CreateWorkloadPolicy"}' \
      http://localhost:8080/inject/timeout
    ```

### 4. Inject an Operation Status

* **Endpoint:** `POST /inject/op-status`
* **Purpose:** Forces a GCE operation (which is polled via `GET /.../operations/{name}`) to return a specific status, such as `RUNNING`, to test client polling logic.
* **Body:**
    ```json
    {
      "sliceName": "my-test-slice",
      "key": "op-createmmig-my-test-slice",
      "status": "RUNNING"
    }
    ```
* **Note on `key`:** The key is the operation name, which typically follows a pattern like `op-createmmig-my-test-slice`. The mock server will also match prefixes (e.g., `op-createmmig-my-test-slice` will match an injection for `op-createmmig-my-test-slice-hash`).
    * **Prefixes:**
        * `op-createpolicy-`
        * `op-deletepolicy-`
        * `op-createmmig-`
        * `op-deletemmig-`
        * `op-attachmigs-`
        * `op-detachmigs-`

> [!NOTE]
> When an operation (`op-attachmigs-` or `op-detachmigs-`) transitions from `RUNNING` to `DONE` via injection, the mock server automatically applies the corresponding infrastructure changes (attaching or detaching the MIG from the MMIG in the simulated state).
* **Example (Inject):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "op-createmmig-my-test-slice", "status": "RUNNING"}' \
      http://localhost:8080/inject/op-status
    ```
* **Example (Clear):**
    ```bash
    curl -X DELETE -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "op-createmmig-my-test-slice"}' \
      http://localhost:8080/inject/op-status
    ```

### 5. Inject MMIG Health Status

* **Endpoint:** `POST /inject/mmig-health`
* **Purpose:** Controls the health status returned by the `GetMMIG` call.
* **Body:**
    ```json
    {
      "sliceName": "my-test-slice",
      "acceleratorTopologyState": "INCOMPLETE",
      "errorMessage": "mock gce injected error"
    }
    ```
* **Allowed `acceleratorTopologyState` values:**
    * `INCOMPLETE`
    * `ACTIVATING`
    * `ACTIVE`
    * `ACTIVE_DEGRADED`
    * `DEACTIVATING`
    * `FAILED`
    * `HEALTH_STATUS_UNSPECIFIED` (or `UNKNOWN`)
* **Example (Inject):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "acceleratorTopologyState": "INCOMPLETE"}' \
      http://localhost:8080/inject/mmig-health
    ```
* **Example (Clear):**
    ```bash
    curl -X DELETE -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice"}' \
      http://localhost:8080/inject/mmig-health
    ```

### 6. Inject MIG Topology Health

* **Endpoint:** `POST /inject/topo-health`
* **Purpose:** Controls the health status returned by the `getAvailableAcceleratorTopologies` call for a *specific* MIG.
* **Body:**
    ```json
    {
      "sliceName": "my-test-slice",
      "key": "my-mig-1",
      "status": "DEGRADED"
    }
    ```
* **Example (Inject):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "my-mig-1", "status": "DEGRADED"}' \
      http://localhost:8080/inject/topo-health
    ```
* **Example (Clear):**
    ```bash
    curl -X DELETE -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "key": "my-mig-1"}' \
      http://localhost:8080/inject/topo-health
    ```

### 7. Initialize a MIG

* **Endpoint:** `POST /inject/mig`
* **Purpose:** Pre-populates a MIG into the server's state for a given slice. This is useful for tests that need to simulate a pre-existing MIG.
* **Body:**
    ```json
    {
      "sliceName": "my-test-slice",
      "migName": "default-pool-mig"
    }
    ```
* **Example (Add):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "migName": "default-pool-mig"}' \
      http://localhost:8080/inject/mig
    ```
* **Example (Delete):**
    ```bash
    curl -X DELETE -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "migName": "default-pool-mig"}' \
      http://localhost:8080/inject/mig
    ```

---

## State Management & Debugging

These endpoints are for test setup, teardown, and debugging.

### List Resources

* **Endpoint:** `GET /list`
* **Purpose:** Dumps the *entire* internal state of the mock server as JSON, including all slice resources and injections.
* **Example (List all):**
    ```bash
    curl http://localhost:8080/list
    ```
* **Example (List specific slice):**
    ```bash
    curl 'http://localhost:8080/list?sliceName=my-test-slice'
    ```

### Get QPS

* **Endpoint:** `GET /qps`
* **Purpose:** Shows QPS (Queries Per Second) statistics for tracked API calls. Currently tracks `GetMMIG` and `GetAvailableAcceleratorTopologies`.
* **Example:**
    ```bash
    curl http://localhost:8080/qps
    ```
* **Example Output:**
    ```text
    QPS Statistics (Uptime: 1m32s):
    - GetMMIG:
      - Current (10s avg):  0.50
      - Count (last 10s):  5
      - Highest (10s peak):  1.20
      - All-time avg:      0.88
    ```

### Clear Injections

* **Endpoint:** `POST /inject/clear`
* **Purpose:** Clears *only* the failure injections for a slice, leaving the simulated resources (MIGs, MMIGs) intact. This is useful for "healing" a slice mid-test.
* **Body:**
    * To clear *all* injections for a slice, send:
        ```json
        {
          "sliceName": "my-test-slice"
        }
        ```
    * To clear *specific* injections for a slice, send:
        ```json
        {
          "sliceName": "my-test-slice",
          "clearOpStatuses": true,
          "clearMmigHealth": true
        }
        ```
* **Example:**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice", "clearOpStatuses": true}' \
      http://localhost:8080/inject/clear
    ```

### Clear All (Reset)

* **Endpoint:** `POST /clear`
* **Purpose:** Completely resets the server state.
* **Example (Clear specific slice):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
      -d '{"sliceName": "my-test-slice"}' \
      http://localhost:8080/clear
    ```
* **Example (Clear entire server):**
    ```bash
    curl -X POST http://localhost:8080/clear
    ```

---

## Simulated GCE API Endpoints

The mock server simulates the following GCE API endpoints. Your client should be pointed to the mock server's address (`http://localhost:8080`) to interact with these.

* **MultiMig**
    * `POST /projects/{p}/regions/{r}/multiMigs` (Create)
    * `GET /projects/{p}/regions/{r}/multiMigs` (List)
    * `GET /projects/{p}/regions/{r}/multiMigs/{name}` (Get)
    * `DELETE /projects/{p}/regions/{r}/multiMigs/{name}` (Delete)
* **ResourcePolicy**
    * `POST /projects/{p}/regions/{r}/resourcePolicies` (Create)
    * `DELETE /projects/{p}/regions/{r}/resourcePolicies/{name}` (Delete)
* **InstanceGroupManager**
    * `GET /projects/{p}/zones/{z}/instanceGroupManagers/{name}` (Get)
    * `PATCH /projects/{p}/zones/{z}/instanceGroupManagers/{name}` (Attach/Detach from MMIG)
    * `GET /projects/{p}/zones/{z}/instanceGroupManagers/{name}/getAvailableAcceleratorTopologies` (Get Health)
* **Operations**
    * `GET /projects/{p}/regions/{r}/operations/{name}` (Get)
    * `GET /projects/{p}/zones/{z}/operations/{name}` (Get)


## How to Start Testing (Examples)

### Automated Tests with Kind

The recommended way to run tests locally is using Kind. A single Makefile target configures a Kind cluster, deploys all components, and runs E2E tests:

```bash
make test-mock-gce-kind
```

This executes `mock_infra/test/kind/run-all-tests.sh`, which validates both happy path and failure scenarios automatically.

### Manual Testing with Local Cluster

The easiest way to start a local test environment for interactive testing is to use the provided script. This script handles the entire setup process, including deploying the controller and the mock GCE server.

### Prerequisites

- A running Kubernetes cluster.
- `kubectl` configured to point to your cluster.
- `make` installed.
- A container registry (like GCR or Docker Hub) that you have push access to.

### Steps

1.  **Run the deployment script:**
    Execute the script to build and deploy all components to a Kind cluster.

    ```bash
    ./test/local_deployment_kind_mock_infra.sh
    ```

This script performs the following actions:
- Installs `cert-manager` if it's not already present.
- Builds and pushes the `slice-controller` image.
- Deploys the `slice-controller`, configuring it to talk to the mock GCE server.
- Builds and pushes the `mock-gce-server` image.
- Deploys the `mock-gce-server`.

After the script finishes, you will have a fully functional environment where the slice controller is interacting with the mock GCE API.

3. **Port-Forward to the Mock GCE Server**

To send commands to the mock GCE server from your local machine, you need to forward a local port to the service running in the cluster.
Open a new terminal and run:

```bash
kubectl port-forward -n slice-controller-system service/mock-gce-service 8080:8080
```

Leave this terminal running. It creates a secure tunnel from your `localhost:8080` to the mock GCE service's port `8080`.

## Testing Walkthrough

The following scripts provide a guided way to test the slice-controller against the mock GCE server.

### 1. Run the Happy Path Test

The `test-happy-path.sh` script automates the entire lifecycle of a slice, from node setup to slice creation, state transitions, and cleanup. It's an interactive script that will prompt you before performing major actions.

**Prerequisites:**
- The mock deployment is running (`./test/local_deployment_with_mock_infra.sh`).
- Port forwarding is active (`kubectl port-forward ...`).
- Your test cluster has nodes in pools named `np-t1` and `np-t2` (or you've updated the `POOL_NAMES` in the script).

**Run the script:**

```bash
./mock_infra/test/script/test-happy-path.sh
```

This script will:
1.  Clear any previous state from the mock server.
2.  Find nodes belonging to the configured node pools.
3.  Annotate and label the nodes to simulate GKE-TPU properties.
4.  Inject the corresponding MIGs into the mock server.
5.  Guide you through creating a `Slice` resource.
6.  Interactively prompt you to inject `INCOMPLETE`, `ACTIVATING`, and `ACTIVE` statuses for the slice's MMIG, waiting for the controller to reconcile each state.
7.  Optionally allow you to inject a `FAILED` status.
8.  Prompt you to delete the `Slice` resource at the end.

### 2. Run the Cleanup Script

If you run the happy path test or perform other manual tests, you may be left with annotations and labels on your cluster nodes. The `cleanup.sh` script removes these to return the nodes to a clean state.

**Run the script:**

```bash
./mock_infra/test/script/cleanup.sh
```

This script will:
1.  Iterate through the configured node pools (`np-t1`, `np-t2`).
2.  Remove the `node.gke.io/created-by-mig` annotation from all nodes in those pools.
3.  Remove all `cloud.google.com/gke-tpu-partition-*-id` labels from the nodes.