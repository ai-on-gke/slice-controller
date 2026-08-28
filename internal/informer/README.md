# WARNING: `internal/informer` is not used and will be removed. Do not add it to the manager during starting up.

# TPU Topology Informer

The `informer` package contains the components responsible for monitoring the state of TPU nodes in a GKE cluster. Its primary goal is to build and maintain an in-memory cache representing the hierarchical topology of the TPU resources. This cache enables efficient and safe querying and management of TPU slices for allocation.

## Key Components

### `TpuTopologyInformer`
This is the main controller that watches for `Node` events (creations, updates, deletions). It determines which topology tree a node event belongs to and enqueues the root of that tree for reconciliation. It holds an instance of the `TopologyRepo` and the `TopologyAllocator`.

### `TopologyAllocator`
This struct encapsulates all logic related to managing the pool of allocatable resources. It is responsible for:
- Tracking which topologies are available for allocation in its internal `allocatableTopologyNodeMap`.
- Providing thread-safe `Allocate()` and `Deallocate()` methods.
- Implementing the strategy for finding and splitting larger topologies to fulfill requests for smaller ones.
- Merging smaller topologies back into larger ones upon deallocation.

### `TopologyRepo`
This struct acts as the central database for all topology nodes. It provides O(1) access to any `TopologyNode` by its UID and maps Kubernetes node names back to their corresponding topology tree root. Access to its maps is protected by a mutex.

### `TopologyNode`
This struct represents a single node in the topology tree. It can be a leaf node (representing a single physical TPU VM) or an intermediate node representing a larger slice composed of smaller ones.
- **`UID`, `Type`, `Topology`**: Define the identity and shape of the slice.
- **`IsReady`**: An aggregated health status. A parent is only ready if all its children are ready. Note that this readiness is determined by the informer based on the state of the underlying Kubernetes nodes and is not necessarily equivalent to the Kubernetes node readiness condition.
- **`AllocationState`**: The claiming status of the slice (`Allocatable`, `PartiallyAllocated`, `Allocated`).
- **`ClaimBySliceID`**: If a node is claimed, this holds the UID of the slice. This value is propagated up the tree.
- **`Children` & `Parent`**: Pointers that form the tree structure.
- **`rootLock`**: A pointer to a shared `RWMutex` for the entire tree, ensuring that updates to a single topology tree are serialized.

## Core Processes

### Reconciliation Logic
1. The `Reconcile` method in the `TpuTopologyInformer` is triggered by a `Node` event. It identifies the root of the node's topology tree and adds it to a work queue.
2. A worker picks up the key and calls `reconcileTopologyNode`.
3. `reconcileTopologyNode` acquires the tree-specific `rootLock`.
4. It lists all nodes from the Kubernetes API that belong to that root topology.
5. It then reconstructs the tree in memory, updating health and allocation states, adding new nodes, and pruning any that are no longer present.
6. Finally, it calls `allocator.updateAllocatableTopologies()` to refresh the allocator's view of what is available based on the new state of the tree.

### Allocation and Deallocation Logic
This logic is fully contained within the `TopologyAllocator`.
- **`allocatableTopologyNodeMap`**: This map is the source of truth for finding allocatable slices. It stores only the *maximal* available topologies to prevent fragmentation.
- **`Allocate(tpuType, topology)`**:
  1. Acquires a global `allocationMutex` to serialize the entire operation.
  2. Searches `allocatableTopologyNodeMap` for an exact match.
  3. If no exact match is found, it looks for the smallest available topology that is *larger* than the request to minimize fragmentation.
  4. After finding and validating a candidate, it updates the tree state and calls `updateAllocatableTopologies` to reflect the change.
- **`Deallocate(topologyUID)`**:
  1. Also serialized by the `allocationMutex`.
  2. Marks the specified topology as `Allocatable`.
  3. Attempts to "merge up" the tree. If the deallocated node's sibling is also allocatable, they are removed from the available list, and their parent is added, promoting larger, contiguous blocks of resources.

## Locking and Concurrency

The informer uses a multi-tiered locking strategy to ensure safety and prevent deadlocks.

1.  **`allocationMutex` (`TopologyAllocator`)**: A global `sync.Mutex` that serializes all `Allocate` and `Deallocate` calls. This is the simplest and most robust way to prevent race conditions where multiple goroutines could attempt to select and allocate the same resource simultaneously.

2.  **`allocatableTopologiesMutex` (`TopologyAllocator`)**: A `sync.RWMutex` that protects the `allocatableTopologyNodeMap`. It is used within the allocator to safely read or modify the map of available topologies.

3.  **`rootLock` (`TopologyNode`)**: Each topology tree has its own `sync.RWMutex`. When a worker reconciles a tree, it acquires a write lock on that specific tree. This allows *different* topology trees to be reconciled in parallel while ensuring that all modifications within a *single* tree are serialized and safe.

This clear separation of locks—one for the allocation process, one for the map of available resources, and one for each topology tree—prevents the complex lock-ordering issues that can lead to deadlocks.
