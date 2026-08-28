/*
Copyright 2025 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package informer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AllocationState defines the claiming status of a topologyNode.
type AllocationState string

const (
	// Slice is ready to be allocated
	StateAllocatable AllocationState = "Allocatable"
	// One of the child is Allocated or PartiallyAllocated
	StatePartiallyAllocated AllocationState = "PartiallyAllocated"
	// Both the child are in Allocated state.
	StateAllocated AllocationState = "Allocated"

	// tpuAcceleratorLabel is the label present on all GKE TPU nodes.
	tpuAcceleratorLabel    = "tpus.gke.io/accelerator"
	tpuTopologyLabelPrefix = "tpus.gke.io/topology-"
	// tpuBlockLabel is the label used to identify the overarching TPU block.
	// The design doc specifies a 'tpu_block' metric label; this is the assumed source node label.
	tpuBlockLabel = "tpus.gke.io/block"
	// tpuSubBlockLabel is the label used to identify the TPU sub-block.
	tpuSubBlockLabel = "tpus.gke.io/sub-block"
	// tpuClaimedBySliceLabel is the label applied to nodes that are part of a Slice CR.
	tpuClaimedBySliceLabel = "accelerator.gke.io/slice-id"
)

var topologyOrder = map[string][]string{
	"tpu7x": {"2x2x1", "2x2x2", "2x2x4", "2x4x4", "4x4x4"},
}

var topologyOrderedMap = map[string]map[string]int{
	"tpu7x": {"2x2x1": 0, "2x2x2": 1, "2x2x4": 2, "2x4x4": 3, "4x4x4": 4},
}

// StringRateLimitingQueue is a non-generic interface that embeds the
// typed workqueue.TypedRateLimitingInterface[string], making it mockable.
// Only used for tests
type StringRateLimitingQueue interface {
	workqueue.TypedRateLimitingInterface[string]
}

// TpuTopologyInformer watches node events, aggregates health, and updates
// the in-memory repository. It implements the manager.Runnable interface.
type TpuTopologyInformer struct {
	Client client.Client
	Log    logr.Logger
	// Number of workers for workQueue
	WorkerCount int
	workqueue   workqueue.TypedRateLimitingInterface[string]
	repo        *TopologyRepo
	allocator   *TopologyAllocator

	// Variable to track if the initial syncing of topologies are done at startup
	hasSynced      bool
	hasSyncedMutex sync.RWMutex

	// For initial sync tracking
	initialSyncKeys      map[string]struct{}
	initialSyncKeysMutex sync.Mutex
}

// GetTopologyRepoSnapshot returns a deep copy of the topology repository for safe concurrent access.
func (n *TpuTopologyInformer) GetTopologyRepoSnapshot() RepoSnapshot {
	return n.repo.GetTopologyRepoSnapshot()
}

// NewTpuTopologyInformer creates a new informer.
func NewTpuTopologyInformer(c client.Client, log logr.Logger, workerCount int) *TpuTopologyInformer {
	repo := NewTopologyRepo(log)
	return &TpuTopologyInformer{
		Client:          c,
		Log:             log.WithName("topology-informer"),
		WorkerCount:     workerCount,
		repo:            repo,
		allocator:       NewTopologyAllocator(log, repo),
		workqueue:       workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		initialSyncKeys: make(map[string]struct{}),
	}
}

// HasSynced returns true if the informer has completed its initial synchronization.
func (n *TpuTopologyInformer) HasSynced() bool {
	n.hasSyncedMutex.RLock()
	defer n.hasSyncedMutex.RUnlock()
	return n.hasSynced
}

func (n *TpuTopologyInformer) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithEventFilter(NodeChangePredicate{}).
		Complete(n)
}

// Reconcile is the entry point for the informer's reconciliation loop.
func (n *TpuTopologyInformer) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	n.Log.Info("Reconciling Node", "Node-Name", req.Name)
	var node corev1.Node
	if err := n.Client.Get(ctx, req.NamespacedName, &node); err != nil {
		// Node was deleted, handle this case by finding its old tree and enqueuing it.
		// client.IgnoreNotFound(err) handles the cleanup.
		n.Log.Info("Deleted Node", "Node-name", req.Name)

		tn := n.repo.RootTNFromNode(req.Name)
		if tn != nil {
			// When a node is deleted, we find the root of the tree it belonged to
			// and enqueue that root for reconciliation.
			rootLabel := tpuTopologyLabelPrefix + tn.Topology + "-id"
			n.workqueue.Add(rootLabel + "=" + tn.UID)
		}

		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// The logic is now simple: enqueue the node's root tree for processing.
	// The heavy lifting is done by the repository's worker queue.
	key := getRootKeyForNode(node)
	if key != "" {
		n.workqueue.Add(key)
	} else {
		n.Log.Info("Node does not have any known topology labels, skipping", "node", node.Name)
	}

	return ctrl.Result{}, nil
}

// Allocate finds and reserves a topology, returning the constituent topology UIDs.
func (n *TpuTopologyInformer) Allocate(tpuType, topology string) ([]string, error) {
	if !n.HasSynced() {
		// TODO: better error wrapping to differentiate errors that are retriable https://go.dev/blog/go1.13-errors
		return nil, fmt.Errorf("informer has not synced yet")
	}
	return n.allocator.Allocate(tpuType, topology)
}

// Deallocate releases a previously allocated TPU topology.
func (n *TpuTopologyInformer) Deallocate(topologyUID string) error {
	if !n.HasSynced() {
		return fmt.Errorf("informer has not synced yet")
	}
	return n.allocator.Deallocate(topologyUID)
}

// Start implements the manager.Runnable interface. The manager calls this once the
// client cache is synced.
func (n *TpuTopologyInformer) Start(ctx context.Context) error {
	n.Log.Info("Cache synced, starting informer workers...", "count", n.WorkerCount)
	var wg sync.WaitGroup
	for i := 0; i < n.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.processTopologyUpdates(ctx)
		}()
	}

	go n.performInitialSync(ctx)

	// The Start method should block until the context is cancelled.
	<-ctx.Done()
	n.workqueue.ShutDown()
	wg.Wait()
	return nil
}

// Startup function to build the initial topologies by listing all TPU nodes and
// queueing them for our workerQueue to process. We also save all the keys we
// are enqueueing so we can track when all the initial items are finished
// processing.
func (n *TpuTopologyInformer) performInitialSync(ctx context.Context) {
	n.Log.Info("Starting initial TPU topology sync...")

	nodes := &corev1.NodeList{}
	if err := n.Client.List(ctx, nodes, client.HasLabels{tpuAcceleratorLabel}); err != nil {
		n.Log.Error(err, "Failed to list nodes for initial sync")
		// If this fails, the informer will not be synced and will be unusable.
		// The controller will likely crash and restart, at which point this may succeed.
		return
	}

	enqueuedRoots := make(map[string]struct{})
	for _, node := range nodes.Items {
		rootKey := getRootKeyForNode(node)
		if rootKey != "" {
			if _, exists := enqueuedRoots[rootKey]; !exists {
				enqueuedRoots[rootKey] = struct{}{}
			}
		}
	}

	if len(enqueuedRoots) == 0 {
		n.Log.Info("No TPU nodes found. Initial sync complete.")
		n.hasSyncedMutex.Lock()
		n.hasSynced = true
		n.hasSyncedMutex.Unlock()
		return
	}

	n.initialSyncKeysMutex.Lock()
	for key := range enqueuedRoots {
		n.initialSyncKeys[key] = struct{}{}
		n.workqueue.Add(key)
	}
	n.initialSyncKeysMutex.Unlock()
	n.Log.Info("Enqueued initial topology roots for sync", "count", len(enqueuedRoots))
}

// Long running go routine that fetches work Items from the workerqueue and
// calls reconcileTopologyNode. If reconcileTopologyNode returns no error then
// we Forget the item from workerQueue else re-queue.
func (n *TpuTopologyInformer) processTopologyUpdates(ctx context.Context) {
	for {
		// Get the next item from the queue.
		key, shutdown := n.workqueue.Get()
		if shutdown {
			return
		}

		// Use an anonymous function to leverage 'defer'.
		func() {
			defer n.workqueue.Done(key)

			// Process the item.
			if err := n.reconcileTopologyNode(ctx, key); err != nil {
				// If processing fails, re-queue the item with rate-limiting.
				n.Log.Error(err, "Failed to reconcile TN, requeuing", "Key", key)
				n.workqueue.AddRateLimited(key)
				return
			}

			// If processing succeeds, tell the queue to forget the item
			// so its rate-limiting history is reset.
			n.workqueue.Forget(key)
			n.Log.Info("Successfully reconciled TN", "key", key)
			n.checkInitialSyncComplete(key)
		}()
	}
}

// Check if the initial sync is complete by first looking at hasSynced==True and
// then if all the keys from initialSyncKeys are processed. If so, mark
// hasSynced=true and return.
func (n *TpuTopologyInformer) checkInitialSyncComplete(key string) {
	n.hasSyncedMutex.RLock()
	if n.hasSynced {
		n.hasSyncedMutex.RUnlock()
		return
	}
	n.hasSyncedMutex.RUnlock()

	n.initialSyncKeysMutex.Lock()
	defer n.initialSyncKeysMutex.Unlock()

	// Re-check hasSynced after acquiring the lock to handle race conditions.
	if n.hasSynced {
		return
	}

	delete(n.initialSyncKeys, key)
	if len(n.initialSyncKeys) == 0 {
		n.hasSyncedMutex.Lock()
		n.hasSynced = true
		n.hasSyncedMutex.Unlock()
		n.Log.Info("Initial TPU topology sync complete.")
	}
}

// reconcileTopologyNode is the core reconciliation logic for a single TPU topology
// tree. It is triggered by a change in any node belonging to that topology. The
// 'key' identifies the root of the topology tree to be processed.
//
// The function performs the following steps:
//  1. Fetches all Kubernetes nodes that are part of the specified root topology.
//  2. Acquires a tree-specific lock to prevent race conditions with other workers
//     processing the same topology.
//  3. Iterates through the fetched nodes and reconstructs the topology tree in memory.
//     - It creates or updates TopologyNode objects for each level of the topology
//     hierarchy present on the node.
//     - It establishes parent-child relationships between these TopologyNodes.
//     - It updates the leaf nodes of the tree with the latest status (readiness,
//     allocation state) from the corresponding Kubernetes node.
//  4. Detects and prunes any leaf nodes from the tree that no longer exist in the
//     latest list of fetched Kubernetes nodes. This cleanup is recursive, removing
//     parent nodes if they become childless.
//  5. After the tree structure is consistent with the state of the cluster, it
//     propagates status upwards from the leaves to the root:
//     - `updateTreeHealth`: Aggregates the readiness status. A parent is ready only
//     if all its children are ready.
//     - `updateAllocationState`: Aggregates the allocation state (Allocatable,
//     PartiallyAllocated, Allocated) and slice claim information.
func (n *TpuTopologyInformer) reconcileTopologyNode(ctx context.Context, key string) error {
	rootTopologyLabel, rootUID, _ := strings.Cut(key, "=")
	rootTopologyShape := strings.TrimSuffix(strings.TrimPrefix(rootTopologyLabel, tpuTopologyLabelPrefix), "-id")

	nodes := &corev1.NodeList{}
	// We are only listing nodes that belongs to this top level TopologyNode.
	// We are using the tpu topology label to do this filter. So if the top level
	// TN is a 16x16, only the nodes with the 16x16 label and its UID will be
	// listed.
	if err := n.Client.List(ctx, nodes, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{rootTopologyLabel: rootUID}),
	}); err != nil {
		n.Log.Error(err, "Failed to list nodes for TN", "topologyID", rootUID)
		return err
	}

	// Find the root and if it exists acquire the lock to the tree. Else create
	// a new lock and acquire it.
	// This logic is protected by the global repoMutex to prevent a race condition
	// where two workers attempt to create the same tree root simultaneously.
	root := n.repo.GetOrAddTree(&TopologyNode{
		UID:      rootUID,
		Topology: rootTopologyShape,
		rootLock: &sync.RWMutex{},
	})

	// Now that we have a guaranteed unique root for this topology, lock the
	// tree-specific lock for the remainder of the reconciliation.
	root.rootLock.Lock()
	defer root.rootLock.Unlock()

	var acceleratorType, blockID, subBlockID string
	// Map to represent the TopologyNodes that we are creating in this fn.
	// This will later be merged with the allTopologyNodes.
	newTNs := make(map[string]*TopologyNode, 0)
	visitedLeafTNs := make(map[string]struct{})
	for _, node := range nodes.Items {
		nodeReady := true
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && (cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown) {
				nodeReady = false
				break
			}
		}
		allocState := StateAllocatable
		claimBySliceId, isClaimed := node.Labels[tpuClaimedBySliceLabel]
		if isClaimed {
			allocState = StateAllocated
		}
		acceleratorType = getLabelOrDefault(node.Labels, tpuAcceleratorLabel)
		blockID = getLabelOrDefault(node.Labels, tpuBlockLabel)
		subBlockID = getLabelOrDefault(node.Labels, tpuSubBlockLabel)

		// If we just created this root, its Type will be empty. Populate it from the
		// first node we process. All nodes in a topology share the same Type, Block,
		// and SubBlock, so we only need to do this once.
		if root.Type == "" {
			root.Type = acceleratorType
			root.Block = blockID
			root.SubBlock = subBlockID
		}

		var prevTN *TopologyNode
		shouldStop := false
		for i, topologyShape := range topologyOrder[acceleratorType] {
			if shouldStop {
				break
			}
			if v, ok := node.Labels[tpuTopologyLabelPrefix+topologyShape+"-id"]; ok {
				if i == 0 {
					visitedLeafTNs[v] = struct{}{}
				}
				currTN := n.repo.TopologyNode(v)
				if currTN != nil {
					// Only update the health and claim status for leaf nodes. They will
					// be propogated up to the tree later.
					if i == 0 {
						// Reset the health
						currTN.IsReady = nodeReady // We will update this in the tree-wide update
						// If the node is claimed, reflect that in our cache
						// We will update this in the tree-wide update
						if isClaimed {
							currTN.ClaimBySliceID = claimBySliceId
							currTN.AllocationState = StateAllocated
						} else {
							// If the node is not claimed, then it is allocatable
							currTN.ClaimBySliceID = ""
							currTN.AllocationState = StateAllocatable
						}
					}
					// If we have reach the first seen the common ancestor, then we dont
					// need to traverse up and we can stop here.
					shouldStop = true
				} else if currTN, ok = newTNs[v]; ok {
					// Check if we recently created a TopologyNode at this topology level
					// Since we have reach the first seen the common ancestor, we dont
					// need to traverse up and we can stop here.
					shouldStop = true
				} else { // If not then a new TopologyNode is being created
					currTN = &TopologyNode{
						Topology:        topologyShape,
						UID:             v,
						Type:            acceleratorType,
						Block:           blockID,
						SubBlock:        subBlockID,
						ClaimBySliceID:  "",             // Assume empty
						IsReady:         false,          // Assume node not ready
						AllocationState: StateAllocated, // Assume already allocated
						Children:        []*TopologyNode{},
						Parent:          nil,
						rootLock:        root.rootLock,
					}
					// Populate NodeName for leaf nodes. Also track new nodes that we
					// are adding.
					if i == 0 {
						currTN.NodeName = node.Name
						currTN.IsReady = nodeReady
						currTN.AllocationState = allocState
					}
					newTNs[currTN.UID] = currTN
				}
				// Build parent and child relationship
				if prevTN != nil {
					currTN.Children = append(currTN.Children, prevTN)
					prevTN.Parent = currTN
				}
				prevTN = currTN
			}
		}
	}
	// First add new TNs into our tree and Repo
	if len(newTNs) != 0 {
		n.repo.Update(&newTNs, root)
	}
	// Call missing leaf nodes if the # of visitedLeafTns is not equal to expected
	if len(visitedLeafTNs) != numNodes(root.Type, root.Topology) {
		prunedUIDs := pruneTree(root, visitedLeafTNs)
		// Perform a batch deletion.
		n.repo.DeleteFromRepo(prunedUIDs)
	}

	updateTreeHealth(root)
	updateAllocationState(root)
	n.allocator.updateAllocatableTopologies(root)
	return nil
}
