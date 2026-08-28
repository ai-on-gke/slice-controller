// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package health

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"slice-controller/internal/action/gcehelper"
	"slice-controller/internal/cache"
	"slice-controller/internal/util"
	"slice-controller/services"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// Polling configuration
	staticPollInterval = 5 * time.Second

	// Queue management
	maxRequeues              = 5
	maxQueueLengthBeforeSkip = 500

	// processing lock cleanup
	lockEntryTimeout    = 5 * time.Minute
	lockCleanupInterval = 10 * time.Minute

	// Error messages
	mmigNotFoundErrorMsg = "mmig notFound"

	// MIG and MMIG url substrings
	migURLSubstring  = "/instanceGroupManagers/"
	mmigURLSubstring = "/multiMigs/"

	// Default expected node counts per MIG for different accelerator types
	tpu7xNodesPerMigDefault = 16
	nodesPerMigFallback     = 16
)

// TerminalError wraps an error to indicate it should not be retried.
type TerminalError struct {
	Err error
}

func (e *TerminalError) Error() string {
	return e.Err.Error()
}

func (e *TerminalError) Unwrap() error {
	return e.Err
}

// NewTerminalError wraps an existing error as terminal.
func NewTerminalError(err error) error {
	return &TerminalError{Err: err}
}

// IsTerminal checks if the error chain contains a TerminalError.
func IsTerminal(err error) bool {
	var termErr *TerminalError
	return errors.As(err, &termErr)
}

// Job defines a single health check item in the work queue.
type Job struct {
	ResourceURL string
}

// Monitor is responsible for continuously polling the health of MIGs and MMIGs.
type Monitor struct {
	client          client.Client
	GCEClient       gcehelper.GCEClient
	relationalCache cache.MonitorRelationalCacheInterface
	healthCache     cache.MonitorHealthCacheInterface
	projectNumber   string
	location        string
	qpsLimit        int
	region          string

	// Queues for different job types
	healthMonitorQueue workqueue.TypedRateLimitingInterface[Job]
	rateLimiter        *rate.Limiter

	// static configuration for polling
	pollInterval time.Duration
	workerCount  int

	// processingLock prevents concurrent processing of the same resource.
	processingLock sync.Map

	clusterHash        string
	enableLinkedRunner bool

	enableSubSlicing bool
}

// NewMonitor creates a new health monitor with initial default settings.
func NewMonitor(
	client client.Client,
	rc cache.MonitorRelationalCacheInterface,
	hc cache.MonitorHealthCacheInterface,
	cs services.ComputeServiceInterface,
	projectNumber string,
	location string,
	qpsLimit int,
	workerCount int,
	clusterHash string,
	enableHighGCEConcurrency bool,
	enableLinkedRunner bool,
	enableSubSlicing bool,
) (*Monitor, error) {
	region, err := util.GetRegion(location)
	if err != nil {
		return nil, fmt.Errorf("failed to extract region from location %s: %w", location, err)
	}

	m := &Monitor{
		client:             client,
		relationalCache:    rc,
		healthCache:        hc,
		GCEClient:          gcehelper.NewClient(cs, enableHighGCEConcurrency, enableSubSlicing),
		projectNumber:      projectNumber,
		location:           location,
		region:             region,
		qpsLimit:           qpsLimit,
		pollInterval:       staticPollInterval,
		workerCount:        workerCount,
		healthMonitorQueue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedItemBasedRateLimiter[Job]()),
		rateLimiter:        rate.NewLimiter(rate.Limit(qpsLimit), qpsLimit),
		processingLock:     sync.Map{},
		clusterHash:        clusterHash,
		enableLinkedRunner: enableLinkedRunner,
		enableSubSlicing:   enableSubSlicing,
	}
	return m, nil
}

// runHealthCacheGC (Garbage Collection) reconciles the health cache against the
// relational cache, removing any stale entries for deleted resources.
func (m *Monitor) runHealthCacheGC(ctx context.Context, migs []string, slices []string) {
	logger := log.FromContext(ctx).WithName("health-cache-gc")

	// 1. Build a "set" of all current, valid resources.
	currentMIGs := make(map[string]struct{}, len(migs))
	for _, migURL := range migs {
		currentMIGs[migURL] = struct{}{}
	}

	currentMMIGs := make(map[string]struct{}, len(slices))
	for _, sliceName := range slices {
		mmigURL := util.ConstructMMIGURL(m.projectNumber, m.region, sliceName, m.clusterHash)
		currentMMIGs[mmigURL] = struct{}{}
	}

	// 2. Reconcile MIGs
	cachedMIGs := m.healthCache.GetAllMIGKeys()
	for migURL := range cachedMIGs {
		if _, found := currentMIGs[migURL]; !found {
			// This MIG is in the health cache but NOT in the relational cache.
			// It's stale and needs to be deleted.
			logger.Info("Removing stale MIG from health cache", "mig", migURL)
			m.healthCache.CleanupMIGHealth(migURL)
			if m.enableSubSlicing {
				m.healthCache.CleanupMIGPartitionsStatus(migURL)
			}
		}
	}

	// 3. Reconcile MMIGs
	cachedMMIGs := m.healthCache.GetAllMMIGKeys()
	for mmigURL := range cachedMMIGs {
		if _, found := currentMMIGs[mmigURL]; !found {
			// This MMIG is in the health cache but NOT in the relational cache.
			// It's stale and needs to be deleted.
			logger.Info("Removing stale MMIG from health cache", "mmig", mmigURL)
			m.healthCache.CleanupMMIGHealth(mmigURL)
		}
	}
}

// runScheduler periodically enqueues all known resources for health checks.
func (m *Monitor) runScheduler(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("health-monitor-scheduler")

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Woke up after the interval
		case <-ctx.Done():
			// Context was cancelled
			return
		}

		logger.V(1).Info("Scheduler waking up to enqueue resources")
		slices := m.relationalCache.GetAllSliceNames()
		migs := m.relationalCache.GetAllMIGNames()
		totalItems := len(migs) + len(slices)
		logger.V(1).Info("Found resources to enqueue", "migCount", len(migs), "mmigCount", len(slices), "totalItems", totalItems)

		if totalItems == 0 {
			logger.V(1).Info("No resources found, updating liveness timestamp")
			continue // Skip the rest of the loop
		}
		// Clean up stale entries in the health cache
		m.runHealthCacheGC(ctx, migs, slices)

		queueLen := m.healthMonitorQueue.Len()
		if queueLen > maxQueueLengthBeforeSkip {
			logger.V(1).Info("Queue length exceeds threshold, skipping this cycle", "queueLength", queueLen)
			continue
		}
		// Enqueue all known MIGs and MMIGs for health checks.
		// This ensures that even if some items were dropped or failed,
		// they will be retried in the next cycle.
		for _, migURL := range migs {
			m.healthMonitorQueue.Add(Job{ResourceURL: migURL})
		}

		for _, sliceName := range slices {
			sliceInfo, found := m.relationalCache.GetSliceInfo(sliceName)
			// Sub-slices do NOT have MMIGs. They are monitored via MIG partitions.
			if m.enableSubSlicing && found && util.IsSubsliceTopology(sliceInfo.Topology) {
				continue
			}
			// Super-slices (MMIG) are ALWAYS monitored regardless of the subslice flag.
			m.healthMonitorQueue.Add(Job{ResourceURL: util.ConstructMMIGURL(m.projectNumber, m.region, sliceName, m.clusterHash)})
		}
		logger.V(1).Info("Finished enqueuing resources", "migCount", len(migs), "mmigCount", len(slices))
	}
}

// SyncHealth fetches the health for a single resource and updates the cache.
func (m *Monitor) SyncHealth(ctx context.Context, resourceURL string) error {
	entry := m.getOrCreateLockEntry(resourceURL)
	if !entry.mu.TryLock() {
		log.FromContext(ctx).Info("Resource is already being processed, skipping", "url", resourceURL)
		return nil
	}
	defer entry.mu.Unlock()
	entry.updateLastAccessed()

	// This simple routing distinguishes MIGs from MMIGs based on URL structure.
	if strings.Contains(resourceURL, migURLSubstring) {
		return m.syncMigHealth(ctx, resourceURL)
	} else if strings.Contains(resourceURL, mmigURLSubstring) {
		return m.syncMmigHealth(ctx, resourceURL)
	}
	log.FromContext(ctx).Info("Unknown resource type in health queue", "url", resourceURL)
	return nil // Don't re-queue unknown types
}

// syncMigHealth handles the logic for a single MIG health check.
func (m *Monitor) syncMigHealth(ctx context.Context, migURL string) error {
	logger := log.FromContext(ctx).WithValues("sync MIG health", migURL)

	// --- 1. Fetch Health from GCE ---
	migName, zone, err := util.ParseMigDetails(migURL)
	if err != nil {
		logger.Error(err, "Failed to parse MIG URL, dropping item")
		return NewTerminalError(fmt.Errorf("failed to parse MIG url: %s", migURL))
	}

	// Verify MIG exists in the relational cache
	migInfo, found := m.relationalCache.GetMIG(migURL)
	if !found {
		logger.Info("MIG not found in relational cache, cannot update health, deleting from health cache", migURL)
		m.healthCache.CleanupMIGHealth(migURL)
		if m.enableSubSlicing {
			m.healthCache.CleanupMIGPartitionsStatus(migURL)
		}
		return NewTerminalError(fmt.Errorf("mig not found in relational cache: %s", migURL))
	}

	// Inlined checkLinkedRunner
	var isLinkedRunner bool
	var controlNode *v1.Node
	if m.enableLinkedRunner {
		var err error
		isLinkedRunner, controlNode, err = util.IsLinkedRunnerMIG(ctx, migInfo, m.client)
		if err != nil {
			logger.Error(err, "Failed to check if MIG is for Linked Runner", "mig", migURL)
			// This is likely a transient error with the k8s API, so we'll let it be retried.
			return fmt.Errorf("failed to check if MIG %s is for Linked Runner: %w", migURL, err)
		}
	}

	// Incomplete check BEFORE GCE call
	var isMIGIncomplete bool
	var incompleteMsg string
	numNodesInMig := nodesPerMigFallback // Default fallback for backward compatibility or if not found
	for _, nodeInfo := range migInfo.Nodes {
		if nodeInfo.AcceleratorType == TpuV7XAcceleratorLabelValue {
			numNodesInMig = tpu7xNodesPerMigDefault
			break
		}
	}

	if m.enableLinkedRunner && isLinkedRunner {
		if controlNode == nil {
			return NewTerminalError(fmt.Errorf("Linked Runner MIG %s reported as true but control node is nil", migURL))
		}
		runnerPartitions, err := util.ExtractRunnerPartitionsFromControlNode(controlNode)
		if err != nil {
			logger.Error(err, "Failed to extract runner partitions from Linked Runner control node", "mig", migURL, "node", controlNode.Name)
			m.healthCache.CleanupMIGHealth(migURL)
			return NewTerminalError(fmt.Errorf("failed to extract runner partitions from control node %s for MIG %s: %w", controlNode.Name, migURL, err))
		}
		if numRunners := len(runnerPartitions); numRunners != numNodesInMig {
			logger.Info("Linked Runner MIG's control node does not have the expected number of runner instances", "mig", migURL, "node", controlNode.Name, "expected", numNodesInMig, "actual", numRunners)

			if !m.enableSubSlicing {
				m.healthCache.CleanupMIGHealth(migURL)
				return NewTerminalError(fmt.Errorf("Linked Runner MIG %s has %d runner instances, expected %d", migURL, numRunners, numNodesInMig))
			}
			isMIGIncomplete = true
			incompleteMsg = fmt.Sprintf("Linked Runner MIG %s has %d runner instances, expected %d", migURL, numRunners, numNodesInMig)
		}
	} else {
		if nodeCnt := len(migInfo.Nodes); nodeCnt != numNodesInMig {
			logger.Info("Standard MIG does not have the required number of nodes", "mig", migURL, "expected", numNodesInMig, "actual", nodeCnt)
			if !m.enableSubSlicing {
				m.healthCache.CleanupMIGHealth(migURL)
				return NewTerminalError(fmt.Errorf("mig does not have full nodes: %s, expected: %d, actual: %d", migURL, numNodesInMig, nodeCnt))
			}
			isMIGIncomplete = true
			incompleteMsg = fmt.Sprintf("mig does not have full nodes: %s, expected: %d, actual: %d", migURL, numNodesInMig, nodeCnt)
		}
	}

	// Wait for a token from the rate limiter before making the API call.
	if err := m.rateLimiter.Wait(ctx); err != nil {
		logger.Error(err, "Context cancelled while waiting for rate limiter")
		return err
	}

	// Get the accelerator topology health from GCE.
	topologyHealthInfo, err := m.GCEClient.GetAvailableAcceleratorTopologies(ctx, m.projectNumber, zone, migName)
	if err != nil {
		// On ANY failure to get health, immediately set the status to Unknown.
		// This guarantees the HealthCache is always consistent.
		logger.Error(err, "Failed to get MIG health from GCE")
		m.healthCache.SetMIGHealth(migURL, cache.MIGHealthStatusUnknown, err.Error())

		// Mark all partitions for this MIG as Unknown in the topologies cache if sub-slicing is enabled.
		if m.enableSubSlicing {
			topos := make(map[string]cache.AcceleratorTopologyInfo)
			m.initializeTopologiesFromCache(topos, migInfo, cache.MIGHealthStatusUnknown)
			m.healthCache.SetMIGPartitionsStatus(migURL, topos)
		}

		// If it's a permanent error, don't re-queue.
		// Otherwise, return the error to re-queue.
		if isPermanentError(err) {
			logger.Info("Permanent error from GCE API, dropping from health checks", "error", err)
			return NewTerminalError(fmt.Errorf("permanent error from GCE API for %s: %w", migURL, err))
		}
		return err
	}
	logger.V(2).Info("DEBUG: Raw GCE API topology response", "response", topologyHealthInfo)

	internalStatus, aggErr := aggregateTopologyHealth(&topologyHealthInfo)
	if aggErr != nil {
		// On failure to parse/aggregate health, set status to Unknown.
		logger.Error(aggErr, "Failed to aggregate topology health")
		internalStatus = cache.MIGHealthStatusUnknown
	}

	statusMsg := ""
	if aggErr != nil {
		statusMsg = aggErr.Error()
	}

	if isMIGIncomplete {
		if aggErr != nil || internalStatus == cache.MIGHealthStatusHealthy || internalStatus == cache.MIGHealthStatusUnknown {
			internalStatus = cache.MIGHealthStatusIncomplete
			aggErr = nil // We "successfully" determined it's Incomplete
			statusMsg = incompleteMsg
		}
	}

	// --- 2. Update Health Cache ---
	if m.enableSubSlicing {
		// Prepare topology-specific health map (topoID -> info)
		topologies := make(map[string]cache.AcceleratorTopologyInfo)

		// If we are missing nodes, some partitions might be missing from GCE response.
		// We default them to Incomplete based on our relational cache.
		if isMIGIncomplete {
			m.initializeTopologiesFromCache(topologies, migInfo, cache.MIGHealthStatusIncomplete)
		}

		// Overwrite with GCE information which is the source of truth for partition health.
		m.updateTopologiesFromGCE(topologies, &topologyHealthInfo, logger)

		m.healthCache.SetMIGPartitionsStatus(migURL, topologies)
	}

	m.healthCache.SetMIGHealth(migURL, internalStatus, statusMsg)

	if aggErr != nil {
		return NewTerminalError(fmt.Errorf("unable to aggregate the health status of the MIG: %w", aggErr))
	}
	logger.V(1).Info("Successfully synced MIG health", "aggregatedStatus", internalStatus)
	return nil
}

func (m *Monitor) initializeTopologiesFromCache(topologies map[string]cache.AcceleratorTopologyInfo, migInfo cache.MIGInfo, status cache.MIGHealthStatus) {
	for _, node := range migInfo.Nodes {
		for topoLabel, topoIDs := range node.TopologyPartitionIds {
			for _, topoID := range topoIDs {
				topologies[topoID] = cache.AcceleratorTopologyInfo{
					AcceleratorTopology:             topoLabel,
					AcceleratorTopologyHealthStatus: status,
				}
			}
		}
	}
}

func (m *Monitor) updateTopologiesFromGCE(topologies map[string]cache.AcceleratorTopologyInfo, topologyHealthInfo *cache.AcceleratorTopologies, logger logr.Logger) {
	// Overwrite with GCE information which is the source of truth for partition health.
	for topoID, info := range topologyHealthInfo.TopologiesInfo {
		// 4x4x4 is a full topology, so we can unconditionally skip it.
		if info.AcceleratorTopology == util.Topology4x4x4 {
			continue
		}

		// Verify ownership via SliceID if mapping exists in our cache.
		if sliceName, exists := m.relationalCache.GetSliceNameByPartitionID(topoID); exists {
			expectedID := util.GetResourceName(sliceName, m.clusterHash)
			if info.SliceID == "" {
				logger.Info("Slice ID is empty during health check, slice might not have formed yet",
					"partition", topoID, "expected", expectedID)
			} else if info.SliceID != expectedID {
				logger.Error(fmt.Errorf("Slice ID mismatch during health check"),
					"Partition not owned by expected slice",
					"partition", topoID, "expected", expectedID, "found", info.SliceID)
			}
		}

		topologies[topoID] = info
	}
}

func (m *Monitor) syncMmigHealth(ctx context.Context, mmigURL string) error {
	logger := log.FromContext(ctx).WithValues("sync MMIG health", mmigURL)
	_, region, mmigName, err := parseMmigDetails(mmigURL)
	if err != nil {
		logger.Error(err, "Failed to parse MMIG URL, dropping item")
		return NewTerminalError(errors.New("failed to parse MMIG URL"))
	}
	sliceName := util.ExtractSliceNameFromResourceName(mmigName, m.clusterHash)
	// Verify Slice exits in the relational cache
	// MMIG name is same as the slice name.
	_, found := m.relationalCache.GetSliceInfo(sliceName)
	if !found {
		logger.Info("MMIG not found in relational cache, cannot update health, deleting from health cache", mmigURL)
		m.healthCache.CleanupMMIGHealth(mmigURL)
		return NewTerminalError(errors.New("MMIG not found in relational cache"))
	}

	// Wait for a token from the rate limiter before making the API call.
	if err := m.rateLimiter.Wait(ctx); err != nil {
		logger.Error(err, "Context cancelled while waiting for rate limiter")
		return err
	}

	// Get current MMIG health from GCE.
	gceMmigHealthStatus, err := m.GCEClient.GetMMIGHealthStatus(ctx, m.projectNumber, region, mmigName)
	if err != nil {
		logger.Error(err, "Failed to get MMIG health from GCE")
		// On ANY failure to get health, immediately set the status to Unknown.
		// This guarantees the HealthCache is always consistent.
		// If it's a permanent error, don't re-queue.
		// Otherwise, return the error to re-queue.
		errMsg := err.Error()
		if isNotFoundError(err) {
			errMsg = mmigNotFoundErrorMsg
			m.healthCache.SetMMIGHealth(mmigURL, cache.MMIGHealthStatusNotFound, errMsg)
			return NewTerminalError(fmt.Errorf("mmig not found: %s", mmigURL))
		}
		if isPermanentError(err) {
			logger.Info("Permanent error from GCE API, dropping from health checks", "error", err)
			m.healthCache.SetMMIGHealth(mmigURL, cache.MMIGHealthStatusProbeFailed, errMsg)
			return NewTerminalError(fmt.Errorf("permanent error from GCE API for %s", mmigURL))
		}
		m.healthCache.SetMMIGHealth(mmigURL, cache.MMIGHealthStatusProbeFailed, errMsg)
		return err
	}

	if len(gceMmigHealthStatus.TopologyHealths) == 0 {
		logger.Info("No topology health information found in MMIG health status, setting to Unknown")
		m.healthCache.SetMMIGHealth(mmigURL, cache.MMIGHealthStatusUnknown, "no topology health information found")
		return NewTerminalError(errors.New("no topology health information found in MMIG health status"))
	}

	sliceHealth := gceMmigHealthStatus.TopologyHealths[0]
	errMsg := ""
	if sliceHealth.HealthStatus == cache.MMIGHealthStatusFailed {
		logger.Info("MMIG topology health indicates failure", "topology", sliceHealth.AcceleratorTopology, "status", sliceHealth.HealthStatus)
		errMsg = strings.Join(sliceHealth.LastCheckErrorMessages, "; ")
	}

	m.healthCache.SetMMIGHealth(mmigURL, sliceHealth.HealthStatus, errMsg)
	logger.V(1).Info("Successfully synced MMIG health", "status", sliceHealth.HealthStatus)
	return nil
}

// worker is the main loop for a single worker goroutine.
func (m *Monitor) worker(ctx context.Context) {
	for m.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem processes a single item from the health monitor queue.
func (m *Monitor) processNextWorkItem(ctx context.Context) bool {
	job, shutdown := m.healthMonitorQueue.Get()
	if shutdown {
		return false
	}
	defer m.healthMonitorQueue.Done(job)

	err := m.SyncHealth(ctx, job.ResourceURL)

	if err == nil {
		m.healthMonitorQueue.Forget(job)
		return true
	}

	// Check if the error is terminal using our helper
	if IsTerminal(err) {
		log.FromContext(ctx).Error(err, "Permanent error, dropping from queue", "resource", job.ResourceURL)
		m.healthMonitorQueue.Forget(job)
		return true
	}

	// Retryable logic
	numRequeues := m.healthMonitorQueue.NumRequeues(job)
	if numRequeues < maxRequeues {
		m.healthMonitorQueue.AddRateLimited(job)
		log.FromContext(ctx).Error(err, "Failed to sync health, re-queuing", "resource", job.ResourceURL)
	} else {
		m.healthMonitorQueue.Forget(job)
		log.FromContext(ctx).Error(err, "Max requeues reached, dropping", "resource", job.ResourceURL, "requeues", numRequeues)
	}

	return true
}

// Start begins the health monitoring process. It's a blocking call.
func (m *Monitor) Start(ctx context.Context) error {

	logger := log.FromContext(ctx).WithName("health-monitor-start")
	logger.Info("Starting health monitor", "workers", m.workerCount, "pollInterval", m.pollInterval.String())

	// --- Start the Scheduler ---
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		m.runScheduler(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		m.runProcessingLockCleanUp(ctx)
	}()

	for i := 0; i < m.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wait.UntilWithContext(ctx, m.worker, time.Second)
		}()
	}

	<-ctx.Done()
	logger.Info("Shutting down health monitor")

	logger.Info("Shutting down queues...")
	m.healthMonitorQueue.ShutDown()

	logger.Info("Waiting for all goroutines to stop...")
	wg.Wait()

	logger.Info("Health monitor shut down complete")
	return nil
}

// NeedLeaderElection indicates whether this monitor requires leader election.
func (m *Monitor) NeedLeaderElection() bool {
	return true
}
