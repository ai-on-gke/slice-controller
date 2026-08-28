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

package cache

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// HealthStatusChangeListener is an interface for listening to health status changes.
type HealthStatusChangeListener interface {
	// OnMIGHealthStatusChange is called whenever the health status of a MIG resource changes.
	OnMIGHealthStatusChange(resourceURL string)
	// OnMIGPartitionStateChange is called whenever the accelerator topologies of a MIG resource change.
	OnMIGPartitionStateChange(resourceURL string)
	// OnMMIGHealthStatusChange is called whenever the health status of an MMIG resource changes.
	OnMMIGHealthStatusChange(resourceURL string)
}

// TimestampedHealth wraps health status (MIG or MMIG) with a
// timestamp and an optional error message.
type TimestampedHealth[T comparable] struct {
	CurrentStatus T
	LastStatus    T
	// LastUpdated is the timestamp when CurrentStatus was last updated.
	LastUpdated time.Time
	// ErrorMessage stores the last error message associated with this resource.
	ErrorMessage string
}

// resourceCache is a generic, thread-safe cache for one type of resource.
type resourceCache[T comparable] struct {
	mu            sync.RWMutex
	health        map[string]TimestampedHealth[T] // Key: Resource URL
	unknownStatus T
}

// newResourceCache creates a new generic resource cache.
func newResourceCache[T comparable](unknownStatus T) *resourceCache[T] {
	return &resourceCache[T]{
		health:        make(map[string]TimestampedHealth[T]),
		unknownStatus: unknownStatus,
	}
}

// SetHealth updates the health status and error message for a given resource.
// It returns a boolean indicating if a change occurred.
func (rc *resourceCache[T]) SetHealth(url string, status T, errMsg string) (changed bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	oldStatus, found := rc.health[url]
	if !found {
		oldStatus = TimestampedHealth[T]{
			CurrentStatus: rc.unknownStatus,
		}
	}

	if oldStatus.CurrentStatus == status && oldStatus.ErrorMessage == errMsg {
		return false
	}

	// A change was detected, create the new record.
	rc.health[url] = TimestampedHealth[T]{
		CurrentStatus: status,
		LastStatus:    oldStatus.CurrentStatus,
		LastUpdated:   time.Now(),
		ErrorMessage:  errMsg,
	}
	return true
}

// GetHealth retrieves the timestamped health status for a given resource.
func (rc *resourceCache[T]) GetHealth(url string) (TimestampedHealth[T], bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	status, found := rc.health[url]
	return status, found
}

// GetAllKeys returns a set of all resource URLs currently in the cache.
func (rc *resourceCache[T]) GetAllKeys() map[string]struct{} {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	keys := make(map[string]struct{}, len(rc.health))
	for k := range rc.health {
		keys[k] = struct{}{}
	}
	return keys
}

// Cleanup removes a resource's health data from the cache.
func (rc *resourceCache[T]) Cleanup(url string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	delete(rc.health, url)
}

// HealthCache stores the latest health status for all monitored MIGs and MMIGs.
type HealthCache struct {
	migHealth  *resourceCache[MIGHealthStatus]
	mmigHealth *resourceCache[MMIGHealthStatus]

	migPartitionsStatus map[string]map[string]AcceleratorTopologyInfo
	migPartitionsMu     sync.RWMutex

	listeners  []HealthStatusChangeListener
	listenerMu sync.RWMutex

	probeFailureMu        sync.Mutex
	probeFailureStart     map[string]time.Time
	probeFailureThreshold time.Duration
}

// NewHealthCache creates and returns a new HealthCache.
func NewHealthCache() *HealthCache {
	return &HealthCache{
		migHealth:             newResourceCache(MIGHealthStatusUnknown),
		mmigHealth:            newResourceCache(MMIGHealthStatusUnknown),
		migPartitionsStatus:   make(map[string]map[string]AcceleratorTopologyInfo),
		listeners:             make([]HealthStatusChangeListener, 0, 1),
		probeFailureStart:     make(map[string]time.Time),
		probeFailureThreshold: 15 * time.Second,
	}
}

// RegisterListener adds a new listener to be notified on health status changes.
func (c *HealthCache) RegisterListener(listener HealthStatusChangeListener) {
	c.listenerMu.Lock()
	defer c.listenerMu.Unlock()
	c.listeners = append(c.listeners, listener)
}

// UnregisterListener removes a listener from the notification list.
func (c *HealthCache) UnregisterListener(listener HealthStatusChangeListener) {
	c.listenerMu.Lock()
	defer c.listenerMu.Unlock()
	for i, l := range c.listeners {
		if l == listener {
			c.listeners[i] = c.listeners[len(c.listeners)-1]
			c.listeners = c.listeners[:len(c.listeners)-1]
			return
		}
	}
}

// --- MIG Methods ---

// SetMIGHealth updates the health status for a given MIG and notifies listeners.
func (c *HealthCache) SetMIGHealth(migURL string, status MIGHealthStatus, errMsg string) {
	changed := c.migHealth.SetHealth(migURL, status, errMsg)
	if !changed {
		return
	}
	c.notifyMIGHealthChange(migURL)
}

func acceleratorTopologyInfoEquals(a, b AcceleratorTopologyInfo) bool {
	if a.AcceleratorTopology != b.AcceleratorTopology ||
		a.AcceleratorTopologyHealthStatus != b.AcceleratorTopologyHealthStatus ||
		a.AcceleratorTopologyState != b.AcceleratorTopologyState ||
		a.SliceID != b.SliceID ||
		a.InstancesHealth != b.InstancesHealth {
		return false
	}
	if len(a.Errors) != len(b.Errors) {
		return false
	}

	errorsA := slices.Clone(a.Errors)
	errorsB := slices.Clone(b.Errors)

	sortFn := func(i, j AcceleratorTopologyError) int {
		if c := cmp.Compare(i.Code, j.Code); c != 0 {
			return c
		}
		return cmp.Compare(i.Message, j.Message)
	}
	slices.SortFunc(errorsA, sortFn)
	slices.SortFunc(errorsB, sortFn)

	for i := range errorsA {
		if errorsA[i].Code != errorsB[i].Code || errorsA[i].Message != errorsB[i].Message {
			return false
		}
	}
	return true
}

func areTopologiesChanged(current, new map[string]AcceleratorTopologyInfo) bool {
	if len(current) != len(new) {
		return true
	}
	for k, newInfo := range new {
		oldInfo, ok := current[k]
		if !ok || !acceleratorTopologyInfoEquals(oldInfo, newInfo) {
			return true
		}
	}
	return false
}

// SetMIGPartitionsStatus updates the accelerator topologies status for a given MIG.
func (c *HealthCache) SetMIGPartitionsStatus(migURL string, topologies map[string]AcceleratorTopologyInfo) {
	c.migPartitionsMu.Lock()
	defer c.migPartitionsMu.Unlock()

	currentTopologies := c.migPartitionsStatus[migURL]

	if !areTopologiesChanged(currentTopologies, topologies) {
		return
	}

	c.migPartitionsStatus[migURL] = topologies
	c.notifyMIGPartitionStateChange(migURL)
}

// GetMIGHealth retrieves the timestamped health status for a given MIG.
func (c *HealthCache) GetMIGHealth(migURL string) (TimestampedHealth[MIGHealthStatus], bool) {
	return c.migHealth.GetHealth(migURL)
}

// GetMIGPartitionsStatus retrieves the topology-specific health statuses for a given MIG.
func (c *HealthCache) GetMIGPartitionsStatus(migURL string) (map[string]AcceleratorTopologyInfo, bool) {
	c.migPartitionsMu.RLock()
	defer c.migPartitionsMu.RUnlock()
	topos, found := c.migPartitionsStatus[migURL]
	if !found {
		return nil, false
	}
	// Return a copy to be safe
	copy := make(map[string]AcceleratorTopologyInfo, len(topos))
	for k, v := range topos {
		copy[k] = v
	}
	return copy, true
}

// CleanupMIGHealth removes a MIG's health data from the cache.
func (c *HealthCache) CleanupMIGHealth(migURL string) {
	c.migHealth.Cleanup(migURL)
}

// CleanupMIGPartitionsStatus removes a MIG's topology data from the cache.
func (c *HealthCache) CleanupMIGPartitionsStatus(migURL string) {
	c.migPartitionsMu.Lock()
	defer c.migPartitionsMu.Unlock()
	delete(c.migPartitionsStatus, migURL)
	c.notifyMIGPartitionStateChange(migURL)
}

// GetAllMIGKeys returns a set of all MIG URLs currently in the cache.
func (c *HealthCache) GetAllMIGKeys() map[string]struct{} {
	return c.migHealth.GetAllKeys()
}

// --- MMIG Methods ---

// SetMMIGHealth updates the health status for a given MMIG and notifies listeners.
func (c *HealthCache) SetMMIGHealth(mmigURL string, status MMIGHealthStatus, errMsg string) {
	probeFailureThreshold := c.probeFailureThreshold

	if status == MMIGHealthStatusProbeFailed {
		c.probeFailureMu.Lock()
		startTime, alreadyFailing := c.probeFailureStart[mmigURL]

		if !alreadyFailing {
			// Case A: First time failure. Start timer, suppress update.
			c.probeFailureStart[mmigURL] = time.Now()
			c.probeFailureMu.Unlock()
			return
		}

		// Case B: Ongoing failure. Check threshold.
		if time.Since(startTime) < probeFailureThreshold {
			// Still within grace period. Suppress update.
			c.probeFailureMu.Unlock()
			return
		}

		// Case C: Threshold exceeded. Proceed to update.
		c.probeFailureMu.Unlock()
	} else {
		// 2. Reset Timer for any other state (Active, Unknown, etc.)
		c.probeFailureMu.Lock()
		delete(c.probeFailureStart, mmigURL)
		c.probeFailureMu.Unlock()
	}

	// 3. Apply Update
	changed := c.mmigHealth.SetHealth(mmigURL, status, errMsg)
	if !changed {
		return
	}
	c.notifyMMIGHealthChange(mmigURL)
}

// GetMMIGHealth retrieves the timestamped health status for a given MMIG.
func (c *HealthCache) GetMMIGHealth(mmigURL string) (TimestampedHealth[MMIGHealthStatus], bool) {
	return c.mmigHealth.GetHealth(mmigURL)
}

// CleanupMMIGHealth removes an MMIG's health data from the cache.
func (c *HealthCache) CleanupMMIGHealth(mmigURL string) {
	c.probeFailureMu.Lock()
	delete(c.probeFailureStart, mmigURL)
	c.probeFailureMu.Unlock()

	c.mmigHealth.Cleanup(mmigURL)
}

// GetAllMMIGKeys returns a set of all MMIG URLs currently in the cache.
func (c *HealthCache) GetAllMMIGKeys() map[string]struct{} {
	return c.mmigHealth.GetAllKeys()
}

// --- Private Notifier Methods ---

// notifyMIGHealthChange notifies all registered listeners about a MIG health status change.
func (c *HealthCache) notifyMIGHealthChange(migURL string) {
	c.listenerMu.RLock()
	defer c.listenerMu.RUnlock()

	for _, l := range c.listeners {
		listenerCopy := l
		go func() {
			// Add the panic recovery
			defer func() {
				if r := recover(); r != nil {
					log.Log.Error(nil, "Panic recovered in MIG health change listener", "error", r)
				}
			}()
			listenerCopy.OnMIGHealthStatusChange(migURL)
		}()
	}
}

// notifyMIGPartitionStateChange notifies all registered listeners about a MIG topologies change.
func (c *HealthCache) notifyMIGPartitionStateChange(migURL string) {
	c.listenerMu.RLock()
	defer c.listenerMu.RUnlock()

	for _, l := range c.listeners {
		listenerCopy := l
		go func() {
			// Add the panic recovery
			defer func() {
				if r := recover(); r != nil {
					log.Log.Error(nil, "Panic recovered in MIG topologies change listener", "error", r)
				}
			}()
			listenerCopy.OnMIGPartitionStateChange(migURL)
		}()
	}
}

// notifyMMIGHealthChange notifies all registered listeners about an MMIG health status change.
func (c *HealthCache) notifyMMIGHealthChange(mmigURL string) {
	c.listenerMu.RLock()
	defer c.listenerMu.RUnlock()

	for _, l := range c.listeners {
		listenerCopy := l
		go func() {
			// Add the panic recovery
			defer func() {
				if r := recover(); r != nil {
					log.Log.Error(nil, "Panic recovered in MMIG health change listener", "error", r)
				}
			}()
			listenerCopy.OnMMIGHealthStatusChange(mmigURL)
		}()
	}
}
