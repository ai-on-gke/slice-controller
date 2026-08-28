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
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// lockEntry represents a single entry in the processing lock map.
type lockEntry struct {
	mu           *sync.Mutex
	lastAccessed time.Time
	accessMu     sync.RWMutex
}

// getOrCreateLockEntry retrieves or creates a lock entry for the given resource URL.
func (m *Monitor) getOrCreateLockEntry(resourceURL string) *lockEntry {
	actual, _ := m.processingLock.LoadOrStore(resourceURL, &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now(),
	})
	entry, ok := actual.(*lockEntry)
	if !ok {
		panic(fmt.Sprintf("invalid type stored in processingLock map for key %s", resourceURL))
	}
	return entry
}

// updateLastAccessed updates the last accessed time of the lock entry.
func (entry *lockEntry) updateLastAccessed() {
	entry.accessMu.Lock()
	entry.lastAccessed = time.Now()
	entry.accessMu.Unlock()
}

// isStale checks if the lock entry is stale based on the provided timeout.
func (entry *lockEntry) isStale() bool {
	entry.accessMu.RLock()
	defer entry.accessMu.RUnlock()
	return time.Since(entry.lastAccessed) > lockEntryTimeout
}

// runProcessingLockCleanUp periodically cleans up stale lock entries.
func (m *Monitor) runProcessingLockCleanUp(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("ProcessingLockCleaner")
	ticker := time.NewTicker(lockCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupStaleLocks(logger)
		case <-ctx.Done():
			logger.Info("Stopping processing lock cleanup")
			return
		}
	}
}

// cleanupStaleLocks removes stale lock entries from the processing lock map.
func (m *Monitor) cleanupStaleLocks(logger logr.Logger) {
	cleaned := 0
	total := 0

	m.processingLock.Range(func(key, value interface{}) bool {
		total++
		entry, ok := value.(*lockEntry)
		if !ok {
			logger.Error(fmt.Errorf("invalid type in processingLock map"), "key", key)
			return true
		}

		if entry.mu.TryLock() {
			if entry.isStale() {
				m.processingLock.Delete(key)
				cleaned++
				logger.V(2).Info("Cleaned up stale lock entry", "resourceURL", key)
			}
			entry.mu.Unlock()
		}

		return true
	})

	if cleaned > 0 {
		logger.Info("Completed processing lock cleanup", "cleaned", cleaned, "total", total)
	}
}
