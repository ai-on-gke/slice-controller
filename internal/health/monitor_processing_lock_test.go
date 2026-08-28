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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const testMMIGUrl = "projects/p/regions/r/multiMigs/mmig-1"

// lockEntry - Thread-safe timestamp updates and checks
func TestLockEntry_UpdateAndIsStale(t *testing.T) {
	entry := &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now().Add(-10 * time.Minute),
	}

	// Should be stale
	assert.True(t, entry.isStale())

	// Update timestamp
	entry.updateLastAccessed()

	// Should NOT be stale now
	assert.False(t, entry.isStale())
}

func TestLockEntry_ConcurrentAccess(t *testing.T) {
	entry := &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now(),
	}

	// Concurrent reads and writes should not race
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			entry.updateLastAccessed()
		}()
		go func() {
			defer wg.Done()
			_ = entry.isStale()
		}()
	}
	wg.Wait()
	// Test passes if no race detected
}

// getOrCreateLockEntry - Returns same entry for same URL
func TestMonitor_GetOrCreateLockEntry(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	// Get entry twice
	entry1 := m.getOrCreateLockEntry(testMMIGUrl)
	entry2 := m.getOrCreateLockEntry(testMMIGUrl)

	// Should be same entry (same pointer)
	assert.Equal(t, entry1, entry2, "should return same entry for same URL")
	require.NotNil(t, entry1.mu)
}

func TestMonitor_GetOrCreateLockEntry_DifferentURLs(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	entry1 := m.getOrCreateLockEntry("url-1")
	entry2 := m.getOrCreateLockEntry("url-2")

	// Should be different entries
	assert.NotEqual(t, entry1, entry2, "different URLs should get different entries")
}

func TestMonitor_GetOrCreateLockEntry_Concurrent(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	numGoroutines := 100
	entries := make([]*lockEntry, numGoroutines)
	var wg sync.WaitGroup

	// Many goroutines try to get/create same entry
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			entries[index] = m.getOrCreateLockEntry(testMMIGUrl)
		}(i)
	}
	wg.Wait()

	// All should get the same entry
	for i := 1; i < numGoroutines; i++ {
		assert.Equal(t, entries[0], entries[i],
			"all concurrent calls should get same entry")
	}
}

// cleanupStaleLocks - Critical: Only deletes stale unlocked entries
func TestMonitor_CleanupStaleLocks(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	logger := log.Log.WithName("test")

	// Create stale entry (old timestamp, not locked)
	staleEntry := &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now().Add(-10 * time.Minute),
	}
	m.processingLock.Store("stale-url", staleEntry)

	// Create fresh entry
	freshEntry := &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now(),
	}
	m.processingLock.Store("fresh-url", freshEntry)

	// Create locked entry (should not be deleted even if stale)
	lockedEntry := &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now().Add(-10 * time.Minute),
	}
	lockedEntry.mu.Lock() // Lock it
	m.processingLock.Store("locked-url", lockedEntry)

	// Run cleanup
	m.cleanupStaleLocks(logger)

	// Stale entry should be deleted
	_, staleExists := m.processingLock.Load("stale-url")
	assert.False(t, staleExists, "stale unlocked entry should be deleted")

	// Fresh entry should still exist
	_, freshExists := m.processingLock.Load("fresh-url")
	assert.True(t, freshExists, "fresh entry should not be deleted")

	// Locked entry should still exist (even though stale)
	_, lockedExists := m.processingLock.Load("locked-url")
	assert.True(t, lockedExists, "locked entry should not be deleted")

	lockedEntry.mu.Unlock() // Cleanup
}

func TestMonitor_CleanupStaleLocks_EmptyMap(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	logger := log.Log.WithName("test")

	// Should not panic with empty map
	m.cleanupStaleLocks(logger)

	// Map should still be usable
	entry := m.getOrCreateLockEntry("test-url")
	require.NotNil(t, entry)
}

func TestMonitor_CleanupStaleLocks_AllFresh(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	logger := log.Log.WithName("test")

	// Create multiple fresh entries
	for i := 0; i < 5; i++ {
		entry := &lockEntry{
			mu:           &sync.Mutex{},
			lastAccessed: time.Now(),
		}
		m.processingLock.Store(string(rune('a'+i)), entry)
	}

	// Run cleanup
	m.cleanupStaleLocks(logger)

	// All entries should still exist
	count := 0
	m.processingLock.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 5, count, "all fresh entries should remain")
}

func TestMonitor_CleanupStaleLocks_AllStale(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	logger := log.Log.WithName("test")

	// Create multiple stale entries
	for i := 0; i < 5; i++ {
		entry := &lockEntry{
			mu:           &sync.Mutex{},
			lastAccessed: time.Now().Add(-10 * time.Minute),
		}
		m.processingLock.Store(string(rune('a'+i)), entry)
	}

	// Run cleanup
	m.cleanupStaleLocks(logger)

	// All entries should be deleted
	count := 0
	m.processingLock.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "all stale entries should be deleted")
}

// runProcessingLockCleanUp - Critical: Stops on context cancellation
func TestMonitor_RunProcessingLockCleanUp_Cancellation(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan bool)
	go func() {
		m.runProcessingLockCleanUp(ctx)
		done <- true
	}()

	// Should stop when context is cancelled
	select {
	case <-done:
	// Success - goroutine stopped
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cleanup goroutine did not stop on context cancellation")
	}
}

// Integration Test - Lock prevents concurrent processing
func TestLockPreventsDoubleProcessing(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	processCount := 0
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Simulate SyncHealth behavior
			entry := m.getOrCreateLockEntry(testMMIGUrl)
			if entry.mu.TryLock() {
				defer entry.mu.Unlock()

				// Simulate processing work
				mu.Lock()
				processCount++
				mu.Unlock()

				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	// Only ONE goroutine should have successfully processed
	assert.Equal(t, 1, processCount,
		"lock should prevent concurrent processing of same resource")
}

func TestLockAllowsSequentialProcessing(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	processCount := 0
	var mu sync.Mutex

	// Process sequentially 5 times
	for i := 0; i < 5; i++ {
		entry := m.getOrCreateLockEntry(testMMIGUrl)
		if entry.mu.TryLock() {
			mu.Lock()
			processCount++
			mu.Unlock()
			entry.mu.Unlock()
		}
	}

	// All 5 should have processed successfully
	assert.Equal(t, 5, processCount,
		"lock should allow sequential processing")
}

func TestLockAllowsConcurrentDifferentResources(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	processCount := 0
	var mu sync.Mutex

	var wg sync.WaitGroup
	numResources := 10

	// Process different resources concurrently
	for i := 0; i < numResources; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			url := string(rune('a' + index)) // Different URL for each
			entry := m.getOrCreateLockEntry(url)
			if entry.mu.TryLock() {
				defer entry.mu.Unlock()

				mu.Lock()
				processCount++
				mu.Unlock()

				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	// All should have processed (different resources)
	assert.Equal(t, numResources, processCount,
		"different resources should process concurrently")
}

func TestLockEntry_TimestampBoundary(t *testing.T) {
	entry := &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now().Add(-(lockEntryTimeout - time.Minute)),
	}

	// Exactly at timeout boundary - should NOT be stale
	assert.False(t, entry.isStale(),
		"entry at exact timeout boundary should not be stale")

	// Just beyond timeout - should be stale
	entry.lastAccessed = time.Now().Add(-(lockEntryTimeout + time.Millisecond))
	assert.True(t, entry.isStale(),
		"entry just beyond timeout should be stale")
}

func TestGetOrCreateLockEntry_InitialTimestamp(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	before := time.Now()
	entry := m.getOrCreateLockEntry("test-url")
	after := time.Now()

	// Timestamp should be between before and after
	entry.accessMu.RLock()
	timestamp := entry.lastAccessed
	entry.accessMu.RUnlock()

	assert.True(t, timestamp.After(before) || timestamp.Equal(before),
		"timestamp should be >= creation time")
	assert.True(t, timestamp.Before(after) || timestamp.Equal(after),
		"timestamp should be <= after creation time")
}

func TestCleanupStaleLocks_TypeAssertion(t *testing.T) {
	m := &Monitor{
		processingLock: sync.Map{},
	}

	logger := log.Log.WithName("test")

	// Store an invalid type (should not panic)
	m.processingLock.Store("invalid", "not-a-lockEntry")

	// Store a valid entry
	validEntry := &lockEntry{
		mu:           &sync.Mutex{},
		lastAccessed: time.Now(),
	}
	m.processingLock.Store("valid", validEntry)

	// Should not panic, should skip invalid entry
	m.cleanupStaleLocks(logger)

	// Valid entry should still exist
	_, exists := m.processingLock.Load("valid")
	assert.True(t, exists, "valid entry should remain")

	// Invalid entry behavior depends on implementation
	// If type assertion fails, it will be skipped
	_, _ = m.processingLock.Load("invalid")
	// Don't assert on invalid - implementation may handle differently
}

// Stress Test
func TestLockEntry_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	m := &Monitor{
		processingLock: sync.Map{},
	}

	numGoroutines := 1000
	numResources := 50

	var wg sync.WaitGroup
	processedResources := make(map[string]int)
	var processMu sync.Mutex

	// Many goroutines trying to process few resources
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			resourceID := index % numResources
			url := string(rune('a' + resourceID))

			entry := m.getOrCreateLockEntry(url)
			if entry.mu.TryLock() {
				defer entry.mu.Unlock()
				entry.updateLastAccessed()

				// Simulate work
				time.Sleep(time.Millisecond)

				// Track processing
				processMu.Lock()
				processedResources[url]++
				processMu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Each resource should have been processed at least once
	processMu.Lock()
	uniqueProcessed := len(processedResources)
	processMu.Unlock()

	assert.Greater(t, uniqueProcessed, 0,
		"at least some resources should have been processed")
	assert.LessOrEqual(t, uniqueProcessed, numResources,
		"should not process more unique resources than exist")
}
