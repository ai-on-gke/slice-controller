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
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// migNotification is a helper struct to pass data over a channel
type migNotification struct {
	resourceURL string
}

// mmigNotification is a helper struct to pass data over a channel
type mmigNotification struct {
	resourceURL string
}

// mockListener is a test implementation of the HealthStatusChangeListener interface
type mockListener struct {
	migNotify  chan migNotification
	mmigNotify chan mmigNotification
}

// newMockListener creates a new mock listener for testing
func newMockListener(bufferSize int) *mockListener {
	return &mockListener{
		migNotify:  make(chan migNotification, bufferSize),
		mmigNotify: make(chan mmigNotification, bufferSize),
	}
}

// OnMIGHealthStatusChange implements the interface and sends the notification on a channel
func (m *mockListener) OnMIGHealthStatusChange(url string) {
	m.migNotify <- migNotification{
		resourceURL: url,
	}
}

// OnMIGPartitionStateChange implements the interface and sends the notification on a channel
func (m *mockListener) OnMIGPartitionStateChange(url string) {
	m.migNotify <- migNotification{
		resourceURL: url,
	}
}

// OnMMIGHealthStatusChange implements the interface and sends the notification on a channel
func (m *mockListener) OnMMIGHealthStatusChange(url string) {
	m.mmigNotify <- mmigNotification{
		resourceURL: url,
	}
}

func checkRecent(t *testing.T, ts time.Time) {
	t.Helper()
	if time.Since(ts) > 5*time.Second {
		t.Errorf("Timestamp is not recent, %v old", time.Since(ts))
	}
}

func TestNewHealthCache(t *testing.T) {
	t.Parallel()
	c := NewHealthCache()

	if c == nil {
		t.Fatal("NewHealthCache() returned nil")
	}
	if c.migHealth == nil {
		t.Error("migHealth cache was not initialized")
	}
	if c.mmigHealth == nil {
		t.Error("mmigHealth cache was not initialized")
	}
}

func TestHealthCache_MIGs(t *testing.T) {
	t.Parallel()
	c := NewHealthCache()
	migURL := "mig-url-1"

	t.Run("Get Non-Existent", func(t *testing.T) {
		_, found := c.GetMIGHealth(migURL)
		if found {
			t.Error("Expected found=false for non-existent MIG")
		}
	})

	t.Run("Set and Get New", func(t *testing.T) {
		c.SetMIGHealth(migURL, MIGHealthStatusHealthy, "all good")
		status, found := c.GetMIGHealth(migURL)
		if !found {
			t.Fatal("Expected found=true for newly set MIG")
		}

		if status.CurrentStatus != MIGHealthStatusHealthy {
			t.Errorf("Want CurrentStatus %v, got %v", MIGHealthStatusHealthy, status.CurrentStatus)
		}
		if status.LastStatus != MIGHealthStatusUnknown {
			t.Errorf("Want LastStatus %v, got %v", MIGHealthStatusUnknown, status.LastStatus)
		}
		if status.ErrorMessage != "all good" {
			t.Errorf("Want ErrorMessage 'all good', got '%s'", status.ErrorMessage)
		}
		checkRecent(t, status.LastUpdated)
	})

	t.Run("Update Existing", func(t *testing.T) {
		c.SetMIGHealth(migURL, MIGHealthStatusUnhealthy, "now bad")
		status, found := c.GetMIGHealth(migURL)
		if !found {
			t.Fatal("Expected found=true for updated MIG")
		}
		if status.CurrentStatus != MIGHealthStatusUnhealthy {
			t.Errorf("Want CurrentStatus %v, got %v", MIGHealthStatusUnhealthy, status.CurrentStatus)
		}
		if status.LastStatus != MIGHealthStatusHealthy {
			t.Errorf("Want LastStatus %v, got %v", MIGHealthStatusHealthy, status.LastStatus)
		}
		if status.ErrorMessage != "now bad" {
			t.Errorf("Want ErrorMessage 'now bad', got '%s'", status.ErrorMessage)
		}
		checkRecent(t, status.LastUpdated)
	})

	t.Run("Cleanup", func(t *testing.T) {
		c.CleanupMIGHealth(migURL)
		_, found := c.GetMIGHealth(migURL)
		if found {
			t.Error("Expected found=false after CleanupMIGHealth")
		}
	})
}

func TestHealthCache_MIGTopologies(t *testing.T) {
	t.Parallel()
	c := NewHealthCache()
	migURL := "mig-url-1"
	topologies := map[string]AcceleratorTopologyInfo{
		"4x4x4": {
			AcceleratorTopology:             "4x4x4",
			AcceleratorTopologyHealthStatus: MIGHealthStatusHealthy,
		},
	}

	t.Run("Get Non-Existent", func(t *testing.T) {
		_, found := c.GetMIGPartitionsStatus(migURL)
		if found {
			t.Error("Expected found=false for non-existent MIG topologies")
		}
	})

	t.Run("Set and Get New", func(t *testing.T) {
		c.SetMIGPartitionsStatus(migURL, topologies)
		got, found := c.GetMIGPartitionsStatus(migURL)
		if !found {
			t.Fatal("Expected found=true for newly set MIG topologies")
		}
		if !reflect.DeepEqual(got, topologies) {
			t.Errorf("Want topologies %v, got %v", topologies, got)
		}
	})

	t.Run("Cleanup", func(t *testing.T) {
		c.CleanupMIGPartitionsStatus(migURL)
		_, found := c.GetMIGPartitionsStatus(migURL)
		if found {
			t.Error("Expected found=false after CleanupMIGPartitionsStatus")
		}
	})
}

func TestHealthCache_MMIGs(t *testing.T) {
	t.Parallel()
	c := NewHealthCache()
	mmigURL := "mmig-url-1"

	t.Run("Get Non-Existent", func(t *testing.T) {
		_, found := c.GetMMIGHealth(mmigURL)
		if found {
			t.Error("Expected found=false for non-existent MMIG")
		}
	})

	t.Run("Set and Get New", func(t *testing.T) {
		c.SetMMIGHealth(mmigURL, MMIGHealthStatusActive, "")

		status, found := c.GetMMIGHealth(mmigURL)
		if !found {
			t.Fatal("Expected found=true for newly set MMIG")
		}

		if status.CurrentStatus != MMIGHealthStatusActive {
			t.Errorf("Want CurrentStatus %v, got %v", MMIGHealthStatusActive, status.CurrentStatus)
		}
		if status.LastStatus != MMIGHealthStatusUnknown {
			t.Errorf("Want LastStatus %v, got %v", MMIGHealthStatusUnknown, status.LastStatus)
		}
		if status.ErrorMessage != "" {
			t.Errorf("Want empty ErrorMessage, got '%s'", status.ErrorMessage)
		}
		checkRecent(t, status.LastUpdated)
	})

	t.Run("Update Existing", func(t *testing.T) {
		c.SetMMIGHealth(mmigURL, MMIGHealthStatusFailed, "mmig error")

		status, found := c.GetMMIGHealth(mmigURL)
		if !found {
			t.Fatal("Expected found=true for updated MMIG")
		}

		if status.CurrentStatus != MMIGHealthStatusFailed {
			t.Errorf("Want CurrentStatus %v, got %v", MMIGHealthStatusFailed, status.CurrentStatus)
		}
		if status.LastStatus != MMIGHealthStatusActive {
			t.Errorf("Want LastStatus %v, got %v", MMIGHealthStatusActive, status.LastStatus)
		}
		if status.ErrorMessage != "mmig error" {
			t.Errorf("Want ErrorMessage 'mmig error', got '%s'", status.ErrorMessage)
		}
		checkRecent(t, status.LastUpdated)
	})

	t.Run("Cleanup", func(t *testing.T) {
		c.CleanupMMIGHealth(mmigURL)
		_, found := c.GetMMIGHealth(mmigURL)
		if found {
			t.Error("Expected found=false after CleanupMMIGHealth")
		}
	})
}

func TestHealthCache_Concurrency(t *testing.T) {
	t.Parallel()
	c := NewHealthCache()
	wg := sync.WaitGroup{}
	routines := 100

	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func(j int) {
			defer wg.Done()
			if j%2 == 0 {
				c.SetMIGHealth("mig-concurrent", MIGHealthStatusHealthy, "")
			} else {
				c.SetMIGHealth("mig-concurrent", MIGHealthStatusUnhealthy, "err")
			}
			c.GetMIGHealth("mig-concurrent")
		}(i)
	}

	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func(j int) {
			defer wg.Done()
			if j%2 == 0 {
				c.SetMMIGHealth("mmig-concurrent", MMIGHealthStatusActive, "")
			} else {
				c.SetMMIGHealth("mmig-concurrent", MMIGHealthStatusFailed, "err")
			}
			c.GetMMIGHealth("mmig-concurrent")
		}(i)
	}

	wg.Wait()

	_, migFound := c.GetMIGHealth("mig-concurrent")
	_, mmigFound := c.GetMMIGHealth("mmig-concurrent")

	if !migFound {
		t.Error("mig-concurrent was not set in concurrency test")
	}
	if !mmigFound {
		t.Error("mmig-concurrent was not set in concurrency test")
	}
}

func TestResourceCache_GetAllKeys(t *testing.T) {
	rc := newResourceCache("UNKNOWN")
	rc.SetHealth("url-1", "HEALTHY", "")
	rc.SetHealth("url-2", "FAILED", "")
	rc.SetHealth("url-3", "HEALTHY", "")

	expected := map[string]struct{}{
		"url-1": {},
		"url-2": {},
		"url-3": {},
	}

	keys := rc.GetAllKeys()

	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("GetAllKeys() = %v, want %v", keys, expected)
	}

	// Test empty case
	rcEmpty := newResourceCache("UNKNOWN")
	keysEmpty := rcEmpty.GetAllKeys()
	if len(keysEmpty) != 0 {
		t.Errorf("GetAllKeys() on empty cache = %v, want empty map", keysEmpty)
	}
}

func TestHealthCache_GetAllMIGKeys(t *testing.T) {
	hc := NewHealthCache()
	hc.SetMIGHealth("mig-1", MIGHealthStatusHealthy, "")
	hc.SetMIGHealth("mig-2", MIGHealthStatusDegraded, "")

	// Add an MMIG to make sure it's isolated
	hc.SetMMIGHealth("mmig-1", MMIGHealthStatusActive, "")
	expected := map[string]struct{}{
		"mig-1": {},
		"mig-2": {},
	}

	keys := hc.GetAllMIGKeys()

	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("GetAllMIGKeys() = %v, want %v", keys, expected)
	}
}

func TestHealthCache_GetAllMMIGKeys(t *testing.T) {
	hc := NewHealthCache()
	hc.SetMMIGHealth("mmig-1", MMIGHealthStatusActive, "")
	hc.SetMMIGHealth("mmig-2", MMIGHealthStatusFailed, "")

	// Add a MIG to make sure it's isolated
	hc.SetMIGHealth("mig-1", MIGHealthStatusHealthy, "")
	expected := map[string]struct{}{
		"mmig-1": {},
		"mmig-2": {},
	}

	keys := hc.GetAllMMIGKeys()

	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("GetAllMMIGKeys() = %v, want %v", keys, expected)
	}
}

func TestHealthCache_MIGNotifications(t *testing.T) {
	t.Parallel()
	notificationTimeout := 1 * time.Second
	noNotifyTimeout := 100 * time.Millisecond
	c := NewHealthCache()
	l := newMockListener(5)
	c.RegisterListener(l)
	migURL := "mig-listener-test"

	// 1. Test: Initial Set (Unknown -> Healthy)
	c.SetMIGHealth(migURL, MIGHealthStatusHealthy, "good")
	var n migNotification
	select {
	case n = <-l.migNotify:
		if n.resourceURL != migURL {
			t.Errorf("Want URL %s, got %s", migURL, n.resourceURL)
		}
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout: Did not receive notification for new MIG")
	}

	health, ok := c.GetMIGHealth(migURL)
	if !ok {
		t.Fatal("MIG health not found in cache after set")
	}
	if health.CurrentStatus != MIGHealthStatusHealthy || health.LastStatus != MIGHealthStatusUnknown || health.ErrorMessage != "good" {
		t.Errorf("Unexpected health status after initial set: %+v", health)
	}

	// 2. Test: Update Existing (Healthy -> Unhealthy)
	c.SetMIGHealth(migURL, MIGHealthStatusUnhealthy, "bad")
	select {
	case n = <-l.migNotify:
		if n.resourceURL != migURL {
			t.Errorf("Want URL %s, got %s", migURL, n.resourceURL)
		}
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout: Did not receive notification for updated MIG")
	}

	health, _ = c.GetMIGHealth(migURL)
	if health.CurrentStatus != MIGHealthStatusUnhealthy || health.LastStatus != MIGHealthStatusHealthy || health.ErrorMessage != "bad" {
		t.Errorf("Unexpected health status after update: %+v", health)
	}

	// 3. Test: No-Op Update (Unhealthy -> Unhealthy)
	c.SetMIGHealth(migURL, MIGHealthStatusUnhealthy, "bad")
	select {
	case <-l.migNotify:
		t.Fatal("Received unexpected notification for no-op update")
	case <-time.After(noNotifyTimeout):
		// Success
	}
}

func TestHealthCache_MIGTopologiesNotifications(t *testing.T) {
	t.Parallel()
	notificationTimeout := 1 * time.Second
	noNotifyTimeout := 100 * time.Millisecond
	c := NewHealthCache()
	l := newMockListener(5)
	c.RegisterListener(l)
	migURL := "mig-listener-test"

	topologies := make(map[string]AcceleratorTopologyInfo)

	// Add 16 2x2x1
	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("topo-2x2x1-%d", i)
		topologies[key] = AcceleratorTopologyInfo{
			AcceleratorTopology:             "2x2x1",
			AcceleratorTopologyHealthStatus: MIGHealthStatusHealthy,
			AcceleratorTopologyState:        MIGPartitionStateActive,
			SliceID:                         fmt.Sprintf("slice-2x2x1-%d", i),
			InstancesHealth:                 "HEALTHY",
			Errors:                          []AcceleratorTopologyError{},
		}
	}

	// Add 8 2x2x2
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("topo-2x2x2-%d", i)
		topologies[key] = AcceleratorTopologyInfo{
			AcceleratorTopology:             "2x2x2",
			AcceleratorTopologyHealthStatus: MIGHealthStatusHealthy,
			AcceleratorTopologyState:        MIGPartitionStateActive,
			SliceID:                         fmt.Sprintf("slice-2x2x2-%d", i),
			InstancesHealth:                 "HEALTHY",
			Errors:                          []AcceleratorTopologyError{},
		}
	}

	// Add 4 2x2x4
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("topo-2x2x4-%d", i)
		topologies[key] = AcceleratorTopologyInfo{
			AcceleratorTopology:             "2x2x4",
			AcceleratorTopologyHealthStatus: MIGHealthStatusHealthy,
			AcceleratorTopologyState:        MIGPartitionStateActive,
			SliceID:                         fmt.Sprintf("slice-2x2x4-%d", i),
			InstancesHealth:                 "HEALTHY",
			Errors:                          []AcceleratorTopologyError{},
		}
	}

	// Add 2 2x4x4
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("topo-2x4x4-%d", i)
		topologies[key] = AcceleratorTopologyInfo{
			AcceleratorTopology:             "2x4x4",
			AcceleratorTopologyHealthStatus: MIGHealthStatusHealthy,
			AcceleratorTopologyState:        MIGPartitionStateActive,
			SliceID:                         fmt.Sprintf("slice-2x4x4-%d", i),
			InstancesHealth:                 "HEALTHY",
			Errors:                          []AcceleratorTopologyError{},
		}
	}

	// 1. Test: Initial Set
	c.SetMIGPartitionsStatus(migURL, topologies)
	var n migNotification
	select {
	case n = <-l.migNotify:
		if n.resourceURL != migURL {
			t.Errorf("Want URL %s, got %s", migURL, n.resourceURL)
		}
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout: Did not receive notification for new MIG topologies")
	}

	// 2. Test: No-Op Update (Identical data)
	c.SetMIGPartitionsStatus(migURL, topologies)
	select {
	case <-l.migNotify:
		t.Fatal("Received unexpected notification for no-op update of topologies")
	case <-time.After(noNotifyTimeout):
		// Success
	}

	// 3. Test: Update with change
	newTopologies := make(map[string]AcceleratorTopologyInfo, len(topologies))
	for k, v := range topologies {
		newTopologies[k] = v
	}
	// Modify one entry to trigger change
	topoToModify := "topo-2x2x1-0"
	info := newTopologies[topoToModify]
	info.InstancesHealth = "UNHEALTHY_OR_MISSING"
	newTopologies[topoToModify] = info

	c.SetMIGPartitionsStatus(migURL, newTopologies)
	select {
	case n = <-l.migNotify:
		if n.resourceURL != migURL {
			t.Errorf("Want URL %s, got %s", migURL, n.resourceURL)
		}
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout: Did not receive notification for changed MIG topologies")
	}
}

func TestHealthCache_MMIGNotifications(t *testing.T) {
	t.Parallel()
	notificationTimeout := 1 * time.Second
	noNotifyTimeout := 100 * time.Millisecond
	c := NewHealthCache()
	l := newMockListener(5)
	c.RegisterListener(l)
	mmigURL := "mmig-listener-test"

	// 1. Test: Initial Set (Unknown -> Active)
	c.SetMMIGHealth(mmigURL, MMIGHealthStatusActive, "")
	var n mmigNotification
	select {
	case n = <-l.mmigNotify:
		if n.resourceURL != mmigURL {
			t.Errorf("Want URL %s, got %s", mmigURL, n.resourceURL)
		}
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout: Did not receive notification for new MMIG")
	}

	// 2. Test: No-Op (Active -> Active)
	c.SetMMIGHealth(mmigURL, MMIGHealthStatusActive, "")
	select {
	case <-l.mmigNotify:
		t.Fatal("Received unexpected notification for no-op update")
	case <-time.After(noNotifyTimeout):
		// Success
	}
}

func TestHealthCache_UnregisterListener(t *testing.T) {
	t.Parallel()
	notificationTimeout := 1 * time.Second
	noNotifyTimeout := 100 * time.Millisecond
	c := NewHealthCache()
	l := newMockListener(1)
	c.RegisterListener(l)

	c.SetMIGHealth("mig-1", MIGHealthStatusHealthy, "")
	select {
	case <-l.migNotify:
		// Expected
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout: Did not receive initial notification")
	}

	c.UnregisterListener(l)
	c.SetMIGHealth("mig-1", MIGHealthStatusUnhealthy, "err")
	select {
	case <-l.migNotify:
		t.Fatal("Received notification after unregistering listener")
	case <-time.After(noNotifyTimeout):
		// Success
	}
}

func TestHealthCache_MultipleListeners(t *testing.T) {
	t.Parallel()
	notificationTimeout := 1 * time.Second
	c := NewHealthCache()
	l1 := newMockListener(1)
	l2 := newMockListener(1)
	c.RegisterListener(l1)
	c.RegisterListener(l2)

	migURL := "mig-multi-listener"
	c.SetMIGHealth(migURL, MIGHealthStatusHealthy, "")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		select {
		case n1 := <-l1.migNotify:
			if n1.resourceURL != migURL {
				t.Errorf("L1: Want URL %s, got %s", migURL, n1.resourceURL)
			}
		case <-time.After(notificationTimeout):
			t.Error("Timeout: Listener 1 did not receive notification")
		}
	}()

	go func() {
		defer wg.Done()
		select {
		case n2 := <-l2.migNotify:
			if n2.resourceURL != migURL {
				t.Errorf("L2: Want URL %s, got %s", migURL, n2.resourceURL)
			}
		case <-time.After(notificationTimeout):
			t.Error("Timeout: Listener 2 did not receive notification")
		}
	}()

	wg.Wait()
}

func TestHealthCache_MMIGProbeFailureHysteresis(t *testing.T) {
	// We do NOT use t.Parallel() here if modifying shared state,
	// but since we modify a specific instance 'c', it is safe.
	t.Parallel()

	// Setup
	c := NewHealthCache()

	// [CRITICAL] Override threshold to 50ms for fast testing
	c.probeFailureThreshold = 50 * time.Millisecond

	l := newMockListener(10)
	c.RegisterListener(l)
	mmigURL := "mmig-hysteresis-test"
	notificationTimeout := 1 * time.Second
	noNotifyTimeout := 20 * time.Millisecond

	// 1. Initial State: Active
	c.SetMMIGHealth(mmigURL, MMIGHealthStatusActive, "")
	select {
	case <-l.mmigNotify:
		// Consumed expected "Active" notification
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout: Failed to set initial Active state")
	}

	// 2. Trigger ProbeFailed (First time) -> Should be SUPPRESSED
	c.SetMMIGHealth(mmigURL, MMIGHealthStatusProbeFailed, "fail 1")

	// Assert: No notification received
	select {
	case <-l.mmigNotify:
		t.Fatal("Hysteresis Failed: Received notification immediately for ProbeFailed")
	case <-time.After(noNotifyTimeout):
		// Success: Notification suppressed
	}

	// Assert: Internal state should STILL be Active (the old state)
	status, _ := c.GetMMIGHealth(mmigURL)
	if status.CurrentStatus != MMIGHealthStatusActive {
		t.Errorf("Hysteresis Failed: State updated too early. Got %v, want Active", status.CurrentStatus)
	}

	// 3. Wait for Threshold to pass (50ms configured -> wait 100ms)
	time.Sleep(100 * time.Millisecond)

	// 4. Trigger ProbeFailed again (After threshold) -> Should be ACCEPTED
	c.SetMMIGHealth(mmigURL, MMIGHealthStatusProbeFailed, "fail 1")

	// Assert: Notification received
	select {
	case n := <-l.mmigNotify:
		if n.resourceURL != mmigURL {
			t.Errorf("Want URL %s, got %s", mmigURL, n.resourceURL)
		}
	case <-time.After(notificationTimeout):
		t.Fatal("Hysteresis Failed: Did not receive notification after threshold passed")
	}

	// Assert: State is now ProbeFailed
	status, _ = c.GetMMIGHealth(mmigURL)
	if status.CurrentStatus != MMIGHealthStatusProbeFailed {
		t.Errorf("Want ProbeFailed, got %v", status.CurrentStatus)
	}

	// 5. Recovery: Set back to Active -> Should RESET timer
	c.SetMMIGHealth(mmigURL, MMIGHealthStatusActive, "recovered")
	select {
	case <-l.mmigNotify:
		// Consumed "Active" notification
	case <-time.After(notificationTimeout):
		t.Fatal("Timeout waiting for recovery notification")
	}

	// 6. Trigger ProbeFailed (Second time) -> Should be SUPPRESSED again (Timer restarted)
	c.SetMMIGHealth(mmigURL, MMIGHealthStatusProbeFailed, "fail 2")

	select {
	case <-l.mmigNotify:
		t.Fatal("Hysteresis Failed: Timer did not reset after recovery")
	case <-time.After(noNotifyTimeout):
		// Success
	}

	// Double Check: State is still Active
	status, _ = c.GetMMIGHealth(mmigURL)
	if status.CurrentStatus != MMIGHealthStatusActive {
		t.Errorf("Want Active, got %v", status.CurrentStatus)
	}
}

func TestAcceleratorTopologyInfoEquals(t *testing.T) {
	t.Parallel()

	err1 := AcceleratorTopologyError{Code: "1", Message: "one"}
	err2 := AcceleratorTopologyError{Code: "2", Message: "two"}

	info1 := AcceleratorTopologyInfo{
		AcceleratorTopology: "2x2x1",
		Errors:              []AcceleratorTopologyError{err1, err2},
	}

	info2 := AcceleratorTopologyInfo{
		AcceleratorTopology: "2x2x1",
		Errors:              []AcceleratorTopologyError{err2, err1},
	}

	if !acceleratorTopologyInfoEquals(info1, info2) {
		t.Errorf("acceleratorTopologyInfoEquals() = false, want true for same errors in different order")
	}

	info3 := AcceleratorTopologyInfo{
		AcceleratorTopology: "2x2x1",
		Errors:              []AcceleratorTopologyError{err1},
	}

	if acceleratorTopologyInfoEquals(info1, info3) {
		t.Errorf("acceleratorTopologyInfoEquals() = true, want false for different number of errors")
	}

	info4 := AcceleratorTopologyInfo{
		AcceleratorTopology: "2x2x1",
		Errors:              []AcceleratorTopologyError{err1, {Code: "3", Message: "three"}},
	}

	if acceleratorTopologyInfoEquals(info1, info4) {
		t.Errorf("acceleratorTopologyInfoEquals() = true, want false for different errors")
	}

	baseInfo := AcceleratorTopologyInfo{
		AcceleratorTopology:             "2x2x1",
		AcceleratorTopologyHealthStatus: MIGHealthStatusHealthy,
		AcceleratorTopologyState:        MIGPartitionStateActive,
		SliceID:                         "slice-1",
		InstancesHealth:                 "HEALTHY",
	}

	diffTopology := baseInfo
	diffTopology.AcceleratorTopology = "2x2x2"
	if acceleratorTopologyInfoEquals(baseInfo, diffTopology) {
		t.Errorf("acceleratorTopologyInfoEquals() = true, want false for different AcceleratorTopology")
	}

	diffHealthStatus := baseInfo
	diffHealthStatus.AcceleratorTopologyHealthStatus = MIGHealthStatusUnhealthy
	if acceleratorTopologyInfoEquals(baseInfo, diffHealthStatus) {
		t.Errorf("acceleratorTopologyInfoEquals() = true, want false for different AcceleratorTopologyHealthStatus")
	}

	diffState := baseInfo
	diffState.AcceleratorTopologyState = MIGPartitionStateFailed
	if acceleratorTopologyInfoEquals(baseInfo, diffState) {
		t.Errorf("acceleratorTopologyInfoEquals() = true, want false for different AcceleratorTopologyState")
	}

	diffSliceID := baseInfo
	diffSliceID.SliceID = "slice-2"
	if acceleratorTopologyInfoEquals(baseInfo, diffSliceID) {
		t.Errorf("acceleratorTopologyInfoEquals() = true, want false for different SliceID")
	}

	diffInstancesHealth := baseInfo
	diffInstancesHealth.InstancesHealth = "UNHEALTHY_OR_MISSING"
	if acceleratorTopologyInfoEquals(baseInfo, diffInstancesHealth) {
		t.Errorf("acceleratorTopologyInfoEquals() = true, want false for different InstancesHealth")
	}
}
