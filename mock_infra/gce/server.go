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

package gce

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	alphacompute "google.golang.org/api/compute/v0.alpha"
)

const (
	resourceTypeMMIG   = "MMIG"
	resourceTypePolicy = "Policy"
	opStatusDone       = "DONE"
	opStatusRunning    = "RUNNING"
	multiMigs          = "multiMigs"
	activeState        = "ACTIVE"
)

// HttpError represents a simulated HTTP error for injection.
type HttpError struct {
	Code    int
	Message string
}

// MmigHealthInjection represents the data for a MMIG health status injection.
type MmigHealthInjection struct {
	AcceleratorTopology      string `json:"acceleratorTopology,omitempty"`
	AcceleratorTopologyState string `json:"acceleratorTopologyState"`
	ErrorMessage             string `json:"errorMessage,omitempty"`
}

type InstancesHealth string

const (
	InstancesHealthAllHealthy         InstancesHealth = "ALL_HEALTHY"
	InstancesHealthUnhealthyOrMissing InstancesHealth = "UNHEALTHY_OR_MISSING"
)

type TopoErrorItem struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type TopoErrors struct {
	Errors []TopoErrorItem `json:"errors,omitempty"`
}

type AcceleratorTopologyStateStruct struct {
	CurrentState          string      `json:"currentState,omitempty"`
	Error                 *TopoErrors `json:"error,omitempty"`
	ErrorTimestamp        string      `json:"errorTimestamp,omitempty"`
	StateDetails          *TopoErrors `json:"stateDetails,omitempty"`
	StateDetailsTimestamp string      `json:"stateDetailsTimestamp,omitempty"`
	ExternalId            string      `json:"externalId,omitempty"`
}

type CustomAcceleratorTopologyInfo struct {
	AcceleratorTopology       string                          `json:"acceleratorTopology,omitempty"`
	AcceleratorTopologyHealth string                          `json:"acceleratorTopologyHealth,omitempty"`
	InstancesHealth           InstancesHealth                 `json:"instancesHealth,omitempty"`
	AcceleratorTopologyState  *AcceleratorTopologyStateStruct `json:"acceleratorTopologyState,omitempty"`
	Parent                    string                          `json:"parent,omitempty"`
	RequestId                 string                          `json:"requestId,omitempty"`
}

// MockMMIG is the internal representation of a MultiMig resource.
type MockMMIG struct {
	Name              string
	ExpectedTopology  string
	ExpectedMigs      int
	AttachedMigs      []string // List of attached MIG names
	DeformationIntent bool
	Raw               *alphacompute.MultiMig
}

// MockMIG is the internal representation of an InstanceGroupManager resource.
type MockMIG struct {
	Name        string
	PartitionID string
	MultiMig    string // Name of the MMIG it is attached to
	SliceName   string
	Raw         *alphacompute.InstanceGroupManager
}

// GCEResources holds all GCE resources tracked by the mock server.
type GCEResources struct {
	Policies      map[string]*alphacompute.ResourcePolicy
	Operations    map[string]*alphacompute.Operation
	Mmigs         map[string]*MockMMIG
	Migs          map[string]*MockMIG
	MigTopologies map[string]map[string]*CustomAcceleratorTopologyInfo // Key: IGM Name, Inner Key: Topo ID
}

// SliceInjections holds all injection controls for a single slice.
type SliceInjections struct {
	StepHttpFailures       map[InjectedOperationStep]HttpError
	StepTimeouts           map[InjectedOperationStep]time.Duration
	InjectedOpStatuses     map[string]string
	InjectedTopoHealth     map[string]string // Key is MIG name
	InjectedMmigHealth     map[string]MmigHealthInjection
	WantedMigs             []string                                              // Track MIGs intended for the slice
	InjectedMigs           []string                                              // Track MIGs injected for the slice
	InjectedTopologyStates map[string]map[string]*AcceleratorTopologyStateStruct // Key: IGM Name, Inner Key: Topo ID
}

// --- Main Server State ---

// Maps slice names to their respective resource states.
var gceResources = &GCEResources{
	Policies:      make(map[string]*alphacompute.ResourcePolicy),
	Mmigs:         make(map[string]*MockMMIG),
	Migs:          make(map[string]*MockMIG),
	Operations:    make(map[string]*alphacompute.Operation),
	MigTopologies: make(map[string]map[string]*CustomAcceleratorTopologyInfo),
}

// Maps slice names to their respective injection controls.
var sliceInjections map[string]*SliceInjections

// QPSStats holds the metrics for a single endpoint.
type QPSStats struct {
	Timestamps    []time.Time // Recent request timestamps for sliding window calculation.
	TotalRequests int64       // All-time total number of requests.
	HighestQPS    float64     // Highest QPS recorded in any 10s window.
}

// Maps endpoint names to their QPS statistics.
var qpsCounters map[string]*QPSStats

// The time the service started, for calculating all-time average QPS.
var serviceStartTime time.Time

// Mutex to protect access to the server state
var mu sync.Mutex

// init initializes the global state maps.
func init() {
	serviceStartTime = time.Now()
	// sliceResources removed
	sliceInjections = make(map[string]*SliceInjections)
	qpsCounters = make(map[string]*QPSStats)
	qpsCounters["GetMMIG"] = &QPSStats{}
	qpsCounters["GetAvailableAcceleratorTopologies"] = &QPSStats{}
	qpsCounters["ListMultiMigMembers"] = &QPSStats{}
}

// InitSlice initializes the maps for a given slice. This should be called
// by tests before making requests for a specific slice.
func InitSlice(sliceName string) {
	mu.Lock()
	defer mu.Unlock()
	initSlice(sliceName)
}

// initSlice is the internal, non-locking version of InitSlice.
// It assumes the caller has already acquired the mutex.
func initSlice(sliceName string) {
	log.Printf("[Mocking GCE] Initializing mock server slice resources for slice: %s", sliceName)

	// sliceResources removed, using global gceResources
	if _, ok := sliceInjections[sliceName]; !ok {
		log.Printf("[Mocking GCE] Initializing new injection map for slice: %s", sliceName)
		sliceInjections[sliceName] = &SliceInjections{
			StepHttpFailures:   make(map[InjectedOperationStep]HttpError),
			StepTimeouts:       make(map[InjectedOperationStep]time.Duration),
			InjectedOpStatuses: make(map[string]string),
			InjectedTopoHealth: make(map[string]string), // Key is MIG name
			InjectedMmigHealth: make(map[string]MmigHealthInjection),
		}
	}
}

// initReadySlice initializes the maps for a given slice and pre-populates it
// with a resource policy, simulating a slice that is ready to be deformed.
func InitReadySlice(sliceName string) {
	mu.Lock()
	defer mu.Unlock()
	// First, ensure the basic slice structures are initialized.
	initSlice(sliceName)

	// Then, add the resource policy.
	initCreateResourcePolicy(sliceName, sliceName)
	initCreateMMIG(sliceName, sliceName)
}

// initCreateResourcePolicy is a test helper to pre-populate a resource policy.
// It assumes the caller has already acquired the mutex.
func initCreateResourcePolicy(sliceName, policyName string) {
	if _, ok := gceResources.Policies[policyName]; !ok {
		log.Printf("[Mocking GCE] Helper: Creating mock resource policy '%s' for slice '%s'", policyName, sliceName)
		gceResources.Policies[policyName] = &alphacompute.ResourcePolicy{
			Name:              policyName,
			CreationTimestamp: time.Now().Format(time.RFC3339),
		}
	}
}

// initCreateMMIG is a test helper to pre-populate a MMIG.
// It assumes the caller has already acquired the mutex.
func initCreateMMIG(sliceName, mmigName string) {
	if _, ok := gceResources.Mmigs[mmigName]; !ok {
		log.Printf("[Mocking GCE] Helper: Creating mock MMIG '%s' for slice '%s'", mmigName, sliceName)
		gceResources.Mmigs[mmigName] = &MockMMIG{
			Name: mmigName,
			Raw: &alphacompute.MultiMig{
				Name:              mmigName,
				CreationTimestamp: time.Now().Format(time.RFC3339),
			},
		}
	}
}

// handleInitSlice handles the initialization of a slice's resources in the mock server.
// It checks for duplicates and ensures that the necessary data structures are created.
// Example:
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "new-test-slice"}' http://localhost:8080/inject/slice
func handleInitSlice(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var reqBody struct {
		SliceName string `json:"sliceName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	if reqBody.SliceName == "" {
		http.Error(w, "sliceName must be provided", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if _, exists := sliceInjections[reqBody.SliceName]; exists {
		msg := fmt.Sprintf("Slice '%s' already exists in slice injections.", reqBody.SliceName)
		log.Printf("[Mocking GCE] Conflict: %s", msg)
		http.Error(w, msg, http.StatusConflict)
		return
	}

	initSlice(reqBody.SliceName)
	if _, err := fmt.Fprintf(w, "Slice '%s' initialized successfully.\n", reqBody.SliceName); err != nil {
		log.Printf("[Mocking GCE] Error writing response: %v", err)
	}
}

// handle is a generic handler that logs the request and encodes a JSON response.
func handle(w http.ResponseWriter, r *http.Request, responseData any) {
	log.Printf("[Mocking GCE] --> Responding to %s %s with OK", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(responseData); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

// Example:
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"acceleratorTopologyState": "INCOMPLETE"}' 'http://localhost:8080/inject/mmig-health?sliceName=test-slice'
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{}' 'http://localhost:8080/inject/mmig-health?sliceName=test-slice'
func handleMmigHealthInjection(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received MMIG health injection request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName                string `json:"sliceName"`
		AcceleratorTopology      string `json:"acceleratorTopology,omitempty"`
		AcceleratorTopologyState string `json:"acceleratorTopologyState"`
		ErrorMessage             string `json:"errorMessage,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" {
		http.Error(w, "sliceName must be provided in the request body", http.StatusBadRequest)
		return
	}

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		if injection.AcceleratorTopologyState == "" {
			http.Error(w, "acceleratorTopologyState is a required field for injection", http.StatusBadRequest)
			return
		}
		if !isValidMMIGHealthStatus(injection.AcceleratorTopologyState) {
			msg := fmt.Sprintf("Invalid health status: %s. Must be one of: %v",
				injection.AcceleratorTopologyState, allMMIGHealthStatuses)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}

		// Set defaults if not provided
		if injection.AcceleratorTopology == "" {
			injection.AcceleratorTopology = "4x4x8"
		}
		if injection.ErrorMessage == "" {
			injection.ErrorMessage = "mock gce injected error"
		}

		mmigName := injection.SliceName + "-testhash"
		updatedInGlobal := false
		if mmig, ok := gceResources.Mmigs[mmigName]; ok {
			if mmig.Raw == nil {
				mmig.Raw = &alphacompute.MultiMig{}
			}
			mmig.Raw.Status = &alphacompute.MultiMigStatus{
				AppliedAcceleratorTopologies: []*alphacompute.MultiMigStatusAcceleratorTopology{
					{
						AcceleratorTopology:      injection.AcceleratorTopology,
						AcceleratorTopologyState: injection.AcceleratorTopologyState,
					},
				},
			}
			updatedInGlobal = true
		}

		sliceInjections[injection.SliceName].InjectedMmigHealth[injection.SliceName] = MmigHealthInjection{
			AcceleratorTopology:      injection.AcceleratorTopology,
			AcceleratorTopologyState: injection.AcceleratorTopologyState,
			ErrorMessage:             injection.ErrorMessage,
		}

		if updatedInGlobal {
			log.Printf("[Mocking GCE] Injected MMIG health for slice '%s' (updated in global resource '%s')",
				injection.SliceName, mmigName)
		} else {
			log.Printf("[Mocking GCE] Injected MMIG health for slice '%s' (pending, '%s' not found in global)",
				injection.SliceName, mmigName)
		}
		log.Printf("[Mocking GCE] Injected MMIG health for slice '%s': %+v", injection.SliceName, injection)
		if _, err := fmt.Fprintf(w, "Injected MMIG health for slice %s: %+v\n", injection.SliceName, injection); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		for name, mmig := range gceResources.Mmigs {
			if strings.HasPrefix(name, injection.SliceName) && mmig.Raw != nil {
				mmig.Raw.Status = nil
				log.Printf("[Mocking GCE] Reset global MMIG '%s' health status to default via prefix match", name)
			}
		}
		delete(sliceInjections[injection.SliceName].InjectedMmigHealth, injection.SliceName)
		log.Printf("[Mocking GCE] Cleared injected MMIG health for slice '%s' from sliceInjections", injection.SliceName)
		if _, err := fmt.Fprintf(w, "Cleared injected MMIG health for slice %s\n", injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for health injection", http.StatusMethodNotAllowed)
	}
}

// handleTopoHealthInjection injects or clears a health status for a specific MIG's topology.
// Example (Inject):
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "mig-1", "status": "DEGRADED"}' http://localhost:8080/inject/topo-health
//
// Example (Clear):
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "mig-1"}' http://localhost:8080/inject/topo-health
func handleTopoHealthInjection(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received Topology health injection request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName string `json:"sliceName"`
		Key       string `json:"key"` // MIG name
		Status    string `json:"status,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.Key == "" {
		http.Error(w, "sliceName and key (migName) must be provided", http.StatusBadRequest)
		return
	}

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		if !isValidAcceleratorTopologyHealthStatus(injection.Status) {
			msg := fmt.Sprintf("Invalid topology health status: %s. Must be one of: %v",
				injection.Status, allAcceleratorTopologyHealthStatuses)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}

		sliceInjections[injection.SliceName].InjectedTopoHealth[injection.Key] = injection.Status
		log.Printf("[Mocking GCE] Injected topology health for MIG '%s' in slice '%s' to '%s'",
			injection.Key, injection.SliceName, injection.Status)
		if _, err := fmt.Fprintf(w, "Injected topology health for MIG %s in slice %s to %s\n",
			injection.Key, injection.SliceName, injection.Status); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		delete(sliceInjections[injection.SliceName].InjectedTopoHealth, injection.Key)
		log.Printf("[Mocking GCE] Cleared injected topology health for MIG '%s' in slice '%s'",
			injection.Key, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Cleared injected topology health for MIG %s in slice %s\n",
			injection.Key, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for topology health injection", http.StatusMethodNotAllowed)
	}
}

// handleInitMig adds or deletes a MIG from the mock server's state.
// Example (Add):
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "migName": "default-pool-mig"}' http://localhost:8080/inject/init-mig
//
// Example (Delete):
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "migName": "default-pool-mig"}' http://localhost:8080/inject/init-mig
func handleInitMig(w http.ResponseWriter, r *http.Request) {
	var injection struct {
		SliceName   string `json:"sliceName"`
		MigName     string `json:"migName"`
		Status      string `json:"status"`
		PartitionID string `json:"partitionId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.MigName == "" {
		http.Error(w, "sliceName and migName must be provided", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received MIG inject request: %s %s", r.Method, r.URL.Path)

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for MIG injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		// Check if the MIG name already exists in *any* slice.
		// Checked globally below
		if sliceInj, ok := sliceInjections[injection.SliceName]; ok {
			for _, m := range sliceInj.InjectedMigs {
				if m == injection.MigName {
					log.Printf("[Mocking GCE] Conflict: MIG '%s' was already injected for slice '%s'",
						injection.MigName, injection.SliceName)
					http.Error(w, fmt.Sprintf("Conflict: MIG %s already exists for slice %s",
						injection.MigName, injection.SliceName), http.StatusConflict)
					return
				}
			}
		}

		if mig, exists := gceResources.Migs[injection.MigName]; exists {
			log.Printf("[Mocking GCE] Adopting existing MIG '%s' for slice '%s'", injection.MigName, injection.SliceName)
			if injection.Status != "" {
				if mig.Raw.Status == nil {
					mig.Raw.Status = &alphacompute.InstanceGroupManagerStatus{}
				}
				if len(mig.Raw.Status.AppliedAcceleratorTopologies) == 0 {
					mig.Raw.Status.AppliedAcceleratorTopologies = []*alphacompute.InstanceGroupManagerStatusAcceleratorTopology{{}}
				}
				mig.Raw.Status.AppliedAcceleratorTopologies[0].State = injection.Status
			}
			if injection.PartitionID != "" {
				mig.PartitionID = injection.PartitionID
			}
			sliceInjections[injection.SliceName].InjectedMigs = append(
				sliceInjections[injection.SliceName].InjectedMigs, injection.MigName)
			if _, err := fmt.Fprintf(w, "Adopted MIG %s into slice %s\n", injection.MigName, injection.SliceName); err != nil {
				log.Printf("[Mocking GCE] Error writing response: %v", err)
			}
			return
		}
		// Construct a plausible selfLink for the injected MIG.
		// This is a bit of a guess since we don't have project/zone from the injection request,
		// but it's better than nothing and makes it consistent with on-the-fly creation.
		// We'll assume a default project and zone for the mock.
		selfLink := fmt.Sprintf("/projects/test-project/zones/us-central1-a/instanceGroupManagers/%s", injection.MigName)

		migStatus := string(MMIGHealthStatusActive)
		if injection.Status != "" {
			migStatus = injection.Status
		}

		gceResources.Migs[injection.MigName] = &MockMIG{
			Name:        injection.MigName,
			PartitionID: injection.PartitionID,
			Raw: &alphacompute.InstanceGroupManager{
				Name:              injection.MigName,
				SelfLink:          selfLink,
				CreationTimestamp: time.Now().Format(time.RFC3339),
				Status: &alphacompute.InstanceGroupManagerStatus{
					AppliedAcceleratorTopologies: []*alphacompute.InstanceGroupManagerStatusAcceleratorTopology{
						{
							State: migStatus,
						},
					},
				},
			},
		}
		sliceInjections[injection.SliceName].InjectedMigs = append(
			sliceInjections[injection.SliceName].InjectedMigs, injection.MigName)
		log.Printf("[Mocking GCE] Initialized generic free MIG '%s' for slice '%s'", injection.MigName, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Initialized free MIG %s\n", injection.MigName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		delete(gceResources.Migs, injection.MigName)
		log.Printf("[Mocking GCE] Deleted MIG '%s' from slice '%s'", injection.MigName, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Deleted MIG %s from slice %s\n", injection.MigName, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for MIG initialization", http.StatusMethodNotAllowed)
	}
}

// handleInitPolicy adds or deletes a resource policy from the mock server's state for testing.
// Example (Add):
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "policyName": "test-slice"}' http://localhost:8080/inject/policy
//
// Example (Delete):
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "policyName": "test-slice"}' http://localhost:8080/inject/policy
func handleInitPolicy(w http.ResponseWriter, r *http.Request) {
	var injection struct {
		SliceName  string `json:"sliceName"`
		PolicyName string `json:"policyName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.PolicyName == "" {
		http.Error(w, "sliceName and policyName must be provided", http.StatusBadRequest)
		return
	}

	// policyName should be either same as slice name, or slice name-some hash
	if injection.PolicyName != injection.SliceName && !strings.HasPrefix(injection.PolicyName, injection.SliceName+"-") {
		msg := fmt.Sprintf("policyName '%s' must either be the same as sliceName '%s' or have the format "+
			"'<sliceName>-<hash>'", injection.PolicyName, injection.SliceName)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received Policy inject request: %s %s", r.Method, r.URL.Path)

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for Policy injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		// Check if the policy name already exists globally.
		if _, exists := gceResources.Policies[injection.PolicyName]; exists {
			msg := fmt.Sprintf("Policy '%s' already exists globally. Cannot add to slice %s.",
				injection.PolicyName, injection.SliceName)
			http.Error(w, msg, http.StatusConflict)
			return
		}

		gceResources.Policies[injection.PolicyName] = &alphacompute.ResourcePolicy{
			Name:              injection.PolicyName,
			CreationTimestamp: time.Now().Format(time.RFC3339),
			// SelfLink could be constructed here if needed for more complex tests
		}
		log.Printf("[Mocking GCE] Initialized Policy '%s' into slice '%s'", injection.PolicyName, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Initialized Policy %s into slice %s\n",
			injection.PolicyName, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		delete(gceResources.Policies, injection.PolicyName)
		log.Printf("[Mocking GCE] Deleted Policy '%s' from slice '%s'", injection.PolicyName, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Deleted Policy %s from slice %s\n",
			injection.PolicyName, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for Policy initialization", http.StatusMethodNotAllowed)
	}
}

// handleInitMmig adds or deletes a MMIG from the mock server's state for testing.
// Example (Add):
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "mmigName": "test-slice"}' http://localhost:8080/inject/mmig
func handleInitMmig(w http.ResponseWriter, r *http.Request) {
	var injection struct {
		SliceName string `json:"sliceName"`
		MmigName  string `json:"mmigName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.MmigName == "" {
		http.Error(w, "sliceName and mmigName must be provided", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received MMIG inject request: %s %s", r.Method, r.URL.Path)

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for MMIG injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		// Check if the MMIG name already exists globally.
		if _, exists := gceResources.Mmigs[injection.MmigName]; exists {
			msg := fmt.Sprintf("MMIG '%s' already exists globally. Cannot add to slice %s.",
				injection.MmigName, injection.SliceName)
			http.Error(w, msg, http.StatusConflict)
			return
		}

		initCreateMMIG(injection.SliceName, injection.MmigName)
		log.Printf("[Mocking GCE] Initialized MMIG '%s' into slice '%s'", injection.MmigName, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Initialized MMIG %s into slice %s\n",
			injection.MmigName, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		delete(gceResources.Mmigs, injection.MmigName)
		log.Printf("[Mocking GCE] Deleted MMIG '%s' from slice '%s'", injection.MmigName, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Deleted MMIG %s from slice %s\n", injection.MmigName, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for MMIG initialization", http.StatusMethodNotAllowed)
	}
}

// handleOpStatusInjection injects or clears a status for a specific operation.
// Example (Inject):
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "op-createmmig-test-slice", "status": "RUNNING"}' \
//	  http://localhost:8080/inject/op-status
//
// Example (Clear):
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "op-createmmig-test-slice"}' http://localhost:8080/inject/op-status
func handleOpStatusInjection(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received Operation Status injection request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName string `json:"sliceName"`
		Key       string `json:"key"` // This is the operation name, e.g., "op-createmmig-test-slice"
		Status    string `json:"status,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.Key == "" {
		http.Error(w, "sliceName and key (operation name) must be provided", http.StatusBadRequest)
		return
	}

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		// Update global gceResources.Operations instead of sliceInjections
		opName := fmt.Sprintf("%s/slice/%s", injection.Key, injection.SliceName)
		currentOp, exists := gceResources.Operations[opName]

		gceResources.Operations[opName] = &alphacompute.Operation{
			Name:   opName,
			Status: injection.Status,
		}
		// Also save in sliceInjections for backward compatibility/intent tracking.
		sliceInjections[injection.SliceName].InjectedOpStatuses[injection.Key] = injection.Status

		// If a detach operation transitions from RUNNING to DONE, perform detachment state changes.
		if strings.HasPrefix(injection.Key, OpPrefixDetachMIGs) && injection.Status == opStatusDone {
			if exists && currentOp.Status == opStatusRunning {
				migName := strings.TrimPrefix(injection.Key, OpPrefixDetachMIGs)
				log.Printf("[Mocking GCE] Detach operation '%s' transitioned from RUNNING to DONE. Performing detachment.", opName)
				performDetachStateChanges(injection.SliceName, migName)
			}
		}

		// If an attach operation transitions from RUNNING to DONE, perform attachment state changes.
		if strings.HasPrefix(injection.Key, OpPrefixAttachMIGs) && injection.Status == opStatusDone {
			if exists && currentOp.Status == opStatusRunning {
				migName := strings.TrimPrefix(injection.Key, OpPrefixAttachMIGs)
				log.Printf("[Mocking GCE] Attach operation '%s' transitioned from RUNNING to DONE. Performing attachment.", opName)
				performAttachStateChanges(injection.SliceName, migName)
			}
		}

		log.Printf("[Mocking GCE] Injected global operation status for '%s' (opName: '%s') to '%s'",
			injection.Key, opName, injection.Status)
		if _, err := fmt.Fprintf(w, "Injected global operation status for %s to %s\n",
			opName, injection.Status); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		opName := fmt.Sprintf("%s/slice/%s", injection.Key, injection.SliceName)
		delete(gceResources.Operations, opName)
		delete(sliceInjections[injection.SliceName].InjectedOpStatuses, injection.Key)
		log.Printf("[Mocking GCE] Cleared global and local injected operation status for '%s' (key: '%s')",
			opName, injection.Key)
		if _, err := fmt.Fprintf(w, "Cleared global injected operation status for %s\n", opName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for operation status injection", http.StatusMethodNotAllowed)
	}
}

// handleInjectHttpError injects an HTTP error for a specific step.
// Example (Inject):
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "createWorkloadPolicy", "code": 500, "message": "Internal Server Error"}' \
//	  http://localhost:8080/inject/http-error
//
// Example (Clear):
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "createWorkloadPolicy"}' http://localhost:8080/inject/http-error
func handleInjectHttpError(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received HTTP Error injection request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName string `json:"sliceName"`
		Key       string `json:"key"` // This is the step name, e.g., "createWorkloadPolicy"
		Code      int    `json:"code,omitempty"`
		Message   string `json:"message,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.Key == "" {
		http.Error(w, "sliceName and key (step name) must be provided", http.StatusBadRequest)
		return
	}

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		sliceInjections[injection.SliceName].StepHttpFailures[InjectedOperationStep(injection.Key)] = HttpError{
			Code: injection.Code, Message: injection.Message}
		log.Printf("[Mocking GCE] Injected HTTP error for '%s' in slice '%s' to Code: %d, Message: %s",
			injection.Key, injection.SliceName, injection.Code, injection.Message)
		if _, err := fmt.Fprintf(w, "Injected HTTP error for %s in slice %s to Code: %d, Message: %s\n",
			injection.Key, injection.SliceName, injection.Code, injection.Message); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		delete(sliceInjections[injection.SliceName].StepHttpFailures, InjectedOperationStep(injection.Key))
		log.Printf("[Mocking GCE] Cleared injected HTTP error for '%s' in slice '%s'", injection.Key, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Cleared injected HTTP error for %s in slice %s\n",
			injection.Key, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for HTTP error injection", http.StatusMethodNotAllowed)
	}
}

// handleInjectTimeout injects a timeout for a specific step.
// Example (Inject):
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "CreateWorkloadPolicy", "duration": 5}' http://localhost:8080/inject/timeout
//
// Example (Clear):
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "key": "CreateWorkloadPolicy"}' http://localhost:8080/inject/timeout
func handleInjectTimeout(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received Timeout injection request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName string `json:"sliceName"`
		Key       string `json:"key"`      // This is the step name, e.g., "CreateWorkloadPolicy"
		Duration  int    `json:"duration"` // in seconds
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.Key == "" {
		http.Error(w, "sliceName and key (step name) must be provided", http.StatusBadRequest)
		return
	}

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		log.Printf("[Mocking GCE] Slice '%s' not found for injection, initializing it.", injection.SliceName)
		initSlice(injection.SliceName)
	}

	switch r.Method {
	case http.MethodPost:
		if injection.Duration <= 0 || injection.Duration > 300 {
			http.Error(w, "duration must be between 1 and 300 seconds", http.StatusBadRequest)
			return
		}
		duration := time.Duration(injection.Duration) * time.Second
		sliceInjections[injection.SliceName].StepTimeouts[InjectedOperationStep(injection.Key)] = duration
		log.Printf("[Mocking GCE] Injected timeout for '%s' in slice '%s' of %v",
			injection.Key, injection.SliceName, duration)
		if _, err := fmt.Fprintf(w, "Injected timeout for %s in slice %s of %v\n",
			injection.Key, injection.SliceName, duration); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	case http.MethodDelete:
		delete(sliceInjections[injection.SliceName].StepTimeouts, InjectedOperationStep(injection.Key))
		log.Printf("[Mocking GCE] Cleared injected timeout for '%s' in slice '%s'", injection.Key, injection.SliceName)
		if _, err := fmt.Fprintf(w, "Cleared injected timeout for %s in slice %s\n",
			injection.Key, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	default:
		http.Error(w, "Method not allowed for timeout injection", http.StatusMethodNotAllowed)
	}
}

// handleMmigAttachment manages the attachment of a MIG to an MMIG for testing purposes.
// It allows adding or removing a MIG from an MMIG's list of attached members.
// Example (Remove):
//
//	curl -X DELETE -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "mmigName": "test-slice", "migName": "np-t1-grp"}' \
//	  http://localhost:8080/inject/mmig-attachment
func handleMmigAttachment(w http.ResponseWriter, r *http.Request) {
	var injection struct {
		SliceName string `json:"sliceName"`
		MmigName  string `json:"mmigName"`
		MigName   string `json:"migName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.MmigName == "" || injection.MigName == "" {
		http.Error(w, "sliceName, mmigName, and migName must be provided", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received MMIG attachment request: %s %s", r.Method, r.URL.Path)

	switch r.Method {
	case http.MethodDelete:
		mockMmig, ok := gceResources.Mmigs[injection.MmigName]
		if !ok {
			log.Printf("[Mocking GCE] MMIG '%s' not found for attachment removal injection.", injection.MmigName)
			if _, err := fmt.Fprintf(w, "MMIG %s not found.\n", injection.MmigName); err != nil {
				log.Printf("[Mocking GCE] Error writing response: %v", err)
			}
			return
		}

		var updatedMigs []string
		found := false
		for _, mName := range mockMmig.AttachedMigs {
			if mName != injection.MigName {
				updatedMigs = append(updatedMigs, mName)
			} else {
				found = true
			}
		}
		if found {
			mockMmig.AttachedMigs = updatedMigs
			if mig, ok := gceResources.Migs[injection.MigName]; ok && mig.MultiMig == injection.MmigName {
				mig.MultiMig = ""
			}
			log.Printf("[Mocking GCE] Injected removal of MIG '%s' from MMIG '%s'.", injection.MigName, injection.MmigName)
		} else {
			log.Printf("[Mocking GCE] MIG '%s' was not attached to MMIG '%s'. Nothing to do.",
				injection.MigName, injection.MmigName)
		}
		log.Printf("[Mocking GCE] Removed MIG '%s' from MMIG '%s' attachments.",
			injection.MigName, injection.MmigName)
		if _, err := fmt.Fprintf(w, "Removed MIG %s from MMIG %s attachments in slice %s\n",
			injection.MigName, injection.MmigName, injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}

	default:
		http.Error(w, "Method not allowed for MMIG attachment injection", http.StatusMethodNotAllowed)
	}
}

// handleClearInjection clears only the injections for a given slice, leaving resources intact.
// Example:
//
//	curl -X POST -H "Content-Type: application/json" \
//	  -d '{"sliceName": "test-slice", "clearOpStatuses": true, "clearMmigHealth": true}' \
//	  http://localhost:8080/inject/clear
func handleClearInjection(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received clear injections request: %s %s", r.Method, r.URL.Path)

	var reqBody struct {
		SliceName         string `json:"sliceName"`
		ClearOpStatuses   bool   `json:"clearOpStatuses,omitempty"`
		ClearMmigHealth   bool   `json:"clearMmigHealth,omitempty"`
		ClearTopoHealth   bool   `json:"clearTopoHealth,omitempty"`
		ClearHttpFailures bool   `json:"clearHttpFailures,omitempty"`
		ClearTimeouts     bool   `json:"clearTimeouts,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if reqBody.SliceName == "" {
		http.Error(w, "sliceName must be provided in the request body", http.StatusBadRequest)
		return
	}

	injections, exists := sliceInjections[reqBody.SliceName]
	if !exists {
		log.Printf("[Mocking GCE] Warning: No injections found for slice '%s' to clear.", reqBody.SliceName)
		if _, err := fmt.Fprintf(w, "Warning: No injections found for slice %s to clear.\n", reqBody.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
		return
	}

	// Check if any specific clear flags are set
	isSpecificClear := reqBody.ClearOpStatuses || reqBody.ClearTopoHealth ||
		reqBody.ClearMmigHealth || reqBody.ClearHttpFailures || reqBody.ClearTimeouts
	clearedItems := []string{}

	if isSpecificClear {
		// Clear specific injection maps
		if reqBody.ClearOpStatuses {
			injections.InjectedOpStatuses = make(map[string]string)
			clearedItems = append(clearedItems, "InjectedOpStatuses")
		}
		if reqBody.ClearMmigHealth {
			injections.InjectedMmigHealth = make(map[string]MmigHealthInjection)
			clearedItems = append(clearedItems, "InjectedMmigHealth")
		}
		if reqBody.ClearTopoHealth {
			injections.InjectedTopoHealth = make(map[string]string)
			clearedItems = append(clearedItems, "InjectedTopoHealth")
		}
		if reqBody.ClearHttpFailures {
			injections.StepHttpFailures = make(map[InjectedOperationStep]HttpError)
			clearedItems = append(clearedItems, "StepHttpFailures")
		}
		if reqBody.ClearTimeouts {
			injections.StepTimeouts = make(map[InjectedOperationStep]time.Duration)
			clearedItems = append(clearedItems, "StepTimeouts")
		}
		log.Printf("[Mocking GCE] Cleared specific injections for slice '%s': %v", reqBody.SliceName, clearedItems)
		if _, err := fmt.Fprintf(w, "Cleared specific injections for slice %s: %v\n",
			reqBody.SliceName, clearedItems); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	} else {
		// Default behavior: clear all injections for the slice
		sliceInjections[reqBody.SliceName] = &SliceInjections{
			StepHttpFailures:   make(map[InjectedOperationStep]HttpError),
			StepTimeouts:       make(map[InjectedOperationStep]time.Duration),
			InjectedOpStatuses: make(map[string]string),
			InjectedTopoHealth: make(map[string]string),
			InjectedMmigHealth: make(map[string]MmigHealthInjection),
		}
		log.Printf("[Mocking GCE] Cleared all injections for slice '%s'", reqBody.SliceName)
		if _, err := fmt.Fprintf(w, "Cleared all injections for slice %s\n", reqBody.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	}
}

// handleClear clears resources and injections.
// If a sliceName is provided, it clears data for that slice only.
// If no sliceName is provided, it clears all data from the mock server.
// Example (slice):
//
//	curl -X POST -H "Content-Type: application/json" -d '{"sliceName": "test-slice"}' http://localhost:8080/clear
//	Example (all):   curl -X POST http://localhost:8080/clear
func handleClear(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received clear request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName string `json:"sliceName"`
	}
	// It's okay if the body is empty or fails to decode, it means clear all.
	_ = json.NewDecoder(r.Body).Decode(&injection)

	if injection.SliceName != "" {
		// Clear a specific slice injections and resources
		cleanupSliceResources(injection.SliceName)
		delete(sliceInjections, injection.SliceName)
		log.Printf("[Mocking GCE] Cleared injections and resources for slice '%s'", injection.SliceName)
		if _, err := fmt.Fprintf(w, "Cleared injections and resources for slice %s\n", injection.SliceName); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	} else {
		// Clear everything
		gceResources = &GCEResources{
			Policies:      make(map[string]*alphacompute.ResourcePolicy),
			Mmigs:         make(map[string]*MockMMIG),
			Migs:          make(map[string]*MockMIG),
			Operations:    make(map[string]*alphacompute.Operation),
			MigTopologies: make(map[string]map[string]*CustomAcceleratorTopologyInfo),
		}
		clear(sliceInjections)
		// Reset QPS counters
		for _, stats := range qpsCounters {
			*stats = QPSStats{}
		}
		serviceStartTime = time.Now()
		log.Printf("[Mocking GCE] Cleared all data from the mock server.")
		if _, err := fmt.Fprint(w, "Cleared all data from the mock server.\n"); err != nil {
			log.Printf("[Mocking GCE] Error writing response: %v", err)
		}
	}
}

// --- Sorting Helper Types ---
type policySlice []*alphacompute.ResourcePolicy
type mmigSlice []*MockMMIG
type migSlice []*MockMIG
type operationSlice []*alphacompute.Operation

// SortedSliceResources holds sorted slices of resources for stable JSON output.
type SortedSliceResources struct {
	Policies   policySlice    `json:"Policies"`
	Mmigs      mmigSlice      `json:"Mmigs"`
	Migs       migSlice       `json:"Migs"`
	Operations operationSlice `json:"Operations"`
}

func toSortedResources(resources *GCEResources) *SortedSliceResources {
	sorted := &SortedSliceResources{}
	for _, v := range resources.Policies {
		sorted.Policies = append(sorted.Policies, v)
	}
	for _, v := range resources.Mmigs {
		sorted.Mmigs = append(sorted.Mmigs, v)
	}
	for _, v := range resources.Migs {
		sorted.Migs = append(sorted.Migs, v)
	}
	for _, v := range resources.Operations {
		sorted.Operations = append(sorted.Operations, v)
	}
	return sorted
}

// handleListResources lists all current resources and injections in the mock server.
// It can be filtered by a slice name using the `sliceName` query parameter.
// Example:
// curl http://localhost:8080/list
// curl 'http://localhost:8080/list?sliceName=test-slice'
func handleListResources(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received request to list resources: %s %s", r.Method, r.URL.Path)

	sliceName := r.URL.Query().Get("sliceName")

	type allStates struct {
		Resources  any `json:"resources"`
		Injections any `json:"injections"`
	}

	var dataToEncode allStates

	// All resources are global now
	sortedRes := toSortedResources(gceResources)
	sort.Slice(sortedRes.Policies, func(i, j int) bool {
		return sortedRes.Policies[i].CreationTimestamp < sortedRes.Policies[j].CreationTimestamp
	})
	sort.Slice(sortedRes.Mmigs, func(i, j int) bool {
		return sortedRes.Mmigs[i].Raw.CreationTimestamp < sortedRes.Mmigs[j].Raw.CreationTimestamp
	})
	sort.Slice(sortedRes.Migs, func(i, j int) bool {
		return sortedRes.Migs[i].Raw.CreationTimestamp < sortedRes.Migs[j].Raw.CreationTimestamp
	})
	sort.Slice(sortedRes.Operations, func(i, j int) bool {
		return sortedRes.Operations[i].InsertTime < sortedRes.Operations[j].InsertTime
	})
	dataToEncode.Resources = sortedRes

	if sliceName != "" {
		if inj, ok := sliceInjections[sliceName]; ok {
			dataToEncode.Injections = inj
		} else {
			dataToEncode.Injections = "Not found"
		}
	} else {
		dataToEncode.Injections = sliceInjections
	}

	w.Header().Set("Content-Type", "application/json")

	b, err := json.MarshalIndent(dataToEncode, "", "  ")
	if err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(b); err != nil {
		log.Printf("[Mocking GCE] Error writing response: %v", err)
	}
}

func shouldReturnInjectedError(sliceName string, stepName InjectedOperationStep) (bool, HttpError) {
	if _, ok := sliceInjections[sliceName]; !ok {
		// If the slice doesn't exist, there can't be an injected error.
		return false, HttpError{}
	}

	if failure, injected := sliceInjections[sliceName].StepHttpFailures[stepName]; injected {
		log.Printf("[Mocking GCE] Found HTTP error injection for step '%s' in slice '%s': Code=%d, Message=%s",
			stepName, sliceName, failure.Code, failure.Message)
		return true, failure
	}

	return false, HttpError{}
}

func sleepIfInjectedTimeout(sliceName string, stepName InjectedOperationStep) {
	// We assume mu is locked by the caller.
	if _, ok := sliceInjections[sliceName]; !ok {
		return
	}

	timeout, injected := sliceInjections[sliceName].StepTimeouts[stepName]
	if !injected {
		return
	}

	log.Printf("[Mocking GCE] Found timeout injection for step '%s' in slice '%s': %v. Releasing lock and sleeping...",
		stepName, sliceName, timeout)

	mu.Unlock()
	time.Sleep(timeout)
	mu.Lock()

	log.Printf("[Mocking GCE] Finished sleeping for step '%s' in slice '%s'. Re-acquired lock.", stepName, sliceName)
}

func handleCreateMMIG(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received request: %s %s", r.Method, r.URL.Path)
	var mmig alphacompute.MultiMig
	if err := json.NewDecoder(r.Body).Decode(&mmig); err != nil {
		log.Printf("[Mocking GCE] Error decoding request body for CreateMMIG: %v", err)
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	// The MMIG name can be just the slice name or sliceName-clusterHash.
	// We use the same logic as for policies to find the base slice name.
	sliceName, found := getSliceNameFromPolicyName(mmig.Name)

	if !found {
		msg := fmt.Sprintf("Slice for MMIG '%s' not found. Please initialize the slice first by calling "+
			"POST /inject/slice with '{\"sliceName\": \"<your-slice-name>\"}'.", mmig.Name)
		log.Printf("[Mocking GCE] Warning: %s", msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	} else {
		sleepIfInjectedTimeout(sliceName, CreateMMIG)
		// Check for injected HTTP error before proceeding
		if shouldInject, failure := shouldReturnInjectedError(sliceName, CreateMMIG); shouldInject {
			log.Printf("[Mocking GCE] Returning injected HTTP error for CreateMMIG: Code=%d, Message=%s",
				failure.Code, failure.Message)
			http.Error(w, failure.Message, failure.Code)
			return
		}
	}

	// Check for op status injection using both the full name and the base name (without hash).
	if status, injected := findInjectedOpStatus(sliceName, OpPrefixCreateMMIG, mmig.Name); injected {
		log.Printf("[Mocking GCE] Found injected op status '%s' for CreateMMIG '%s'. Skipping resource creation.",
			status, mmig.Name)
		region := getResourceFromPath(r.URL.Path, "regions")
		opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixCreateMMIG, mmig.Name, sliceName)
		selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
		targetLink := fmt.Sprintf("%s/%s", r.URL.Path, mmig.Name)
		op := operationDone(opName, "insert", selfLink, targetLink, region, "")
		op.Status = status // Override with the injected status.
		handle(w, r, op)
		return
	}

	if _, exists := gceResources.Mmigs[mmig.Name]; exists {
		log.Printf("[Mocking GCE] Conflict: MMIG '%s' already exists in slice '%s'", mmig.Name, sliceName)
		http.Error(w, "Conflict", http.StatusConflict)
		return
	}

	log.Printf("[Mocking GCE] Creating MMIG resource '%s' in slice '%s'", mmig.Name, sliceName)
	mmig.CreationTimestamp = time.Now().Format(time.RFC3339)

	var oldMmig *MockMMIG
	var foundMmig bool

	if om, exists := gceResources.Mmigs[sliceName]; exists {
		oldMmig = om
		foundMmig = true
		log.Printf("[Mocking GCE] Found initialized MMIG for slice '%s'. Aliasing to '%s'.", sliceName, mmig.Name)
	} else {
		for mName, mmigInstance := range gceResources.Mmigs {
			if strings.HasPrefix(mName, sliceName) || strings.HasPrefix(sliceName, mName) {
				oldMmig = mmigInstance
				foundMmig = true
				log.Printf("[Mocking GCE] Found prefix-matched initialized MMIG '%s' for slice '%s'. Aliasing to '%s'.",
					mName, sliceName, mmig.Name)
				break
			}
		}
	}

	if foundMmig {
		oldMmig.Name = mmig.Name
		oldMmig.Raw = &mmig
		gceResources.Mmigs[mmig.Name] = oldMmig // Alias
	} else {
		gceResources.Mmigs[mmig.Name] = &MockMMIG{
			Name: mmig.Name,
			Raw:  &mmig,
		}
	}
	region := getResourceFromPath(r.URL.Path, "regions")
	opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixCreateMMIG, mmig.Name, sliceName)
	selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
	targetLink := fmt.Sprintf("%s/%s", r.URL.Path, mmig.Name)
	op := operationDone(opName, "insert", selfLink, targetLink, region, "")
	log.Printf("[Mocking GCE] Creating operation '%s' for CreateMMIG '%s'", opName, mmig.Name)
	gceResources.Operations[opName] = op
	handle(w, r, op)
}

func findMMIG(resourceName string) (*MockMMIG, bool) {
	mmig, exists := gceResources.Mmigs[resourceName]
	if !exists {
		for mmigName, mmigInstance := range gceResources.Mmigs {
			if isHashedVersionOf(resourceName, mmigName) || isHashedVersionOf(mmigName, resourceName) {
				log.Printf("[Mocking GCE] Found matching MMIG '%s' for request '%s' in global resources.", mmigName, resourceName)
				return mmigInstance, true
			}
		}
	}
	return mmig, exists
}

func populateMMIGStatus(mmig *MockMMIG) {
	if mmig.Raw.Status == nil {
		mmig.Raw.Status = &alphacompute.MultiMigStatus{}
	}
	memberURLs := make([]string, 0, len(mmig.AttachedMigs))
	for _, migName := range mmig.AttachedMigs {
		migUrl := fmt.Sprintf("projects/test-project/zones/us-central1-a/instanceGroupManagers/%s", migName) // Fallback
		if mig, exists := gceResources.Migs[migName]; exists && mig.Raw != nil && mig.Raw.SelfLink != "" {
			migUrl = mig.Raw.SelfLink
		}
		memberURLs = append(memberURLs, migUrl)
	}
	mmig.Raw.Status.MemberInstanceGroupManagers = memberURLs
	mmig.Raw.Status.MembersCount = int64(len(memberURLs))
}

func getInjectedMMIGHealth(sliceName, resourceName string) (MmigHealthInjection, bool) {
	if sliceName != "" {
		if injection, ok := sliceInjections[sliceName]; ok {
			injectedHealth, existsHealth := injection.InjectedMmigHealth[resourceName]
			if !existsHealth {
				injectedHealth, existsHealth = injection.InjectedMmigHealth[sliceName]
			}
			return injectedHealth, existsHealth
		}
	}
	return MmigHealthInjection{}, false
}

func computeMMIGStatus(mmig *MockMMIG, sliceName, resourceName string) string {
	log.Printf("[Mocking GCE] No injected health for MMIG '%s', computing from attached members", resourceName)

	expectedMigs := 0
	if injection, ok := sliceInjections[sliceName]; ok && len(injection.WantedMigs) > 0 {
		expectedMigs = len(injection.WantedMigs)
		log.Printf("[Mocking GCE] Using WantedMigs from SliceInjections (%d) as expected count for MMIG '%s'",
			expectedMigs, resourceName)
	}

	if expectedMigs == 0 {
		if mmig.ExpectedMigs == 0 {
			mmig.ExpectedMigs = 1
			if mmig.Raw.ResourcePolicies != nil && mmig.Raw.ResourcePolicies.WorkloadPolicy != "" {
				parts := strings.Split(mmig.Raw.ResourcePolicies.WorkloadPolicy, "/")
				policyName := parts[len(parts)-1]
				if policy, ok := gceResources.Policies[policyName]; ok {
					if policy.WorkloadPolicy != nil {
						topo := policy.WorkloadPolicy.AcceleratorTopology
						log.Printf("[Mocking GCE] Found policy '%s' with topology '%s' for MMIG '%s'", policyName, topo, resourceName)
						switch topo {
						case "4x4x4":
							mmig.ExpectedMigs = 1
						case "4x4x8":
							mmig.ExpectedMigs = 2
						case "4x8x8":
							mmig.ExpectedMigs = 4
						case "8x8x8":
							mmig.ExpectedMigs = 8
						default:
							log.Printf("[Mocking GCE] Unknown topology '%s' for MMIG '%s', defaulting to 1 cube", topo, resourceName)
							mmig.ExpectedMigs = 1
						}
						mmig.ExpectedTopology = topo
					}
				}
			}
		}
		expectedMigs = mmig.ExpectedMigs
	}

	actualCount := len(mmig.AttachedMigs)
	computedStatus := "ACTIVATING"

	if mmig.DeformationIntent {
		log.Printf("[Mocking GCE] Mutation Intent (Deformation) detected for MMIG '%s'", resourceName)
		if actualCount == expectedMigs {
			computedStatus = "DEACTIVATING"
		} else if actualCount < expectedMigs {
			computedStatus = string(MMIGHealthStatusIncomplete)
		}
	} else {
		log.Printf("[Mocking GCE] Normal Creation/Stable state for MMIG '%s'", resourceName)
		if actualCount == 0 {
			computedStatus = string(MMIGHealthStatusActive)
		} else if actualCount < expectedMigs {
			computedStatus = "ACTIVATING"
		} else if actualCount == expectedMigs {
			hasDegraded := false
			hasUnhealthy := false
			hasHealthy := false

			for _, migName := range mmig.AttachedMigs {
				if mig, ok := gceResources.Migs[migName]; ok {
					if mig.Raw.Status != nil && len(mig.Raw.Status.AppliedAcceleratorTopologies) > 0 {
						migState := mig.Raw.Status.AppliedAcceleratorTopologies[0].State
						switch migState {
						case string(AcceleratorTopologyHealthStatusDegraded), string(MMIGHealthStatusActiveDegraded):
							hasDegraded = true
						case string(AcceleratorTopologyHealthStatusUnhealthy), string(MMIGHealthStatusFailed):
							hasUnhealthy = true
						case string(MMIGHealthStatusActive), string(AcceleratorTopologyHealthStatusHealthy):
							hasHealthy = true
						}
					} else {
						hasHealthy = true
					}
				}
			}

			if hasUnhealthy {
				computedStatus = string(AcceleratorTopologyHealthStatusUnhealthy)
			} else if hasDegraded {
				computedStatus = string(MMIGHealthStatusActiveDegraded)
			} else if hasHealthy {
				computedStatus = string(MMIGHealthStatusActive)
			} else {
				computedStatus = string(MMIGHealthStatusActive)
			}
		}
	}

	log.Printf("[Mocking GCE] Computed MMIG status for '%s' is '%s' (Deforming: %v)",
		resourceName, computedStatus, mmig.DeformationIntent)
	return computedStatus
}

func handleGetMMIG(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	recordQPS("GetMMIG")
	resourceName := getResourceName(r.URL.Path)

	log.Printf("[Mocking GCE] <-- Received request: %s %s", r.Method, r.URL.Path)

	sliceName, sliceExists := getSliceNameFromPolicyName(resourceName)

	// Check for injected HTTP error before proceeding
	if sliceExists {
		sleepIfInjectedTimeout(sliceName, GetMMIG)
		if shouldInject, failure := shouldReturnInjectedError(sliceName, GetMMIG); shouldInject {
			log.Printf("[Mocking GCE] Returning injected HTTP error for GetMMIG: Code=%d, Message=%s",
				failure.Code, failure.Message)
			http.Error(w, failure.Message, failure.Code)
			return
		}
	} else {
		log.Printf("[Mocking GCE] Not Found: Slice '%s' does not exist", sliceName)
		http.NotFound(w, r)
		return
	}
	mmig, exists := findMMIG(resourceName)
	if !exists {
		log.Printf("[Mocking GCE] Not Found: MMIG '%s' does not exist in global resources", resourceName)
		http.NotFound(w, r)
		return
	}

	populateMMIGStatus(mmig)

	injectedHealth, existsHealth := getInjectedMMIGHealth(sliceName, resourceName)
	// Use global state if already set (not empty)

	if existsHealth {
		log.Printf("[Mocking GCE] Found injected MMIG health for slice '%s' from sliceInjections: %+v",
			sliceName, injectedHealth)
		topo := &alphacompute.MultiMigStatusAcceleratorTopology{
			AcceleratorTopology:      injectedHealth.AcceleratorTopology,
			AcceleratorTopologyState: injectedHealth.AcceleratorTopologyState,
		}
		if injectedHealth.ErrorMessage != "" {
			lastCheck := &alphacompute.MultiMigStatusAcceleratorTopologyAcceleratorTopologyStateLastCheck{
				Error: &alphacompute.MultiMigStatusAcceleratorTopologyAcceleratorTopologyStateLastCheckError{
					Errors: []*alphacompute.MultiMigStatusAcceleratorTopologyAcceleratorTopologyStateLastCheckErrorErrors{
						{Message: injectedHealth.ErrorMessage},
					},
				},
			}
			topo.AcceleratorTopologyStateLastCheck = lastCheck
		}
		mmig.Raw.Status.AppliedAcceleratorTopologies = []*alphacompute.MultiMigStatusAcceleratorTopology{topo}
	} else {
		computedStatus := computeMMIGStatus(mmig, sliceName, resourceName)
		mmig.Raw.Status.AppliedAcceleratorTopologies = []*alphacompute.MultiMigStatusAcceleratorTopology{{
			AcceleratorTopologyState: computedStatus,
		}}
	}

	// Hybrid response: include 'items' for members list
	var mmigMap map[string]interface{}
	mmigBytes, _ := json.Marshal(mmig.Raw)
	if err := json.Unmarshal(mmigBytes, &mmigMap); err != nil {
		log.Printf("[Mocking GCE] Error unmarshaling MMIG: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	items := make([]map[string]interface{}, 0, len(mmig.AttachedMigs))
	for _, migName := range mmig.AttachedMigs {
		items = append(items, map[string]interface{}{
			"status": map[string]interface{}{
				"instanceGroupManager": fmt.Sprintf("projects/123/zones/test-zone/instanceGroupManagers/%s", migName),
			},
		})
	}
	mmigMap["items"] = items

	handle(w, r, mmigMap)
}

func handleListMMIGs(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received request to list MMIGs: %s %s", r.Method, r.URL.Path)

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// Expected format: projects/{project}/regions/{region}/multiMigs
	if len(parts) < 5 {
		http.Error(w, "malformed URL for ListMMIGs", http.StatusBadRequest)
		return
	}
	reqProject := parts[1]
	reqRegion := parts[3]

	matchingMMIGs := []*alphacompute.MultiMig{}
	for _, mmig := range gceResources.Mmigs {
		if mmig == nil || mmig.Raw == nil || mmig.Raw.Region == "" {
			continue
		}

		// The MMIG's region field is a full URL, so we extract the name.
		// e.g., projects/test-project/regions/us-central1
		mmigRegionParts := strings.Split(mmig.Raw.Region, "/")
		var mmigProject, mmigRegion string
		if len(mmigRegionParts) > 3 {
			mmigProject = mmigRegionParts[1]
			mmigRegion = mmigRegionParts[3]
		}

		if mmigProject == reqProject && mmigRegion == reqRegion {
			matchingMMIGs = append(matchingMMIGs, mmig.Raw)
		}
	}

	log.Printf("[Mocking GCE] Found %d MMIGs in project '%s' and region '%s'", len(matchingMMIGs), reqProject, reqRegion)

	// The client library expects a paginated response object.
	response := &alphacompute.MultiMigsList{
		Items: matchingMMIGs,
		Kind:  "compute#multiMigsList",
		Id:    fmt.Sprintf("projects/%s/regions/%s/multiMigs", reqProject, reqRegion),
		// SelfLink can be set to the request URL for completeness.
		SelfLink: r.URL.String(),
	}
	handle(w, r, response)
}

func handleCreateResourcePolicy(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received request: %s %s", r.Method, r.URL.Path)
	var policy alphacompute.ResourcePolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		log.Printf("[Mocking GCE] Error decoding request body for CreateResourcePolicy: %v", err)
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	// Find the corresponding sliceName from the policy name.
	sliceName, found := getSliceNameFromPolicyName(policy.Name)

	if !found {
		msg := fmt.Sprintf("Slice for policy '%s' not found and could not be derived. "+
			"Please initialize the slice first by calling POST /inject/slice.", policy.Name)
		log.Printf("[Mocking GCE] Error: %s", msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// Initialize slice injections on-the-fly if not present.
	if _, exists := sliceInjections[sliceName]; !exists {
		log.Printf("[Mocking GCE] Slice '%s' not found in injections. Initializing it on-the-fly from policy name '%s'.",
			sliceName, policy.Name)
		initSlice(sliceName)
	}

	// Check for an injected operation status *before* any other logic.
	// If an op status is injected, we should not create the resource, just return the operation.
	if status, injected := findInjectedOpStatus(sliceName, OpPrefixCreatePolicy, policy.Name); injected {
		log.Printf("[Mocking GCE] Found injected op status '%s' for CreateWorkloadPolicy '%s'. "+
			"Skipping resource creation.", status, policy.Name)
		region := getResourceFromPath(r.URL.Path, "regions")
		opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixCreatePolicy, policy.Name, sliceName)
		selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
		targetLink := fmt.Sprintf("%s/%s", r.URL.Path, policy.Name)
		op := operationDone(opName, "insert", selfLink, targetLink, region, "")
		op.Status = status // Override with the injected status.
		handle(w, r, op)
		return
	}

	sleepIfInjectedTimeout(sliceName, CreateWorkloadPolicy)
	// Check for injected HTTP error before proceeding
	if shouldInject, failure := shouldReturnInjectedError(sliceName, CreateWorkloadPolicy); shouldInject {
		log.Printf("[Mocking GCE] Returning injected HTTP error for CreateWorkloadPolicy: Code=%d, Message=%s",
			failure.Code, failure.Message)
		http.Error(w, failure.Message, failure.Code)
		return
	}

	log.Printf("[Mocking GCE] Found slice '%s' for policy '%s'", sliceName, policy.Name)

	if _, exists := gceResources.Policies[policy.Name]; exists {
		log.Printf("[Mocking GCE] Conflict: ResourcePolicy '%s' already exists in slice '%s'", policy.Name, sliceName)
		http.Error(w, "Conflict", http.StatusConflict)
		return
	}

	log.Printf("[Mocking GCE] Creating ResourcePolicy resource '%s' in slice '%s'", policy.Name, sliceName)
	policy.CreationTimestamp = time.Now().Format(time.RFC3339)
	gceResources.Policies[policy.Name] = &policy

	region := getResourceFromPath(r.URL.Path, "regions")
	opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixCreatePolicy, policy.Name, sliceName)
	selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
	targetLink := fmt.Sprintf("%s/%s", r.URL.Path, policy.Name)
	op := operationDone(opName, "insert", selfLink, targetLink, region, "")
	gceResources.Operations[opName] = op
	log.Printf("[Mocking GCE] Creating operation '%s' for CreateResourcePolicy '%s'", opName, policy.Name)
	handle(w, r, op)
}

func handleGetInstanceGroupManager(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	resourceName := getResourceName(r.URL.Path)

	log.Printf("[Mocking GCE] <-- Received request: %s %s", r.Method, r.URL.Path)

	sliceName, found := getSliceNameFromResource(resourceName)
	if !found {
		log.Printf("[Mocking GCE] Not Found: Slice for MIG '%s' does not exist.", resourceName)
		http.NotFound(w, r)
		return
	}
	if _, sliceExists := sliceInjections[sliceName]; !sliceExists {
		log.Printf("[Mocking GCE] Not Found: Slice '%s' does not exist in injections.", sliceName)
		http.NotFound(w, r)
		return
	}
	// Check for injected HTTP error before proceeding
	sleepIfInjectedTimeout(sliceName, GetInstanceGroupManager)
	if shouldInject, failure := shouldReturnInjectedError(sliceName, GetInstanceGroupManager); shouldInject {
		log.Printf("[Mocking GCE] Returning injected HTTP error for GetInstanceGroupManager: Code=%d, Message=%s",
			failure.Code, failure.Message)
		http.Error(w, failure.Message, failure.Code)
		return
	}

	mig, exists := gceResources.Migs[resourceName]
	if !exists {
		log.Printf("[Mocking GCE] MIG '%s' not found in global resources", resourceName)
		http.NotFound(w, r)
		return
	}

	// In a real scenario, you might inject health status here.
	handle(w, r, mig)
}

func handleAttachMIGToMMIG(w http.ResponseWriter, r *http.Request, patchIgm *alphacompute.InstanceGroupManager) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received ATTACH request: %s %s", r.Method, r.URL.Path)

	migName := getResourceName(r.URL.Path)
	mmigName := getResourceName(patchIgm.MultiMig)
	sliceName, found := getSliceNameFromPolicyName(mmigName)

	opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixAttachMIGs, migName, sliceName)
	if op, exists := gceResources.Operations[opName]; exists {
		log.Printf("[Mocking GCE] Found existing/injected operation '%s' with status '%s' for AttachMIG '%s'. "+
			"Skipping resource modification.", opName, op.Status, migName)
		handle(w, r, op)
		return
	}

	if found {
		sleepIfInjectedTimeout(sliceName, AttachMIGToMMIG)
		// Check for injected HTTP error after finding the slice
		if shouldInject, failure := shouldReturnInjectedError(sliceName, AttachMIGToMMIG); shouldInject {
			log.Printf("[Mocking GCE] Returning injected HTTP error for AttachMIGToMMIG: Code=%d, Message=%s",
				failure.Code, failure.Message)
			http.Error(w, failure.Message, failure.Code)
			return
		}
	} else {
		log.Printf("[Mocking GCE] Not Found: slice for MMIG '%s' does not exist", mmigName)
		http.NotFound(w, r)
		return
	}
	log.Printf("[Mocking GCE] Found slice '%s' for MMIG '%s'", sliceName, mmigName)

	_, exists := gceResources.Mmigs[mmigName]
	if !exists {
		log.Printf("[Mocking GCE] Not Found: MMIG '%s' does not exist in slice '%s' for attach", mmigName, sliceName)
		http.NotFound(w, r)
		return
	}

	mig, exists := gceResources.Migs[migName]
	if !exists {
		log.Printf("[Mocking GCE] MIG '%s' not found in slice '%s' for attach, creating it on-the-fly.", migName, sliceName)
		mig = &MockMIG{
			Name: migName,
			Raw: &alphacompute.InstanceGroupManager{
				Name:              migName,
				SelfLink:          r.URL.Path,
				CreationTimestamp: time.Now().Format(time.RFC3339),
			},
		}
		gceResources.Migs[migName] = mig
	}

	// Enforce single-MIG attachment constraint using object state
	if mig.MultiMig != "" && mig.MultiMig != mmigName {
		log.Printf("[Mocking GCE] Error: MIG '%s' is already attached to a different MMIG '%s'", migName, mig.MultiMig)
		http.Error(w, fmt.Sprintf("MIG %s is already attached to MMIG %s", migName, mig.MultiMig), http.StatusConflict)
		return
	}

	// Simulate attaching the MIG to the MMIG.
	alreadyAttachedToSame := (mig.MultiMig == mmigName)

	if alreadyAttachedToSame {
		log.Printf("[Mocking GCE] MIG '%s' is already attached to MMIG '%s'. No-op.", migName, mmigName)
	} else {
		log.Printf("[Mocking GCE] Attaching MIG '%s' in slice '%s' to MultiMig: '%s'", migName, sliceName, patchIgm.MultiMig)
		mig.MultiMig = patchIgm.MultiMig
		gceResources.Mmigs[mmigName].AttachedMigs = append(gceResources.Mmigs[mmigName].AttachedMigs, migName)

		// Also track in WantedMigs (intent)
		if injection, ok := sliceInjections[sliceName]; ok {
			foundWanted := false
			for _, m := range injection.WantedMigs {
				if m == migName {
					foundWanted = true
					break
				}
			}
			if !foundWanted {
				injection.WantedMigs = append(injection.WantedMigs, migName)
				log.Printf("[Mocking GCE] Tracked MIG '%s' in WantedMigs for slice '%s'", migName, sliceName)
			}
		}
	}

	zone := getResourceFromPath(r.URL.Path, "zones")
	selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
	targetLink := r.URL.Path
	op := operationDone(opName, "patch", selfLink, targetLink, "", zone)
	log.Printf("[Mocking GCE] Creating operation '%s' for AttachMIG '%s'", opName, migName)
	gceResources.Operations[opName] = op
	handle(w, r, op)
}

func handleDetachMIGFromMMIG(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	migName := getResourceName(r.URL.Path)
	log.Printf("[Mocking GCE] <-- Received DETACH request: %s %s", r.Method, r.URL.Path)
	mig, exists := gceResources.Migs[migName]
	found := exists
	var sliceName string
	if exists {
		if mig.MultiMig != "" {
			mmigName := getResourceName(mig.MultiMig)
			sliceName, _ = getSliceNameFromPolicyName(mmigName)
		} else if mig.SliceName != "" {
			sliceName = mig.SliceName
		}
	}
	if sliceName == "" {
		sliceName, _ = getSliceNameFromResource(migName) // Fallback
	}

	opKey := OpPrefixDetachMIGs + migName
	if found {
		var mmigName string
		if mig.MultiMig != "" {
			mmigName = getResourceName(mig.MultiMig)
		} else if mig.SliceName != "" {
			for mName := range gceResources.Mmigs {
				if strings.HasPrefix(mName, mig.SliceName) {
					mmigName = mName
					break
				}
			}
		}
		var injected bool
		var status string
		if sliceName != "" {
			if injection, ok := sliceInjections[sliceName]; ok {
				status, injected = injection.InjectedOpStatuses[opKey]
			}
		}

		// If failure is injected, return immediately without setting DeformationIntent
		if injected && status == "FAIL" {
			log.Printf("[Mocking GCE] Found injected FAIL status for DetachMIG '%s'. Skipping resource modification.", migName)
			zone := getResourceFromPath(r.URL.Path, "zones")
			opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixDetachMIGs, migName, sliceName)
			selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
			targetLink := r.URL.Path
			op := operationDone(opName, "patch", selfLink, targetLink, "", zone)
			op.Status = status
			handle(w, r, op)
			return
		}

		if mmigName != "" {
			if mockMmig, ok := gceResources.Mmigs[mmigName]; ok {
				mockMmig.DeformationIntent = true
				log.Printf("[Mocking GCE] Set DeformationIntent=true for MMIG '%s' upon receiving detach call for MIG '%s'",
					mmigName, migName)
			}
		}

		// If running is injected, return early after setting DeformationIntent to simulate hanging op
		if injected && status == opStatusRunning {
			log.Printf("[Mocking GCE] Found injected RUNNING status for DetachMIG '%s'. Skipping modification.", migName)
			zone := getResourceFromPath(r.URL.Path, "zones")
			opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixDetachMIGs, migName, sliceName)
			selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
			targetLink := r.URL.Path
			op := operationDone(opName, "patch", selfLink, targetLink, "", zone)
			op.Status = status
			handle(w, r, op)
			return
		}
	}

	if found {
		sleepIfInjectedTimeout(sliceName, DetachMIGFromMMIG)
		// Check for injected HTTP error after finding the slice
		if shouldInject, failure := shouldReturnInjectedError(sliceName, DetachMIGFromMMIG); shouldInject {
			log.Printf("[Mocking GCE] Returning injected HTTP error for DetachMIGFromMMIG: Code=%d, Message=%s",
				failure.Code, failure.Message)
			http.Error(w, failure.Message, failure.Code)
			return
		}
	} else {
		log.Printf("[Mocking GCE] Warning: MIG '%s' not found in any slice for detach. "+
			"Falling back to default success.", migName)
		sliceName = defaultSliceName
		opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixDetachMIGs, migName, sliceName)
		zone := getResourceFromPath(r.URL.Path, "zones")
		selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
		targetLink := r.URL.Path
		op := operationDone(opName, "patch", selfLink, targetLink, "", zone)
		gceResources.Operations[opName] = op
		handle(w, r, op)
		return
	}

	opName := fmt.Sprintf("%s%s/slice/%s", OpPrefixDetachMIGs, migName, sliceName)
	if op, exists := gceResources.Operations[opName]; exists && op.Status == "RUNNING" {
		log.Printf("[Mocking GCE] Detach operation '%s' already exists and is RUNNING (hanging). "+
			"Skipping detachment state changes.", opName)
		handle(w, r, op)
		return
	}

	// Simulate detaching the MIG from the MMIG.
	performDetachStateChanges(sliceName, migName)
	zone := getResourceFromPath(r.URL.Path, "zones")
	opName = fmt.Sprintf("%s%s/slice/%s", OpPrefixDetachMIGs, migName, sliceName)
	selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
	targetLink := r.URL.Path
	op := operationDone(opName, "patch", selfLink, targetLink, "", zone)
	log.Printf("[Mocking GCE] Creating operation '%s' for DetachMIG '%s'", opName, migName)
	gceResources.Operations[opName] = op
	handle(w, r, op)
}

func performDetachStateChanges(sliceName, migName string) {
	mig, ok := gceResources.Migs[migName]
	if !ok {
		log.Printf("[Mocking GCE] MIG '%s' not found for detach state changes", migName)
		return
	}

	if mig.MultiMig != "" || mig.SliceName != "" {
		mmigName := getResourceName(mig.MultiMig)
		if mmigName == "" && mig.SliceName != "" {
			for mName := range gceResources.Mmigs {
				if strings.HasPrefix(mName, mig.SliceName) {
					mmigName = mName
					break
				}
			}
		}
		log.Printf("[Mocking GCE] Detaching MIG '%s' (Slice '%s') from MultiMig '%s'", migName, sliceName, mmigName)
		mig.MultiMig = ""

		if mmigName != "" {
			if mockMmig, ok := gceResources.Mmigs[mmigName]; ok {
				mockMmig.DeformationIntent = true
				var updatedMigs []string
				for _, m := range mockMmig.AttachedMigs {
					if m != migName {
						updatedMigs = append(updatedMigs, m)
					}
				}
				mockMmig.AttachedMigs = updatedMigs
				log.Printf("[Mocking GCE] Set DeformationIntent=true for MMIG '%s', removed MIG '%s', new attached count=%d",
					mmigName, migName, len(updatedMigs))
			}
		}
	}
}

func performAttachStateChanges(sliceName, migName string) {
	mig, ok := gceResources.Migs[migName]
	if !ok {
		log.Printf("[Mocking GCE] MIG '%s' not found for attach state changes", migName)
		return
	}

	var mmigName string
	for mName := range gceResources.Mmigs {
		if strings.HasPrefix(mName, sliceName) {
			mmigName = mName
			break
		}
	}

	if mmigName == "" {
		log.Printf("[Mocking GCE] MMIG for slice '%s' not found for attach state changes", sliceName)
		return
	}

	log.Printf("[Mocking GCE] Attaching MIG '%s' (Slice '%s') to MultiMig '%s'", migName, sliceName, mmigName)
	mig.MultiMig = mmigName // Using name as simple identifier

	if mockMmig, ok := gceResources.Mmigs[mmigName]; ok {
		alreadyAttached := false
		for _, m := range mockMmig.AttachedMigs {
			if m == migName {
				alreadyAttached = true
				break
			}
		}
		if !alreadyAttached {
			mockMmig.AttachedMigs = append(mockMmig.AttachedMigs, migName)
			log.Printf("[Mocking GCE] Attached MIG '%s' to MMIG '%s', new attached count=%d",
				migName, mmigName, len(mockMmig.AttachedMigs))
		}
	}

	// Also track in WantedMigs (intent) to ensure health check computes correct expected count.
	if injection, ok := sliceInjections[sliceName]; ok {
		foundWanted := false
		for _, m := range injection.WantedMigs {
			if m == migName {
				foundWanted = true
				break
			}
		}
		if !foundWanted {
			injection.WantedMigs = append(injection.WantedMigs, migName)
			log.Printf("[Mocking GCE] Tracked MIG '%s' in WantedMigs for slice '%s' "+
				"during operation transition", migName, sliceName)
		}
	}
}

func handleGetOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	mu.Lock()
	defer mu.Unlock()
	sliceName, opName, found := getSliceAndOpNameFromURL(r.URL.Path)

	if !found {
		// Fallback: try to extract the operation name directly from the URL path.
		// This handles operations (like configureAcceleratorTopology) that don't
		// include /slice/{sliceName} in their operation name.
		opName = getResourceName(r.URL.Path)
		if op, exists := gceResources.Operations[opName]; exists {
			log.Printf("[Mocking GCE] <-- Found operation '%s' by direct lookup (status: %s)", opName, op.Status)
			handle(w, r, op)
			return
		}
		// If not found by direct lookup either, return a generic DONE operation.
		log.Printf("[Mocking GCE] Operation '%s' not found by any method, returning generic DONE.", opName)
		region := getResourceFromPath(r.URL.Path, "regions")
		zone := getResourceFromPath(r.URL.Path, "zones")
		op := operationDone(opName, "get", r.URL.Path, r.URL.Path, region, zone)
		handle(w, r, op)
		return
	}

	log.Printf("[Mocking GCE] <-- Received request to get op: %s %s, sliceName: %s, opName: %s",
		r.Method, r.URL.Path, sliceName, opName)

	sleepIfInjectedTimeout(sliceName, InjectedOperationStep("GetOperation"))
	if shouldInject, failure := shouldReturnInjectedError(sliceName, InjectedOperationStep("GetOperation")); shouldInject {
		log.Printf("[Mocking GCE] Injected HTTP error GetOp: Code=%d, Msg=%s", failure.Code, failure.Message)
		http.Error(w, failure.Message, failure.Code)
		return
	}

	op, exists := gceResources.Operations[fmt.Sprintf("%s/slice/%s", opName, sliceName)] //nolint:staticcheck
	if !exists {
		// If op is not found, create a generic DONE one to avoid breaking clients
		// that poll on operations that completed instantly.
		log.Printf("[Mocking GCE] Operation '%s' not found, returning a generic DONE operation.", opName)
		region := getResourceFromPath(r.URL.Path, "regions")
		zone := getResourceFromPath(r.URL.Path, "zones")
		op = operationDone(opName, "get", r.URL.Path, r.URL.Path, region, zone)
	} else {
		log.Printf("[Mocking GCE] Found operation '%s' with status '%s'", opName, op.Status)
	}

	handle(w, r, op)
}

func handleGetAvailableAcceleratorTopologies(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	recordQPS("GetAvailableAcceleratorTopologies")
	log.Printf("[Mocking GCE] <-- Received request: %s %s", r.Method, r.URL.Path)

	parts := strings.Split(r.URL.Path, "/")
	// Expected path: /projects/{project}/zones/{zone}/instanceGroupManagers/{igmName}/getAvailableAcceleratorTopologies
	if len(parts) < 2 || parts[len(parts)-1] != "getAvailableAcceleratorTopologies" {
		http.Error(w, "malformed URL for getAvailableAcceleratorTopologies", http.StatusBadRequest)
		return
	}
	igmName := parts[len(parts)-2] //nolint:staticcheck

	sliceName, found := getSliceNameFromPolicyName(igmName)
	if !found {
		if mig, ok := gceResources.Migs[igmName]; ok {
			sliceName = "" // Free MIG, no slice association yet
			if mig.MultiMig != "" {
				// If it has MultiMig, we can derive slice from it
				mmigName := getResourceName(mig.MultiMig)
				if sn, ok := getSliceNameFromPolicyName(mmigName); ok {
					sliceName = sn
				}
			}
			log.Printf("[Mocking GCE] Free or unassociated MIG '%s' found (Slice: '%s'), proceeding with default health",
				igmName, sliceName)
			if sliceName == "" {
				for sn, inj := range sliceInjections {
					if _, ok := inj.InjectedTopoHealth[igmName]; ok {
						log.Printf("[Mocking GCE] Found injected topology health for free MIG '%s' in slice '%s'", igmName, sn)
						sliceName = sn
						break
					}
					for _, m := range inj.InjectedMigs {
						if m == igmName {
							log.Printf("[Mocking GCE] Found injected MIG '%s' in slice '%s'", igmName, sn)
							sliceName = sn
							break
						}
					}
					if sliceName != "" {
						break
					}
				}
			}
		} else {
			log.Printf("[Mocking GCE] Warning: IGM '%s' not found in any slice resource. "+
				"Falling back to default HEALTHY state.", igmName)
			sliceName = defaultSliceName
		}
	}
	// Use sliceName derived from getSliceNameFromPolicyName

	sleepIfInjectedTimeout(sliceName, GetAvailableTopologies)
	if shouldInject, failure := shouldReturnInjectedError(sliceName, GetAvailableTopologies); shouldInject {
		log.Printf("[Mocking GCE] Returning injected HTTP error for GetAvailableAcceleratorTopologies: "+
			"Code=%d, Message=%s", failure.Code, failure.Message)
		http.Error(w, failure.Message, failure.Code)
		return
	}

	resp := &alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse{
		AcceleratorTopologiesInfo: make(map[string]alphacompute.
			InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo),
	}

	// Check if we have injected topologies for this MIG (SubSlice path)
	if topos, ok := gceResources.MigTopologies[igmName]; ok && len(topos) > 0 {
		log.Printf("[Mocking GCE] Found %d injected topologies for IGM '%s', returning them", len(topos), igmName)

		type (
			topoInfoType = alphacompute.
					InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo
			topoStateType = alphacompute.
					InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyState
			topoErrorType = alphacompute.
					InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyStateError
			topoErrorItemType = alphacompute.
						InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyStateErrorErrors
		)

		for topoID, info := range topos {
			// Apply injection override if present
			healthStatus := info.AcceleratorTopologyHealth
			if injection, ok := sliceInjections[sliceName]; ok {
				if injHealth, healthInjected := injection.InjectedTopoHealth[topoID]; healthInjected {
					healthStatus = injHealth
					log.Printf("[Mocking GCE] Using injected health status '%s' for topology '%s'", healthStatus, topoID)
				}
			}

			apiInfo := topoInfoType{
				AcceleratorTopology:       info.AcceleratorTopology,
				AcceleratorTopologyHealth: healthStatus,
				InstancesHealth:           string(info.InstancesHealth),
				Parent:                    info.Parent,
			}
			if info.AcceleratorTopologyState != nil {
				var errs *topoErrorType
				if info.AcceleratorTopologyState.Error != nil {
					errs = &topoErrorType{
						Errors: make([]*topoErrorItemType, len(info.AcceleratorTopologyState.Error.Errors)),
					}
					for idx, e := range info.AcceleratorTopologyState.Error.Errors {
						errs.Errors[idx] = &topoErrorItemType{
							Code:    e.Code,
							Message: e.Message,
						}
					}
				}

				currentState := info.AcceleratorTopologyState.CurrentState
				if currentState == "ACTIVATE" {
					currentState = "ACTIVE"
				} else if currentState == "DEACTIVATE" {
					currentState = "INACTIVE"
				}

				apiInfo.AcceleratorTopologyState = &topoStateType{
					CurrentState: currentState,
					ExternalId:   info.AcceleratorTopologyState.ExternalId,
				}
				if errs != nil {
					apiInfo.AcceleratorTopologyState.Error = errs
				}
			}
			resp.AcceleratorTopologiesInfo[topoID] = apiInfo
		}
	} else {
		// Check for injected MIG health status (Fallback / SuperSlice path)
		var healthStatus string
		var injectedHealth string
		var healthInjected bool
		if injection, ok := sliceInjections[sliceName]; ok {
			injectedHealth, healthInjected = injection.InjectedTopoHealth[igmName]
		}
		if healthInjected {
			healthStatus = injectedHealth
			log.Printf("[Mocking GCE] Using injected health status '%s' for IGM '%s'", healthStatus, igmName)
		} else {
			healthStatus = string(AcceleratorTopologyHealthStatusHealthy)
			log.Printf("[Mocking GCE] No health status injected for IGM '%s', defaulting to '%s'", igmName, healthStatus)
		}

		resp.AcceleratorTopologiesInfo[igmName] = alphacompute.
			InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
			AcceleratorTopology:       "4x4x4",
			AcceleratorTopologyHealth: healthStatus,
		}
		log.Printf("[Mocking GCE] Returning default available topologies for IGM '%s' in slice '%s' with health '%s'",
			igmName, sliceName, healthStatus)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[Mocking GCE] Error encoding response: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

// recordQPS adds a timestamp for the given endpoint for QPS calculation.
// This function assumes the caller holds the mutex.
func recordQPS(endpoint string) {
	stats := qpsCounters[endpoint]
	stats.Timestamps = append(stats.Timestamps, time.Now())
	stats.TotalRequests++
}

// handleQPS calculates and displays QPS statistics for tracked endpoints.
func handleQPS(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	now := time.Now()
	const duration = 10 * time.Second
	slidingWindowStart := now.Add(-duration)
	uptime := now.Sub(serviceStartTime)

	var response strings.Builder
	response.WriteString(fmt.Sprintf("QPS Statistics (Uptime: %v):\n", uptime.Round(time.Second)))

	for endpoint, stats := range qpsCounters {
		// 1. Prune old timestamps to maintain the sliding window.
		firstValidIndex := 0
		for i, ts := range stats.Timestamps {
			if !ts.Before(slidingWindowStart) {
				firstValidIndex = i
				break
			}
		}
		stats.Timestamps = stats.Timestamps[firstValidIndex:]

		// 2. Calculate current QPS over the sliding window.
		countInWindow := len(stats.Timestamps)
		currentQPS := float64(countInWindow) / duration.Seconds()

		// 3. Update the highest QPS if the current QPS is greater.
		if currentQPS > stats.HighestQPS {
			stats.HighestQPS = currentQPS
		}

		// 4. Calculate all-time average QPS and format output.
		allTimeAvgQPS := float64(stats.TotalRequests) / uptime.Seconds()

		response.WriteString(fmt.Sprintf("- %s:\n", endpoint))
		response.WriteString(fmt.Sprintf("  - Current (10s avg):  %.2f\n", currentQPS))
		response.WriteString(fmt.Sprintf("  - Count (last 10s):  %d\n", countInWindow))
		response.WriteString(fmt.Sprintf("  - Highest (10s peak):  %.2f\n", stats.HighestQPS))
		response.WriteString(fmt.Sprintf("  - All-time avg:      %.2f\n", allTimeAvgQPS))
	}
	if _, err := fmt.Fprint(w, response.String()); err != nil {
		log.Printf("[Mocking GCE] Error writing response: %v", err)
	}
}

// multiMigsHandler dispatches to specific handlers based on the HTTP method and path.
func multiMigsHandler(w http.ResponseWriter, r *http.Request) {
	// Path for list is like /projects/{p}/regions/{r}/multiMigs
	// Path for get/delete is like /projects/{p}/regions/{r}/multiMigs/{name}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	log.Printf("[Mocking GCE] multiMigsHandler: path=%s, parts len=%d, parts=%v, query=%v",
		r.URL.Path, len(parts), parts, r.URL.Query())
	isListRequest := len(parts) == 5 && parts[4] == multiMigs
	isMembersRequest := (len(parts) == 7 && parts[4] == multiMigs &&
		(parts[6] == "listMembers" || parts[6] == "multiMigMembers")) ||
		(len(parts) == 6 && parts[4] == multiMigs && strings.HasSuffix(parts[5], ":listMembers"))

	switch r.Method {
	case http.MethodGet:
		if isMembersRequest {
			handleListMultiMigMembers(w, r)
		} else if isListRequest {
			handleListMMIGs(w, r)
		} else {
			handleGetMMIG(w, r)
		}
	case http.MethodPost:
		if isListRequest { // POST is only for creation at the collection level
			handleCreateMMIG(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	case http.MethodDelete:
		if !isListRequest { // DELETE is only for specific resources
			handleDeleteResource(w, r, resourceTypeMMIG)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func handleListMultiMigMembers(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	recordQPS("ListMultiMigMembers")

	mmigURL := getResourceFromPath(r.URL.Path, "multiMigs")
	if mmigURL == "" {
		http.Error(w, "Bad Request: could not find multiMigs in path", http.StatusBadRequest)
		return
	}
	mmigName := getResourceName(mmigURL)

	log.Printf("[Mocking GCE] <-- Received request: %s %s", r.Method, r.URL.Path)

	// sliceName not used here anymore as resources are global

	resolvedMmigName := mmigName
	_, ok := gceResources.Mmigs[resolvedMmigName]
	if !ok {
		for mName := range gceResources.Mmigs {
			if strings.HasPrefix(mName, resolvedMmigName) || strings.HasPrefix(resolvedMmigName, mName) {
				resolvedMmigName = mName
				ok = true
				break
			}
		}
	}

	var items []*alphacompute.MultiMigMember
	if ok {
		if mockMmig, exists := gceResources.Mmigs[resolvedMmigName]; exists {
			for _, mName := range mockMmig.AttachedMigs {
				migUrl := fmt.Sprintf("projects/test-project/zones/us-central1-a/instanceGroupManagers/%s", mName)
				if mig, exists := gceResources.Migs[mName]; exists && mig.Raw != nil && mig.Raw.SelfLink != "" {
					migUrl = mig.Raw.SelfLink
				}
				items = append(items, &alphacompute.MultiMigMember{
					Status: &alphacompute.MultiMigMemberStatus{
						InstanceGroupManager: migUrl,
					},
				})
			}
		}
	}

	resp := &alphacompute.MultiMigMemberList{Items: items}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[Mocking GCE] Error encoding response: %v", err)
	}
}

// resourcePoliciesHandler dispatches to specific handlers based on the HTTP method and path.
func resourcePoliciesHandler(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "/resourcePolicies") {
		if r.Method == http.MethodPost {
			handleCreateResourcePolicy(w, r)
		} else if r.Method == http.MethodDelete {
			handleDeleteResource(w, r, resourceTypePolicy)
		} else {
			http.NotFound(w, r)
		}
	}
}

// instanceGroupManagersHandler dispatches to specific handlers based on the HTTP method and path.
func instanceGroupManagersHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if strings.HasSuffix(r.URL.Path, "/getAvailableAcceleratorTopologies") {
			handleGetAvailableAcceleratorTopologies(w, r)
		} else {
			handleGetInstanceGroupManager(w, r)
		}
		return
	case http.MethodPatch: // Used for both attach and detach
		var patchIgm alphacompute.InstanceGroupManager
		if err := json.NewDecoder(r.Body).Decode(&patchIgm); err != nil {
			log.Printf("[Mocking GCE] Error decoding request body for Patch IGM: %v", err)
			http.Error(w, "Failed to decode request body", http.StatusBadRequest)
			return
		}
		if patchIgm.MultiMig != "" {
			handleAttachMIGToMMIG(w, r, &patchIgm)
		} else {
			handleDetachMIGFromMMIG(w, r)
		}
		return
	case http.MethodPost:
		if strings.Contains(r.URL.Path, "/configureAcceleratorTopology") ||
			strings.Contains(r.URL.Path, "/configureAcceleratorTopologies") {
			handleConfigureAcceleratorTopologies(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleDeleteResource is a generic handler for deleting a resource (MMIG or Policy).
func handleDeleteResource(w http.ResponseWriter, r *http.Request, resourceType string) {
	mu.Lock()
	defer mu.Unlock()
	resourceName := getResourceName(r.URL.Path)

	log.Printf("[Mocking GCE] <-- Received request to delete %s: %s %s", resourceType, r.Method, r.URL.Path)

	sliceName, found := getSliceNameFromPolicyName(resourceName)
	if !found {
		log.Printf("[Mocking GCE] Not Found: Slice for %s '%s' does not exist.", resourceType, resourceName)
		http.NotFound(w, r)
		return
	}

	var step InjectedOperationStep
	var opPrefix string
	var resourceMap any

	if resourceType == resourceTypeMMIG {
		step = DeleteMMIG
		opPrefix = OpPrefixDeleteMMIG
		resourceMap = gceResources.Mmigs
	} else { // resourceTypePolicy
		step = DeleteWorkloadPolicy
		opPrefix = OpPrefixDeletePolicy
		resourceMap = gceResources.Policies
	}

	sleepIfInjectedTimeout(sliceName, step)
	if shouldInject, failure := shouldReturnInjectedError(sliceName, step); shouldInject {
		log.Printf("[Mocking GCE] Returning injected HTTP error for Delete%s: Code=%d, Message=%s",
			resourceType, failure.Code, failure.Message)
		http.Error(w, failure.Message, failure.Code)
		return
	}

	if status, injected := findInjectedOpStatus(sliceName, opPrefix, resourceName); injected {
		log.Printf("[Mocking GCE] Found injected op status '%s' for Delete%s '%s'. Skipping resource deletion.",
			status, resourceType, resourceName)
		region := getResourceFromPath(r.URL.Path, "regions")
		opName := fmt.Sprintf("%s%s/slice/%s", opPrefix, resourceName, sliceName)
		selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
		targetLink := r.URL.Path
		op := operationDone(opName, "delete", selfLink, targetLink, region, "")
		op.Status = status // Override with the injected status.
		handle(w, r, op)
		return
	}

	if m, ok := resourceMap.(map[string]*MockMMIG); ok && resourceType == resourceTypeMMIG {
		targetKey := resourceName
		if _, exists := m[targetKey]; !exists {
			for mName := range m {
				if strings.HasPrefix(mName, resourceName) || strings.HasPrefix(resourceName, mName) {
					targetKey = mName
					break
				}
			}
		}

		if mockMmig, ok := m[targetKey]; ok {
			if len(mockMmig.AttachedMigs) > 0 {
				log.Printf("[Mocking GCE] Conflict: MMIG '%s' is not empty, cannot delete", targetKey)
				http.Error(w, fmt.Sprintf("MultiMig %s is not empty", targetKey), http.StatusBadRequest)
				return
			}
		}
		delete(m, targetKey)

		// Clear WantedMigs for the slice as it is being deleted
		if injection, ok := sliceInjections[sliceName]; ok {
			injection.WantedMigs = nil
			log.Printf("[Mocking GCE] Cleared WantedMigs for slice '%s' due to MMIG deletion", sliceName)
		}

		// Clear operations for the slice
		for opName := range gceResources.Operations {
			if strings.Contains(opName, "/slice/"+sliceName) {
				delete(gceResources.Operations, opName)
				log.Printf("[Mocking GCE] Cleared operation '%s' due to MMIG deletion", opName)
			}
		}
	} else if p, ok := resourceMap.(map[string]*alphacompute.ResourcePolicy); ok && resourceType == resourceTypePolicy {
		if _, exists := p[resourceName]; !exists {
			http.NotFound(w, r)
			return
		}
		delete(p, resourceName)
	}

	log.Printf("[Mocking GCE] Deleting %s resource '%s' from slice '%s'", resourceType, resourceName, sliceName)
	region := getResourceFromPath(r.URL.Path, "regions")
	opName := fmt.Sprintf("%s%s/slice/%s", opPrefix, resourceName, sliceName)
	selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
	targetLink := r.URL.Path
	op := operationDone(opName, "delete", selfLink, targetLink, region, "")
	log.Printf("[Mocking GCE] Creating operation '%s' for Delete%s '%s'", opName, resourceType, resourceName)
	gceResources.Operations[opName] = op
	handle(w, r, op)
}

// findInjectedOpStatus checks for an injected operation status using two keys:
// 1. The full resource name (e.g., "my-slice-abcdef").
// 2. The base resource name without the hash (e.g., "my-slice").
// This makes injections more robust.
func findInjectedOpStatus(sliceName, opPrefix, resourceName string) (string, bool) {
	if _, ok := sliceInjections[sliceName]; !ok {
		log.Printf("[Mocking GCE] findInjectedOpStatus: sliceName '%s' not found in sliceInjections", sliceName)
		return "", false
	}

	// Key 1: Full name (e.g., op-createpolicy-my-slice-abcdef)
	fullKey := opPrefix + resourceName
	if status, injected := sliceInjections[sliceName].InjectedOpStatuses[fullKey]; injected {
		log.Printf("[Mocking GCE] findInjectedOpStatus: Found fullKey '%s' with status '%s' in sliceInjections for '%s'",
			fullKey, status, sliceName)
		return status, true
	}

	// Key 2: Base name (e.g., op-createpolicy-my-slice)
	baseKey := opPrefix + sliceName
	if status, injected := sliceInjections[sliceName].InjectedOpStatuses[baseKey]; injected {
		log.Printf("[Mocking GCE] findInjectedOpStatus: Found baseKey '%s' with status '%s' in sliceInjections for '%s'",
			baseKey, status, sliceName)
		return status, true
	}

	log.Printf("[Mocking GCE] findInjectedOpStatus: No injected status found for '%s' (keys tried: '%s', '%s')",
		sliceName, fullKey, baseKey)
	return "", false
}

// CreateMux creates a new ServeMux and registers all the mock GCE API handlers.
func CreateMux() *http.ServeMux {
	mu.Lock()
	gceResources = &GCEResources{
		Policies:      make(map[string]*alphacompute.ResourcePolicy),
		Mmigs:         make(map[string]*MockMMIG),
		Migs:          make(map[string]*MockMIG),
		Operations:    make(map[string]*alphacompute.Operation),
		MigTopologies: make(map[string]map[string]*CustomAcceleratorTopologyInfo),
	}
	clear(sliceInjections)
	mu.Unlock()
	// qpsCounters are intentionally not cleared on mux creation

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { // Root handler
		var handler http.HandlerFunc
		path := r.URL.Path

		// Dispatch based on path prefixes
		switch {
		case strings.HasPrefix(path, "/projects/"):
			if strings.Contains(path, "/operations/") {
				handler = handleGetOperation
			} else if strings.Contains(path, "/multiMigs") {
				handler = multiMigsHandler
			} else if strings.Contains(path, "/resourcePolicies") {
				handler = resourcePoliciesHandler
			} else if strings.Contains(path, "/instanceGroupManagers") {
				handler = instanceGroupManagersHandler
			}
		case path == "/inject/mmig-health":
			handler = handleMmigHealthInjection
		case path == "/inject/topo-health":
			handler = handleTopoHealthInjection
		case path == "/inject/topology-info":
			handler = handleTopologyInfoInjection
		case path == "/inject/topology-state":
			handler = handleTopologyStateInjection
		case path == "/inject/op-status":
			handler = handleOpStatusInjection
		case path == "/inject/clear":
			handler = handleClearInjection
		case path == "/inject/timeout":
			handler = handleInjectTimeout
		case path == "/inject/http-error":
			handler = handleInjectHttpError
		case path == "/inject/slice":
			handler = handleInitSlice
		case path == "/inject/policy":
			handler = handleInitPolicy
		case path == "/inject/mmig":
			handler = handleInitMmig
		case path == "/inject/mmig-attachment":
			handler = handleMmigAttachment // Renamed from handleInitMig to avoid confusion
		case path == "/inject/mig":
			handler = handleInitMig
		case path == "/clear":
			handler = handleClear
		case path == "/list":
			handler = handleListResources
		case path == "/qps":
			handler = handleQPS
		}

		if handler != nil {
			handler(w, r)
		} else {
			log.Printf("[Mocking GCE] Mock server received unhandled request for: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	})
	return mux
}

func handleTopologyInfoInjection(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received Topology info injection request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName  string                                   `json:"sliceName"`
		IgmName    string                                   `json:"igmName"`
		Topologies map[string]CustomAcceleratorTopologyInfo `json:"topologies"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.IgmName == "" {
		http.Error(w, "sliceName and igmName must be provided", http.StatusBadRequest)
		return
	}

	if gceResources.MigTopologies == nil {
		gceResources.MigTopologies = make(map[string]map[string]*CustomAcceleratorTopologyInfo)
	}

	if gceResources.MigTopologies[injection.IgmName] == nil {
		gceResources.MigTopologies[injection.IgmName] = make(map[string]*CustomAcceleratorTopologyInfo)
	}

	for id, info := range injection.Topologies {
		newInfo := info
		gceResources.MigTopologies[injection.IgmName][id] = &newInfo
	}

	log.Printf("[Mocking GCE] Injected %d topologies for IGM '%s' in slice '%s'",
		len(injection.Topologies), injection.IgmName, injection.SliceName)
	if _, err := fmt.Fprintf(w, "Injected %d topologies for IGM %s in slice %s\n",
		len(injection.Topologies), injection.IgmName, injection.SliceName); err != nil {
		log.Printf("[Mocking GCE] Error writing response: %v", err)
	}
}

func handleTopologyStateInjection(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received Topology state injection request: %s %s", r.Method, r.URL.Path)

	var injection struct {
		SliceName string                          `json:"sliceName"`
		IgmName   string                          `json:"igmName"`
		TopoID    string                          `json:"topoId"`
		State     *AcceleratorTopologyStateStruct `json:"state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&injection); err != nil {
		http.Error(w, "Failed to decode injection body", http.StatusBadRequest)
		return
	}

	if injection.SliceName == "" || injection.IgmName == "" || injection.TopoID == "" {
		http.Error(w, "sliceName, igmName, and topoId must be provided", http.StatusBadRequest)
		return
	}

	if _, ok := sliceInjections[injection.SliceName]; !ok {
		initSlice(injection.SliceName)
	}

	if sliceInjections[injection.SliceName].InjectedTopologyStates == nil {
		sliceInjections[injection.SliceName].InjectedTopologyStates =
			make(map[string]map[string]*AcceleratorTopologyStateStruct)
	}

	if sliceInjections[injection.SliceName].InjectedTopologyStates[injection.IgmName] == nil {
		sliceInjections[injection.SliceName].InjectedTopologyStates[injection.IgmName] =
			make(map[string]*AcceleratorTopologyStateStruct)
	}

	sliceInjections[injection.SliceName].InjectedTopologyStates[injection.IgmName][injection.TopoID] = injection.State
	log.Printf("[Mocking GCE] Injected state for topology '%s' for IGM '%s' in slice '%s'",
		injection.TopoID, injection.IgmName, injection.SliceName)
	if _, err := fmt.Fprintf(w, "Injected state for topology %s for IGM %s in slice %s\n",
		injection.TopoID, injection.IgmName, injection.SliceName); err != nil {
		log.Printf("[Mocking GCE] Error writing response: %v", err)
	}
}

func handleConfigureAcceleratorTopologies(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Mocking GCE] <-- Received request: %s %s", r.Method, r.URL.Path)
	requestId := r.URL.Query().Get("requestId")
	if requestId != "" {
		log.Printf("[Mocking GCE] Request ID: %s", requestId)
	}

	igmName := ""
	if name := getResourceFromPath(r.URL.Path, "instanceGroupManagers"); name != "" {
		igmName = getResourceName(name)
	}

	if igmName == "" {
		log.Printf("[Mocking GCE] Error: Could not extract igmName from URL: %s", r.URL.Path)
		http.Error(w, "malformed URL for configureAcceleratorTopology", http.StatusBadRequest)
		return
	}

	var req alphacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	log.Printf("[Mocking GCE] Received request body for %s: %+v", igmName, req)

	var topoID, action, externalId string
	for k, v := range req.AcceleratorTopologyConfigurations {
		topoID = k
		action = v.Action
		externalId = v.ExternalId
	}

	sliceName, found := getSliceNameFromPolicyName(externalId)
	var err error
	if !found {
		sliceName, err = findSliceForIGM(igmName)
		if err != nil {
			sliceName = ""
		}
	}

	stepKey := InjectedOperationStep("ConfigureAcceleratorTopologies")
	sleepIfInjectedTimeout(sliceName, stepKey)
	if shouldInject, failure := shouldReturnInjectedError(sliceName, stepKey); shouldInject {
		// Inspect request body action.
		// Only trigger the injected error if we are performing an ACTIVATE action.
		isActivate := false
		for _, config := range req.AcceleratorTopologyConfigurations {
			if config.Action == "ACTIVATE" {
				isActivate = true
				break
			}
		}
		if isActivate {
			log.Printf("[Mocking GCE] Returning injected HTTP error for ConfigureAcceleratorTopologies (ACTIVATE): "+
				"Code=%d, Message=%s", failure.Code, failure.Message)
			http.Error(w, failure.Message, failure.Code)
			return
		}
	}

	if len(req.AcceleratorTopologyConfigurations) != 1 {
		http.Error(w, "Exactly one topology configuration must be provided", http.StatusBadRequest)
		return
	}

	if err != nil {
		log.Printf("[Mocking GCE] Error finding slice for IGM %s: %v", igmName, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if gceResources.MigTopologies == nil ||
		gceResources.MigTopologies[igmName] == nil ||
		gceResources.MigTopologies[igmName][topoID] == nil {
		log.Printf("[Mocking GCE] Error: Topology %s not found for IGM %s", topoID, igmName)
		http.Error(w, "Topology not found", http.StatusNotFound)
		return
	}

	topoInfo := gceResources.MigTopologies[igmName][topoID]

	isDeactivation := action == "INACTIVE" || action == "DEACTIVATE"
	if topoInfo.AcceleratorTopologyState != nil &&
		topoInfo.AcceleratorTopologyState.CurrentState == activeState &&
		!isDeactivation {
		if topoInfo.RequestId != "" && requestId != "" && topoInfo.RequestId != requestId {
			msg := fmt.Sprintf("Conflict: topology '%s' is already claimed by RequestId '%s', but request has '%s'",
				topoID, topoInfo.RequestId, requestId)
			log.Printf("[Mocking GCE] %s", msg)
			http.Error(w, msg, http.StatusConflict)
			return
		}
	}

	if topoInfo.AcceleratorTopologyState == nil {
		topoInfo.AcceleratorTopologyState = &AcceleratorTopologyStateStruct{}
	}
	topoInfo.AcceleratorTopologyState.CurrentState = action
	topoInfo.RequestId = requestId
	if isDeactivation {
		topoInfo.AcceleratorTopologyState.ExternalId = ""
	} else {
		topoInfo.AcceleratorTopologyState.ExternalId = externalId
	}

	log.Printf("[Mocking GCE] Updated state for topology '%s' for IGM '%s' to '%s'", topoID, igmName, action)

	opName := fmt.Sprintf("op-configure-%s-%s", igmName, topoID)
	selfLink := fmt.Sprintf("%s/operations/%s", getBasePath(r.URL.Path), opName)
	targetLink := r.URL.Path
	op := operationDone(opName, "configure", selfLink, targetLink, "us-central2", "")
	// Store under raw key for direct lookup by handleGetOperation.
	gceResources.Operations[opName] = op
	// Also store under /slice/{sliceName} key for consistency with inject/op-status.
	sliceName, _ = findSliceForIGM(igmName)
	if sliceName != "" {
		gceResources.Operations[fmt.Sprintf("%s/slice/%s", opName, sliceName)] = op
	}
	handle(w, r, op)
}

func findSliceForIGM(igmName string) (string, error) {
	for mmigName, mmig := range gceResources.Mmigs {
		for _, attachedMig := range mmig.AttachedMigs {
			if attachedMig == igmName {
				parts := strings.Split(mmigName, "-")
				if len(parts) > 1 {
					return strings.Join(parts[:len(parts)-1], "-"), nil
				}
				return mmigName, nil
			}
		}
	}

	// Fallback: Check sliceInjections where injected MIGs are registered directly (important for sub-slices!)
	for sliceName, injection := range sliceInjections {
		for _, mig := range injection.InjectedMigs {
			if mig == igmName {
				return sliceName, nil
			}
		}
	}

	return "", fmt.Errorf("slice not found for IGM %s", igmName)
}

// cleanupSliceResources cleans up slice data from global maps.
// The caller MUST hold mu mutex.
func cleanupSliceResources(sliceName string) {
	log.Printf("[Mocking GCE] Cleaning up resources for slice '%s'", sliceName)

	// Find MIGs attached to MMIGs we are about to delete (to reset their MultiMig field)
	attachedMigsToReset := make(map[string]bool)
	for mmigName, mmig := range gceResources.Mmigs {
		if strings.HasPrefix(mmigName, sliceName) || isHashedVersionOf(mmigName, sliceName) {
			if mmig != nil {
				for _, migName := range mmig.AttachedMigs {
					attachedMigsToReset[migName] = true
				}
			}
		}
	}

	// 1. Clean up Operations
	// Match sliced operations and delete both the sliced and unsliced keys.
	for k := range gceResources.Operations {
		if strings.Contains(k, "/slice/"+sliceName) {
			log.Printf("[Mocking GCE] Deleting Operation (sliced): %s", k)
			delete(gceResources.Operations, k)
			unSlicedOpName := strings.Split(k, "/slice/")[0]
			log.Printf("[Mocking GCE] Deleting Operation (unsliced): %s", unSlicedOpName)
			delete(gceResources.Operations, unSlicedOpName)
		} else if sn, _ := getSliceNameFromResource(k); sn == sliceName {
			log.Printf("[Mocking GCE] Deleting Operation (by name): %s", k)
			delete(gceResources.Operations, k)
		}
	}

	// 2. Clean up MIGs
	for k, mig := range gceResources.Migs {
		if mig == nil {
			delete(gceResources.Migs, k)
			continue
		}
		if mig.SliceName == sliceName || strings.HasPrefix(k, sliceName) {
			log.Printf("[Mocking GCE] Deleting MIG: %s", k)
			delete(gceResources.Migs, k)
			delete(gceResources.MigTopologies, k)
		} else if attachedMigsToReset[k] {
			log.Printf("[Mocking GCE] Resetting MultiMig field for shared MIG: %s", k)
			mig.MultiMig = ""
			if mig.Raw != nil {
				mig.Raw.MultiMig = ""
			}
			delete(gceResources.MigTopologies, k)
		}
	}

	// 3. Clean up MMIGs
	for k := range gceResources.Mmigs {
		if strings.HasPrefix(k, sliceName) || isHashedVersionOf(k, sliceName) {
			log.Printf("[Mocking GCE] Deleting MMIG: %s", k)
			delete(gceResources.Mmigs, k)
		}
	}

	// 4. Clean up Policies
	for k := range gceResources.Policies {
		if k == sliceName || isHashedVersionOf(k, sliceName) {
			log.Printf("[Mocking GCE] Deleting Policy: %s", k)
			delete(gceResources.Policies, k)
		}
	}
}
