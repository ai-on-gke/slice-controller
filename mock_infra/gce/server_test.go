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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	alphacompute "google.golang.org/api/compute/v0.alpha"
)

const (
	testProject = "test-project"
	testRegion  = "us-central1"
	testZone    = "us-central1-a"
	testIgmName = "np-t1-grp"
)

func TestMockGCEServer(t *testing.T) {
	t.Run("InitSliceAndPolicyHandlers", testInitSliceAndPolicyHandlers)
	t.Run("PolicyInjection", testPolicyInjection)
	t.Run("ResourcePolicyHandlers", testResourcePolicyHandlers)
	t.Run("MMIGHandlers", testMMIGHandlers)
	t.Run("MMIGHealthInjection", testMMIGHealthInjection)
	t.Run("MMIGHandlersWithInit", testMMIGHandlersWithInit)
	t.Run("ListMMIGs", testListMMIGs)
	t.Run("MMIGOpStatusInjection", testMMIGOpStatusInjection)
	t.Run("OpStatusInjection", testOpStatusInjection)
	t.Run("MIGInjection", testMIGInjection)
	t.Run("AttachDetachMIGs", testAttachDetachMIGs)
	t.Run("DetachMIGFallbackSuccess", testDetachMIGFallbackSuccess)
	t.Run("AttachDetachSuccess", testAttachDetachSuccess)
	t.Run("GetSliceNameFromOperation", testGetSliceNameFromOperation)
	t.Run("AttachMIGOpInjection", testAttachMIGOpInjection)
	t.Run("AttachOpTransitionDone", testAttachOpTransitionDone)
	t.Run("DetachOpTransitionDone", testDetachOpTransitionDone)
	t.Run("CreateWorkloadPolicyHttpErrorInjection", testCreateWorkloadPolicyHttpErrorInjection)
	t.Run("DetachMIGHttpErrorInjection", testDetachMIGHttpErrorInjection)
	t.Run("AttachToInjectedMIG", testAttachToInjectedMIG)
	t.Run("TopologyHealthInjection", testTopologyHealthInjection)
	t.Run("DetachMIGOpInjection", testDetachMIGOpInjection)
	t.Run("AttachMIGHttpErrorInjection", testAttachMIGHttpErrorInjection)
	t.Run("GetAvailableAcceleratorTopologies", testGetAvailableAcceleratorTopologies)
	t.Run("GetTopologiesMultipleSlicesWithInjection", testGetTopologiesMultipleSlicesWithInjection)
	t.Run("ClearInjectionsHandler", testClearInjectionsHandler)
	t.Run("TopologyInfoInjection", testTopologyInfoInjection)
	t.Run("TopologyStateInjection", testTopologyStateInjection)
	t.Run("ConfigureAcceleratorTopologies", testConfigureAcceleratorTopologies)
	t.Run("TimeoutInjection", TestTimeoutInjection)
	t.Run("QPSCounter", TestQPSCounter)
	t.Run("ClearHandlers", testClearHandlers)
	t.Run("ListMultiMigMembers", testListMultiMigMembers)
	t.Run("WantedMigsState", testWantedMigsState)
}

func testListMultiMigMembers(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mmigName := "test-mmig"
	url := server.URL + "/projects/" + testProject + "/regions/" + testRegion +
		"/multiMigs/" + mmigName + "/multiMigMembers"

	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name:         mmigName,
		AttachedMigs: []string{"mig1", "mig2"},
	}
	gceResources.Migs["mig1"] = &MockMIG{
		Name: "mig1",
		Raw: &alphacompute.InstanceGroupManager{
			Name:     "mig1",
			SelfLink: "projects/test-project/zones/us-central1-a/instanceGroupManagers/mig1",
		},
	}
	gceResources.Migs["mig2"] = &MockMIG{
		Name: "mig2",
		Raw: &alphacompute.InstanceGroupManager{
			Name:     "mig2",
			SelfLink: "projects/test-project/zones/us-central1-a/instanceGroupManagers/mig2",
		},
	}
	mu.Unlock()

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var memberList alphacompute.MultiMigMemberList
	if err := json.NewDecoder(resp.Body).Decode(&memberList); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(memberList.Items) != 2 {
		t.Errorf("Expected 2 members, got %d", len(memberList.Items))
	}
}

func testResourcePolicyHandlers(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "testslice123"
	policyName := sliceName + "-somehash"
	url := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/resourcePolicies"

	policy := &alphacompute.ResourcePolicy{
		Name:   policyName,
		Region: "projects/" + testProject + "/regions/" + testRegion,
	}
	body, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("Failed to marshal policy: %v", err)
	}

	// Pre-initialize the slice for these tests.
	InitSlice(sliceName)

	// 1. Test successful creation
	t.Run("CreatePolicy_Success", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, ok := sliceInjections[sliceName]; !ok {
			t.Fatalf("Slice '%s' was not initialized in mock server state", sliceName)
		}
		createdPolicy, ok := gceResources.Policies[policyName]
		if !ok {
			t.Errorf("Policy '%s' was not added to the mock server state", policyName)
		}
		if diff := cmp.Diff(policy, createdPolicy, cmpopts.IgnoreFields(alphacompute.ResourcePolicy{},
			"CreationTimestamp")); diff != "" {
			t.Errorf("Created policy mismatch (-want +got):\n%s", diff)
		}
	})

	// 2. Test conflict on duplicate creation
	t.Run("CreatePolicy_Conflict", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code %d on duplicate create, got %d", http.StatusConflict, resp.StatusCode)
		}
	})

	// 3. Test successful deletion
	t.Run("DeletePolicy_Success", func(t *testing.T) {
		deleteURL := url + "/" + policyName
		req, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for delete, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, ok := gceResources.Policies[policyName]; ok {
			t.Errorf("Policy '%s' was not removed from the mock server state after deletion", policyName)
		}
	})

	// 4. Test not found on deleting a non-existent policy
	t.Run("DeletePolicy_NotFound", func(t *testing.T) {
		deleteURL := url + "/" + policyName // Already deleted
		req, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code %d for deleting non-existent policy, got %d", http.StatusNotFound, resp.StatusCode)
		}
	})

	// 5. Test that injecting an op status prevents resource deletion
	testDeleteResourceWithOpStatusInjection(t, server.URL, "Policy", url)

	// 5. Test that injecting an op status prevents resource creation
	t.Run("CreatePolicy_WithOpStatusInjection", func(t *testing.T) {
		opSliceName := "op-create-policy-injection-slice"
		opPolicyName := opSliceName + "-policy"
		opPolicy := &alphacompute.ResourcePolicy{Name: opPolicyName}
		opBody, _ := json.Marshal(opPolicy)

		// Initialize the slice for this test
		InitSlice(opSliceName)

		// Inject a 'RUNNING' status for the create operation.
		// The key is constructed from the prefix and the policy name.
		opKey := OpPrefixCreatePolicy + opPolicyName
		injectionURL := server.URL + "/inject/op-status"
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, opSliceName, opKey, opStatusRunning)
		injectReq, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		injectReq.Header.Set("Content-Type", "application/json")
		injectResp, err := http.DefaultClient.Do(injectReq)
		if err != nil || injectResp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to inject op status: %v, status: %d", err, injectResp.StatusCode)
		}
		_ = injectResp.Body.Close()

		// Attempt to create the policy
		createReq, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(opBody))
		createResp, err := http.DefaultClient.Do(createReq)
		if err != nil {
			t.Fatalf("Failed to send create policy request: %v", err)
		}
		defer func() {
			_ = createResp.Body.Close()
		}()

		// The create call itself should still return a 200 OK with an operation object.
		if createResp.StatusCode != http.StatusOK {
			t.Errorf("Expected status OK for create policy with injection, got %d", createResp.StatusCode)
		}

		var gotOp alphacompute.Operation
		if err := json.NewDecoder(createResp.Body).Decode(&gotOp); err != nil {
			t.Fatalf("Failed to decode operation response: %v", err)
		}

		// The returned operation should reflect the injected status.
		if gotOp.Status != opStatusRunning {
			t.Errorf("Expected operation status to be '%s', but got '%s'", opStatusRunning, gotOp.Status)
		}

		// Crucially, verify that the policy was NOT created in the resource map.
		mu.Lock()
		defer mu.Unlock()
		if _, exists := gceResources.Policies[opPolicyName]; exists {
			t.Errorf("Policy '%s' should not have been created in the resource map due to op status injection, but it was",
				opPolicyName)
		}
	})
}

func testAttachToInjectedMIG(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "injected-mig-attach-slice"
	mmigName := sliceName + "-mmig"
	migName := "injected-mig-for-attach"

	policyName := sliceName + "-policy"

	project := testProject
	region := testRegion
	zone := testZone

	injectMigURL := server.URL + "/inject/mig"
	mmigCollectionURL := fmt.Sprintf("%s/projects/%s/regions/%s/multiMigs", server.URL, project, region)
	policyCollectionURL := fmt.Sprintf("%s/projects/%s/regions/%s/resourcePolicies", server.URL, project, region)
	// The migURL for the PATCH request needs the full server URL.
	migURL := fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/%s", server.URL, project, zone, migName)
	fullMmigURL := fmt.Sprintf("%s/projects/%s/regions/%s/multiMigs/%s", server.URL, project, region, mmigName)

	// 2. Inject the MIG into the server's state. This simulates a pre-existing MIG.
	injectBody := `{"sliceName": "` + sliceName + `", "migName": "` + migName + `"}`
	injectReq, _ := http.NewRequest(http.MethodPost, injectMigURL, bytes.NewBufferString(injectBody))
	injectReq.Header.Set("Content-Type", "application/json")
	injectResp, err := http.DefaultClient.Do(injectReq)
	if err != nil || injectResp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to inject MIG: %v, status: %d", err, injectResp.StatusCode)
	}
	_ = injectResp.Body.Close()

	// 3. Create the Workload Policy.
	policyToCreate := &alphacompute.ResourcePolicy{Name: policyName}
	policyBody, _ := json.Marshal(policyToCreate)
	createPolicyReq, _ := http.NewRequest(http.MethodPost, policyCollectionURL, bytes.NewBuffer(policyBody))
	createPolicyResp, err := http.DefaultClient.Do(createPolicyReq)
	if err != nil || createPolicyResp == nil || createPolicyResp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to create Workload Policy: %v, status: %d", err, createPolicyResp.StatusCode)
	}
	_ = createPolicyResp.Body.Close()

	// 4. Create the MMIG that we will attach to.
	mmigToCreate := &alphacompute.MultiMig{Name: mmigName}
	mmigBody, _ := json.Marshal(mmigToCreate)
	createMmigReq, _ := http.NewRequest(http.MethodPost, mmigCollectionURL, bytes.NewBuffer(mmigBody))
	createMmigResp, err := http.DefaultClient.Do(createMmigReq)
	if err != nil || createMmigResp == nil || createMmigResp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to create MMIG: %v, status: %d", err, createMmigResp.StatusCode)
	}
	_ = createMmigResp.Body.Close()

	// Verify the MIG exists before attaching.
	mu.Lock()
	if _, ok := gceResources.Migs[migName]; !ok {
		mu.Unlock()
		t.Fatal("Pre-condition failed: Injected MIG does not exist in server state before attach.")
	}
	mu.Unlock()

	// 5. Perform the PATCH request to attach the pre-injected MIG.
	patchBody := &alphacompute.InstanceGroupManager{MultiMig: fullMmigURL}
	patchBodyBytes, _ := json.Marshal(patchBody)
	patchReq, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(patchBodyBytes))
	patchResp, err := http.DefaultClient.Do(patchReq)
	if err != nil {
		t.Fatalf("Failed to send PATCH request to attach MIG: %v", err)
	}
	defer func() {
		_ = patchResp.Body.Close()
	}()

	if patchResp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK for attaching injected MIG, got %d", patchResp.StatusCode)
	}

	// 6. Verify the final internal state of the server.
	mu.Lock()
	defer mu.Unlock()
	mig, ok := gceResources.Migs[migName]
	if !ok {
		t.Fatal("MIG not found in state after attach operation.")
	}
	if mig.MultiMig != fullMmigURL {
		t.Errorf("MIG was not correctly attached. Expected MultiMig field to be '%s', got '%s'", fullMmigURL, mig.MultiMig)
	}

	// The expected self-link in the attachments map comes from the injected MIG, which has a relative path.
	mockMmig, ok := gceResources.Mmigs[mmigName]
	if !ok {
		t.Fatal("MMIG not found in state after attach operation.")
	}
	if len(mockMmig.AttachedMigs) != 1 || mockMmig.AttachedMigs[0] != migName {
		t.Errorf("MMIG attached MIGs are incorrect. Expected ['%s'], got %v", migName, mockMmig.AttachedMigs)
	}
}

func TestQPSCounter(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	// Reset QPS counters to ensure a clean state for this test.
	mu.Lock()
	for _, stats := range qpsCounters {
		stats.Timestamps = []time.Time{}
		stats.TotalRequests = 0
		stats.HighestQPS = 0
	}
	mu.Unlock()
	sliceName := "qps-test-slice"
	mmigName := sliceName + "-mmig"
	igmName := sliceName + "-igm"

	getMmigURL := fmt.Sprintf("%s/projects/p/regions/r/multiMigs/%s", server.URL, mmigName)
	getTopoURL := fmt.Sprintf("%s/projects/p/zones/z/instanceGroupManagers/%s/getAvailableAcceleratorTopologies",
		server.URL, igmName)
	qpsURL := server.URL + "/qps"

	// Initialize the slice and necessary resources for the endpoints.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{Name: mmigName, Raw: &alphacompute.MultiMig{Name: mmigName}}
	gceResources.Migs[igmName] = &MockMIG{Name: igmName, Raw: &alphacompute.InstanceGroupManager{Name: igmName}}
	mu.Unlock()

	// 1. Test initial QPS is zero.
	t.Run("InitialQPS", func(t *testing.T) {
		resp, err := http.Get(qpsURL)
		if err != nil {
			t.Fatalf("Failed to get /qps endpoint: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		body := &bytes.Buffer{}
		if _, err := body.ReadFrom(resp.Body); err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}
		bodyStr := body.String()

		// Check for GetMMIG initial stats
		getMmigRegex := `- GetMMIG:\n  - Current \(10s avg\):  0.00\n  - Count \(last 10s\):  0\n` +
			`  - Highest \(10s peak\):  0.00`
		if !regexp.MustCompile(getMmigRegex).MatchString(bodyStr) {
			t.Errorf("Expected initial GetMMIG QPS stats to be zero, got:\n%s", bodyStr)
		}

		// Check for GetAvailableAcceleratorTopologies initial stats
		getTopoRegex := `- GetAvailableAcceleratorTopologies:\n  - Current \(10s avg\):  0.00\n` +
			`  - Count \(last 10s\):  0\n  - Highest \(10s peak\):  0.00`
		if !regexp.MustCompile(getTopoRegex).MatchString(bodyStr) {
			t.Errorf("Expected initial GetAvailableAcceleratorTopologies QPS stats to be zero, got:\n%s", bodyStr)
		}
	})

	// 2. Make some requests and check QPS.
	t.Run("CalculateQPS", func(t *testing.T) {
		// Make 5 requests to GetMMIG and 3 to GetAvailableAcceleratorTopologies.
		for i := 0; i < 5; i++ {
			if _, err := http.Get(getMmigURL); err != nil {
				t.Logf("Ignoring error from http.Get: %v", err)
			}
		}
		for i := 0; i < 3; i++ {
			if _, err := http.Get(getTopoURL); err != nil {
				t.Logf("Ignoring error from http.Get: %v", err)
			}
		}
		resp, err := http.Get(qpsURL)
		if err != nil {
			t.Fatalf("Failed to get /qps endpoint: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		body := &bytes.Buffer{} // Renamed to avoid shadowing
		if _, err := body.ReadFrom(resp.Body); err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}
		bodyStr := body.String()

		// Check GetMMIG stats: 5 requests / 10s = 0.5 QPS
		getMmigRegex := `- GetMMIG:\n  - Current \(10s avg\):  0.50\n  - Count \(last 10s\):  5\n` +
			`  - Highest \(10s peak\):  0.50`
		if !regexp.MustCompile(getMmigRegex).MatchString(bodyStr) {
			t.Errorf("Expected GetMMIG QPS stats to be 0.50/5, got:\n%s", bodyStr)
		}

		// Check GetAvailableAcceleratorTopologies stats: 3 requests / 10s = 0.3 QPS
		getTopoRegex := `- GetAvailableAcceleratorTopologies:\n  - Current \(10s avg\):  0.30\n` +
			`  - Count \(last 10s\):  3\n  - Highest \(10s peak\):  0.30`
		if !regexp.MustCompile(getTopoRegex).MatchString(bodyStr) {
			t.Errorf("Expected GetAvailableAcceleratorTopologies QPS stats to be 0.30/3, got:\n%s", bodyStr)
		}
	})
}

func TestTimeoutInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "timeout-slice"
	policyName := sliceName + "-somehash"
	region := testRegion
	policyURL := server.URL + "/projects/test-project/regions/" + region + "/resourcePolicies"
	injectionURL := server.URL + "/inject/timeout"

	// 1. Initialize server state with a slice.
	InitSlice(sliceName)

	// 2. Define the policy to be created.
	policy := &alphacompute.ResourcePolicy{
		Name:   policyName,
		Region: "projects/test-project/regions/" + region,
	}
	body, err := policy.MarshalJSON()
	if err != nil {
		t.Fatalf("Failed to marshal policy: %v", err)
	}

	// 3. Inject a timeout for the CreateWorkloadPolicy operation.
	t.Run("InjectTimeout_CreatePolicy", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName + `", "key": "` + string(CreateWorkloadPolicy) + `", "duration": 2}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for timeout injection, got %d", resp.StatusCode)
		}
		defer func() { _ = resp.Body.Close() }()

		// Now attempt to create the policy and measure the time.
		start := time.Now()
		createReq, _ := http.NewRequest(http.MethodPost, policyURL, bytes.NewBuffer(body))
		createResp, err := http.DefaultClient.Do(createReq)
		if err != nil {
			t.Fatalf("Failed to send POST request to create policy: %v", err)
		}
		defer func() {
			_ = createResp.Body.Close()
		}()

		duration := time.Since(start)
		if duration < 2*time.Second {
			t.Errorf("Expected request to take at least 2s due to injected timeout, but it took %v", duration)
		}
		if createResp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for create policy, got %d", http.StatusOK, createResp.StatusCode)
		}
	})

	// 4. Clear the injected timeout.
	clearBody := `{"sliceName": "` + sliceName + `", "key": "` + string(CreateWorkloadPolicy) + `"}`
	req, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(clearBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to clear timeout injection: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
}

func testCreateWorkloadPolicyHttpErrorInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "policy-http-error-slice"
	policyName := sliceName + "-somehash"
	region := testRegion
	policyURL := server.URL + "/projects/" + testProject + "/regions/" + region + "/resourcePolicies"
	injectionURL := server.URL + "/inject/http-error"

	// 1. Initialize server state with a slice.
	InitSlice(sliceName)

	// 2. Define the policy to be created.
	policy := &alphacompute.ResourcePolicy{
		Name:   policyName,
		Region: "projects/test-project/regions/" + region,
	}
	body, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("Failed to marshal policy: %v", err)
	}

	// 3. Inject an HTTP error for the CreateWorkloadPolicy operation.
	testInjectAndClearHttpError(t, "InjectHttpError_CreatePolicy", injectionURL, policyURL, http.MethodPost, body,
		sliceName, string(CreateWorkloadPolicy), http.StatusInternalServerError)

	// 4. Clear the injected HTTP error.
	testInjectAndClearHttpError(t, "ClearHttpError_CreatePolicy", injectionURL, policyURL, http.MethodPost, body,
		sliceName, string(CreateWorkloadPolicy), http.StatusOK)
}

func testDetachMIGHttpErrorInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "detach-http-error-slice"
	mmigName := sliceName + "-somehash"
	migName := "mig-for-detach-http-error"
	injectionURL := server.URL + "/inject/http-error"

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName

	// 1. Initialize server state with a slice, an MMIG, and an attached MIG.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name:         mmigName,
		AttachedMigs: []string{migName},
		Raw: &alphacompute.MultiMig{
			Name:     mmigName,
			SelfLink: mmigURL,
		},
	}
	gceResources.Migs[migName] = &MockMIG{
		Name:      migName,
		SliceName: sliceName,
		MultiMig:  mmigURL,
		Raw: &alphacompute.InstanceGroupManager{
			Name:     migName,
			SelfLink: migURL,
		},
	}
	mu.Unlock()

	// 2. Inject an HTTP error for the DetachMIGFromMMIG operation.
	patchBody := &alphacompute.InstanceGroupManager{MultiMig: ""} // Detach
	body, _ := json.Marshal(patchBody)
	testInjectAndClearHttpError(t, "InjectHttpError_DetachMIG", injectionURL, migURL,
		http.MethodPatch, body, sliceName, string(DetachMIGFromMMIG), http.StatusInternalServerError)

	// 3. Clear the injected HTTP error and verify success.
	testInjectAndClearHttpError(t, "ClearHttpError_DetachMIG", injectionURL, migURL,
		http.MethodPatch, body, sliceName, string(DetachMIGFromMMIG), http.StatusOK)
}

func testAttachMIGHttpErrorInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	sliceName := "attach-http-error-slice"
	mmigName := sliceName + "-somehash"
	migName := "mig-for-attach-http-error"
	injectionURL := server.URL + "/inject/http-error"

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName

	// 1. Initialize server state with a slice and an MMIG.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name: mmigName,
		Raw: &alphacompute.MultiMig{
			Name:     mmigName,
			SelfLink: mmigURL,
		},
	}
	mu.Unlock()

	// 2. Inject an HTTP error for the AttachMIGsToMMIG operation.
	patchBody := &alphacompute.InstanceGroupManager{MultiMig: mmigURL}
	body, _ := json.Marshal(patchBody)
	testInjectAndClearHttpError(t, "InjectHttpError_AttachMIG", injectionURL, migURL,
		http.MethodPatch, body, sliceName, string(AttachMIGToMMIG), http.StatusInternalServerError)

	// 3. Clear the injected HTTP error and verify success.
	testInjectAndClearHttpError(t, "ClearHttpError_AttachMIG", injectionURL, migURL,
		http.MethodPatch, body, sliceName, string(AttachMIGToMMIG), http.StatusOK)
}

func setupMMIGTest(t *testing.T) (*httptest.Server, string, []byte, *alphacompute.MultiMig) {
	t.Helper()
	server := httptest.NewServer(CreateMux())

	// Common variables
	sliceName := "testslice"
	mmigName := sliceName

	mmig := &alphacompute.MultiMig{
		Name:   mmigName,
		Region: "projects/" + testProject + "/regions/" + testRegion,
	}
	body, err := json.Marshal(mmig)
	if err != nil {
		t.Fatalf("Failed to marshal mmig: %v", err)
	}

	// Initialize the slice in the mock server state before making requests.
	InitSlice(sliceName)
	return server, mmigName, body, mmig
}

func testMMIGHandlers(t *testing.T) {
	server, mmigName, body, mmig := setupMMIGTest(t)
	defer server.Close()
	url := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs"

	// 1. Test successful creation
	t.Run("CreateMMIG_Success", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		createdMmig, ok := gceResources.Mmigs[mmigName]
		if !ok {
			t.Errorf("MMIG '%s' was not added to the mock server state", mmigName)
		}
		if diff := cmp.Diff(mmig, createdMmig.Raw, cmpopts.IgnoreFields(alphacompute.MultiMig{},
			"CreationTimestamp", "SelfLink")); diff != "" {
			t.Errorf("Created MMIG mismatch (-want +got):\n%s", diff)
		}
	})

	// 2. Test successful get
	t.Run("GetMMIG_Success", func(t *testing.T) {
		getURL := url + "/" + mmigName
		req, _ := http.NewRequest(http.MethodGet, getURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for get, got %d", http.StatusOK, resp.StatusCode)
		}

		var gotMmig alphacompute.MultiMig
		if err := json.NewDecoder(resp.Body).Decode(&gotMmig); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		// Add a MIG attachment to test the status population
		mu.Lock()
		migSelfLink := "projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/mig-testslice-1"
		mockMmig := gceResources.Mmigs[mmigName]
		mockMmig.AttachedMigs = append(mockMmig.AttachedMigs, "mig-testslice-1")
		gceResources.Migs["mig-testslice-1"] = &MockMIG{
			Name: "mig-testslice-1",
			Raw: &alphacompute.InstanceGroupManager{
				Name:     "mig-testslice-1",
				SelfLink: migSelfLink,
			},
		}
		mu.Unlock()

		// Re-run the get request to check for the attachment
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send second request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if err := json.NewDecoder(resp.Body).Decode(&gotMmig); err != nil {
			t.Fatalf("Failed to decode second response: %v", err)
		}

		// Expected MMIG should have the default ACTIVE status
		wantMmig := *mmig
		wantMmig.Status = &alphacompute.MultiMigStatus{
			MemberInstanceGroupManagers: []string{migSelfLink},
			MembersCount:                1,
			AppliedAcceleratorTopologies: []*alphacompute.MultiMigStatusAcceleratorTopology{
				{AcceleratorTopologyState: string(MMIGHealthStatusActive)},
			},
		}

		if diff := cmp.Diff(&wantMmig, &gotMmig, cmpopts.IgnoreFields(alphacompute.MultiMig{},
			"CreationTimestamp")); diff != "" {
			t.Errorf("Got MMIG mismatch (-want +got):\n%s", diff)
		}
	})

	// 3. Test get not found
	t.Run("GetMMIG_NotFound", func(t *testing.T) {
		getURL := url + "/non-existent-mmig"
		req, _ := http.NewRequest(http.MethodGet, getURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code %d for get not found, got %d", http.StatusNotFound, resp.StatusCode)
		}
	})

	// 4. Test conflict on duplicate creation
	t.Run("CreateMMIG_Conflict", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code %d on duplicate create, got %d", http.StatusConflict, resp.StatusCode)
		}
	})

	// 8. Test that injecting an op status prevents MMIG deletion
	testDeleteResourceWithOpStatusInjection(t, server.URL, "MMIG", url)
}

func testMMIGOpStatusInjection(t *testing.T) {
	server, _, _, _ := setupMMIGTest(t)
	defer server.Close()
	url := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs"
	t.Run("DeleteMMIG_Success", func(t *testing.T) {
		sliceName := "testslice"
		// Manually create the MMIG since setupMMIGTest only initializes the slice.
		mu.Lock()
		gceResources.Mmigs[sliceName] = &MockMMIG{
			Name: sliceName,
			Raw: &alphacompute.MultiMig{
				Name:   sliceName,
				Region: "projects/" + testProject + "/regions/" + testRegion,
			},
		}
		mu.Unlock()
		mmigName := sliceName
		deleteURL := url + "/" + mmigName
		req, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for delete, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, ok := gceResources.Mmigs[mmigName]; ok {
			t.Errorf("MMIG '%s' was not removed from the mock server state after deletion", mmigName)
		}
	})
	t.Run("CreateMMIG_WithOpStatusInjection", func(t *testing.T) {
		opSliceName := "op-create-mmig-slice"
		opMmigName := opSliceName + "-mmig"
		opMmig := &alphacompute.MultiMig{Name: opMmigName}
		opBody, _ := json.Marshal(opMmig)

		// Initialize the slice for this test
		InitSlice(opSliceName)

		// Inject a 'RUNNING' status for the create operation.
		opKey := OpPrefixCreateMMIG + opMmigName
		injectionURL := server.URL + "/inject/op-status"
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, opSliceName, opKey, opStatusRunning)
		injectReq, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		injectReq.Header.Set("Content-Type", "application/json")
		injectResp, err := http.DefaultClient.Do(injectReq)
		if err != nil || injectResp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to inject op status: %v, status: %d", err, injectResp.StatusCode)
		}
		if injectResp != nil && injectResp.Body != nil {
			_ = injectResp.Body.Close()
		}

		// Attempt to create the MMIG
		createReq, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(opBody))
		createResp, err := http.DefaultClient.Do(createReq)
		if err != nil {
			t.Fatalf("Failed to send create MMIG request: %v", err)
		}
		defer func() {
			_ = createResp.Body.Close()
		}()

		// Verify the MMIG was NOT created in the resource map.
		mu.Lock()
		defer mu.Unlock()
		if _, exists := gceResources.Mmigs[opMmigName]; exists {
			t.Errorf("MMIG '%s' should not have been created in the resource map due to op status injection, but it was",
				opMmigName)
		}
	})
}

func testMMIGHandlersWithInit(t *testing.T) {
	server, _, _, _ := setupMMIGTest(t)
	defer server.Close()

	// 6. Test GetMMIG with pre-injected health before slice/MMIG creation
	t.Run("GetMMIG_WithPreInjectedHealth", func(t *testing.T) {
		sliceName := "pre-injected-health-slice"
		mmigNameWithHash := sliceName + "-testhash" // Mimic real behavior where MMIG name includes a hash.
		mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigNameWithHash
		injectionURL := server.URL + "/inject/mmig-health"

		// 1. Inject health for a slice that doesn't exist yet.
		// The handler should create the slice on-the-fly.
		injectionBody := `{"sliceName": "` + sliceName + `", "acceleratorTopologyState": "INCOMPLETE"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for health injection, got %d", resp.StatusCode)
		}
		_ = resp.Body.Close()

		// Verify that the injection was stored with the slice name as the key.
		mu.Lock()
		if _, ok := sliceInjections[sliceName].InjectedMmigHealth[sliceName]; !ok {
			t.Fatalf("Injected MMIG health was not found in the resource map under the slice name key '%s'", sliceName)
		}
		mu.Unlock()

		// 2. Manually create the MMIG resource in the now-initialized slice.
		mu.Lock()
		gceResources.Mmigs[mmigNameWithHash] = &MockMMIG{
			Name: mmigNameWithHash,
			Raw: &alphacompute.MultiMig{
				Name:   mmigNameWithHash,
				Region: "projects/" + testProject + "/regions/" + testRegion,
			},
		}
		mu.Unlock()

		// 3. Get the MMIG using both the name with and without the hash, and verify both work.
		testCases := []struct {
			name   string
			getURL string
		}{
			{
				name:   "WithHash",
				getURL: mmigURL, // URL with sliceName-clusterhash
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				getReq, _ := http.NewRequest(http.MethodGet, tc.getURL, nil)
				getResp, err := http.DefaultClient.Do(getReq)
				if err != nil {
					t.Fatalf("Failed to send GET request after injection: %v", err)
				}
				defer func() {
					_ = getResp.Body.Close()
				}()

				if getResp.StatusCode != http.StatusOK {
					t.Fatalf("Expected status OK for GetMMIG, got %d", getResp.StatusCode)
				}

				var gotMmig alphacompute.MultiMig
				if err := json.NewDecoder(getResp.Body).Decode(&gotMmig); err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}

				// The name of the returned MMIG should always be the one that's actually stored.
				if gotMmig.Name != mmigNameWithHash {
					t.Errorf("Expected returned MMIG name to be '%s', but got '%s'", mmigNameWithHash, gotMmig.Name)
				}

				if gotMmig.Status == nil || len(gotMmig.Status.AppliedAcceleratorTopologies) != 1 {
					t.Fatalf("Expected 1 applied topology, got %d", len(gotMmig.Status.AppliedAcceleratorTopologies))
				}
				if state := gotMmig.Status.AppliedAcceleratorTopologies[0].AcceleratorTopologyState; state != "INCOMPLETE" {
					t.Errorf("Expected injected health status to be INCOMPLETE, got %s", state)
				}
			})
		}
	})
}

func testInitSliceAndPolicyHandlers(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	initURL := server.URL + "/inject/slice"
	policyURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/resourcePolicies"
	sliceName := "new-slice-for-test"

	// === Test /inject/slice Handler ===

	// 1. Test successful slice initialization
	t.Run("InitSlice_Success", func(t *testing.T) {
		body := `{"sliceName": "` + sliceName + `"}`
		req, _ := http.NewRequest(http.MethodPost, initURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, exists := sliceInjections[sliceName]; !exists {
			t.Errorf("Slice '%s' was not initialized in the mock server's state", sliceName)
		}
	})

	// 2. Test conflict on duplicate slice initialization
	t.Run("InitSlice_Conflict", func(t *testing.T) {
		body := `{"sliceName": "` + sliceName + `"}`
		req, _ := http.NewRequest(http.MethodPost, initURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code %d on duplicate init, got %d", http.StatusConflict, resp.StatusCode)
		}
	})

	// 3. Test request with empty slice name
	t.Run("InitSlice_EmptyName", func(t *testing.T) {
		body := `{"sliceName": ""}`
		req, _ := http.NewRequest(http.MethodPost, initURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status code %d for empty slice name, got %d", http.StatusBadRequest, resp.StatusCode)
		}
	})

	// === Test Updated Resource Policy Handlers ===

	// 4. Test creating a policy for a slice that has NOT been initialized and has no hash (should fail)
	t.Run("CreatePolicy_OnTheFlyInit_NoHash_Fails", func(t *testing.T) {
		uninitializedSlicePolicy := "uninitializedslice" // No dash, so can't derive slice name
		policy := &alphacompute.ResourcePolicy{Name: uninitializedSlicePolicy}
		body, _ := json.Marshal(policy)

		req, _ := http.NewRequest(http.MethodPost, policyURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status code %d when creating policy for uninitialized slice with no hash, got %d",
				http.StatusBadRequest, resp.StatusCode)
		}
	})

	// 5. Test creating a policy for a slice that has NOT been initialized but has a hash (should succeed)
	t.Run("CreatePolicy_OnTheFlyInit_WithHash_Succeeds", func(t *testing.T) {
		derivedSliceName := "on-the-fly-slice"
		policyNameWithHash := derivedSliceName + "-somehash"
		policy := &alphacompute.ResourcePolicy{Name: policyNameWithHash}
		body, _ := json.Marshal(policy)

		req, _ := http.NewRequest(http.MethodPost, policyURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for on-the-fly slice initialization, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, exists := sliceInjections[derivedSliceName]; !exists {
			t.Errorf("Slice '%s' was not initialized on-the-fly in the mock server's state", derivedSliceName)
		}
	})

	// 6. Test creating a policy for a slice that HAS been initialized
	t.Run("CreatePolicy_PreviouslyInitialized", func(t *testing.T) {
		// `sliceName` was initialized in the first sub-test
		policy := &alphacompute.ResourcePolicy{Name: sliceName}
		body, _ := json.Marshal(policy)

		req, _ := http.NewRequest(http.MethodPost, policyURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d when creating policy for initialized slice, got %d",
				http.StatusOK, resp.StatusCode)
		}
	})

	// 7. Test creating and deleting a policy with a cluster hash in its name
	testCreateDeleteResourceWithHash(t, initURL, policyURL, "Policy", "slice-with-hash", "somehash123")
}

func testListMMIGs(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	initURL := server.URL + "/inject/slice"
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs"

	// Setup initial state
	mu.Lock()
	// Reset global state
	gceResources = &GCEResources{
		Policies:   make(map[string]*alphacompute.ResourcePolicy),
		Mmigs:      make(map[string]*MockMMIG),
		Migs:       make(map[string]*MockMIG),
		Operations: make(map[string]*alphacompute.Operation),
	}
	clear(sliceInjections)

	// MMIG that should be found
	mmig1 := &alphacompute.MultiMig{Name: "mmig-1", Region: "projects/project-a/regions/region-1"}
	mmig2 := &alphacompute.MultiMig{Name: "mmig-2", Region: "projects/project-a/regions/region-1"}
	mmig3 := &alphacompute.MultiMig{Name: "mmig-3", Region: "projects/project-a/regions/region-2"}
	mmig4 := &alphacompute.MultiMig{Name: "mmig-4", Region: "projects/project-b/regions/region-1"}
	mmig5 := &alphacompute.MultiMig{Name: "mmig-5", Region: "invalid-region-url"}

	gceResources.Mmigs["mmig-1"] = &MockMMIG{Name: "mmig-1", Raw: mmig1}
	gceResources.Mmigs["mmig-2"] = &MockMMIG{Name: "mmig-2", Raw: mmig2}
	gceResources.Mmigs["mmig-3"] = &MockMMIG{Name: "mmig-3", Raw: mmig3}
	gceResources.Mmigs["mmig-4"] = &MockMMIG{Name: "mmig-4", Raw: mmig4}
	gceResources.Mmigs["mmig-5"] = &MockMMIG{Name: "mmig-5", Raw: mmig5}

	mu.Unlock()

	testCases := []struct {
		name               string
		url                string
		expectedStatusCode int
		expectedItems      []*alphacompute.MultiMig
	}{
		{
			name:               "Success with matching MMIGs",
			url:                fmt.Sprintf("%s/projects/project-a/regions/region-1/multiMigs", server.URL),
			expectedStatusCode: http.StatusOK,
			expectedItems:      []*alphacompute.MultiMig{mmig1, mmig2},
		},
		{
			name:               "No matching MMIGs in region",
			url:                fmt.Sprintf("%s/projects/project-a/regions/region-nonexistent/multiMigs", server.URL),
			expectedStatusCode: http.StatusOK,
			expectedItems:      []*alphacompute.MultiMig{},
		},
		{
			name:               "No matching MMIGs in project",
			url:                fmt.Sprintf("%s/projects/project-nonexistent/regions/region-1/multiMigs", server.URL),
			expectedStatusCode: http.StatusOK,
			expectedItems:      []*alphacompute.MultiMig{},
		},
		{
			name:               "Malformed URL",
			url:                fmt.Sprintf("%s/projects/project-a/regions/multiMigs", server.URL),
			expectedStatusCode: http.StatusNotFound,
			expectedItems:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, tc.url, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Failed to send request: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tc.expectedStatusCode {
				t.Errorf("Expected status code %d, got %d", tc.expectedStatusCode, resp.StatusCode)
			}

			if tc.expectedItems != nil {
				gotList := alphacompute.MultiMigsList{Items: []*alphacompute.MultiMig{}}

				if err := json.NewDecoder(resp.Body).Decode(&gotList); err != nil {
					t.Fatalf("Failed to decode response body: %v", err)
				}
				// Sort slices before comparing to handle non-deterministic map iteration order.
				sort.Slice(gotList.Items, func(i, j int) bool {
					return gotList.Items[i].Name < gotList.Items[j].Name
				})

				if diff := cmp.Diff(tc.expectedItems, gotList.Items); diff != "" {
					t.Errorf("ListMMIGs response mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
	// 1. Test creating an MMIG for a slice that has NOT been initialized
	t.Run("CreateMMIG_SliceNotInitialized", func(t *testing.T) {
		uninitializedSliceMmig := "uninitialized-slice-for-mmig"
		mmig := &alphacompute.MultiMig{Name: uninitializedSliceMmig}
		body, _ := json.Marshal(mmig)

		req, _ := http.NewRequest(http.MethodPost, mmigURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status code %d when creating MMIG for uninitialized slice, got %d",
				http.StatusBadRequest, resp.StatusCode)
		}
	})

	// 5. Test GetMMIG with a name that has a hash suffix
	t.Run("GetMMIG_WithHashSuffix", func(t *testing.T) {
		sliceName := "get-mmig-hash-slice"
		baseMmigName := sliceName                  // The name stored in the mock server is the base name.
		requestMmigName := sliceName + "-somehash" // The name used in the GET request

		// Initialize the slice and create the MMIG with the base name.
		InitSlice(sliceName)
		mu.Lock()
		gceResources.Mmigs[baseMmigName] = &MockMMIG{
			Name: baseMmigName,
			Raw: &alphacompute.MultiMig{
				Name:   baseMmigName,
				Region: "projects/" + testProject + "/regions/" + testRegion,
			},
		}
		mu.Unlock()

		// The request URL uses the name with the hash.
		getURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + requestMmigName
		req, _ := http.NewRequest(http.MethodGet, getURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for get with hash, got %d", http.StatusOK, resp.StatusCode)
		}

		var gotMmig alphacompute.MultiMig
		if err := json.NewDecoder(resp.Body).Decode(&gotMmig); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		// The handler should find the base MMIG and return it.
		if gotMmig.Name != baseMmigName {
			t.Errorf("Expected to get MMIG with name '%s', but got '%s'", baseMmigName, gotMmig.Name)
		}

		// Clean up
		gceResources = &GCEResources{
			Policies:   make(map[string]*alphacompute.ResourcePolicy),
			Mmigs:      make(map[string]*MockMMIG),
			Migs:       make(map[string]*MockMIG),
			Operations: make(map[string]*alphacompute.Operation),
		}
	})

	// 2. Test creating and deleting an MMIG with a cluster hash in its name
	testCreateDeleteResourceWithHash(t, initURL, mmigURL, "MMIG", "slice-for-mmig-with-hash", "somehash456")
}

func testMMIGHealthInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "health-test-slice"
	mmigName := sliceName + "-somehash"
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName
	injectionURL := server.URL + "/inject/mmig-health"

	// Pre-create the MMIG resource for testing.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name: mmigName,
		Raw: &alphacompute.MultiMig{
			Name:   mmigName,
			Region: "projects/" + testProject + "/regions/" + testRegion,
		},
	}
	mu.Unlock()

	// 1. Test GetMMIG with default health
	t.Run("GetMMIG_DefaultHealth", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, mmigURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send GET request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		var gotMmig alphacompute.MultiMig
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}
		t.Logf("[Test] GET MMIG %s response body: %s", mmigURL, buf.String())
		if err := json.NewDecoder(&buf).Decode(&gotMmig); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if gotMmig.Status == nil || len(gotMmig.Status.AppliedAcceleratorTopologies) == 0 {
			t.Fatalf("Expected MMIG status to be populated, but it was not")
		}
		appliedTopology := gotMmig.Status.AppliedAcceleratorTopologies[0]
		state := appliedTopology.AcceleratorTopologyState

		if state != string(MMIGHealthStatusActive) {
			t.Errorf("Expected default health status to be %s, got %s", MMIGHealthStatusActive, state)
		}
	})

	// 2. Test Injecting a 'FAILED' health status
	t.Run("InjectHealth_Success", func(t *testing.T) {
		injectionBody := `{"sliceName": "health-test-slice", "acceleratorTopologyState": "FAILED", ` +
			`"acceleratorTopology": "test-topo", "errorMessage": "injected failure"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for health injection, got %d", resp.StatusCode)
		}

		// Now get the MMIG and check its health
		getReq, _ := http.NewRequest(http.MethodGet, mmigURL, nil)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatalf("Failed to send GET request after injection: %v", err)
		}
		defer func() {
			_ = getResp.Body.Close()
		}()

		var gotMmig alphacompute.MultiMig
		if err := json.NewDecoder(getResp.Body).Decode(&gotMmig); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if len(gotMmig.Status.AppliedAcceleratorTopologies) != 1 {
			t.Fatalf("Expected 1 applied topology, got %d", len(gotMmig.Status.AppliedAcceleratorTopologies))
		}
		topoStatus := gotMmig.Status.AppliedAcceleratorTopologies[0]
		if state := topoStatus.AcceleratorTopologyState; state != "FAILED" {
			t.Errorf("Expected injected health status to be FAILED, got %s", state)
		}
		if topo := topoStatus.AcceleratorTopology; topo != "test-topo" {
			t.Errorf("Expected injected topology to be 'test-topo', got '%s'", topo)
		}
		lastCheck := topoStatus.AcceleratorTopologyStateLastCheck
		if lastCheck == nil || lastCheck.Error == nil || len(lastCheck.Error.Errors) != 1 ||
			lastCheck.Error.Errors[0].Message != "injected failure" {
			t.Errorf("Expected injected error message 'injected failure', but got different or no error: %+v",
				lastCheck)
		}
	})

	// 2a. Test Injecting an invalid health status
	t.Run("InjectHealth_InvalidStatus", func(t *testing.T) {
		injectionBody := `{"sliceName": "test-slice-invalid-health", "acceleratorTopologyState": "INVALID_STATUS"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status %d for invalid health injection, got %d", http.StatusBadRequest, resp.StatusCode)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
	})

	// 3. Test Clearing the injected health status
	t.Run("ClearInjectedHealth_Success", func(t *testing.T) {
		body := `{"sliceName": "health-test-slice"}`
		req, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send DELETE request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for clearing health injection, got %d", resp.StatusCode)
		}

		// Get the MMIG again and verify it has reverted to the default health
		getReq, _ := http.NewRequest(http.MethodGet, mmigURL, nil)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatalf("Failed to send GET request after clearing injection: %v", err)
		}
		defer func() {
			_ = getResp.Body.Close()
		}()

		var gotMmig alphacompute.MultiMig
		if err := json.NewDecoder(getResp.Body).Decode(&gotMmig); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if state :=
			gotMmig.Status.AppliedAcceleratorTopologies[0].AcceleratorTopologyState; state != string(MMIGHealthStatusActive) {
			t.Errorf("Expected health status to be default %s after clearing, got %s", MMIGHealthStatusActive, state)
		}
	})
}

func testOpStatusInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "op-status-test-slice"
	opKey := OpPrefixCreateMMIG + "op-status-test-slice" // The operation name, used as the injection key.
	opURL := fmt.Sprintf("%s/projects/p/regions/r/operations/%s/slice/%s", server.URL, opKey, sliceName)
	injectionURL := server.URL + "/inject/op-status"

	// Pre-create the slice and a dummy operation for testing.
	InitSlice(sliceName)
	mu.Lock()
	opName := fmt.Sprintf("%s/slice/%s", opKey, sliceName)
	gceResources.Operations[opName] = operationDone(opName, "insert", opURL, opURL, "r", "")
	mu.Unlock()

	// 1. Test GetOperation with default status (DONE)
	t.Run("GetOperation_DefaultStatus", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, opURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send GET request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		var gotOp alphacompute.Operation
		if err := json.NewDecoder(resp.Body).Decode(&gotOp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if gotOp.Status != opStatusDone {
			t.Errorf("Expected default op status to be %s, got %s", opStatusDone, gotOp.Status)
		}
	})

	// 2. Test Injecting a 'RUNNING' status
	t.Run("InjectOpStatus_Success", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName + `", "key": "` + opKey + `", "status": "RUNNING"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for op status injection, got %d", resp.StatusCode)
		}

		// Now get the operation and check its status
		getReq, _ := http.NewRequest(http.MethodGet, opURL, nil)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatalf("Failed to send GET request after injection: %v", err)
		}
		defer func() {
			_ = getResp.Body.Close()
		}()

		var gotOp alphacompute.Operation
		if err := json.NewDecoder(getResp.Body).Decode(&gotOp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if gotOp.Status != opStatusRunning {
			t.Errorf("Expected injected op status to be %s, got %s", opStatusRunning, gotOp.Status)
		}
	})

	// 3. Test Clearing the injected op status
	t.Run("ClearInjectedOpStatus_Success", func(t *testing.T) {
		body := `{"sliceName": "` + sliceName + `", "key": "` + opKey + `"}`
		req, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send DELETE request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for clearing op status injection, got %d", resp.StatusCode)
		}

		// Verify the internal state is cleared
		mu.Lock()
		defer mu.Unlock()
		if _, exists := sliceInjections[sliceName].InjectedOpStatuses[opKey]; exists {
			t.Errorf("Injected op status for key '%s' was not cleared from the mock server state", opKey)
		}
	})
}

func testClearInjectionsHandler(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "clear-injections-test-slice"
	injectClearURL := server.URL + "/inject/clear"

	// Setup: Initialize a slice with all kinds of injections.
	setup := func() {
		InitSlice(sliceName)
		sliceInjections[sliceName].InjectedMmigHealth["some-mmig"] =
			MmigHealthInjection{AcceleratorTopologyState: "INCOMPLETE"}
		sliceInjections[sliceName].InjectedOpStatuses["some-op"] = "RUNNING"
		sliceInjections[sliceName].InjectedTopoHealth["some-mig"] = "UNHEALTHY"
		sliceInjections[sliceName].StepHttpFailures["some-step"] = HttpError{Code: 500, Message: "fail"}
		sliceInjections[sliceName].StepTimeouts["some-step"] = 1
	}

	// 1. Test clearing a specific injection (InjectedMmigHealth)
	t.Run("ClearSpecificInjection_MmigHealth", func(t *testing.T) {
		setup()

		body := `{"sliceName": "` + sliceName + `", "clearMmigHealth": true}`
		req, _ := http.NewRequest(http.MethodPost, injectClearURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK, got %d", resp.StatusCode)
		}

		mu.Lock()
		defer mu.Unlock()
		si := sliceInjections[sliceName]
		if len(si.InjectedMmigHealth) != 0 {
			t.Error("InjectedMmigHealth should have been cleared, but was not")
		}
		if len(si.InjectedOpStatuses) == 0 {
			t.Error("InjectedOpStatuses should not have been cleared")
		}
	})

	// 2. Test clearing multiple specific injections
	t.Run("ClearMultipleSpecificInjections", func(t *testing.T) {
		setup()

		body := `{"sliceName": "` + sliceName + `", "clearOpStatuses": true, "clearTimeouts": true, "clearTopoHealth": true}`
		req, _ := http.NewRequest(http.MethodPost, injectClearURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK, got %d", resp.StatusCode)
		}

		mu.Lock()
		defer mu.Unlock()
		si := sliceInjections[sliceName]
		if len(si.InjectedOpStatuses) != 0 {
			t.Error("InjectedOpStatuses should have been cleared")
		}
		if len(si.StepTimeouts) != 0 {
			t.Error("StepTimeouts should have been cleared")
		}
		if len(si.InjectedTopoHealth) != 0 {
			t.Error("InjectedTopoHealth should have been cleared")
		}
		if len(si.InjectedMmigHealth) == 0 {
			t.Error("InjectedMmigHealth should not have been cleared")
		}
	})

	// 3. Test clearing all injections (original behavior)
	t.Run("ClearAllInjectionsForSlice", func(t *testing.T) {
		setup()

		body := `{"sliceName": "` + sliceName + `"}` // No specific flags
		req, _ := http.NewRequest(http.MethodPost, injectClearURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK, got %d", resp.StatusCode)
		}

		mu.Lock()
		defer mu.Unlock()
		si := sliceInjections[sliceName]
		if len(si.InjectedMmigHealth) != 0 || len(si.InjectedOpStatuses) != 0 ||
			len(si.InjectedTopoHealth) != 0 || len(si.StepHttpFailures) != 0 || len(si.StepTimeouts) != 0 {
			t.Errorf("Not all injection maps were cleared. Got: %+v", si)
		}
	})
}

func testClearHandlers(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. Test /inject/clear - should only clear injections, not resources.
	t.Run("ClearInjectionsOnly", func(t *testing.T) {
		sliceName := "clear-injections-slice"
		injectClearURL := server.URL + "/inject/clear"

		// Setup: Inject both resource and injection data.
		InitSlice(sliceName)
		mu.Lock()
		gceResources.Policies["some-policy"] = &alphacompute.ResourcePolicy{}
		sliceInjections[sliceName].InjectedMmigHealth["some-mmig"] =
			MmigHealthInjection{AcceleratorTopologyState: "INCOMPLETE", ErrorMessage: "test error"}
		mu.Unlock()

		// Action: Call /inject/clear
		body := `{"sliceName": "` + sliceName + `"}`
		req, _ := http.NewRequest(http.MethodPost, injectClearURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to clear URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for /inject/clear, got %d", resp.StatusCode)
		}

		// Verification
		mu.Lock()
		defer mu.Unlock()
		si, ok := sliceInjections[sliceName]
		if !ok {
			t.Fatal("Injections for slice should not have been deleted after calling /inject/clear")
		}
		// Check if the inner maps are empty.
		if len(si.InjectedMmigHealth) != 0 {
			t.Errorf("InjectedMmigHealth map was not cleared. Got: %v", si.InjectedMmigHealth)
		}
		if len(si.StepHttpFailures) != 0 || len(si.StepTimeouts) != 0 ||
			len(si.InjectedOpStatuses) != 0 || len(si.InjectedTopoHealth) != 0 {
			t.Error("One or more injection maps were not cleared properly")
		}
		if _, ok := gceResources.Policies["some-policy"]; !ok {
			t.Error("Resources should not have been cleared by /inject/clear")
		}
		// Cleanup for next test
		delete(sliceInjections, sliceName)
	})

	// 2. Test /clear with a specific slice - should clear both resources and injections.
	t.Run("ClearSpecificSlice", func(t *testing.T) {
		sliceName := "clear-specific-slice"
		clearURL := server.URL + "/clear"

		// Setup: Inject both resource and injection data.
		InitSlice(sliceName)
		mu.Lock()
		gceResources.Policies["some-policy"] = &alphacompute.ResourcePolicy{}
		sliceInjections[sliceName].InjectedMmigHealth["some-mmig"] =
			MmigHealthInjection{AcceleratorTopologyState: "FAILED", ErrorMessage: "test error"}
		mu.Unlock()

		// Action: Call /clear with sliceName
		body := `{"sliceName": "` + sliceName + `"}`
		req, _ := http.NewRequest(http.MethodPost, clearURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to clear URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for /clear with slice, got %d", resp.StatusCode)
		}

		// Verification
		mu.Lock()
		defer mu.Unlock()
		if _, ok := sliceInjections[sliceName]; ok {
			t.Error("Injections for slice were not cleared after calling /clear")
		}
		if _, ok := gceResources.Policies["some-policy"]; !ok {
			t.Error("Resources should not have been cleared after calling /clear with sliceName")
		}
	})

	// 3. Test /clear with no sliceName - should clear everything.
	t.Run("ClearEverything", func(t *testing.T) {
		clearURL := server.URL + "/clear"
		// Setup: Inject data for multiple slices and add some QPS data.
		InitSlice("slice-a")
		InitSlice("slice-b")
		mu.Lock()
		stats := qpsCounters["GetMMIG"]
		stats.TotalRequests = 10
		stats.HighestQPS = 5.0
		stats.Timestamps = []time.Time{time.Now()}
		mu.Unlock()

		// Action: Call /clear with empty body
		req, _ := http.NewRequest(http.MethodPost, clearURL, nil)
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for clearing everything, got %d", resp.StatusCode)
		}

		// Verification
		mu.Lock()
		defer mu.Unlock()
		if len(sliceInjections) != 0 || len(gceResources.Policies) != 0 || len(gceResources.Mmigs) != 0 {
			t.Errorf("sliceInjections or gceResources was not cleared. Injections: %v, Policies: %v, Mmigs: %v",
				sliceInjections, gceResources.Policies, gceResources.Mmigs)

		}
		if stats.TotalRequests != 0 || stats.HighestQPS != 0 || len(stats.Timestamps) != 0 {
			t.Errorf("QPS data was not cleared. Got: %+v", stats)
		}
	})
}

func testMIGInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "mig-injection-slice"
	migName := "test-mig-from-injection"
	injectionURL := server.URL + "/inject/mig"

	InitSlice(sliceName)
	testCreateResourceViaInjection(t, sliceName, migName, injectionURL, "migName", func(s, r string) bool {
		_, ok := gceResources.Migs[r]
		return ok
	}, func(s, r string) string {
		return gceResources.Migs[r].Name
	})

	// 2. Test conflict on duplicate MIG creation
	t.Run("CreateMIG_Conflict", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName + `", "migName": "` + migName + `"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code %d on duplicate MIG create, got %d", http.StatusConflict, resp.StatusCode)
		}
	})

	// 3. Test successful MIG deletion
	t.Run("DeleteMIG_Success", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName + `", "migName": "` + migName + `"}`
		req, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send DELETE request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for MIG deletion, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, ok := gceResources.Migs[migName]; ok {
			t.Errorf("MIG '%s' was not removed from the mock server state after deletion", migName)
		}
	})

	// 4. Create a second MIG in the same slice
	migName2 := "test-mig-2"
	t.Run("CreateMIG2_SameSlice", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName + `", "migName": "` + migName2 + `"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for MIG 2 injection, got %d", resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, ok := gceResources.Migs[migName2]; !ok {
			t.Errorf("MIG '%s' was not added to the mock server state", migName2)
		}
	})

	// 5. Create a third MIG in a different slice
	sliceName2 := "mig-injection-slice-2"
	migName3 := "test-mig-3"
	t.Run("CreateMIG3_DifferentSlice", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName2 + `", "migName": "` + migName3 + `"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for MIG 3 injection, got %d", resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, ok := gceResources.Migs[migName3]; !ok {
			t.Errorf("MIG '%s' was not added to the mock server state for slice '%s'", migName3, sliceName2)
		}
	})

	// 6. Test deletion of the second and third MIGs
	t.Run("DeleteMIG2_and_MIG3_Success", func(t *testing.T) {
		// Delete MIG 2
		body2 := `{"sliceName": "` + sliceName + `", "migName": "` + migName2 + `"}`
		req2, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(body2))
		req2.Header.Set("Content-Type", "application/json")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil || resp2.StatusCode != http.StatusOK {
			t.Fatalf("Failed to delete MIG 2. err: %v, status: %d", err, resp2.StatusCode)
		}
		defer func() {
			_ = resp2.Body.Close()
		}()

		// Delete MIG 3
		body3 := `{"sliceName": "` + sliceName2 + `", "migName": "` + migName3 + `"}`
		req3, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(body3))
		req3.Header.Set("Content-Type", "application/json")
		resp3, err := http.DefaultClient.Do(req3)
		if err != nil || resp3.StatusCode != http.StatusOK {
			t.Fatalf("Failed to delete MIG 3. err: %v, status: %d", err, resp3.StatusCode)
		}
		defer func() {
			_ = resp3.Body.Close()
		}()
	})
}

func testPolicyInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "policy-injection-slice"
	injectionURL := server.URL + "/inject/policy"

	// Initialize the slice once for all sub-tests.
	InitSlice(sliceName)

	// 1. Test successful Policy creation (policyName is same as sliceName)
	testCreateResourceViaInjection(t, sliceName, sliceName, injectionURL, "policyName", func(s, r string) bool {
		_, ok := gceResources.Policies[r]
		return ok
	}, func(s, r string) string {
		return gceResources.Policies[r].Name
	})

	// 2. Test conflict on duplicate Policy creation in another slice
	t.Run("CreatePolicy_Conflict", func(t *testing.T) {
		// Attempt to create the *exact same policy* again to trigger a conflict.
		injectionBody := `{"sliceName": "` + sliceName + `", "policyName": "` + sliceName + `"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code %d on duplicate Policy create, got %d", http.StatusConflict, resp.StatusCode)
		}
	})

	// 3. Test creating a policy with an invalid name format
	t.Run("CreatePolicy_InvalidName", func(t *testing.T) {
		invalidPolicyName := "completely-unrelated-name"
		injectionBody := `{"sliceName": "` + sliceName + `", "policyName": "` + invalidPolicyName + `"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status code %d for invalid policy name, got %d", http.StatusBadRequest, resp.StatusCode)
		}
	})

	// 3. Test successful Policy deletion
	t.Run("DeletePolicy_Success", func(t *testing.T) {
		// Use the policy created in the first successful test
		injectionBody := `{"sliceName": "` + sliceName + `", "policyName": "` + sliceName + `"}`
		req, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp == nil {
			t.Fatalf("Failed to send DELETE request to injection URL: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for Policy deletion, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state
		mu.Lock()
		defer mu.Unlock()
		if _, ok := gceResources.Policies[sliceName]; ok {
			t.Errorf("Policy '%s' was not removed from the mock server state after deletion", sliceName)
		}
	})
}

func testCreateResourceViaInjection(t *testing.T,
	sliceName, resourceName, injectionURL, resourceNameKey string,
	existsCheck func(string, string) bool,
	nameCheck func(string, string) string,
) {
	t.Helper()
	resourceType := strings.TrimSuffix(resourceNameKey, "Name")
	t.Run(fmt.Sprintf("Create%s_Success", resourceType), func(t *testing.T) {
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "%s": "%s"}`, sliceName, resourceNameKey, resourceName)
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for injection, got %d", resp.StatusCode)
		}

		mu.Lock()
		defer mu.Unlock()
		if _, ok := sliceInjections[sliceName]; !ok {
			t.Fatalf("Slice '%s' was not initialized in mock server state", sliceName)
		}
		if !existsCheck(sliceName, resourceName) {
			t.Errorf("Resource '%s' was not added to the mock server state", resourceName)
		}
		if nameCheck(sliceName, resourceName) != resourceName {
			t.Errorf("Expected resource name to be '%s', got '%s'", resourceName, nameCheck(sliceName, resourceName))
		}
	})
}

func testGetSliceNameFromOperation(t *testing.T) {
	testCases := []struct {
		name       string
		urlPath    string // The URL path to test.
		wantSlice  string // The expected slice name extracted from the URL.
		wantOpName string // The expected operation name extracted from the URL.
		wantFound  bool   // Whether a slice name was expected to be found.
	}{
		{
			name:       "new format - create mmig",
			urlPath:    fmt.Sprintf("/projects/p/regions/r/operations/%sv6e-slice-1/slice/v6e-slice-1", OpPrefixCreateMMIG),
			wantSlice:  "v6e-slice-1",
			wantOpName: OpPrefixCreateMMIG + "v6e-slice-1",
			wantFound:  true,
		},
		{
			name:       "new format - attach migs",
			urlPath:    fmt.Sprintf("/projects/p/zones/z/operations/%stestslice/slice/testslice", OpPrefixAttachMIGs),
			wantSlice:  "testslice",
			wantOpName: OpPrefixAttachMIGs + "testslice",
			wantFound:  true,
		},
		{
			name:       "new format - delete policy with special characters",
			urlPath:    fmt.Sprintf("/projects/p/regions/r/operations/%s-test-1-/slice/-test-1-", OpPrefixDeletePolicy),
			wantSlice:  "-test-1-",
			wantOpName: OpPrefixDeletePolicy + "-test-1-",
			wantFound:  true,
		},
		{
			name:       "old format - create mmig with prefix",
			urlPath:    fmt.Sprintf("/projects/p/regions/r/operations/%sfallback-slice", OpPrefixCreateMMIG),
			wantSlice:  "fallback-slice",
			wantOpName: OpPrefixCreateMMIG + "fallback-slice",
			wantFound:  true,
		},
		{
			name:       "unsupported format - no prefix",
			urlPath:    "/projects/p/regions/r/operations/unprefixed-resource-name",
			wantSlice:  "unprefixed-resource-name", // Falls back to op name
			wantOpName: "unprefixed-resource-name",
			wantFound:  false, // But indicates it was not found via a known pattern.
		},
		{
			name:       "unsupported format - internal reason",
			urlPath:    "/projects/p/zones/z/operations/SliceStepCheckpoint",
			wantSlice:  "SliceStepCheckpoint", // Falls back to op name
			wantOpName: "SliceStepCheckpoint",
			wantFound:  false, // Indicates not found.
		},
		{
			name: "new format with encoded slash in slice name",
			urlPath: fmt.Sprintf("/projects/p/regions/r/operations/%stest-slice-with-slash%%2Fslice%%2Ftest-slice-with-slash",
				OpPrefixCreateMMIG),
			wantSlice:  "test-slice-with-slash",
			wantOpName: "op-createmmig-test-slice-with-slash",
			wantFound:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSlice, gotOpName, gotFound := getSliceAndOpNameFromURL(tc.urlPath)
			if gotSlice != tc.wantSlice {
				t.Errorf("getSliceAndOpNameFromURL(%q) got slice %q; want %q", tc.urlPath, gotSlice, tc.wantSlice)
			}
			if gotOpName != tc.wantOpName {
				t.Errorf("getSliceAndOpNameFromURL(%q) got op name %q; want %q", tc.urlPath, gotOpName, tc.wantOpName)
			}
			if gotFound != tc.wantFound {
				t.Errorf("getSliceAndOpNameFromURL(%q) got found %t; want %t", tc.urlPath, gotFound, tc.wantFound)
			}
		})
	}
}

func setupAttachDetachTest(t *testing.T) (*httptest.Server, string, string, string, string, string) {
	t.Helper()
	server := httptest.NewServer(CreateMux())

	// Common variables
	sliceName := "attach-detach-slice"
	mmigName := sliceName
	migName := "mig-for-attach-detach"

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName

	// 1. Initialize server state with a slice and an MMIG.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name: mmigName,
		Raw: &alphacompute.MultiMig{
			Name:     mmigName,
			SelfLink: mmigURL,
		},
	}
	mu.Unlock()
	return server, sliceName, mmigName, migName, migURL, mmigURL
}

func testAttachDetachSuccess(t *testing.T) {
	server, sliceName, mmigName, migName, migURL, mmigURL := setupAttachDetachTest(t)
	defer server.Close()
	InitSlice(sliceName)

	// 2. Test successful attachment of a MIG to an MMIG.
	t.Run("AttachMIG_Success", func(t *testing.T) {
		patchBody := &alphacompute.InstanceGroupManager{
			MultiMig: mmigURL,
		}
		body, err := json.Marshal(patchBody)
		if err != nil {
			t.Fatalf("Failed to marshal attach body: %v", err)
		}

		req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PATCH request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for attach, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state after attachment
		mu.Lock()
		defer mu.Unlock()

		mig, migExists := gceResources.Migs[migName]
		if !migExists {
			t.Fatalf("MIG '%s' was not created on-the-fly during attach", migName)
		}
		if mig.MultiMig != mmigURL {
			t.Errorf("MIG's MultiMig field was not set correctly. got %q, want %q", mig.MultiMig, mmigURL)
		}

		mockMmig, mmigExists := gceResources.Mmigs[mmigName]
		if !mmigExists {
			t.Fatalf("MMIG '%s' not found in state after attach", mmigName)
		}
		if len(mockMmig.AttachedMigs) != 1 || mockMmig.AttachedMigs[0] != migName {
			t.Fatalf("MMIG attached MIGs not updated correctly. Got %v", mockMmig.AttachedMigs)
		}
	})

	// 3. Test successful detachment of the MIG from the MMIG.
	t.Run("DetachMIG_Success", func(t *testing.T) {
		// The body for detach is an empty InstanceGroupManager object
		patchBody := &alphacompute.InstanceGroupManager{}
		body, err := json.Marshal(patchBody)
		if err != nil {
			t.Fatalf("Failed to marshal detach body: %v", err)
		}

		req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PATCH request for detach: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for detach, got %d", http.StatusOK, resp.StatusCode)
		}

		// Verify internal state after detachment
		mu.Lock()
		defer mu.Unlock()

		mig, migExists := gceResources.Migs[migName]
		if !migExists {
			t.Fatal("MIG should still exist after detach")
		}
		if mig.MultiMig != "" {
			t.Errorf("MIG's MultiMig field was not cleared. got %q, want empty", mig.MultiMig)
		}

		mockMmig, mmigExists := gceResources.Mmigs[mmigName]
		if !mmigExists {
			t.Fatalf("MMIG '%s' not found in state after detach", mmigName)
		}
		if len(mockMmig.AttachedMigs) != 0 {
			t.Errorf("MIG not removed from MMIG attached MIGs. Got %v, want empty", mockMmig.AttachedMigs)
		}
	})
}

func testDetachMIGFallbackSuccess(t *testing.T) {
	server, _, _, _, _, _ := setupAttachDetachTest(t)
	defer server.Close()

	nonExistentMigURL := fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/mig-does-not-exist",
		server.URL, testProject, testZone)
	patchBody := &alphacompute.InstanceGroupManager{} // Empty body for detach
	body, _ := json.Marshal(patchBody)
	req, _ := http.NewRequest(http.MethodPatch, nonExistentMigURL, bytes.NewBuffer(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK for detaching a non-existent MIG (fallback success), got %d", resp.StatusCode)
	}

	// Also verify that a dummy operation was created and is DONE
	var gotOp alphacompute.Operation
	if err := json.NewDecoder(resp.Body).Decode(&gotOp); err != nil {
		t.Fatalf("Failed to decode operation response: %v", err)
	}
	if gotOp.Status != "DONE" {
		t.Errorf("Expected operation status DONE, got %s", gotOp.Status)
	}
	opName := OpPrefixDetachMIGs + "mig-does-not-exist/slice/" + defaultSliceName
	if gotOp.Name != opName {
		t.Errorf("Expected operation name %s, got %s", opName, gotOp.Name)
	}
}

func testAttachDetachMIGs(t *testing.T) {
	server, _, _, _, migURL, _ := setupAttachDetachTest(t)
	defer server.Close()
	// 4. Test attaching to a non-existent MMIG
	t.Run("AttachMIG_MMIGNotFound", func(t *testing.T) {
		nonExistentMmigURL := fmt.Sprintf("%s/projects/%s/regions/%s/multiMigs/non-existent-mmig",
			server.URL, testProject, testRegion)
		patchBody := &alphacompute.InstanceGroupManager{
			MultiMig: nonExistentMmigURL,
		}
		body, err := json.Marshal(patchBody)
		if err != nil {
			t.Fatalf("Failed to marshal attach body: %v", err)
		}

		// We need to initialize the slice for the non-existent MMIG because the handler
		// derives the slice name from the MMIG name.
		InitSlice("non-existent-mmig")

		req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PATCH request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code %d for attaching to non-existent MMIG, got %d", http.StatusNotFound, resp.StatusCode)
		}
	})

	// 5. Test detaching a MIG that is not attached to anything
	t.Run("DetachMIG_NotAttached", func(t *testing.T) {
		// Manually create the MIG so it exists for the detach operation.
		mu.Lock()
		gceResources.Migs["mig-for-attach-detach"] = &MockMIG{
			Name: "mig-for-attach-detach",
			Raw: &alphacompute.InstanceGroupManager{
				Name: "mig-for-attach-detach",
			},
		}
		mu.Unlock()
		patchBody := &alphacompute.InstanceGroupManager{} // Empty body for detach
		body, _ := json.Marshal(patchBody)
		req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
		resp, _ := http.DefaultClient.Do(req)

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status OK for detaching an already-detached MIG, got %d", resp.StatusCode)
		}
	})

	// 6. Test attaching and detaching multiple MIGs
	t.Run("AttachDetachMultipleMIGs", func(t *testing.T) {
		multiSliceName := "multi-attach-slice"
		multiMmigName := multiSliceName
		migNames := []string{"mig-a", "mig-b", "mig-c"}
		multiMmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + multiMmigName

		InitSlice(multiSliceName)
		mu.Lock()
		gceResources.Mmigs[multiMmigName] = &MockMMIG{
			Name: multiMmigName,
			Raw: &alphacompute.MultiMig{
				Name:     multiMmigName,
				SelfLink: multiMmigURL,
			},
		}
		mu.Unlock()

		// Attach multiple MIGs
		for _, migName := range migNames {
			migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
			patchBody := &alphacompute.InstanceGroupManager{
				MultiMig: multiMmigURL,
			}
			body, err := json.Marshal(patchBody)
			if err != nil {
				t.Fatalf("Failed to marshal attach body for %s: %v", migName, err)
			}

			req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Failed to send PATCH request for %s: %v", migName, err)
			}
			defer func() {
				_ = resp.Body.Close()
			}()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Expected status code %d for attach of %s, got %d", http.StatusOK, migName, resp.StatusCode)
			}
		}

		// Verify state after attaching all
		mu.Lock()
		mockMmig, mmigExists := gceResources.Mmigs[multiMmigName]
		if !mmigExists {
			t.Fatalf("MMIG '%s' not found in state after attach", multiMmigName)
		}
		if len(mockMmig.AttachedMigs) != len(migNames) {
			t.Fatalf("MMIG attached MIGs not updated correctly. Got %d attachments, want %d",
				len(mockMmig.AttachedMigs), len(migNames))
		}
		for _, migName := range migNames {
			if _, ok := gceResources.Migs[migName]; !ok {
				t.Errorf("MIG '%s' was not created on-the-fly during attach", migName)
			}
		}
		mu.Unlock()

		// Detach multiple MIGs
		for _, migName := range migNames {
			migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
			patchBody := &alphacompute.InstanceGroupManager{} // Empty body for detach
			body, _ := json.Marshal(patchBody)
			req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Failed to send PATCH request for detach of %s: %v", migName, err)
			}
			defer func() {
				_ = resp.Body.Close()
			}()
		}

		// Verify state after detaching all
		mu.Lock()
		mockMmigDetach, mmigExistsDetach := gceResources.Mmigs[multiMmigName]
		if !mmigExistsDetach {
			t.Fatalf("MMIG '%s' not found in state after detach_all", multiMmigName)
		}
		if len(mockMmigDetach.AttachedMigs) != 0 {
			t.Errorf("MIGs not removed from MMIG attached MIGs. Got %v, want empty", mockMmigDetach.AttachedMigs)
		}
		mu.Unlock()
	})

	// 7. Test attaching a MIG with an op status injection
	t.Run("AttachMIG_WithOpStatusInjection", func(t *testing.T) {
		opSliceName := "op-attach-slice"
		opMmigName := opSliceName
		opMigName := "op-mig-for-attach"
		opMigURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + opMigName
		opMmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + opMmigName

		// Initialize server state
		InitSlice(opSliceName)
		mu.Lock()
		gceResources.Mmigs[opMmigName] = &MockMMIG{
			Name: opMmigName,
			Raw: &alphacompute.MultiMig{
				Name:     opMmigName,
				SelfLink: opMmigURL,
			},
		}
		mu.Unlock()

		// Inject a 'RUNNING' status for the attach operation.
		opKey := OpPrefixAttachMIGs + opMigName
		injectionURL := server.URL + "/inject/op-status"
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, opSliceName, opKey, opStatusRunning)
		injectReq, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		injectReq.Header.Set("Content-Type", "application/json")
		injectResp, err := http.DefaultClient.Do(injectReq)
		if err != nil || injectResp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to inject op status: %v, status: %d", err, injectResp.StatusCode)
		}
		_ = injectResp.Body.Close()

		// Attempt to attach the MIG
		patchBody := &alphacompute.InstanceGroupManager{MultiMig: opMmigURL}
		body, _ := json.Marshal(patchBody)
		req, _ := http.NewRequest(http.MethodPatch, opMigURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PATCH request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		// Verify internal state: MIG should not be attached.
		mu.Lock()
		defer mu.Unlock()
		mockMmig, mmigExists := gceResources.Mmigs[opMmigName]
		if mmigExists && len(mockMmig.AttachedMigs) > 0 {
			t.Errorf("MIG should not have been attached due to op status injection, but attachments list is: %v",
				mockMmig.AttachedMigs)
		}
	})

	// 8. Test detaching a MIG with an op status injection
	t.Run("DetachMIG_WithOpStatusInjection", func(t *testing.T) {
		opSliceName := "op-detach-slice"
		opMmigName := opSliceName + "-somehash"
		opMigName := "op-mig-for-detach"
		opMigURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + opMigName
		opMmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + opMmigName

		// Initialize server state with an attached MIG
		InitSlice(opSliceName)
		mu.Lock()
		gceResources.Mmigs[opMmigName] = &MockMMIG{
			Name:         opMmigName,
			AttachedMigs: []string{opMigName},
			Raw: &alphacompute.MultiMig{
				Name:     opMmigName,
				SelfLink: opMmigURL,
			},
		}
		gceResources.Migs[opMigName] = &MockMIG{
			Name:     opMigName,
			MultiMig: opMmigURL,
			Raw: &alphacompute.InstanceGroupManager{
				Name:     opMigName,
				SelfLink: opMigURL,
			},
		}
		mu.Unlock()

		// Inject a 'RUNNING' status for the detach operation.
		opKey := OpPrefixDetachMIGs + opMigName
		injectionURL := server.URL + "/inject/op-status"
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, opSliceName, opKey, opStatusRunning)
		injectReq, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		injectReq.Header.Set("Content-Type", "application/json")
		injectResp, err := http.DefaultClient.Do(injectReq)
		if err != nil || injectResp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to inject op status: %v, status: %d", err, injectResp.StatusCode)
		}
		_ = injectResp.Body.Close()

		// Attempt to detach the MIG
		patchBody := &alphacompute.InstanceGroupManager{} // Empty body for detach
		body, _ := json.Marshal(patchBody)
		req, _ := http.NewRequest(http.MethodPatch, opMigURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PATCH request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		// Verify internal state: MIG should still be attached.
		mu.Lock()
		defer mu.Unlock()
		mockMmig, mmigExists := gceResources.Mmigs[opMmigName]
		if !mmigExists || len(mockMmig.AttachedMigs) != 1 {
			t.Errorf("MIG should have remained attached due to op status injection, but attachments are: %v",
				mockMmig.AttachedMigs)
		}
	})
}

func testAttachMIGOpInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "attach-op-slice"
	mmigName := sliceName
	migName := "mig-for-attach-op"
	opKey := OpPrefixAttachMIGs + migName // op-attachmigs-migName

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName
	opURL := fmt.Sprintf("%s/projects/%s/zones/%s/operations/%s/slice/%s",
		server.URL, testProject, testZone, opKey, sliceName)
	injectionURL := server.URL + "/inject/op-status"

	// 1. Initialize server state with a slice and an MMIG.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name: mmigName,
		Raw: &alphacompute.MultiMig{
			Name:     mmigName,
			SelfLink: mmigURL,
		},
	}
	mu.Unlock()

	// 2. Attach the MIG to get an operation.
	var op alphacompute.Operation
	t.Run("AttachMIG_GetOperation", func(t *testing.T) {
		patchBody := &alphacompute.InstanceGroupManager{MultiMig: mmigURL}
		body, _ := json.Marshal(patchBody)
		req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PATCH request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for attach, got %d", resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(&op); err != nil {
			t.Fatalf("Failed to decode operation response: %v", err)
		}
		if op.Status != opStatusDone { //nolint:testifylint // Not using testify.
			t.Errorf("Expected default op status to be %s, got %s", opStatusDone, op.Status)
		}
	})

	// 3. Inject a 'RUNNING' status for the attach operation.
	t.Run("InjectAttachOpStatus_Success", func(t *testing.T) {
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, sliceName, opKey, opStatusRunning)
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for op status injection, got %d", resp.StatusCode)
		}
		defer func() { _ = resp.Body.Close() }()

		// Now get the operation and check its status
		getReq, _ := http.NewRequest(http.MethodGet, opURL, nil)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatalf("Failed to send GET request after injection: %v", err)
		}
		defer func() {
			_ = getResp.Body.Close()
		}()

		var gotOp alphacompute.Operation
		if err := json.NewDecoder(getResp.Body).Decode(&gotOp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}
		defer func() { _ = getResp.Body.Close() }()

		if gotOp.Status != opStatusRunning {
			t.Errorf("Expected injected op status to be %s, got %s", opStatusRunning, gotOp.Status)
		}
	})
}

func testDetachMIGOpInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "detach-op-slice"
	mmigName := sliceName
	migName := "mig-for-detach-op"
	opKey := OpPrefixDetachMIGs + migName // op-detachmigs-migName

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName
	opURL := fmt.Sprintf("%s/projects/%s/zones/%s/operations/%s/slice/%s",
		server.URL, testProject, testZone, opKey, sliceName)
	injectionURL := server.URL + "/inject/op-status"

	// 1. Initialize server state with a slice, an MMIG, and an attached MIG.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name:         mmigName,
		AttachedMigs: []string{migName},
		Raw: &alphacompute.MultiMig{
			Name:     mmigName,
			SelfLink: mmigURL,
		},
	}
	gceResources.Migs[migName] = &MockMIG{
		Name:     migName,
		MultiMig: mmigURL,
		Raw: &alphacompute.InstanceGroupManager{
			Name:     migName,
			SelfLink: migURL,
		},
	}
	mu.Unlock()

	// 2. Detach the MIG to get an operation.
	var op alphacompute.Operation
	t.Run("DetachMIG_GetOperation", func(t *testing.T) {
		patchBody := &alphacompute.InstanceGroupManager{MultiMig: ""} // Detach
		body, _ := json.Marshal(patchBody)
		req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PATCH request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for detach, got %d", resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(&op); err != nil {
			t.Fatalf("Failed to decode operation response: %v", err)
		}
		if op.Status != opStatusDone {
			t.Errorf("Expected default op status to be %s, got %s", opStatusDone, op.Status)
		}
	})

	// 3. Inject a 'RUNNING' status for the detach operation.
	t.Run("InjectDetachOpStatus_Success", func(t *testing.T) {
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, sliceName, opKey, opStatusRunning)
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for op status injection, got %d", resp.StatusCode)
		}
		defer func() { _ = resp.Body.Close() }()

		// Now get the operation and check its status
		getResp, err := http.DefaultClient.Get(opURL)
		if err != nil {
			t.Fatalf("Failed to get response after injection: %v", err)
		}
		defer func() {
			_ = getResp.Body.Close()
		}()
		var gotOp alphacompute.Operation
		if err := json.NewDecoder(getResp.Body).Decode(&gotOp); err != nil {
			t.Fatalf("Failed to decode operation response: %v", err)
		}
		if gotOp.Status != opStatusRunning {
			t.Errorf("Expected injected op status to be %s, got %s", opStatusRunning, gotOp.Status)
		}
	})
}

func testAttachOpTransitionDone(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "attach-done-slice"
	mmigName := sliceName
	migName := "mig-for-attach-done"
	opKey := OpPrefixAttachMIGs + migName

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName
	injectionURL := server.URL + "/inject/op-status"

	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name: mmigName,
		Raw: &alphacompute.MultiMig{
			Name:     mmigName,
			SelfLink: mmigURL,
		},
	}
	gceResources.Migs[migName] = &MockMIG{
		Name: migName,
		Raw: &alphacompute.InstanceGroupManager{
			Name: migName,
		},
	}
	mu.Unlock()

	injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, sliceName, opKey, opStatusRunning)
	req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to inject RUNNING status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	patchBody := &alphacompute.InstanceGroupManager{MultiMig: mmigURL}
	body, _ := json.Marshal(patchBody)
	req, _ = http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to call attach API: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	mu.Lock()
	if gceResources.Migs[migName].MultiMig != "" {
		t.Error("MIG should not be attached yet")
	}
	mu.Unlock()

	injectionBody = fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, sliceName, opKey, opStatusDone)
	req, _ = http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to inject DONE status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	mig := gceResources.Migs[migName]
	if mig.MultiMig != mmigName {
		t.Errorf("Expected MultiMig to be %s, got %s", mmigName, mig.MultiMig)
	}
	mockMmig := gceResources.Mmigs[mmigName]
	if len(mockMmig.AttachedMigs) != 1 || mockMmig.AttachedMigs[0] != migName {
		t.Errorf("Expected AttachedMigs to contain %s, got %v", migName, mockMmig.AttachedMigs)
	}

	// Verify WantedMigs is also updated.
	injection, ok := sliceInjections[sliceName]
	if !ok {
		t.Errorf("Expected sliceInjections for %s to exist", sliceName)
	} else {
		foundWanted := false
		for _, m := range injection.WantedMigs {
			if m == migName {
				foundWanted = true
				break
			}
		}
		if !foundWanted {
			t.Errorf("Expected WantedMigs to contain %s, got %v", migName, injection.WantedMigs)
		}
	}
}

func testDetachOpTransitionDone(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "detach-done-slice"
	mmigName := sliceName
	migName := "mig-for-detach-done"
	opKey := OpPrefixDetachMIGs + migName

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs/" + mmigName
	injectionURL := server.URL + "/inject/op-status"

	InitSlice(sliceName)
	mu.Lock()
	gceResources.Mmigs[mmigName] = &MockMMIG{
		Name:         mmigName,
		AttachedMigs: []string{migName},
		Raw: &alphacompute.MultiMig{
			Name:     mmigName,
			SelfLink: mmigURL,
		},
	}
	gceResources.Migs[migName] = &MockMIG{
		Name:     migName,
		MultiMig: mmigURL,
		Raw: &alphacompute.InstanceGroupManager{
			Name: migName,
		},
	}
	mu.Unlock()

	injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, sliceName, opKey, opStatusRunning)
	req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to inject RUNNING status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	patchBody := &alphacompute.InstanceGroupManager{}
	body, _ := json.Marshal(patchBody)
	req, _ = http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(body))
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to call detach API: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	mu.Lock()
	if gceResources.Migs[migName].MultiMig == "" {
		t.Error("MIG should still be attached")
	}
	mu.Unlock()

	injectionBody = fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, sliceName, opKey, opStatusDone)
	req, _ = http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to inject DONE status: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	mig := gceResources.Migs[migName]
	if mig.MultiMig != "" {
		t.Errorf("Expected MultiMig to be empty, got %s", mig.MultiMig)
	}
	mockMmig := gceResources.Mmigs[mmigName]
	if len(mockMmig.AttachedMigs) != 0 {
		t.Errorf("Expected AttachedMigs to be empty, got %v", mockMmig.AttachedMigs)
	}
}

func testTopologyHealthInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "topo-health-slice"
	igmName := "test-mig-name"
	project := testProject
	zone := testZone

	url := fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/%s/getAvailableAcceleratorTopologies",
		server.URL, project, zone, igmName)
	injectionURL := server.URL + "/inject/topo-health"

	// Pre-initialize the slice for testing.
	InitSlice(sliceName)
	mu.Lock()
	gceResources.Migs[igmName] = &MockMIG{
		Name:      igmName,
		SliceName: sliceName,
		Raw: &alphacompute.InstanceGroupManager{
			Name: igmName,
		},
	}
	mu.Unlock()

	// 1. Test GetAvailableAcceleratorTopologies with default health (HEALTHY)
	t.Run("GetTopologies_DefaultHealth", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK, got %d", resp.StatusCode)
		}

		if err != nil {
			t.Fatalf("Failed to send GET request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		var gotResp alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse
		if err := json.NewDecoder(resp.Body).Decode(&gotResp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		topoInfo, ok := gotResp.AcceleratorTopologiesInfo[igmName]
		if !ok {
			t.Fatalf("Expected topology info for IGM '%s', but it was not found", igmName)
		}
		if topoInfo.AcceleratorTopologyHealth != string(AcceleratorTopologyHealthStatusHealthy) {
			t.Errorf("Expected default health status to be %s, got %s",
				AcceleratorTopologyHealthStatusHealthy, topoInfo.AcceleratorTopologyHealth)
		}
	})

	// 2. Test Injecting a 'DEGRADED' health status
	t.Run("InjectTopoHealth_Degraded", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName + `", "key": "` + igmName + `", "status": "DEGRADED"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for health injection, got %d", resp.StatusCode)
		}
		_ = resp.Body.Close()

		// Now get the topologies and check the health
		getReq, _ := http.NewRequest(http.MethodGet, url, nil)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatalf("Failed to send GET request after injection: %v", err)
		}
		defer func() {
			_ = getResp.Body.Close()
		}()

		var gotResp alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse
		if err := json.NewDecoder(getResp.Body).Decode(&gotResp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if gotResp.AcceleratorTopologiesInfo[igmName].AcceleratorTopologyHealth != "DEGRADED" {
			t.Errorf("Expected injected health status to be DEGRADED, got %s",
				gotResp.AcceleratorTopologiesInfo[igmName].AcceleratorTopologyHealth)
		}
	})

	// 3. Test injecting an invalid health status
	t.Run("InjectTopoHealth_InvalidStatus", func(t *testing.T) {
		injectionBody := `{"sliceName": "` + sliceName + `", "key": "` + igmName + `", "status": "INVALID_STATUS"}`
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status %d for invalid health injection, got %d", http.StatusBadRequest, resp.StatusCode)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
	})

	// 4. Test Clearing the injected health status
	t.Run("ClearInjectedTopoHealth_Success", func(t *testing.T) {
		clearBody := `{"sliceName": "` + sliceName + `", "key": "` + igmName + `"}`
		req, _ := http.NewRequest(http.MethodDelete, injectionURL, bytes.NewBufferString(clearBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send DELETE request to injection URL: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK for clearing health injection, got %d", resp.StatusCode)
		}
		_ = resp.Body.Close()

		// Verify it has reverted to default
		getReq, _ := http.NewRequest(http.MethodGet, url, nil)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatalf("Failed to send GET request after clearing injection: %v", err)
		}
		defer func() {
			_ = getResp.Body.Close()
		}()

		var gotResp alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse
		if err := json.NewDecoder(getResp.Body).Decode(&gotResp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		topoInfo, ok := gotResp.AcceleratorTopologiesInfo[igmName]
		if !ok || topoInfo.AcceleratorTopologyHealth != string(AcceleratorTopologyHealthStatusHealthy) {
			t.Errorf("Expected health status to be %s after clearing, got %s",
				AcceleratorTopologyHealthStatusHealthy, topoInfo.AcceleratorTopologyHealth)
		}

	})
}

func testGetAvailableAcceleratorTopologies(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	zone := testZone
	project := testProject

	// 1. Test successful get when the slice is initialized.
	t.Run("GetTopologies_Success", func(t *testing.T) {
		// The handler expects the slice to be initialized.
		sliceName := "topo-slice-success"
		igmName := "igm-success"
		InitSlice(sliceName)
		mu.Lock()
		gceResources.Migs[igmName] = &MockMIG{
			Name:      igmName,
			SliceName: sliceName,
			Raw: &alphacompute.InstanceGroupManager{
				Name: igmName,
			},
		}
		mu.Unlock()

		url := fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/%s/getAvailableAcceleratorTopologies",
			server.URL, project, zone, igmName)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
		}

		var gotResp alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse
		if err := json.NewDecoder(resp.Body).Decode(&gotResp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		wantInfo := alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
			AcceleratorTopology:       "4x4x4",
			AcceleratorTopologyHealth: string(AcceleratorTopologyHealthStatusHealthy),
		}

		if diff := cmp.Diff(wantInfo, gotResp.AcceleratorTopologiesInfo[igmName]); diff != "" {
			t.Errorf("Got response mismatch (-want +got):\n%s", diff)
		}
	})

	// 2. Test not found when the slice is not initialized.
	t.Run("GetTopologies_SliceNotFound", func(t *testing.T) {
		igmName := "igm-not-found"
		url := fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/%s/getAvailableAcceleratorTopologies",
			server.URL, project, zone, igmName)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
		}

		var gotResp alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse
		if err := json.NewDecoder(resp.Body).Decode(&gotResp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		wantInfo := alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
			AcceleratorTopology:       "4x4x4",
			AcceleratorTopologyHealth: string(AcceleratorTopologyHealthStatusHealthy),
		}

		if diff := cmp.Diff(wantInfo, gotResp.AcceleratorTopologiesInfo[igmName]); diff != "" {
			t.Errorf("Got response mismatch (-want +got):\n%s", diff)
		}
	})

	// 3. Test ambiguous IGM found in multiple slices.
	t.Run("CreateMIG_AmbiguousIGM", func(t *testing.T) {
		sliceA := "slice-a"
		sliceB := "slice-b"
		ambiguousIgmName := "igm-ambiguous"
		injectionURL := server.URL + "/inject/mig"

		// Create the MIG in the first slice successfully.
		body1 := fmt.Sprintf(`{"sliceName": "%s", "migName": "%s"}`, sliceA, ambiguousIgmName)
		req1, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(body1))
		req1.Header.Set("Content-Type", "application/json")
		resp1, err := http.DefaultClient.Do(req1)
		if err != nil || resp1.StatusCode != http.StatusOK {
			t.Fatalf("Expected to create MIG in slice A successfully, but failed. err: %v, status: %d", err, resp1.StatusCode)
		}
		if resp1 != nil && resp1.Body != nil {
			_ = resp1.Body.Close()
		}

		// Attempt to create the same MIG in a second slice, which should fail.
		body2 := fmt.Sprintf(`{"sliceName": "%s", "migName": "%s"}`, sliceB, ambiguousIgmName)
		req2, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(body2))
		req2.Header.Set("Content-Type", "application/json")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatalf("Error on second MIG creation request: %v", err)
		}
		defer func() {
			_ = resp2.Body.Close()
		}()

		if resp2.StatusCode != http.StatusOK {
			t.Errorf("Expected status code %d for reassigned IGM creation (adoption), got %d", http.StatusOK, resp2.StatusCode)
		}
	})
}

func testGetTopologiesMultipleSlicesWithInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	zone := testZone
	project := testProject

	slice1 := "slice1-inject"
	mig1 := "mig1-inject"
	slice2 := "slice2-inject"
	mig2 := "mig2-inject"

	injectionURL := server.URL + "/inject/mig"
	topoInjURL := server.URL + "/inject/topology-info"

	// Use the injection endpoint to create the MIGs and topologies explicitly.
	for _, tc := range []struct{ s, m string }{{slice1, mig1}, {slice2, mig2}} {
		body := fmt.Sprintf(`{"sliceName": "%s", "migName": "%s"}`, tc.s, tc.m)
		req, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to inject mig %s for slice %s. err: %v, status: %d", tc.m, tc.s, err, resp.StatusCode)
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		topoBody := fmt.Sprintf(`{"sliceName": "%s", "igmName": "%s", "topologies": {`+
			`"%s": {"acceleratorTopology": "4x4x8", "acceleratorTopologyHealth": "HEALTHY", `+
			`"instancesHealth": "ALL_HEALTHY"}}}`, tc.s, tc.m, tc.m)
		reqTopo, _ := http.NewRequest(http.MethodPost, topoInjURL, bytes.NewBufferString(topoBody))
		reqTopo.Header.Set("Content-Type", "application/json")
		respTopo, errTopo := http.DefaultClient.Do(reqTopo)
		if errTopo != nil || respTopo.StatusCode != http.StatusOK {
			t.Fatalf("Failed to inject topology for %s. err: %v", tc.m, errTopo)
		}
		if respTopo != nil && respTopo.Body != nil {
			_ = respTopo.Body.Close()
		}
	}

	url := fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/%s/getAvailableAcceleratorTopologies",
		server.URL, project, zone, mig1)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var gotResp alphacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&gotResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if _, ok := gotResp.AcceleratorTopologiesInfo[mig1]; !ok {
		t.Errorf("Expected to find info for mig1, but it was not in the response: %+v", gotResp)
	}
	if _, ok := gotResp.AcceleratorTopologiesInfo[mig2]; ok {
		t.Errorf("Did not expect to find info for mig2, but it was in the response: %+v", gotResp)
	}
}

func testCreateDeleteResourceWithHash(t *testing.T, initURL, resourceURL, resourceType, sliceWithHash, hash string) {
	t.Helper()
	resourceNameWithHash := sliceWithHash + "-" + hash

	// Initialize the base slice
	initBody := `{"sliceName": "` + sliceWithHash + `"}`
	initReq, _ := http.NewRequest(http.MethodPost, initURL, bytes.NewBufferString(initBody))
	initReq.Header.Set("Content-Type", "application/json")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil || initResp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to initialize slice for hash test: %v, status: %d", err, initResp.StatusCode)
	}
	_ = initResp.Body.Close()

	// Create the resource with the hash
	var resource any
	if resourceType == resourceTypePolicy {
		resource = &alphacompute.ResourcePolicy{Name: resourceNameWithHash}
	} else {
		resource = &alphacompute.MultiMig{Name: resourceNameWithHash}
	}
	createBody, _ := json.Marshal(resource)
	createReq, _ := http.NewRequest(http.MethodPost, resourceURL, bytes.NewBuffer(createBody))
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil || createResp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to create %s with hash: %v, status: %d", resourceType, err, createResp.StatusCode)
	}
	_ = createResp.Body.Close()

	// Verify it was created
	mu.Lock()
	var exists bool
	if resourceType == resourceTypePolicy {
		_, exists = gceResources.Policies[resourceNameWithHash]
	} else {
		_, exists = gceResources.Mmigs[resourceNameWithHash]
	}
	if !exists {
		mu.Unlock()
		t.Fatalf("%s with hash '%s' was not found in slice '%s'", resourceType, resourceNameWithHash, sliceWithHash)
	}
	mu.Unlock()

	// Delete the resource with the hash
	deleteURL := resourceURL + "/" + resourceNameWithHash
	deleteReq, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil || deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to delete %s with hash: %v, status: %d", resourceType, err, deleteResp.StatusCode)
	}
	_ = deleteResp.Body.Close()

	// Verify it was deleted
	mu.Lock()
	defer mu.Unlock()
	if resourceType == resourceTypePolicy {
		_, exists = gceResources.Policies[resourceNameWithHash]
	} else {
		_, exists = gceResources.Mmigs[resourceNameWithHash]
	}
	if exists {
		t.Errorf("%s with hash '%s' was not deleted from slice '%s'", resourceType, resourceNameWithHash, sliceWithHash)
	}
}

func testDeleteResourceWithOpStatusInjection(t *testing.T, serverURL, resourceType, resourceCollectionURL string) {
	t.Helper()
	t.Run(fmt.Sprintf("Delete%s_WithOpStatusInjection", resourceType), func(t *testing.T) {
		opSliceName := fmt.Sprintf("op-delete-%s-slice", strings.ToLower(resourceType))
		opResourceName := opSliceName + "-resource"
		deleteURL := resourceCollectionURL + "/" + opResourceName

		InitSlice(opSliceName)
		mu.Lock()
		if resourceType == resourceTypePolicy {
			gceResources.Policies[opResourceName] = &alphacompute.ResourcePolicy{Name: opResourceName}
		} else {
			gceResources.Mmigs[opResourceName] = &MockMMIG{
				Name: opResourceName,
				Raw: &alphacompute.MultiMig{
					Name: opResourceName,
				},
			}
		}
		mu.Unlock()

		var opKeyPrefix string
		if resourceType == resourceTypePolicy {
			opKeyPrefix = OpPrefixDeletePolicy
		} else {
			opKeyPrefix = OpPrefixDeleteMMIG
		}
		opKey := opKeyPrefix + opResourceName
		injectionURL := serverURL + "/inject/op-status"
		injectionBody := fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "status": "%s"}`, opSliceName, opKey, opStatusRunning)
		injectReq, _ := http.NewRequest(http.MethodPost, injectionURL, bytes.NewBufferString(injectionBody))
		injectReq.Header.Set("Content-Type", "application/json")
		injectResp, err := http.DefaultClient.Do(injectReq)
		if err != nil || injectResp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to inject op status: %v, status: %d", err, injectResp.StatusCode)
		}
		_ = injectResp.Body.Close()

		deleteReq, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
		deleteResp, err := http.DefaultClient.Do(deleteReq)
		if err != nil {
			t.Fatalf("Failed to send delete %s request: %v", resourceType, err)
		}
		defer func() { _ = deleteResp.Body.Close() }()

		mu.Lock()
		defer mu.Unlock()
		var exists bool
		if resourceType == resourceTypePolicy {
			_, exists = gceResources.Policies[opResourceName]
		} else { // MMIG
			_, exists = gceResources.Mmigs[opResourceName]
		}
		if !exists {
			t.Errorf("%s '%s' should have remained in the resource map due to op status injection, but it was deleted",
				resourceType, opResourceName)
		}
	})
}

func testInjectAndClearHttpError(t *testing.T, testName, injectionURL, targetURL, method string,
	body []byte, sliceName, key string, expectedStatus int,
) {
	t.Helper()
	t.Run(testName, func(t *testing.T) {
		var reqBody string
		var httpMethod string
		if strings.HasPrefix(testName, "Inject") {
			reqBody = fmt.Sprintf(`{"sliceName": "%s", "key": "%s", "code": 500, "message": "Internal Server Error"}`,
				sliceName, key)
			httpMethod = http.MethodPost
		} else { // Clear
			reqBody = fmt.Sprintf(`{"sliceName": "%s", "key": "%s"}`, sliceName, key)
			httpMethod = http.MethodDelete
		}

		req, _ := http.NewRequest(httpMethod, injectionURL, bytes.NewBufferString(reqBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to send %s request to injection URL: %v, status: %d", httpMethod, err, resp.StatusCode)
		}
		_ = resp.Body.Close()

		targetReq, _ := http.NewRequest(method, targetURL, bytes.NewBuffer(body))
		targetResp, err := http.DefaultClient.Do(targetReq)
		if err != nil {
			t.Fatalf("Failed to send %s request to target URL: %v", method, err)
		}
		defer func() { _ = targetResp.Body.Close() }()

		if targetResp.StatusCode != expectedStatus {
			t.Errorf("Expected status code %d, got %d", expectedStatus, targetResp.StatusCode)
		}
	})
}

const testMigName1 = "mig-test-1"

func testWantedMigsState(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "wanted-migs-slice"
	mmigName := sliceName + "-mmig"
	migName := testMigName1

	// Reset state
	mu.Lock()
	gceResources.Mmigs = make(map[string]*MockMMIG)
	gceResources.Migs = make(map[string]*MockMIG)
	sliceInjections = make(map[string]*SliceInjections)
	mu.Unlock()

	// 1. Initialize free MIG
	t.Run("InitializeFreeMIG", func(t *testing.T) {
		helperInitializeFreeMIG(t, server.URL, sliceName, migName)
	})

	// 2. Create MMIG
	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs"
	t.Run("CreateMMIG", func(t *testing.T) {
		helperCreateMMIG(t, mmigURL, mmigName)
	})

	// 3. Attach MIG to MMIG
	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	t.Run("AttachMIG", func(t *testing.T) {
		helperAttachMIG(t, migURL, mmigURL, mmigName, migName, sliceName)
	})

	// 4. Get MMIG (Verify ACTIVE)
	getReq, _ := http.NewRequest(http.MethodGet, mmigURL+"/"+mmigName, nil)
	t.Run("GetMMIG_Active", func(t *testing.T) {
		helperVerifyMMIGActive(t, getReq)
	})

	// 5. Detach MIG from MMIG
	t.Run("DetachMIG", func(t *testing.T) {
		helperDetachMIG(t, migURL, migName, sliceName)
	})

	// 6. Get MMIG (Verify INCOMPLETE)
	t.Run("GetMMIG_Incomplete", func(t *testing.T) {
		helperVerifyMMIGIncomplete(t, getReq)
	})

	// 7. Delete MMIG (Verify WantedMigs cleared)
	t.Run("DeleteMMIG", func(t *testing.T) {
		helperDeleteMMIG(t, mmigURL, mmigName, sliceName)
	})
}

func helperInitializeFreeMIG(t *testing.T, serverURL, sliceName, migName string) {
	injectURL := serverURL + "/inject/mig"
	injectReqBody := fmt.Sprintf(`{"sliceName": "%s", "migName": "%s"}`, sliceName, migName)
	req, _ := http.NewRequest(http.MethodPost, injectURL, bytes.NewBufferString(injectReqBody))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to initialize MIG: %v, status: %d", err, resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func helperCreateMMIG(t *testing.T, mmigURL, mmigName string) {
	mmig := &alphacompute.MultiMig{Name: mmigName}
	mmigBytes, _ := json.Marshal(mmig)
	req, _ := http.NewRequest(http.MethodPost, mmigURL, bytes.NewBuffer(mmigBytes))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to create MMIG: %v, status: %d", err, resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func helperAttachMIG(t *testing.T, migURL, mmigURL, mmigName, migName, sliceName string) {
	patchBody := &alphacompute.InstanceGroupManager{MultiMig: mmigURL + "/" + mmigName}
	patchBytes, _ := json.Marshal(patchBody)
	req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(patchBytes))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to attach MIG: %v, status: %d", err, resp.StatusCode)
	}
	_ = resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if mockMig, ok := gceResources.Migs[migName]; !ok || mockMig.MultiMig == "" {
		t.Errorf("MIG %s not attached (MultiMig empty)", migName)
	}
	if injs, ok := sliceInjections[sliceName]; !ok || len(injs.WantedMigs) != 1 {
		t.Errorf("WantedMigs not tracked for slice %s", sliceName)
	}
}

func helperVerifyMMIGActive(t *testing.T, getReq *http.Request) {
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to get MMIG: %v, status: %d", err, resp.StatusCode)
	}
	var gotMmig alphacompute.MultiMig
	if err := json.NewDecoder(resp.Body).Decode(&gotMmig); err != nil {
		t.Fatalf("Failed to decode MMIG response: %v", err)
	}
	_ = resp.Body.Close()

	if gotMmig.Status == nil || len(gotMmig.Status.AppliedAcceleratorTopologies) == 0 {
		t.Errorf("MMIG status or topology empty")
	} else if gotMmig.Status.AppliedAcceleratorTopologies[0].AcceleratorTopologyState != activeState {
		t.Errorf("Expected ACTIVE, got %s", gotMmig.Status.AppliedAcceleratorTopologies[0].AcceleratorTopologyState)
	}
}

func helperDetachMIG(t *testing.T, migURL, migName, sliceName string) {
	patchBodyDetach := &alphacompute.InstanceGroupManager{MultiMig: ""}
	patchDetachBytes, _ := json.Marshal(patchBodyDetach)
	req, _ := http.NewRequest(http.MethodPatch, migURL, bytes.NewBuffer(patchDetachBytes))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to detach MIG: %v, status: %d", err, resp.StatusCode)
	}
	_ = resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if mockMig, ok := gceResources.Migs[migName]; !ok || mockMig.MultiMig != "" {
		t.Errorf("MIG %s still attached after detach", migName)
	}
	if injs, ok := sliceInjections[sliceName]; !ok || len(injs.WantedMigs) != 1 {
		t.Errorf("WantedMigs cleared on detach, want 1")
	}
}

func helperVerifyMMIGIncomplete(t *testing.T, getReq *http.Request) {
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to get MMIG after detach: %v, status: %d", err, resp.StatusCode)
	}
	var gotMmigDetach alphacompute.MultiMig
	if err := json.NewDecoder(resp.Body).Decode(&gotMmigDetach); err != nil {
		t.Fatalf("Failed to decode MMIG response after detach: %v", err)
	}
	_ = resp.Body.Close()

	if gotMmigDetach.Status == nil || len(gotMmigDetach.Status.AppliedAcceleratorTopologies) == 0 {
		t.Errorf("MMIG status or topology empty after detach")
	} else if gotMmigDetach.Status.AppliedAcceleratorTopologies[0].AcceleratorTopologyState != "INCOMPLETE" {
		t.Errorf("Expected INCOMPLETE after detach, got %s",
			gotMmigDetach.Status.AppliedAcceleratorTopologies[0].AcceleratorTopologyState)
	}
}

func helperDeleteMMIG(t *testing.T, mmigURL, mmigName, sliceName string) {
	delReq, _ := http.NewRequest(http.MethodDelete, mmigURL+"/"+mmigName, nil)
	resp, err := http.DefaultClient.Do(delReq)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to delete MMIG: %v, status: %d", err, resp.StatusCode)
	}
	_ = resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if injs, ok := sliceInjections[sliceName]; ok && len(injs.WantedMigs) > 0 {
		t.Errorf("WantedMigs not cleared on MMIG delete")
	}
}

func TestMMIGLifecycle_Complex(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	originalClient := http.DefaultClient
	http.DefaultClient = server.Client()
	defer func() { http.DefaultClient = originalClient }()

	sliceName := "test-complex-slice"
	mmigName := sliceName + "-mmig"
	migName1 := testMigName1
	migName2 := "mig-test-2"

	mu.Lock()
	gceResources.Mmigs = make(map[string]*MockMMIG)
	gceResources.Migs = make(map[string]*MockMIG)
	sliceInjections = make(map[string]*SliceInjections)
	mu.Unlock()

	helperInitializeFreeMIG(t, server.URL, sliceName, migName1)
	helperInitializeFreeMIG(t, server.URL, sliceName, migName2)

	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs"
	helperCreateMMIG(t, mmigURL, mmigName)

	migURL1 := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName1
	helperAttachMIG(t, migURL1, mmigURL, mmigName, migName1, sliceName)

	helperDetachMIG(t, migURL1, migName1, sliceName)

	helperDeleteMMIG(t, mmigURL, mmigName, sliceName)

	helperCreateMMIG(t, mmigURL, mmigName)

	migURL2 := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName2
	helperAttachMIG(t, migURL2, mmigURL, mmigName, migName2, sliceName)

	helperDetachMIG(t, migURL2, migName2, sliceName)

	helperDeleteMMIG(t, mmigURL, mmigName, sliceName)
}

func TestDeleteNonEmptyMMIG(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	originalClient := http.DefaultClient
	http.DefaultClient = server.Client()
	defer func() { http.DefaultClient = originalClient }()

	sliceName := "test-delete-non-empty"
	mmigName := sliceName + "-mmig"
	migName := testMigName1

	mu.Lock()
	gceResources.Mmigs = make(map[string]*MockMMIG)
	gceResources.Migs = make(map[string]*MockMIG)
	sliceInjections = make(map[string]*SliceInjections)
	mu.Unlock()

	helperInitializeFreeMIG(t, server.URL, sliceName, migName)

	mmigURL := server.URL + "/projects/" + testProject + "/regions/" + testRegion + "/multiMigs"
	helperCreateMMIG(t, mmigURL, mmigName)

	migURL := server.URL + "/projects/" + testProject + "/zones/" + testZone + "/instanceGroupManagers/" + migName
	helperAttachMIG(t, migURL, mmigURL, mmigName, migName, sliceName)

	delReq, _ := http.NewRequest(http.MethodDelete, mmigURL+"/"+mmigName, nil)
	resp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("Failed to send delete request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 for non-empty delete, got %d", resp.StatusCode)
	}
}

func testTopologyInfoInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "test-topo-info"
	igmName := testIgmName

	payload := fmt.Sprintf(`{"sliceName": "%s", "igmName": "%s", `+
		`"topologies": {"partition-0": {"acceleratorTopology": "4x4x4"}}}`,
		sliceName, igmName)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/inject/topology-info", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()
	if gceResources.MigTopologies == nil ||
		gceResources.MigTopologies[igmName] == nil ||
		gceResources.MigTopologies[igmName]["partition-0"] == nil {
		t.Fatal("Topology info not found in global resources")
	}
	topo := gceResources.MigTopologies[igmName]["partition-0"]
	if topo.AcceleratorTopology != "4x4x4" {
		t.Errorf("Expected topology 4x4x4, got %s", topo.AcceleratorTopology)
	}
}

func testTopologyStateInjection(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "test-topo-state"
	igmName := testIgmName
	topoID := "partition-0"

	payload := fmt.Sprintf(`{"sliceName": "%s", "igmName": "%s", "topoId": "%s", `+
		`"state": {"currentState": "%s"}}`,
		sliceName, igmName, topoID, activeState)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/inject/topology-state", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()
	if sliceInjections[sliceName] == nil ||
		sliceInjections[sliceName].InjectedTopologyStates == nil ||
		sliceInjections[sliceName].InjectedTopologyStates[igmName] == nil ||
		sliceInjections[sliceName].InjectedTopologyStates[igmName][topoID] == nil {
		t.Fatal("Topology state not found in injections")
	}
	state := sliceInjections[sliceName].InjectedTopologyStates[igmName][topoID]
	if state.CurrentState != activeState {
		t.Errorf("Expected state ACTIVE, got %s", state.CurrentState)
	}
}

func testConfigureAcceleratorTopologies(t *testing.T) {
	mux := CreateMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	sliceName := "test-configure"
	igmName := testIgmName
	topoID := "partition-0"

	mu.Lock()
	if gceResources.MigTopologies == nil {
		gceResources.MigTopologies = make(map[string]map[string]*CustomAcceleratorTopologyInfo)
	}
	if gceResources.MigTopologies[igmName] == nil {
		gceResources.MigTopologies[igmName] = make(map[string]*CustomAcceleratorTopologyInfo)
	}
	gceResources.MigTopologies[igmName][topoID] = &CustomAcceleratorTopologyInfo{
		AcceleratorTopology: "4x4x4",
		AcceleratorTopologyState: &AcceleratorTopologyStateStruct{
			CurrentState: "INACTIVE",
		},
	}
	if gceResources.Mmigs == nil {
		gceResources.Mmigs = make(map[string]*MockMMIG)
	}
	gceResources.Mmigs[sliceName+"-mmig"] = &MockMMIG{
		AttachedMigs: []string{igmName},
	}
	mu.Unlock()

	payload := fmt.Sprintf(`{"acceleratorTopologyConfigurations": {"%s": {"action": "%s"}}}`, topoID, activeState)
	url := fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/%s/configureAcceleratorTopology",
		server.URL, testProject, testZone, igmName)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()
	topo := gceResources.MigTopologies[igmName][topoID]
	if topo.AcceleratorTopologyState.CurrentState != activeState {
		t.Errorf("Expected state %s, got %s", activeState, topo.AcceleratorTopologyState.CurrentState)
	}
}
