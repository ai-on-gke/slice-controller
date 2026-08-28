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

package util

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	slicev1beta1 "slice-controller/api/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
	"slice-controller/internal/metrics"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestFindStateCondition(t *testing.T) {
	cases := []struct {
		name  string
		slice *slicev1beta1.Slice
		want  string
	}{
		{
			name: "slice has state condition",
			slice: &slicev1beta1.Slice{
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: slicev1beta1.SliceStateConditionType, Status: metav1.ConditionFalse, Reason: string(MMIGHealthStatusIncomplete)},
					},
				},
			},
			want: string(MMIGHealthStatusIncomplete),
		},
		{
			name: "slice has no state conditions",
			slice: &slicev1beta1.Slice{
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: "SomeOtherCondition", Status: metav1.ConditionTrue},
					},
				},
			},
			want: "",
		},
		{
			name: "slice has no conditions",
			slice: &slicev1beta1.Slice{
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{},
				},
			},
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := FindStateCondition(tc.slice)
			if got != tc.want {
				t.Errorf("FindStateCondition() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFindSliceCondition(t *testing.T) {
	condition := metav1.Condition{Type: "TestCondition", Status: metav1.ConditionTrue}
	cases := []struct {
		name          string
		slice         *slicev1beta1.Slice
		conditionName string
		wantCond      metav1.Condition
		wantOk        bool
	}{
		{
			name: "condition exists",
			slice: &slicev1beta1.Slice{
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{condition},
				},
			},
			conditionName: "TestCondition",
			wantCond:      condition,
			wantOk:        true,
		},
		{
			name: "condition does not exist",
			slice: &slicev1beta1.Slice{
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{},
				},
			},
			conditionName: "TestCondition",
			wantCond:      metav1.Condition{},
			wantOk:        false,
		},
		{
			name: "multiple conditions, find correct one",
			slice: &slicev1beta1.Slice{
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: "Other", Status: metav1.ConditionFalse},
						condition,
					},
				},
			},
			conditionName: "TestCondition",
			wantCond:      condition,
			wantOk:        true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCond, gotOk := FindSliceCondition(tc.slice, tc.conditionName)
			if gotOk != tc.wantOk {
				t.Errorf("FindSliceCondition() gotOk = %v, want %v", gotOk, tc.wantOk)
			}
			if !cmp.Equal(gotCond, tc.wantCond) {
				t.Errorf("FindSliceCondition() gotCond = %v, want %v", gotCond, tc.wantCond)
			}
		})
	}
}

func TestDeleteSliceCondition(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := slicev1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add slicev1beta1 to scheme: %v", err)
	}

	testCases := []struct {
		name               string
		initialSlice       *slicev1beta1.Slice
		conditionType      string
		expectedConditions []metav1.Condition
	}{
		{
			name: "delete an existing condition",
			initialSlice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice", Namespace: "default"},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: "CondA", Status: metav1.ConditionTrue},
						{Type: "CondB", Status: metav1.ConditionTrue},
					},
				},
			},
			conditionType: "CondA",
			expectedConditions: []metav1.Condition{
				{Type: "CondB", Status: metav1.ConditionTrue},
			},
		},
		{
			name: "delete a non-existent condition",
			initialSlice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice", Namespace: "default"},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: "CondA", Status: metav1.ConditionTrue},
						{Type: "CondB", Status: metav1.ConditionTrue},
					},
				},
			},
			conditionType: "CondC",
			expectedConditions: []metav1.Condition{
				{Type: "CondA", Status: metav1.ConditionTrue},
				{Type: "CondB", Status: metav1.ConditionTrue},
			},
		},
		{
			name: "delete the only condition",
			initialSlice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice", Namespace: "default"},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: "CondA", Status: metav1.ConditionTrue},
					},
				},
			},
			conditionType:      "CondA",
			expectedConditions: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sliceForUpdate := tc.initialSlice.DeepCopy()
			fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(sliceForUpdate).WithStatusSubresource(sliceForUpdate).Build()
			ctx := context.Background()

			err := DeleteSliceCondition(sliceForUpdate, tc.conditionType, fakeClient, ctx)
			if err != nil {
				t.Fatalf("DeleteSliceCondition() returned an unexpected error: %v", err)
			}

			updatedSlice := &slicev1beta1.Slice{}
			key := types.NamespacedName{Name: tc.initialSlice.Name, Namespace: tc.initialSlice.Namespace}
			if err := fakeClient.Get(ctx, key, updatedSlice); err != nil {
				t.Fatalf("Failed to get updated slice from fake client: %v", err)
			}

			opts := cmp.Comparer(func(a, b metav1.Time) bool {
				return true
			})

			if diff := cmp.Diff(tc.expectedConditions, updatedSlice.Status.Conditions, opts); diff != "" {
				t.Errorf("Unexpected slice conditions (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetResourceName(t *testing.T) {
	testCases := []struct {
		name        string
		sliceName   string
		clusterHash string
		want        string
	}{
		{
			name:        "hash longer than 8 chars",
			sliceName:   "my-slice",
			clusterHash: "1234567890abcdef",
			want:        "my-slice-12345678",
		},
		{
			name:        "hash is 8 chars",
			sliceName:   "my-slice",
			clusterHash: "12345678",
			want:        "my-slice-12345678",
		},
		{
			name:        "hash shorter than 8 chars",
			sliceName:   "my-slice",
			clusterHash: "1234",
			want:        "my-slice-1234",
		},
		{
			name:        "empty hash",
			sliceName:   "my-slice",
			clusterHash: "",
			want:        "my-slice",
		},
		{
			name:        "empty slice name",
			sliceName:   "",
			clusterHash: "12345678",
			want:        "-12345678",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := GetResourceName(tc.sliceName, tc.clusterHash)
			if got != tc.want {
				t.Errorf("GetResourceName(%q, %q) = %q, want %q", tc.sliceName, tc.clusterHash, got, tc.want)
			}
		})
	}
}

func TestPatchSliceCondition(t *testing.T) {
	// 1. Setup Scheme (Register your CRD types)
	scheme := runtime.NewScheme()
	_ = slicev1beta1.AddToScheme(scheme)

	// Helper to create a dummy slice
	createSlice := func(name string, conditions []metav1.Condition) *slicev1beta1.Slice {
		return &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				// ResourceVersion is needed to verify if Patch was skipped (optimization check)
				ResourceVersion: "1",
			},
			Status: slicev1beta1.SliceStatus{
				Conditions: conditions,
			},
		}
	}

	tests := []struct {
		name          string
		initialObject *slicev1beta1.Slice
		patchArgs     struct {
			state   string
			status  metav1.ConditionStatus
			reason  string
			message string
		}
		expectError     bool
		expectUpdate    bool // true if we expect a Patch call (ResourceVersion change)
		validateResult  func(t *testing.T, s *slicev1beta1.Slice)
		metricsExporter *metrics.MetricsExporter
	}{
		{
			name:          "Add new condition",
			initialObject: createSlice("slice-1", []metav1.Condition{}),
			patchArgs: struct {
				state   string
				status  metav1.ConditionStatus
				reason  string
				message string
			}{
				state:   "Ready",
				status:  metav1.ConditionTrue,
				reason:  "Provisioned",
				message: "Slice is ready",
			},
			expectError:  false,
			expectUpdate: true,
			validateResult: func(t *testing.T, s *slicev1beta1.Slice) {
				cond := meta.FindStatusCondition(s.Status.Conditions, "Ready")
				assert.NotNil(t, cond)
				assert.Equal(t, metav1.ConditionTrue, cond.Status)
				assert.Equal(t, "Provisioned", cond.Reason)
			},
		},
		{
			name: "Update existing condition with new status",
			initialObject: createSlice("slice-2", []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionFalse,
					Reason: "Pending",
				},
			}),
			patchArgs: struct {
				state   string
				status  metav1.ConditionStatus
				reason  string
				message string
			}{
				state:   "Ready",
				status:  metav1.ConditionTrue,
				reason:  "Resolved",
				message: "Fixed issue",
			},
			expectError:  false,
			expectUpdate: true,
			validateResult: func(t *testing.T, s *slicev1beta1.Slice) {
				cond := meta.FindStatusCondition(s.Status.Conditions, "Ready")
				assert.NotNil(t, cond)
				assert.Equal(t, metav1.ConditionTrue, cond.Status)
				assert.Equal(t, "Resolved", cond.Reason)
			},
		},
		{
			name: "No-op: Same condition data (Optimization Check)",
			initialObject: createSlice("slice-3", []metav1.Condition{
				{
					Type:    "Ready",
					Status:  metav1.ConditionTrue,
					Reason:  "Stable",
					Message: "All good",
					// Ensure LastTransitionTime is older so checking equality works
					LastTransitionTime: metav1.NewTime(time.Now().Add(-1 * time.Hour)),
				},
			}),
			patchArgs: struct {
				state   string
				status  metav1.ConditionStatus
				reason  string
				message string
			}{
				state:   "Ready",
				status:  metav1.ConditionTrue,
				reason:  "Stable",
				message: "All good",
			},
			expectError:  false,
			expectUpdate: false, // Should NOT trigger a Patch because !changed
			validateResult: func(t *testing.T, s *slicev1beta1.Slice) {
				cond := meta.FindStatusCondition(s.Status.Conditions, "Ready")
				assert.NotNil(t, cond)
				// Verify timestamp did NOT update to Now()
				assert.True(t, time.Since(cond.LastTransitionTime.Time) > 10*time.Minute, "Timestamp should not have updated")
			},
		},
		{
			name: "Update existing condition with empty status and reason (partial patch)",
			initialObject: createSlice("slice-4", []metav1.Condition{
				{
					Type:    "Ready",
					Status:  metav1.ConditionFalse,
					Reason:  "Failed",
					Message: "Old message",
				},
			}),
			patchArgs: struct {
				state   string
				status  metav1.ConditionStatus
				reason  string
				message string
			}{
				state:   "Ready",
				status:  "",
				reason:  "",
				message: "New message",
			},
			expectError:  false,
			expectUpdate: true,
			validateResult: func(t *testing.T, s *slicev1beta1.Slice) {
				cond := meta.FindStatusCondition(s.Status.Conditions, "Ready")
				assert.NotNil(t, cond)
				assert.Equal(t, metav1.ConditionFalse, cond.Status)
				assert.Equal(t, "Failed", cond.Reason)
				assert.Equal(t, "New message", cond.Message)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 2. Create Fake Client with initial object
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.initialObject).
				WithStatusSubresource(tt.initialObject). // Important: Enable status subresource
				Build()

			ctx := context.TODO()

			// 3. Run the function
			// Note: Use a DeepCopy of initialObject because the function modifies it in-place
			objToPass := tt.initialObject.DeepCopy()

			_, err := PatchSliceCondition(
				ctx,
				fakeClient,
				objToPass,
				tt.patchArgs.state,
				tt.patchArgs.status,
				tt.patchArgs.reason,
				tt.patchArgs.message,
				tt.metricsExporter,
			)

			// 4. Assertions
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// 5. Fetch the object from the Fake Client to ensure changes were PERSISTED
			finalObj := &slicev1beta1.Slice{}
			err = fakeClient.Get(ctx, client.ObjectKeyFromObject(tt.initialObject), finalObj)
			assert.NoError(t, err)

			// Validate logic specific to the test case
			if tt.validateResult != nil {
				tt.validateResult(t, finalObj)
			}

			// Validate the optimization (ResourceVersion check)
			if tt.expectUpdate {
				// Fake client increments ResourceVersion on writes
				assert.NotEqual(t, tt.initialObject.ResourceVersion, finalObj.ResourceVersion, "Expected ResourceVersion to change")
			} else {
				assert.Equal(t, tt.initialObject.ResourceVersion, finalObj.ResourceVersion, "Expected ResourceVersion to remain unchanged")
			}
		})
	}
}

// TestRetryGCECall_IgnoredError verifies that the function does not retry and returns nil result for specific ignored errors.
func TestGenerateRequestID(t *testing.T) {
	uid := types.UID("123e4567-e89b-12d3-a456-426614174000") // standard 36-char K8s UID
	generation := int64(2)
	resourceName := "gke-e2e-duplicated-c-np-inc-block-cub-33c272e6-grp"

	// Regex for a standard RFC 4122 UUID format (lowercase hex with dashes)
	uuidRegex := regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)

	t.Run("Basic Format and Length", func(t *testing.T) {
		reqID := GenerateRequestID(uid, generation, resourceName, AttachOperation, 0)

		if len(reqID) != 36 {
			t.Errorf("Request ID must be exactly 36 characters long, got %d: %s", len(reqID), reqID)
		}
		if !uuidRegex.MatchString(reqID) {
			t.Errorf("Request ID does not match strict RFC 4122 UUID format: %s", reqID)
		}
	})

	t.Run("Determinism (Idempotency)", func(t *testing.T) {
		reqID1 := GenerateRequestID(uid, generation, resourceName, AttachOperation, 0)
		reqID2 := GenerateRequestID(uid, generation, resourceName, AttachOperation, 0)

		if reqID1 != reqID2 {
			t.Errorf("Expected identical IDs for identical inputs, got %s and %s", reqID1, reqID2)
		}
	})

	t.Run("Uniqueness Across Resources", func(t *testing.T) {
		reqID1 := GenerateRequestID(uid, generation, "mig-a", AttachOperation, 0)
		reqID2 := GenerateRequestID(uid, generation, "mig-b", AttachOperation, 0)

		if reqID1 == reqID2 {
			t.Errorf("Expected different IDs for different MIGs, got identical: %s", reqID1)
		}
	})

	t.Run("Uniqueness Across Operations", func(t *testing.T) {
		reqIDAttach := GenerateRequestID(uid, generation, resourceName, AttachOperation, 0)
		reqIDDetach := GenerateRequestID(uid, generation, resourceName, DetachOperation, 0)

		if reqIDAttach == reqIDDetach {
			t.Errorf("Expected different IDs for Attach and Detach operations, got identical: %s", reqIDAttach)
		}
	})

	t.Run("Extreme Inputs Yield Valid UUID", func(t *testing.T) {
		// Use MaxInt64 to force a huge numeric input to ensure the hash handles it cleanly
		hugeGeneration := int64(9223372036854775807)
		reqID := GenerateRequestID(uid, hugeGeneration, resourceName, AttachOperation, 0)

		if len(reqID) != 36 {
			t.Errorf("Expected exactly 36 chars for huge generation, got %d: %s", len(reqID), reqID)
		}
		if !uuidRegex.MatchString(reqID) {
			t.Errorf("Request ID does not match strict RFC 4122 UUID format: %s", reqID)
		}
	})
}
func TestPatchSliceCondition_Metrics(t *testing.T) {
	// 1. Setup scheme
	scheme := runtime.NewScheme()
	_ = slicev1beta1.AddToScheme(scheme)

	// 2. Create initial object
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-slice",
			Namespace: "test-ns",
			UID:       "test-uid",
			Annotations: map[string]string{
				LastReadyConditionTransitionTimeAnnotation: time.Now().Add(-60 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: slicev1beta1.SliceSpec{
			Type:     "tpu7x",
			Topology: "2x2x2",
		},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{
					Type:    string(slicev1beta1.SliceStateConditionType),
					Status:  metav1.ConditionTrue,
					Reason:  "ACTIVATING",
					Message: "Old state",
				},
			},
		},
	}

	// 3. Create Fake Client
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(slice).
		WithStatusSubresource(slice).
		Build()

	// 4. Update Slice Status Condition
	_, err := PatchSliceCondition(
		context.Background(),
		fakeClient,
		slice,
		string(slicev1beta1.SliceStateConditionType),
		metav1.ConditionTrue,
		"ACTIVE",
		"New state",
		nil,
	)
	if err != nil {
		t.Fatalf("PatchSliceCondition returned error: %v", err)
	}

	assert.NoError(t, err)
}

func TestIsSubslice(t *testing.T) {
	testCases := []struct {
		name string
		spec *slicev1beta1.SliceSpec
		want bool
	}{
		{
			name: "tpu7x allowed topology 2x2x1",
			spec: &slicev1beta1.SliceSpec{
				Type:         slicev1beta1.TypeTpu7x,
				Topology:     "2x2x1",
				PartitionIds: []string{"partition-1"},
			},
			want: true,
		},
		{
			name: "tpu7x allowed topology 2x4x4",
			spec: &slicev1beta1.SliceSpec{
				Type:         slicev1beta1.TypeTpu7x,
				Topology:     "2x4x4",
				PartitionIds: []string{"partition-1"},
			},
			want: true,
		},
		{
			name: "tpu7x disallowed topology 4x4x4",
			spec: &slicev1beta1.SliceSpec{
				Type:         slicev1beta1.TypeTpu7x,
				Topology:     "4x4x4",
				PartitionIds: []string{"partition-1"},
			},
			want: false,
		},
		{
			name: "multiple partition IDs",
			spec: &slicev1beta1.SliceSpec{
				Type:         slicev1beta1.TypeTpu7x,
				Topology:     "2x2x1",
				PartitionIds: []string{"partition-1", "partition-2"},
			},
			want: false,
		},
		{
			name: "no partition IDs",
			spec: &slicev1beta1.SliceSpec{
				Type:         slicev1beta1.TypeTpu7x,
				Topology:     "2x2x1",
				PartitionIds: []string{},
			},
			want: true,
		},
		{
			name: "unknown TPU type",
			spec: &slicev1beta1.SliceSpec{
				Type:         "unknown",
				Topology:     "2x2x1",
				PartitionIds: []string{"partition-1"},
			},
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsSubslice(tc.spec)
			if got != tc.want {
				t.Errorf("IsSubslice() = %v, want %v", got, tc.want)
			}
		})
	}
}
