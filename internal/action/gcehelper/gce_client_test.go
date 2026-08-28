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

package gcehelper

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/cache"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
	betacompute "google.golang.org/api/compute/v0.beta"
	"google.golang.org/api/googleapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	testPartialFailure                = "Partial Failure"
	testPartialFailureWithErrorEnrich = "Partial Failure with error enrichment"
)

func TestAttachMIGsToMMIG(t *testing.T) {
	testCases := []struct {
		name               string
		cubes              []util.CubeInfo
		mockPatchErr       error
		mockOpErr          error
		expectedErr        bool
		expectedPatchCalls int
		expectedOpCalls    int
		expectedErrType    interface{}
	}{
		{
			name: "Success",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
				{Zone: "zone-b", MigName: "mig-b"},
			},
			expectedErr:        false,
			expectedPatchCalls: 2,
			expectedOpCalls:    2,
		},
		{
			name: "Success with more than numWorkers cubes",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
				{Zone: "zone-b", MigName: "mig-b"},
				{Zone: "zone-c", MigName: "mig-c"},
				{Zone: "zone-d", MigName: "mig-d"},
				{Zone: "zone-e", MigName: "mig-e"},
				{Zone: "zone-f", MigName: "mig-f"},
				{Zone: "zone-g", MigName: "mig-g"},
				{Zone: "zone-h", MigName: "mig-h"},
				{Zone: "zone-i", MigName: "mig-i"},
				{Zone: "zone-j", MigName: "mig-j"},
				{Zone: "zone-k", MigName: "mig-k"},
				{Zone: "zone-l", MigName: "mig-l"},
				{Zone: "zone-m", MigName: "mig-m"},
				{Zone: "zone-n", MigName: "mig-n"},
			},
			expectedErr:        false,
			expectedPatchCalls: 14,
			expectedOpCalls:    14,
		},
		{
			name: "PatchInstanceGroupManager returns error",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
			},
			mockPatchErr:       errors.New("patch error"),
			expectedErr:        true,
			expectedPatchCalls: util.GCERetryTimes + 1,
			expectedOpCalls:    0,
			expectedErrType:    &util.APICallError{},
		},
		{
			name: "waitForOperation returns error",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
			},
			mockOpErr:          errors.New("operation error"),
			expectedErr:        true,
			expectedPatchCalls: 1,
			expectedOpCalls:    util.GCERetryTimes + 1,
			expectedErrType:    &util.APICallError{},
		},
		{
			name: testPartialFailure,
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
				{Zone: "zone-b", MigName: "mig-b"},
			},
			expectedErr: true,
		},
		{
			name: testPartialFailureWithErrorEnrich,
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a", PartitionId: "partition-a"},
				{Zone: "zone-b", MigName: "mig-b", PartitionId: "partition-b"},
			},

			expectedErr: true,
		},
	}

	// Dummy values for the new UID and Generation parameters
	dummyUID := types.UID("test-uid-12345")
	dummyGeneration := int64(1)
	dummyRetryAttempt := int64(0)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			if tc.name == testPartialFailure || tc.name == testPartialFailureWithErrorEnrich {
				opA := &betacompute.Operation{Name: "test-op-a", Status: "DONE"}
				opB := &betacompute.Operation{Name: "test-op-b", Status: "PENDING"}

				// mig-a succeeds
				computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), "test-project", "zone-a", "mig-a", gomock.Any(), gomock.Any()).Return(opA, nil).Times(1)
				computeService.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "zone-a", "test-op-a").Return(opA, nil).Times(1)

				// mig-b fails on wait
				computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), "test-project", "zone-b", "mig-b", gomock.Any(), gomock.Any()).Return(opB, nil).Times(1)
				computeService.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "zone-b", "test-op-b").Return(nil, errors.New("operation error")).Times(util.GCERetryTimes + 1)

			} else {
				op := &betacompute.Operation{Name: "test-op", Status: "DONE"}
				if tc.expectedPatchCalls > 0 {
					// Updated to 6 gomock.Any() calls to match the new signature
					computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(op, tc.mockPatchErr).Times(tc.expectedPatchCalls)
				}

				if tc.expectedOpCalls > 0 {
					if tc.mockOpErr != nil {
						computeService.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(nil, tc.mockOpErr).Times(tc.expectedOpCalls)
					} else {
						computeService.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(op, nil).Times(tc.expectedOpCalls)
					}
				}

			}

			// Execute the call with the new dummyUID and dummyGeneration arguments
			err := client.AttachMIGsToMMIG(context.Background(), "test-project", "test-region", "test-slice", tc.cubes, "test-cluster", dummyUID, dummyGeneration, dummyRetryAttempt)
			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
				if tc.name == testPartialFailureWithErrorEnrich {
					assert.Contains(t, err.Error(), "failed to attach partition-b")
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDetachMIGsFromMMIG(t *testing.T) {
	testCases := []struct {
		name               string
		cubes              []util.CubeInfo
		mockPatchErr       error
		mockOpErr          error
		expectedErr        bool
		expectedPatchCalls int
		expectedOpCalls    int
		expectedErrType    interface{}
	}{
		{
			name: "Success",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
				{Zone: "zone-b", MigName: "mig-b"},
			},
			expectedErr:        false,
			expectedPatchCalls: 2,
			expectedOpCalls:    2,
		},
		{
			name: "Success with more than numWorkers cubes",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
				{Zone: "zone-b", MigName: "mig-b"},
				{Zone: "zone-c", MigName: "mig-c"},
				{Zone: "zone-d", MigName: "mig-d"},
				{Zone: "zone-e", MigName: "mig-e"},
				{Zone: "zone-f", MigName: "mig-f"},
				{Zone: "zone-g", MigName: "mig-g"},
				{Zone: "zone-h", MigName: "mig-h"},
				{Zone: "zone-i", MigName: "mig-i"},
				{Zone: "zone-j", MigName: "mig-j"},
				{Zone: "zone-k", MigName: "mig-k"},
				{Zone: "zone-l", MigName: "mig-l"},
				{Zone: "zone-m", MigName: "mig-m"},
				{Zone: "zone-n", MigName: "mig-n"},
			},
			expectedErr:        false,
			expectedPatchCalls: 14,
			expectedOpCalls:    14,
		},
		{
			name: "PatchInstanceGroupManager returns error",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
			},
			mockPatchErr:       errors.New("patch error"),
			expectedErr:        true,
			expectedPatchCalls: util.GCERetryTimes + 1,
			expectedOpCalls:    0,
			expectedErrType:    &util.APICallError{},
		},
		{
			name: "waitForOperation returns error",
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
			},
			mockOpErr:          errors.New("operation error"),
			expectedErr:        true,
			expectedPatchCalls: 1,
			expectedOpCalls:    util.GCERetryTimes + 1,
			expectedErrType:    &util.APICallError{},
		},
		{
			name: testPartialFailure,
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a"},
				{Zone: "zone-b", MigName: "mig-b"},
			},
			expectedErr: true,
		},
		{
			name: testPartialFailureWithErrorEnrich,
			cubes: []util.CubeInfo{
				{Zone: "zone-a", MigName: "mig-a", PartitionId: "partition-a"},
				{Zone: "zone-b", MigName: "mig-b", PartitionId: "partition-b"},
			},

			expectedErr: true,
		},
	}

	// Dummy values for the new UID and Generation parameters
	dummyUID := types.UID("test-uid-12345")
	dummyGeneration := int64(1)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			if tc.name == testPartialFailure || tc.name == testPartialFailureWithErrorEnrich {
				opA := &betacompute.Operation{Name: "test-op-a", Status: "DONE"}
				opB := &betacompute.Operation{Name: "test-op-b", Status: "PENDING"}

				// mig-a succeeds
				computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), "test-project", "zone-a", "mig-a", gomock.Any(), gomock.Any()).Return(opA, nil).Times(1)
				computeService.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "zone-a", "test-op-a").Return(opA, nil).Times(1)

				// mig-b fails on wait
				computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), "test-project", "zone-b", "mig-b", gomock.Any(), gomock.Any()).Return(opB, nil).Times(1)
				computeService.EXPECT().GetZonalOperation(gomock.Any(), "test-project", "zone-b", "test-op-b").Return(nil, errors.New("operation error")).Times(util.GCERetryTimes + 1)

			} else {
				op := &betacompute.Operation{Name: "test-op", Status: "DONE"}
				if tc.expectedPatchCalls > 0 {
					// Updated to 6 gomock.Any() calls to match the new signature
					computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(op, tc.mockPatchErr).Times(tc.expectedPatchCalls)
				}
				if tc.expectedOpCalls > 0 {
					if tc.mockOpErr != nil {
						computeService.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(nil, tc.mockOpErr).Times(tc.expectedOpCalls)
					} else {
						computeService.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(op, nil).Times(tc.expectedOpCalls)
					}
				}

			}

			// Execute the call with the new dummyUID and dummyGeneration arguments appended
			err := client.DetachMIGsFromMMIG(context.Background(), "test-project", "test-region", "test-slice", tc.cubes, "test-cluster", dummyUID, dummyGeneration)

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
				if tc.name == "Partial Failure with error enrichment" {
					assert.Contains(t, err.Error(), "failed to detach partition-b")
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDetachPivotMIGFromMMIG(t *testing.T) {
	testCases := []struct {
		name            string
		pivotCube       util.CubeInfo
		mockPatchErr    error
		expectedErr     bool
		expectedOp      string
		expectedErrType interface{}
	}{
		{
			name: "Success",
			pivotCube: util.CubeInfo{
				Zone:    "zone-a",
				MigName: "mig-a",
			},
			expectedErr: false,
			expectedOp:  "test-op",
		},
		{
			name: "PatchInstanceGroupManager returns error",
			pivotCube: util.CubeInfo{
				Zone:    "zone-a",
				MigName: "mig-a",
			},
			mockPatchErr:    errors.New("patch error"),
			expectedErr:     true,
			expectedErrType: &util.APICallError{},
		},
	}

	// Dummy values for the new UID and Generation parameters
	dummyUID := types.UID("test-uid-12345")
	dummyGeneration := int64(1)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			op := &betacompute.Operation{Name: "test-op", Status: "DONE"}
			if tc.mockPatchErr != nil {
				// Updated to 6 arguments: ctx, projectID, zone, migName, igm, requestID
				computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), "test-project", tc.pivotCube.Zone, tc.pivotCube.MigName, gomock.Any(), gomock.Any()).Return(op, tc.mockPatchErr).Times(util.GCERetryTimes + 1)
			} else {
				computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), "test-project", tc.pivotCube.Zone, tc.pivotCube.MigName, gomock.Any(), gomock.Any()).Return(op, nil).Times(1)
			}

			// Append dummyUID and dummyGeneration to the function call
			opName, err := client.DetachPivotMIGFromMMIG(context.Background(), "test-project", "test-region", tc.pivotCube, dummyUID, dummyGeneration)

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedOp, opName)
			}
		})
	}
}

func TestDetachPivotMIGFromMMIG_SendsCorrectPayload(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
	client := NewClient(computeService, true, true)

	projectID := "test-project"
	region := "us-central1"
	pivotCube := util.CubeInfo{
		Zone:    "us-central1-a",
		MigName: "test-mig",
	}
	expectedOpName := "test-operation"

	// Dummy values for the new UID and Generation parameters
	dummyUID := types.UID("test-uid-12345")
	dummyGeneration := int64(1)

	// Configure the mock to capture the IGM and return a successful operation.
	var capturedIgm *betacompute.InstanceGroupManager
	op := &betacompute.Operation{Name: expectedOpName}

	// Updated to expect 6 arguments (added context and requestID)
	computeService.EXPECT().PatchInstanceGroupManager(gomock.Any(), projectID, pivotCube.Zone, pivotCube.MigName, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _, _ string, igm *betacompute.InstanceGroupManager, _ string) (*betacompute.Operation, error) {
			capturedIgm = igm // Capture the argument
			return op, nil
		})

	// Call the method under test with dummyUID and dummyGeneration
	opName, err := client.DetachPivotMIGFromMMIG(context.Background(), projectID, region, pivotCube, dummyUID, dummyGeneration)
	if err != nil {
		t.Fatalf("DetachPivotMIGFromMMIG() returned an unexpected error: %v", err)
	}

	if opName != expectedOpName {
		t.Errorf("Expected operation name %q, got %q", expectedOpName, opName)
	}

	// Assert the captured InstanceGroupManager is correct.
	expectedIgm := &betacompute.InstanceGroupManager{
		NullFields: []string{"MultiMig"},
	}

	if diff := cmp.Diff(expectedIgm, capturedIgm); diff != "" {
		t.Errorf("PatchInstanceGroupManager() received unexpected IGM (-want +got):\n%s", diff)
	}

	// Marshal to JSON to verify the request body.
	// The Google API client library handles the marshalling. It will see
	// NullFields and include "multiMig" in the JSON with a null value.
	jsonBytes, err := json.Marshal(capturedIgm)
	if err != nil {
		t.Fatalf("Failed to marshal captured IGM to JSON: %v", err)
	}
	assert.JSONEq(t, `{"multiMig":null}`, string(jsonBytes))
}

func TestCreateWorkloadPolicy(t *testing.T) {
	testCases := []struct {
		name            string
		slice           *slicev1beta1.Slice
		clusterHash     string
		mockCreateErr   error
		mockOpErr       error
		mockOpStatusErr *betacompute.OperationError
		expectedErr     bool
		expectedErrStr  string
		expectedErrType interface{}
	}{
		{
			name: "Success",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
				Spec:       slicev1beta1.SliceSpec{Topology: "test-topology"},
			},
			clusterHash: "cluster-hash-1",
			expectedErr: false,
		},
		{
			name: "Policy already exists",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
				Spec:       slicev1beta1.SliceSpec{Topology: "test-topology"},
			},
			clusterHash:   "cluster-hash-1",
			mockCreateErr: &googleapi.Error{Code: 409},
			expectedErr:   false,
		},
		{
			name: "New workload policy succeeded",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
				Spec:       slicev1beta1.SliceSpec{Topology: "test-topology"},
			},
			clusterHash: "cluster-hash-2",
			expectedErr: false,
		},
		{
			name: "CreateResourcePolicy returns error",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
				Spec:       slicev1beta1.SliceSpec{Topology: "test-topology"},
			},
			clusterHash:     "cluster-hash-1",
			mockCreateErr:   errors.New("create error"),
			expectedErr:     true,
			expectedErrType: &util.APICallError{},
		},
		{
			name: "waitForOperation returns error",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
				Spec:       slicev1beta1.SliceSpec{Topology: "test-topology"},
			},
			clusterHash:     "cluster-hash-1",
			mockOpErr:       errors.New("operation error"),
			expectedErr:     true,
			expectedErrType: &util.OperationError{},
		},
		{
			name: "waitForOperation timeout",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
				Spec:       slicev1beta1.SliceSpec{Topology: "test-topology"},
			},
			clusterHash:     "cluster-hash-1",
			mockOpErr:       context.DeadlineExceeded,
			expectedErr:     true,
			expectedErrType: &util.OperationError{},
		},
		{
			name: "waitForOperation returns operation error",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
				Spec:       slicev1beta1.SliceSpec{Topology: "test-topology"},
			},
			clusterHash: "cluster-hash-1",
			mockOpStatusErr: &betacompute.OperationError{
				Errors: []*betacompute.OperationErrorErrors{
					{Code: "ERROR_CODE_1", Message: "Detailed error message 1"},
					{Code: "ERROR_CODE_2", Message: "Detailed error message 2"},
				},
			},
			expectedErr:     true,
			expectedErrStr:  "GCE operation error: failed waiting for operation: operation test-op failed: {Code: ERROR_CODE_1, Message: Detailed error message 1}, {Code: ERROR_CODE_2, Message: Detailed error message 2}",
			expectedErrType: &util.OperationError{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			op := &betacompute.Operation{Name: "test-op", Status: "DONE"}
			if tc.mockOpStatusErr != nil {
				op.Error = tc.mockOpStatusErr
			}

			if tc.name == "CreateResourcePolicy returns error" {
				computeService.EXPECT().CreateResourcePolicy(gomock.Any(), gomock.Any(), gomock.Any()).Return((*betacompute.Operation)(nil), tc.mockCreateErr).Times(util.GCERetryTimes + 1)
			} else {
				computeService.EXPECT().CreateResourcePolicy(gomock.Any(), gomock.Any(), gomock.Any()).Return(op, tc.mockCreateErr).Times(1)
			}
			if tc.mockCreateErr == nil {
				if tc.mockOpErr != nil {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return((*betacompute.Operation)(nil), tc.mockOpErr).Times(util.GCERetryTimes + 1)
				} else {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(op, nil).Times(1)
				}
			}

			err := client.CreateWorkloadPolicy(context.Background(), "test-project", "test-region", tc.slice, tc.clusterHash)

			if tc.expectedErr {
				if tc.expectedErrStr != "" {
					assert.EqualError(t, err, tc.expectedErrStr)
				} else {
					assert.Error(t, err)
				}
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeleteWorkloadPolicy(t *testing.T) {
	testCases := []struct {
		name            string
		sliceName       string
		clusterHash     string
		mockDeleteErr   error
		mockOpErr       error
		expectedErr     bool
		expectedErrType interface{}
	}{
		{
			name:        "Success",
			sliceName:   "test-slice",
			clusterHash: "cluster-hash-1",
			expectedErr: false,
		},
		{
			name:          "Policy not found",
			sliceName:     "test-slice",
			clusterHash:   "cluster-hash-2",
			mockDeleteErr: &googleapi.Error{Code: 404},
			expectedErr:   false,
		},
		{
			name:            "DeleteResourcePolicy returns error",
			sliceName:       "test-slice",
			clusterHash:     "cluster-hash-1",
			mockDeleteErr:   errors.New("delete error"),
			expectedErr:     true,
			expectedErrType: &util.APICallError{},
		},
		{
			name:            "waitForOperation returns error",
			sliceName:       "test-slice",
			clusterHash:     "cluster-hash-1",
			mockOpErr:       errors.New("operation error"),
			expectedErr:     true,
			expectedErrType: &util.OperationError{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			op := &betacompute.Operation{Name: "test-op", Status: "DONE"}

			if tc.name == "DeleteResourcePolicy returns error" {
				computeService.EXPECT().DeleteResourcePolicy(gomock.Any(), gomock.Any(), gomock.Any()).Return((*betacompute.Operation)(nil), tc.mockDeleteErr).Times(util.GCERetryTimes + 1)
			} else {
				computeService.EXPECT().DeleteResourcePolicy(gomock.Any(), gomock.Any(), gomock.Any()).Return(op, tc.mockDeleteErr).Times(1)
			}
			if tc.mockDeleteErr == nil {
				if tc.mockOpErr != nil {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return((*betacompute.Operation)(nil), tc.mockOpErr).Times(util.GCERetryTimes + 1)
				} else {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(op, nil).Times(1)
				}
			}

			err := client.DeleteWorkloadPolicy(context.Background(), "test-project", "test-region", tc.sliceName, tc.clusterHash)

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateMMIG(t *testing.T) {
	testCases := []struct {
		name            string
		slice           *slicev1beta1.Slice
		clusterHash     string
		mockCreateErr   error
		mockOpErr       error
		expectedErr     bool
		expectedErrType interface{}
	}{
		{
			name: "Success",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
			},
			clusterHash: "cluster-hash-1",
			expectedErr: false,
		},
		{
			name: "MMIG already exists",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
			},
			clusterHash:   "cluster-hash-1",
			mockCreateErr: &googleapi.Error{Code: 409},
			expectedErr:   false,
		},
		{
			name: "Create a new MMIG succeeded",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
			},
			clusterHash: "cluster-hash-2",
			expectedErr: false,
		},
		{
			name: "CreateMMIG returns error",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
			},
			clusterHash:     "cluster-hash-1",
			mockCreateErr:   errors.New("create error"),
			expectedErr:     true,
			expectedErrType: &util.APICallError{},
		},
		{
			name: "waitForOperation returns error",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
			},
			clusterHash:     "cluster-hash-1",
			mockOpErr:       errors.New("operation error"),
			expectedErr:     true,
			expectedErrType: &util.OperationError{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			op := &betacompute.Operation{Name: "test-op", Status: "DONE"}

			if tc.name == "CreateMMIG returns error" {
				computeService.EXPECT().CreateMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return((*betacompute.Operation)(nil), tc.mockCreateErr).Times(util.GCERetryTimes + 1)
			} else {
				computeService.EXPECT().CreateMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(op, tc.mockCreateErr).Times(1)
			}
			if tc.mockCreateErr == nil {
				if tc.mockOpErr != nil {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return((*betacompute.Operation)(nil), tc.mockOpErr).Times(util.GCERetryTimes + 1)
				} else {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(op, nil).Times(1)
				}
			}

			err := client.CreateMMIG(context.Background(), "test-project", "test-region", tc.slice, tc.clusterHash)

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeleteMMIG(t *testing.T) {
	testCases := []struct {
		name            string
		sliceName       string
		clusterHash     string
		mockDeleteErr   error
		mockOpErr       error
		expectedErr     bool
		expectedErrType interface{}
	}{
		{
			name:        "Success",
			sliceName:   "test-slice",
			clusterHash: "cluster-hash-1",
			expectedErr: false,
		},
		{
			name:          "MMIG not found",
			sliceName:     "test-slice",
			clusterHash:   "cluster-hash-2",
			mockDeleteErr: &googleapi.Error{Code: 404},
			expectedErr:   false,
		},
		{
			name:            "DeleteMMIG returns error",
			sliceName:       "test-slice",
			clusterHash:     "cluster-hash-1",
			mockDeleteErr:   errors.New("delete error"),
			expectedErr:     true,
			expectedErrType: &util.APICallError{},
		},
		{
			name:            "waitForOperation returns error",
			sliceName:       "test-slice",
			clusterHash:     "cluster-hash-1",
			mockOpErr:       errors.New("operation error"),
			expectedErr:     true,
			expectedErrType: &util.OperationError{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			op := &betacompute.Operation{Name: "test-op", Status: "DONE"}

			if tc.name == "DeleteMMIG returns error" {
				computeService.EXPECT().DeleteMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return((*betacompute.Operation)(nil), tc.mockDeleteErr).Times(util.GCERetryTimes + 1)
			} else {
				computeService.EXPECT().DeleteMMIG(gomock.Any(), gomock.Any(), gomock.Any()).Return(op, tc.mockDeleteErr).Times(1)
			}
			if tc.mockDeleteErr == nil {
				if tc.mockOpErr != nil {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return((*betacompute.Operation)(nil), tc.mockOpErr).Times(util.GCERetryTimes + 1)
				} else {
					computeService.EXPECT().GetRegionalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(op, nil).Times(1)
				}
			}

			err := client.DeleteMMIG(context.Background(), "test-project", "test-region", tc.sliceName, tc.clusterHash)

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetAvailableAcceleratorTopologies(t *testing.T) {
	testCases := []struct {
		name             string
		enableSubSlicing bool
		mockResponse     *betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse
		mockErr          error
		expectedResponse cache.AcceleratorTopologies
		expectedErr      bool
	}{
		{
			name:             "Success",
			enableSubSlicing: true,
			mockResponse: &betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse{
				AcceleratorTopologiesInfo: map[string]betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:       "4x4",
						AcceleratorTopologyHealth: "HEALTHY",
					},
					"topo-id-2": {
						AcceleratorTopology:       "2x2",
						AcceleratorTopologyHealth: "DEGRADED",
					},
				},
			},
			expectedResponse: cache.AcceleratorTopologies{
				MigName: "test-igm",
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:             "4x4",
						AcceleratorTopologyHealthStatus: "HEALTHY",
					},
					"topo-id-2": {
						AcceleratorTopology:             "2x2",
						AcceleratorTopologyHealthStatus: "DEGRADED",
					},
				},
			},
			expectedErr: false,
		},
		{
			name:             "Success with sub-slicing enabled and unhealthy instances",
			enableSubSlicing: true,
			mockResponse: &betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse{
				AcceleratorTopologiesInfo: map[string]betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:       "4x4",
						AcceleratorTopologyHealth: "HEALTHY",
						InstancesHealth:           "UNHEALTHY_OR_MISSING",
					},
				},
			},
			expectedResponse: cache.AcceleratorTopologies{
				MigName: "test-igm",
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:             "4x4",
						AcceleratorTopologyHealthStatus: "UNHEALTHY",
						InstancesHealth:                 "UNHEALTHY_OR_MISSING",
					},
				},
			},
			expectedErr: false,
		},
		{
			name:             "Success with sub-slicing disabled and unhealthy instances (should ignore instances health)",
			enableSubSlicing: false,
			mockResponse: &betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse{
				AcceleratorTopologiesInfo: map[string]betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:       "4x4",
						AcceleratorTopologyHealth: "HEALTHY",
						InstancesHealth:           "UNHEALTHY_OR_MISSING",
					},
				},
			},
			expectedResponse: cache.AcceleratorTopologies{
				MigName: "test-igm",
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:             "4x4",
						AcceleratorTopologyHealthStatus: "HEALTHY",
					},
				},
			},
			expectedErr: false,
		},
		{
			name:             "Success with empty response",
			enableSubSlicing: true,
			mockResponse: &betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse{
				AcceleratorTopologiesInfo: map[string]betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{},
			},
			expectedResponse: cache.AcceleratorTopologies{
				MigName:        "test-igm",
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{},
			},
			expectedErr: false,
		},
		{
			name:             "GCE API call fails",
			enableSubSlicing: true,
			mockResponse:     nil,
			mockErr:          errors.New("gce error"),
			// No expectedResponse needed when an error is expected.
			expectedErr: true,
		},
		{
			name:             "Success with unhealthy topology",
			enableSubSlicing: true,
			mockResponse: &betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse{
				AcceleratorTopologiesInfo: map[string]betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:       "4x4",
						AcceleratorTopologyHealth: "UNHEALTHY",
					},
				},
			},
			expectedResponse: cache.AcceleratorTopologies{
				MigName: "test-igm",
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:             "4x4",
						AcceleratorTopologyHealthStatus: "UNHEALTHY",
					},
				},
			},
			expectedErr: false,
		},
		{
			name:             "Success with SliceID",
			enableSubSlicing: true,
			mockResponse: &betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse{
				AcceleratorTopologiesInfo: map[string]betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:       "4x4",
						AcceleratorTopologyHealth: "HEALTHY",
						AcceleratorTopologyState: &betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponseAcceleratorTopologyState{
							CurrentState: "ACTIVE",
							ExternalId:   "test-external-id",
						},
					},
				},
			},
			expectedResponse: cache.AcceleratorTopologies{
				MigName: "test-igm",
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"topo-id-1": {
						AcceleratorTopology:             "4x4",
						AcceleratorTopologyHealthStatus: "HEALTHY",
						AcceleratorTopologyState:        "ACTIVE",
						SliceID:                         "test-external-id",
					},
				},
			},
			expectedErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, tc.enableSubSlicing)

			// For the failure case, we expect it to be called multiple times due to the retry logic.
			// For success cases, it's only called once.
			if tc.mockErr != nil {
				computeService.EXPECT().
					GetAvailableAcceleratorTopologies("test-project", "test-zone", "test-igm").
					Return(tc.mockResponse, tc.mockErr).
					Times(util.GCERetryTimes + 1) // Expect retries on failure
			} else {
				computeService.EXPECT().
					GetAvailableAcceleratorTopologies("test-project", "test-zone", "test-igm").
					Return(tc.mockResponse, tc.mockErr).
					Times(1) // Expect a single call on success
			}

			resp, err := client.GetAvailableAcceleratorTopologies(context.Background(), "test-project", "test-zone", "test-igm")

			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResponse, resp)
			}
		})
	}
}

func TestGetMMIGStatus(t *testing.T) {
	testCases := []struct {
		name           string
		mockMmig       *betacompute.MultiMig
		mockGetErr     error
		expectedStatus cache.SuperSliceHealthStatus
		expectedErr    bool
	}{
		{
			name: "Success with full status details",
			mockMmig: &betacompute.MultiMig{
				Name: "test-slice",
				Status: &betacompute.MultiMigStatus{
					AppliedAcceleratorTopologies: []*betacompute.MultiMigStatusAcceleratorTopology{
						{
							AcceleratorTopology:      "topo-a",
							AcceleratorTopologyState: "ACTIVE",
							AcceleratorTopologyStateLastCheck: &betacompute.MultiMigStatusAcceleratorTopologyAcceleratorTopologyStateLastCheck{
								Timestamp: "2025-10-09T20:54:45Z",
								Error: &betacompute.MultiMigStatusAcceleratorTopologyAcceleratorTopologyStateLastCheckError{
									Errors: []*betacompute.MultiMigStatusAcceleratorTopologyAcceleratorTopologyStateLastCheckErrorErrors{
										{Message: "gpu-1 link degraded"},
									},
								},
							},
						},
					},
				},
			},
			expectedStatus: cache.SuperSliceHealthStatus{
				MMIGName: "test-slice",
				TopologyHealths: []*cache.SuperSliceTopologyHealth{
					{
						AcceleratorTopology:    "topo-a",
						HealthStatus:           cache.MMIGHealthStatus("ACTIVE"),
						LastCheckErrorMessages: []string{"gpu-1 link degraded"},
						LastCheckTime:          "2025-10-09T20:54:45Z",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "Success with no last check info",
			mockMmig: &betacompute.MultiMig{
				Name: "test-slice",
				Status: &betacompute.MultiMigStatus{
					AppliedAcceleratorTopologies: []*betacompute.MultiMigStatusAcceleratorTopology{
						{
							AcceleratorTopology:               "topo-b",
							AcceleratorTopologyState:          "ACTIVATING",
							AcceleratorTopologyStateLastCheck: nil, // No check data available
						},
					},
				},
			},
			expectedStatus: cache.SuperSliceHealthStatus{
				MMIGName: "test-slice",
				TopologyHealths: []*cache.SuperSliceTopologyHealth{
					{
						AcceleratorTopology: "topo-b",
						HealthStatus:        cache.MMIGHealthStatus("ACTIVATING"),
						// LastCheckTime and LastCheckErrorMessages are zero values
					},
				},
			},
			expectedErr: false,
		},
		{
			name:        "GetMMIG returns a generic error",
			mockGetErr:  errors.New("get error"),
			expectedErr: true,
		},
		{
			name: "MMIG response is missing the Status field",
			mockMmig: &betacompute.MultiMig{
				Name:   "test-slice",
				Status: nil, // Status is nil
			},
			expectedErr: true,
		},
		{
			name: "MMIG Status is missing AppliedAcceleratorTopologies",
			mockMmig: &betacompute.MultiMig{
				Name: "test-slice",
				Status: &betacompute.MultiMigStatus{
					AppliedAcceleratorTopologies: []*betacompute.MultiMigStatusAcceleratorTopology{}, // Empty slice
				},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			// Assuming NewClient is how you instantiate your client
			client := NewClient(computeService, true, true)

			if tc.mockGetErr != nil {
				// Expect retries on API failure
				computeService.EXPECT().GetMMIG("test-project", "test-region", "test-slice").Return(nil, tc.mockGetErr).Times(util.GCERetryTimes + 1)
			} else {
				// Expect a single call for non-error cases
				computeService.EXPECT().GetMMIG("test-project", "test-region", "test-slice").Return(tc.mockMmig, nil).Times(1)
			}

			status, err := client.GetMMIGHealthStatus(context.Background(), "test-project", "test-region", "test-slice")

			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedStatus, status)
			}
		})
	}
}

func TestListMultiMigMembers(t *testing.T) {
	testCases := []struct {
		name           string
		setupMock      func(computeService *mocks.MockComputeServiceInterface)
		expectedResult []*betacompute.MultiMigMember
		expectedErr    bool
	}{
		{
			name: "Success with single page",
			setupMock: func(computeService *mocks.MockComputeServiceInterface) {
				resp := &betacompute.MultiMigMemberList{
					Items: []*betacompute.MultiMigMember{
						{Name: "mig-zone-a"},
						{Name: "mig-zone-b"},
					},
					NextPageToken: "", // No more pages
				}
				computeService.EXPECT().
					ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice", "").
					Return(resp, nil).Times(1)
			},
			expectedResult: []*betacompute.MultiMigMember{
				{Name: "mig-zone-a"},
				{Name: "mig-zone-b"},
			},
			expectedErr: false,
		},
		{
			name: "Success with pagination (multiple pages)",
			setupMock: func(computeService *mocks.MockComputeServiceInterface) {
				page1 := &betacompute.MultiMigMemberList{
					Items: []*betacompute.MultiMigMember{
						{Name: "mig-1"},
					},
					NextPageToken: "token-for-page-2",
				}
				page2 := &betacompute.MultiMigMemberList{
					Items: []*betacompute.MultiMigMember{
						{Name: "mig-2"},
					},
					NextPageToken: "", // End of pagination
				}

				// Expect Call 1: Initial request with empty token
				computeService.EXPECT().
					ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice", "").
					Return(page1, nil).Times(1)

				// Expect Call 2: Second request with the token returned from page 1
				computeService.EXPECT().
					ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice", "token-for-page-2").
					Return(page2, nil).Times(1)
			},
			expectedResult: []*betacompute.MultiMigMember{
				{Name: "mig-1"},
				{Name: "mig-2"},
			},
			expectedErr: false,
		},
		{
			name: "ListMultiMigMembers returns an error",
			setupMock: func(computeService *mocks.MockComputeServiceInterface) {
				// Expect retries on API failure, matching the util.RetryGCECall behavior
				computeService.EXPECT().
					ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice", "").
					Return(nil, errors.New("generic api error")).Times(util.GCERetryTimes + 1)
			},
			expectedResult: nil,
			expectedErr:    true,
		},
		{
			name: "Success with nil items (empty MMIG)",
			setupMock: func(computeService *mocks.MockComputeServiceInterface) {
				resp := &betacompute.MultiMigMemberList{
					Items:         nil,
					NextPageToken: "",
				}
				computeService.EXPECT().
					ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice", "").
					Return(resp, nil).Times(1)
			},
			expectedResult: nil,
			expectedErr:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)

			// Apply the specific mock expectations for this test case
			tc.setupMock(computeService)

			// Instantiate the client
			client := NewClient(computeService, true, true)

			// Execute the method under test
			members, err := client.ListMultiMigMembers(context.Background(), "test-project", "test-region", "test-slice")

			// Assertions
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResult, members)
			}
		})
	}
}

func TestConfigureAcceleratorTopology(t *testing.T) {
	testCases := []struct {
		name            string
		mockCreateErr   error
		mockOpErr       error
		expectedErr     bool
		expectedErrType interface{}
	}{
		{
			name:        "Success",
			expectedErr: false,
		},
		{
			name:            "ConfigureAcceleratorTopology returns error",
			mockCreateErr:   errors.New("configure error"),
			expectedErr:     true,
			expectedErrType: &util.APICallError{},
		},
		{
			name:            "waitForOperation returns error",
			mockOpErr:       errors.New("operation error"),
			expectedErr:     true,
			expectedErrType: &util.OperationError{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			computeService := mocks.NewMockComputeServiceInterface(mockCtrl)
			client := NewClient(computeService, true, true)

			op := &betacompute.Operation{Name: "test-op", Status: "DONE"}

			computeService.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(op, tc.mockCreateErr).Times(1)

			if tc.mockCreateErr == nil {
				if tc.mockOpErr != nil {
					computeService.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return((*betacompute.Operation)(nil), tc.mockOpErr).Times(util.GCERetryTimes + 1)
				} else {
					computeService.EXPECT().GetZonalOperation(gomock.Any(), gomock.Any(), gomock.Any(), "test-op").Return(op, nil).Times(1)
				}
			}

			req := &betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest{}
			_, err := client.ConfigureAcceleratorTopology(context.Background(), "test-project", "test-zone", "test-igm", req, "test-request-id")

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrType != nil {
					var apiCallErr *util.APICallError
					var opErr *util.OperationError
					switch tc.expectedErrType.(type) {
					case *util.APICallError:
						assert.True(t, errors.As(err, &apiCallErr), "expected error to contain APICallError")
					case *util.OperationError:
						assert.True(t, errors.As(err, &opErr), "expected error to contain OperationError")
					}
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
