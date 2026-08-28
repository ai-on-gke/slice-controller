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

//nolint:goconst
package action

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/cache"
	"slice-controller/internal/mocks"
	mock_client "slice-controller/internal/mocks/client"
	"slice-controller/internal/util"
	betacompute "google.golang.org/api/compute/v0.beta"
)

const (
	testProject     = "test-project"
	testRegion      = "test-region"
	testPartitionID = "locus-0"
	testTopology    = "2x2x1"
	testMigURL      = "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"
	testMigName     = "test-mig"
	testZone        = "test-zone"
	testClusterHash = "abc123d4"
)

func TestFormSubsliceStep_Do(t *testing.T) {
	ctx := context.Background()
	project := testProject
	region := testRegion
	partitionId := testPartitionID
	topology := testTopology
	migURL := testMigURL
	migName := testMigName
	zone := testZone
	clusterHash := testClusterHash
	expectedSliceID := "test-slice-abc123d4"

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid"},
		Spec: slicev1beta1.SliceSpec{
			Topology:     topology,
			PartitionIds: []string{partitionId},
		},
	}

	tests := []struct {
		name          string
		mockSetup     func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache)
		expectedError string
	}{
		{
			name: "Success - Activate Sub-slice",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
				mg.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
						config, ok := req.AcceleratorTopologyConfigurations[partitionId]
						if !ok || config.Action != actionActivate || config.ExternalId != expectedSliceID {
							return nil, fmt.Errorf("unexpected request configuration")
						}
						return nil, nil
					})
			},
		},
		{
			name: "Error - No Nodes",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{}, false)
			},
			expectedError: "no MIG found for partitionId locus-0",
		},
		{
			name: "Error - Operation Failed",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
				mg.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
					Return(nil, &util.OperationError{
						Err: &util.GCEOperationFailureError{
							OperationName: "test-op",
							Details: []util.GCEErrorDetail{
								{Code: "CONDITION_NOT_MET", Message: "Already attached to Multi-MIG"},
							},
						},
					}).Times(util.GCERetryTimes + 1)
			},
			expectedError: "failed to configure accelerator topologies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockGCE := mocks.NewMockGCEClient(ctrl)
			mockCache := mocks.NewMockSliceRelationalCache(ctrl)
			tt.mockSetup(mockGCE, mockCache)

			step := &FormSubsliceStep{
				gceClient:   mockGCE,
				rc:          mockCache,
				project:     project,
				region:      region,
				clusterHash: clusterHash,
			}

			err := step.Do(ctx, slice)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeformSubsliceStep_Do(t *testing.T) {
	ctx := context.Background()
	project := testProject
	region := testRegion
	partitionId := testPartitionID
	topology := testTopology
	migURL := testMigURL
	migName := testMigName
	zone := testZone
	clusterHash := testClusterHash
	expectedSliceID := "test-slice-abc123d4"

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid"},
		Spec: slicev1beta1.SliceSpec{
			Topology:     topology,
			PartitionIds: []string{partitionId},
		},
	}

	tests := []struct {
		name          string
		mockSetup     func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache)
		expectedError string
	}{
		{
			name: "Success - Deactivate Sub-slice",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
				mg.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
						config, ok := req.AcceleratorTopologyConfigurations[partitionId]
						if !ok || config.Action != actionDeactivate || config.ExternalId != expectedSliceID {
							return nil, fmt.Errorf("unexpected request configuration")
						}
						return nil, nil
					})
			},
		},
		{
			name: "Success - No Nodes Found",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{}, false)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockGCE := mocks.NewMockGCEClient(ctrl)
			mockCache := mocks.NewMockSliceRelationalCache(ctrl)
			tt.mockSetup(mockGCE, mockCache)

			step := &DeformSubsliceStep{
				gceClient:   mockGCE,
				rc:          mockCache,
				project:     project,
				region:      region,
				clusterHash: clusterHash,
				partitionId: partitionId,
			}

			err := step.Do(ctx, slice)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFormSubsliceAction(t *testing.T) {
	action := FormSubsliceAction(nil, nil, "proj", "region", "hash")
	steps := action.Steps()
	if len(steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(steps))
	}
	if steps[0].Name() != "FormSubsliceStep" {
		t.Errorf("Expected step Name FormSubsliceStep, got %s", steps[0].Name())
	}
}

func TestDeformSubsliceAction(t *testing.T) {
	action := DeformSubsliceAction(nil, nil, "proj", "region", "hash", "test-partition")
	steps := action.Steps()
	if len(steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(steps))
	}
	if steps[0].Name() != "DeformSubsliceStep" {
		t.Errorf("Expected step Name DeformSubsliceStep, got %s", steps[0].Name())
	}
}

// --- CL 1944683: Update workflow tests ---

func TestUpdateFormSubsliceStep_Do(t *testing.T) {
	ctx := context.Background()
	project := "test-project"
	partitionId := "locus-0"
	topology := "2x2x1"
	migURL := "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"
	migName := "test-mig"
	zone := "test-zone"
	clusterHash := "abc123d4"
	expectedSliceID := "test-slice-abc123d4"

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid"},
		Spec: slicev1beta1.SliceSpec{
			Topology:     topology,
			PartitionIds: []string{"new-locus-1"}, // Spec has new partition, but we are updating old/new explicitly
		},
	}

	tests := []struct {
		name          string
		mockSetup     func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache, mkc *mock_client.MockClient)
		expectedError string
	}{
		{
			name: "Success - Update Activate Sub-slice",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache, mkc *mock_client.MockClient) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
				mg.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
						config, ok := req.AcceleratorTopologyConfigurations[partitionId]
						if !ok || config.Action != actionActivate || config.ExternalId != expectedSliceID {
							return nil, fmt.Errorf("unexpected request configuration")
						}
						return &betacompute.Operation{Name: "op-123", Status: "PENDING"}, nil
					})

				// Expect patching of the operation ID
				mkc.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
		},
		{
			name: "Failure - MIG Not Found",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache, mkc *mock_client.MockClient) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{}, false)
			},
			expectedError: "no MIG found for partitionId locus-0",
		},
		{
			name: "Failure - Configure API Error",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache, mkc *mock_client.MockClient) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
				mg.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
					Return(nil, fmt.Errorf("api error")).AnyTimes()
			},
			expectedError: "api error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockGCEClient := mocks.NewMockGCEClient(ctrl)
			mockRelationalCache := mocks.NewMockSliceRelationalCache(ctrl)
			mockKubeClient := mock_client.NewMockClient(ctrl)

			if tc.mockSetup != nil {
				tc.mockSetup(mockGCEClient, mockRelationalCache, mockKubeClient)
			}

			step := &UpdateFormSubsliceStep{
				gceClient:   mockGCEClient,
				kubeClient:  mockKubeClient,
				rc:          mockRelationalCache,
				project:     project,
				clusterHash: clusterHash,
				partitionId: partitionId,
			}

			err := step.Do(ctx, slice)

			if tc.expectedError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectedError) {
					t.Errorf("expected error containing %q, got %v", tc.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestUpdateDeformSubsliceStep_Do(t *testing.T) {
	ctx := context.Background()
	project := "test-project"
	partitionId := "locus-0"
	topology := "2x2x1"
	migURL := "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"
	migName := "test-mig"
	zone := "test-zone"
	clusterHash := "abc123d4"
	expectedSliceID := "test-slice-abc123d4"

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid"},
		Spec: slicev1beta1.SliceSpec{
			Topology: topology,
		},
	}

	tests := []struct {
		name          string
		mockSetup     func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache, mkc *mock_client.MockClient)
		expectedError string
	}{
		{
			name: "Success - Update Deactivate Sub-slice",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache, mkc *mock_client.MockClient) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
				mg.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
						config, ok := req.AcceleratorTopologyConfigurations[partitionId]
						if !ok || config.Action != actionDeactivate || config.ExternalId != expectedSliceID {
							return nil, fmt.Errorf("unexpected request configuration")
						}
						return &betacompute.Operation{Name: "op-456", Status: "PENDING"}, nil
					})
				mkc.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
		},
		{
			name: "Success - MIG Not Found Deactivate Skips",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache, mkc *mock_client.MockClient) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{}, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockGCEClient := mocks.NewMockGCEClient(ctrl)
			mockRelationalCache := mocks.NewMockSliceRelationalCache(ctrl)
			mockKubeClient := mock_client.NewMockClient(ctrl)

			if tc.mockSetup != nil {
				tc.mockSetup(mockGCEClient, mockRelationalCache, mockKubeClient)
			}

			step := &UpdateDeformSubsliceStep{
				gceClient:   mockGCEClient,
				kubeClient:  mockKubeClient,
				rc:          mockRelationalCache,
				project:     project,
				clusterHash: clusterHash,
				partitionId: partitionId,
			}

			err := step.Do(ctx, slice)

			if tc.expectedError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectedError) {
					t.Errorf("expected error containing %q, got %v", tc.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestUpdateFormSubsliceAction(t *testing.T) {
	action := UpdateFormSubsliceAction(nil, nil, nil, "project", "hash", "part1")
	if action == nil || len(action.Steps()) != 1 {
		t.Fatalf("expected action with 1 step")
	}
	if action.Steps()[0].Name() != "UpdateFormSubsliceStep" {
		t.Errorf("unexpected step name: %s", action.Steps()[0].Name())
	}
}

func TestUpdateDeformSubsliceAction(t *testing.T) {
	action := UpdateDeformSubsliceAction(nil, nil, nil, "project", "hash", "part1")
	if action == nil || len(action.Steps()) != 1 {
		t.Fatalf("expected action with 1 step")
	}
	if action.Steps()[0].Name() != "UpdateDeformSubsliceStep" {
		t.Errorf("unexpected step name: %s", action.Steps()[0].Name())
	}
}

// --- CL 1955659: Annotation tracking tests for deletion zombie prevention ---

// TestFormSubsliceStep_ReturnsOperationOnSuccess verifies that when
// ConfigureAcceleratorTopology succeeds and returns an operation, the step
// stores the operation name in SubsliceLastOpAnnotation in-memory.
func TestFormSubsliceStep_ReturnsOperationOnSuccess(t *testing.T) {
	ctx := context.Background()
	project := testProject
	region := testRegion
	partitionId := testPartitionID
	topology := testTopology
	migURL := testMigURL
	migName := testMigName
	zone := testZone
	clusterHash := testClusterHash

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid"},
		Spec: slicev1beta1.SliceSpec{
			Topology:     topology,
			PartitionIds: []string{partitionId},
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCE := mocks.NewMockGCEClient(ctrl)
	mockCache := mocks.NewMockSliceRelationalCache(ctrl)

	mockCache.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
	mockGCE.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
		Return(&betacompute.Operation{Name: "op-activate-123", Status: "RUNNING"}, nil)

	step := &FormSubsliceStep{
		gceClient:   mockGCE,
		rc:          mockCache,
		project:     project,
		region:      region,
		clusterHash: clusterHash,
	}

	err := step.Do(ctx, slice)
	assert.NoError(t, err)

	// Verify the SubsliceLastOpAnnotation was set in-memory.
	assert.Equal(t, "op-activate-123", slice.Annotations[util.SubsliceLastOpAnnotation])
}

// TestFormSubsliceStep_OperationWithSyncErrors verifies that if the operation
// returned by ConfigureAcceleratorTopology has errors in the Error field,
// the step returns an error. The annotation is still set because the op was dispatched.
func TestFormSubsliceStep_OperationWithSyncErrors(t *testing.T) {
	ctx := context.Background()
	project := testProject
	region := testRegion
	partitionId := testPartitionID
	migURL := testMigURL
	migName := testMigName
	zone := testZone
	clusterHash := testClusterHash

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid"},
		Spec: slicev1beta1.SliceSpec{
			Topology:     "2x2x1",
			PartitionIds: []string{partitionId},
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCE := mocks.NewMockGCEClient(ctrl)
	mockCache := mocks.NewMockSliceRelationalCache(ctrl)

	// The operation is returned with synchronous errors in its Error field.
	opWithErrors := &betacompute.Operation{
		Name:   "op-fail",
		Status: "DONE",
		Error: &betacompute.OperationError{
			Errors: []*betacompute.OperationErrorErrors{
				{Code: "RESOURCE_ALREADY_EXISTS", Message: "already active"},
			},
		},
	}

	mockCache.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
	mockGCE.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
		Return(opWithErrors, nil)

	step := &FormSubsliceStep{
		gceClient:   mockGCE,
		rc:          mockCache,
		project:     project,
		region:      region,
		clusterHash: clusterHash,
	}

	err := step.Do(ctx, slice)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "configure accelerator topologies operation failed")

	// The annotation is still set because the op was dispatched before the error was detected.
	assert.Equal(t, "op-fail", slice.Annotations[util.SubsliceLastOpAnnotation])
}

// TestDeformSubsliceStep_DoesNotSetAnnotation verifies that the deform step
// calls DEACTIVATE but does NOT set the SubsliceLastOpAnnotation on the
// slice. Annotation tracking is only relevant for ACTIVATE operations.
func TestDeformSubsliceStep_DoesNotSetAnnotation(t *testing.T) {
	ctx := context.Background()
	project := testProject
	region := testRegion
	partitionId := testPartitionID
	migURL := testMigURL
	migName := testMigName
	zone := testZone
	clusterHash := testClusterHash

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
			UID:  "test-uid",
			Annotations: map[string]string{
				"existing-annotation": "preserve-me",
			},
		},
		Spec: slicev1beta1.SliceSpec{
			Topology:     "2x2x1",
			PartitionIds: []string{partitionId},
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCE := mocks.NewMockGCEClient(ctrl)
	mockCache := mocks.NewMockSliceRelationalCache(ctrl)

	mockCache.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
	mockGCE.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
			config := req.AcceleratorTopologyConfigurations[partitionId]
			assert.Equal(t, "DEACTIVATE", config.Action)
			return &betacompute.Operation{Name: "op-deactivate-456"}, nil
		})

	step := &DeformSubsliceStep{
		gceClient:   mockGCE,
		rc:          mockCache,
		project:     project,
		region:      region,
		clusterHash: clusterHash,
		partitionId: partitionId,
	}

	err := step.Do(ctx, slice)
	assert.NoError(t, err)

	// Verify no SubsliceLastOpAnnotation was set.
	_, hasOpAnnotation := slice.Annotations[util.SubsliceLastOpAnnotation]
	assert.False(t, hasOpAnnotation, "deform should not set operation annotation")

	// Existing annotations should remain.
	// Existing annotations should remain.
	assert.Equal(t, "preserve-me", slice.Annotations["existing-annotation"])
}

// TestDeformSubsliceStep_PreemptedSlice verifies that deactivation works when
// the scheduler has preempted a slice by clearing Spec.PartitionIds.
// The effective partition comes from the provisioned-partition annotation.
func TestDeformSubsliceStep_PreemptedSlice(t *testing.T) {
	ctx := context.Background()
	project := testProject
	partitionId := testPartitionID
	topology := testTopology
	migURL := testMigURL
	migName := testMigName
	zone := testZone
	clusterHash := testClusterHash
	expectedSliceID := "test-slice-abc123d4"

	// Slice with EMPTY PartitionIds (scheduler cleared them during preemption)
	// but the provisioned-partition annotation is still set from the original activation.
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
			UID:  "test-uid",
			Annotations: map[string]string{
				util.SubsliceProvisionedPartitionAnnotation: partitionId,
			},
		},
		Spec: slicev1beta1.SliceSpec{
			Topology:     topology,
			PartitionIds: []string{}, // Empty - scheduler preempted!
		},
	}

	tests := []struct {
		name          string
		mockSetup     func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache)
		expectedError string
	}{
		{
			name: "Success - DEACTIVATE using annotation partition after preemption",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{Name: migURL}, true)
				mg.EXPECT().ConfigureAcceleratorTopology(gomock.Any(), project, zone, migName, gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, project, zone, igm string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest, requestId string) (*betacompute.Operation, error) {
						config, ok := req.AcceleratorTopologyConfigurations[partitionId]
						if !ok {
							return nil, fmt.Errorf("expected partition %s in request", partitionId)
						}
						if config.Action != actionDeactivate {
							return nil, fmt.Errorf("expected DEACTIVATE action, got %s", config.Action)
						}
						if config.ExternalId != expectedSliceID {
							return nil, fmt.Errorf("expected externalId %s, got %s", expectedSliceID, config.ExternalId)
						}
						return nil, nil
					})
			},
		},
		{
			name: "Success - No MIG during preempted deactivation (cleanup already done)",
			mockSetup: func(mg *mocks.MockGCEClient, mc *mocks.MockSliceRelationalCache) {
				mc.EXPECT().GetMIGByPartitionID(partitionId).Return(cache.MIGInfo{}, false)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockGCE := mocks.NewMockGCEClient(ctrl)
			mockCache := mocks.NewMockSliceRelationalCache(ctrl)
			tt.mockSetup(mockGCE, mockCache)

			step := &DeformSubsliceStep{
				gceClient:   mockGCE,
				rc:          mockCache,
				project:     project,
				region:      testRegion,
				clusterHash: clusterHash,
				partitionId: partitionId, // From getEffectivePartition (annotation)
			}

			err := step.Do(ctx, slice)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDeformSubsliceStep_EmptyPartitionNoAnnotation verifies the error case
// when both Spec.PartitionIds and explicit partition are empty.
func TestDeformSubsliceStep_EmptyPartitionNoAnnotation(t *testing.T) {
	ctx := context.Background()

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan-slice", UID: "test-uid"},
		Spec: slicev1beta1.SliceSpec{
			Topology:     "2x2x1",
			PartitionIds: []string{}, // Empty
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCE := mocks.NewMockGCEClient(ctrl)
	mockCache := mocks.NewMockSliceRelationalCache(ctrl)

	step := &DeformSubsliceStep{
		gceClient:   mockGCE,
		rc:          mockCache,
		project:     testProject,
		region:      testRegion,
		clusterHash: testClusterHash,
		partitionId: "", // No effective partition either
	}

	err := step.Do(ctx, slice)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no partition ID available for DEACTIVATE")
}
