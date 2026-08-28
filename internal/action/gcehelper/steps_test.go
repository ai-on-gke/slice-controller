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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/cache"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
	betacompute "google.golang.org/api/compute/v0.beta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

func TestCreateWorkloadPolicyStep(t *testing.T) {
	testCases := []struct {
		name        string
		mockErr     error
		expectedErr bool
	}{
		{"Success", nil, false},
		{"Failure", errors.New("mock error"), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockGCE := mocks.NewMockGCEClient(mockCtrl)
			slice := &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: "test-slice"}}
			step := &CreateWorkloadPolicyStep{Client: mockGCE, ProjectID: "test-project", Region: "test-region", ClusterHash: "hash"}
			mockGCE.EXPECT().CreateWorkloadPolicy(gomock.Any(), "test-project", "test-region", slice, "hash").Return(tc.mockErr).Times(1)
			err := step.Do(context.Background(), slice)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeleteWorkloadPolicyStep(t *testing.T) {
	testCases := []struct {
		name        string
		mockErr     error
		expectedErr bool
	}{
		{"Success", nil, false},
		{"Failure", errors.New("mock error"), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockGCE := mocks.NewMockGCEClient(mockCtrl)
			slice := &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: "test-slice"}}
			step := &DeleteWorkloadPolicyStep{Client: mockGCE, ProjectID: "test-project", Region: "test-region", ClusterHash: "hash"}
			mockGCE.EXPECT().DeleteWorkloadPolicy(gomock.Any(), "test-project", "test-region", "test-slice", "hash").Return(tc.mockErr).Times(1)
			err := step.Do(context.Background(), slice)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateMMIGStep(t *testing.T) {
	testCases := []struct {
		name        string
		mockErr     error
		expectedErr bool
	}{
		{"Success", nil, false},
		{"Failure", errors.New("mock error"), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockGCE := mocks.NewMockGCEClient(mockCtrl)
			slice := &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: "test-slice"}}
			step := &CreateMMIGStep{Client: mockGCE, ProjectID: "test-project", Region: "test-region", ClusterHash: "hash"}
			mockGCE.EXPECT().CreateMMIG(gomock.Any(), "test-project", "test-region", slice, "hash").Return(tc.mockErr).Times(1)
			err := step.Do(context.Background(), slice)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeleteMMIGStep(t *testing.T) {
	testCases := []struct {
		name          string
		mockDeleteErr error
		mockCacheErr  error
		expectedErr   bool
	}{
		{"Success", nil, nil, false},
		{"DeleteMMIG Failure", errors.New("mock error"), nil, true},
		{"Cache Delete Failure (should not fail step)", nil, errors.New("mock cache error"), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockGCE := mocks.NewMockGCEClient(mockCtrl)
			mockCache := mocks.NewMockSliceRelationalCache(mockCtrl)
			slice := &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: "test-slice"}}
			step := &DeleteMMIGStep{Client: mockGCE, ProjectID: "test-project", Region: "test-region", ClusterHash: "hash", RelationalCache: mockCache}

			mockGCE.EXPECT().DeleteMMIG(gomock.Any(), "test-project", "test-region", "test-slice", "hash").Return(tc.mockDeleteErr).Times(1)
			if tc.mockDeleteErr == nil {
				mockCache.EXPECT().DeleteSlice("test-slice").Return(tc.mockCacheErr).Times(1)
			}

			err := step.Do(context.Background(), slice)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAttachMIGsToMMIGStep(t *testing.T) {
	testCases := []struct {
		name        string
		mockErr     error
		expectedErr bool
	}{
		{"Success", nil, false},
		{"Failure", errors.New("mock error"), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockGCE := mocks.NewMockGCEClient(mockCtrl)
			mockCache := mocks.NewMockSliceRelationalCache(mockCtrl)
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid", Generation: 1},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p-1"}},
				Status:     slicev1beta1.SliceStatus{ObservedGeneration: 1},
			}
			step := &AttachMIGsToMMIGStep{Client: mockGCE, ProjectID: "test-project", Region: "test-region", ClusterHash: "hash", RelationalCache: mockCache}

			migInfo := cache.MIGInfo{Name: "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"}
			mockCache.EXPECT().GetMIGByPartitionID("p-1").Return(migInfo, true).Times(1)

			cubes := []util.CubeInfo{{MigName: "test-mig", Zone: "test-zone", PartitionId: "p-1"}}
			mockGCE.EXPECT().AttachMIGsToMMIG(gomock.Any(), "test-project", "test-region", "test-slice", cubes, "hash", slice.UID, slice.Status.ObservedGeneration, gomock.Any()).Return(tc.mockErr).Times(1)

			if tc.mockErr != nil {
				mockGCE.EXPECT().ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice-hash").Return(nil, errors.New("list error")).Times(1)
			}

			err := step.Do(context.Background(), slice)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTriggerMMIGDeformationStep(t *testing.T) {
	testCases := []struct {
		name        string
		mockErr     error
		expectedErr bool
	}{
		{"Success", nil, false},
		{"Failure", errors.New("mock error"), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockGCE := mocks.NewMockGCEClient(mockCtrl)
			mockCache := mocks.NewMockSliceRelationalCache(mockCtrl)
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid", Generation: 1},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p-1"}},
				Status:     slicev1beta1.SliceStatus{ObservedGeneration: 1},
			}
			step := &TriggerMMIGDeformationStep{Client: mockGCE, ProjectID: "test-project", Region: "test-region", ClusterHash: "hash", RelationalCache: mockCache}

			migInfo := cache.MIGInfo{Name: "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"}
			mockCache.EXPECT().GetMIGByPartitionID("p-1").Return(migInfo, true).Times(1)

			members := []*betacompute.MultiMigMember{{Status: &betacompute.MultiMigMemberStatus{InstanceGroupManager: "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"}}}
			mockGCE.EXPECT().ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice-hash").Return(members, nil).Times(1)

			mockGCE.EXPECT().DetachPivotMIGFromMMIG(gomock.Any(), "test-project", "test-region", gomock.Any(), slice.UID, slice.Status.ObservedGeneration).Return("test-op", tc.mockErr).Times(1)

			if tc.mockErr != nil {
				mockGCE.EXPECT().ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice-hash").Return(nil, errors.New("list error")).Times(1)
			}

			err := step.Do(context.Background(), slice)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDetachMIGsFromMMIGStep(t *testing.T) {
	testCases := []struct {
		name        string
		mockErr     error
		expectedErr bool
	}{
		{"Success", nil, false},
		{"Failure", errors.New("mock error"), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockGCE := mocks.NewMockGCEClient(mockCtrl)
			mockCache := mocks.NewMockSliceRelationalCache(mockCtrl)
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "test-slice", UID: "test-uid", Generation: 1},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p-1"}},
				Status:     slicev1beta1.SliceStatus{ObservedGeneration: 1},
			}
			step := &DetachMIGsFromMMIGStep{Client: mockGCE, ProjectID: "test-project", Region: "test-region", ClusterHash: "hash", RelationalCache: mockCache}

			migInfo := cache.MIGInfo{Name: "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"}
			mockCache.EXPECT().GetMIGByPartitionID("p-1").Return(migInfo, true).Times(1)

			members := []*betacompute.MultiMigMember{{Status: &betacompute.MultiMigMemberStatus{InstanceGroupManager: "projects/test-project/zones/test-zone/instanceGroupManagers/test-mig"}}}
			mockGCE.EXPECT().ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice-hash").Return(members, nil).Times(1)

			cubes := []util.CubeInfo{{MigName: "test-mig", Zone: "test-zone", PartitionId: "p-1"}}
			mockGCE.EXPECT().DetachMIGsFromMMIG(gomock.Any(), "test-project", "test-region", "test-slice", cubes, "hash", slice.UID, slice.Status.ObservedGeneration).Return(tc.mockErr).Times(1)

			if tc.mockErr != nil {
				mockGCE.EXPECT().ListMultiMigMembers(gomock.Any(), "test-project", "test-region", "test-slice-hash").Return(nil, errors.New("list error")).Times(1)
			}

			err := step.Do(context.Background(), slice)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSyncCubesWithMMIGMembers(t *testing.T) {
	logger := klog.Background()

	members := []*betacompute.MultiMigMember{
		{Status: &betacompute.MultiMigMemberStatus{InstanceGroupManager: "projects/p/zones/z1/instanceGroupManagers/mig1"}},
	}
	cachedCubes := []util.CubeInfo{}

	// Case 1: Spec is empty
	truthCubes := syncCubesWithMMIGMembers(members, cachedCubes, true, logger)
	assert.Len(t, truthCubes, 1)
	assert.Equal(t, "mig1", truthCubes[0].MigName)

	// Case 2: Spec is not empty
	truthCubes = syncCubesWithMMIGMembers(members, cachedCubes, false, logger)
	assert.Len(t, truthCubes, 1)
	assert.Equal(t, "mig1", truthCubes[0].MigName)
}
