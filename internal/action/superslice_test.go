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

package action

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/action/gcehelper"
	"slice-controller/internal/action/kubehelper"
	"slice-controller/internal/cache"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
	betacompute "google.golang.org/api/compute/v0.beta"
	"google.golang.org/api/googleapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewTriggerDeformationAction(t *testing.T) {
	action := NewTriggerDeformationAction(nil, "proj", "region", "hash", nil)
	steps := action.Steps()
	if len(steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(steps))
	}
	if steps[0].Name() != gcehelper.TriggerMMIGDeformationStepName {
		t.Errorf("Expected step %s, got %s", gcehelper.TriggerMMIGDeformationStepName, steps[0].Name())
	}
}

func TestNewCleanupGCEAction(t *testing.T) {
	action := NewCleanupGCEAction(nil, "proj", "region", "loc", "cluster", "hash", nil, nil)
	steps := action.Steps()
	expectedSteps := []string{
		gcehelper.DetachMIGsFromMMIGStepName,
		gcehelper.DeleteMMIGStepName,
		gcehelper.DeleteWorkloadPolicyStepName,
	}
	if len(steps) != len(expectedSteps) {
		t.Fatalf("Expected %d steps, got %d", len(expectedSteps), len(steps))
	}
	for i, step := range steps {
		if step.Name() != expectedSteps[i] {
			t.Errorf("Expected step %s, got %s", expectedSteps[i], step.Name())
		}
	}
}

func TestNewCreateGCEResourcesAction(t *testing.T) {
	action := NewCreateGCEResourcesAction(nil, "proj", "region", "loc", "cluster", "hash", nil, nil)
	steps := action.Steps()
	expectedSteps := []string{
		gcehelper.CreateWorkloadPolicyStepName,
		gcehelper.CreateMMIGStepName,
		gcehelper.AttachMIGsToMMIGStepName,
	}
	if len(steps) != len(expectedSteps) {
		t.Fatalf("Expected %d steps, got %d", len(expectedSteps), len(steps))
	}
	for i, step := range steps {
		if step.Name() != expectedSteps[i] {
			t.Errorf("Expected step %s, got %s", expectedSteps[i], step.Name())
		}
	}
}

func TestNewCreateWPAndMMIGAction(t *testing.T) {
	action := NewCreateWPAndMMIGAction(nil, "proj", "region", "hash")
	steps := action.Steps()
	expectedSteps := []string{
		gcehelper.CreateWorkloadPolicyStepName,
		gcehelper.CreateMMIGStepName,
	}
	if len(steps) != len(expectedSteps) {
		t.Fatalf("Expected %d steps, got %d", len(expectedSteps), len(steps))
	}
	for i, step := range steps {
		if step.Name() != expectedSteps[i] {
			t.Errorf("Expected step %s, got %s", expectedSteps[i], step.Name())
		}
	}
}

func TestNewAttachMIGsAction(t *testing.T) {
	action := NewAttachMIGsAction(nil, "proj", "region", "loc", "cluster", "hash", nil)
	steps := action.Steps()
	if len(steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(steps))
	}
	if steps[0].Name() != gcehelper.AttachMIGsToMMIGStepName {
		t.Errorf("Expected step %s, got %s", gcehelper.AttachMIGsToMMIGStepName, steps[0].Name())
	}
}

func TestNewCleanupMMIGAndUnlabelAction(t *testing.T) {
	action := NewCleanupMMIGAndUnlabelAction(nil, nil, nil, "proj", "region", "loc", "cluster", "hash", nil, util.NodeLabelKeys{}, false, "test-slice")
	steps := action.Steps()
	expectedSteps := []string{
		"CleanupMMIG",
		kubehelper.UnlabelNodeGroupStepName,
	}
	if len(steps) != len(expectedSteps) {
		t.Fatalf("Expected %d steps, got %d", len(expectedSteps), len(steps))
	}
	for i, step := range steps {
		if step.Name() != expectedSteps[i] {
			t.Errorf("Expected step %s, got %s", expectedSteps[i], step.Name())
		}
	}
}

func TestCleanupMMIGStep_Do(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockComputeService := mocks.NewMockComputeServiceInterface(mockCtrl)
	mockGCEClient := mocks.NewMockGCEClient(mockCtrl)
	mockRelationalCache := mocks.NewMockSliceRelationalCache(mockCtrl)

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
			UID:  "test-uid",
		},
		Spec: slicev1beta1.SliceSpec{
			PartitionIds: []string{"test-partition"},
		},
	}

	step := &CleanupMMIGStep{
		Client:          mockGCEClient,
		ComputeService:  mockComputeService,
		ProjectID:       "test-project",
		Region:          "test-region",
		Location:        "test-loc",
		ClusterName:     "test-cluster",
		ClusterHash:     "test-hash",
		RelationalCache: mockRelationalCache,
	}

	ctx := context.Background()
	mmigName := util.GetResourceName(slice.Name, step.ClusterHash)

	t.Run("MMIG exists", func(t *testing.T) {
		mockComputeService.EXPECT().GetMMIG(step.ProjectID, step.Region, mmigName).Return(&betacompute.MultiMig{}, nil).Times(1)

		// The inner steps like TriggerMMIGDeformationStep will fail because we aren't fully
		// mocking the relational cache. We just want to prove it proceeded past GetMMIG.
		mockRelationalCache.EXPECT().GetMIGByPartitionID("test-partition").Return(cache.MIGInfo{}, false).AnyTimes()
		mockGCEClient.EXPECT().ListMultiMigMembers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		mockGCEClient.EXPECT().DeleteMMIG(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		mockRelationalCache.EXPECT().DeleteSlice(gomock.Any()).Return(nil).AnyTimes()

		err := step.Do(ctx, slice)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("MMIG does not exist", func(t *testing.T) {
		mockComputeService.EXPECT().GetMMIG(step.ProjectID, step.Region, mmigName).Return(nil, &googleapi.Error{Code: 404}).Times(1)
		err := step.Do(ctx, slice)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})
}
