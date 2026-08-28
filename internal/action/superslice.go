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
	"fmt"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/action/gcehelper"
	kubehelper "slice-controller/internal/action/kubehelper"
	"slice-controller/internal/cache"
	"slice-controller/internal/util"
	"slice-controller/services"
	betacompute "google.golang.org/api/compute/v0.beta"
	"k8s.io/klog/v2"
)

// NewTriggerDeformationAction creates an action to trigger MMIG deformation.
func NewTriggerDeformationAction(
	client gcehelper.GCEClient,
	projectID, region, clusterHash string,
	relationalCache cache.SliceRelationalCache,
) util.Action {
	return &StepActions{
		steps: []util.Step{
			&gcehelper.TriggerMMIGDeformationStep{
				Client:          client,
				ProjectID:       projectID,
				Region:          region,
				ClusterHash:     clusterHash,
				RelationalCache: relationalCache,
			},
		},
	}
}

// NewCleanupGCEAction creates an action to clean up GCE resources (Detach, Delete MMIG, Delete WP).
func NewCleanupGCEAction(
	client gcehelper.GCEClient,
	projectID, region, location, clusterName, clusterHash string,
	relationalCache cache.SliceRelationalCache,
	kubeClient kubehelper.Client,
) util.Action {
	return &StepActions{
		steps: []util.Step{
			&gcehelper.DetachMIGsFromMMIGStep{
				Client:          client,
				ProjectID:       projectID,
				Region:          region,
				Location:        location,
				ClusterName:     clusterName,
				ClusterHash:     clusterHash,
				RelationalCache: relationalCache,
			},
			&gcehelper.DeleteMMIGStep{
				Client:          client,
				ProjectID:       projectID,
				Region:          region,
				ClusterHash:     clusterHash,
				RelationalCache: relationalCache,
			},
			&gcehelper.DeleteWorkloadPolicyStep{
				Client:      client,
				ProjectID:   projectID,
				Region:      region,
				ClusterHash: clusterHash,
			},
		},
	}
}

// NewCreateGCEResourcesAction creates an action to create GCE resources (Create WP, Create MMIG, Attach MIGs).
func NewCreateGCEResourcesAction(
	client gcehelper.GCEClient,
	projectID, region, location, clusterName, clusterHash string,
	relationalCache cache.SliceRelationalCache,
	kubeClient kubehelper.Client,
) util.Action {
	return &StepActions{
		steps: []util.Step{
			&gcehelper.CreateWorkloadPolicyStep{
				Client:      client,
				ProjectID:   projectID,
				Region:      region,
				ClusterHash: clusterHash,
			},
			&gcehelper.CreateMMIGStep{
				Client:      client,
				ProjectID:   projectID,
				Region:      region,
				ClusterHash: clusterHash,
			},
			&gcehelper.AttachMIGsToMMIGStep{
				Client:          client,
				ProjectID:       projectID,
				Region:          region,
				Location:        location,
				ClusterName:     clusterName,
				ClusterHash:     clusterHash,
				RelationalCache: relationalCache,
			},
		},
	}
}

// CleanupMMIGStep checks if the MMIG exists before executing deformation and deletion steps.
type CleanupMMIGStep struct {
	Client          gcehelper.GCEClient
	ComputeService  services.ComputeServiceInterface
	ProjectID       string
	Region          string
	Location        string
	ClusterName     string
	ClusterHash     string
	RelationalCache cache.SliceRelationalCache
}

func (s *CleanupMMIGStep) Name() string {
	return "CleanupMMIG"
}

func (s *CleanupMMIGStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name)
	mmigName := util.GetResourceName(slice.Name, s.ClusterHash)

	apiCall := func() (*betacompute.MultiMig, error) {
		return s.ComputeService.GetMMIG(s.ProjectID, s.Region, mmigName)
	}
	mmig, err := util.RetryGCECall(ctx, apiCall, util.ResourceNotFound)
	if err != nil {
		return fmt.Errorf("failed to check MMIG existence: %w", err)
	}
	if mmig == nil {
		logger.Info("MMIG does not exist, skipping GCE deformation and deletion steps", "MMIGName", mmigName)
		return nil
	}

	logger.Info("MMIG exists, proceeding with deformation and deletion", "MMIGName", mmigName)

	triggerStep := &gcehelper.TriggerMMIGDeformationStep{
		Client:          s.Client,
		ProjectID:       s.ProjectID,
		Region:          s.Region,
		Location:        s.Location,
		ClusterName:     s.ClusterName,
		ClusterHash:     s.ClusterHash,
		RelationalCache: s.RelationalCache,
	}
	if err := triggerStep.Do(ctx, slice); err != nil && err != util.ErrDeformationSkipped {
		return err
	}

	detachStep := &gcehelper.DetachMIGsFromMMIGStep{
		Client:          s.Client,
		ProjectID:       s.ProjectID,
		Region:          s.Region,
		Location:        s.Location,
		ClusterName:     s.ClusterName,
		ClusterHash:     s.ClusterHash,
		RelationalCache: s.RelationalCache,
	}
	if err := detachStep.Do(ctx, slice); err != nil {
		return err
	}

	deleteStep := &gcehelper.DeleteMMIGStep{
		Client:          s.Client,
		ProjectID:       s.ProjectID,
		Region:          s.Region,
		ClusterHash:     s.ClusterHash,
		RelationalCache: s.RelationalCache,
	}
	return deleteStep.Do(ctx, slice)
}

// NewCleanupMMIGAndUnlabelAction creates an action to detach and delete MMIG, and unlabel nodes.
func NewCleanupMMIGAndUnlabelAction(
	client gcehelper.GCEClient,
	kubeClient kubehelper.Client,
	computeService services.ComputeServiceInterface,
	projectID, region, location, clusterName, clusterHash string,
	relationalCache cache.SliceRelationalCache,
	labelKeys util.NodeLabelKeys,
	enableHighConcurrency bool,
	sliceName string,
) util.Action {
	var partitions []string
	found := false
	if relationalCache != nil {
		var sliceInfo cache.SliceInfo
		sliceInfo, found = relationalCache.GetSliceInfo(sliceName)
		if found {
			partitions = sliceInfo.PartitionIDs
		}
	}
	klog.Infof("NewCleanupMMIGAndUnlabelAction: looked up partitions for slice %s in cache: found=%v, partitions=%v", sliceName, found, partitions)

	return &StepActions{
		steps: []util.Step{
			&CleanupMMIGStep{
				Client:          client,
				ComputeService:  computeService,
				ProjectID:       projectID,
				Region:          region,
				Location:        location,
				ClusterName:     clusterName,
				ClusterHash:     clusterHash,
				RelationalCache: relationalCache,
			},
			&kubehelper.UnlabelNodeGroupStep{
				Client:                kubeClient,
				EnableHighConcurrency: enableHighConcurrency,
				LabelKeys:             labelKeys,
				PartitionIDs:          partitions,
			},
		},
	}
}

// NewCreateWPAndMMIGAction creates an action to create GCE resources (Create WP, Create MMIG).
func NewCreateWPAndMMIGAction(
	client gcehelper.GCEClient,
	projectID, region, clusterHash string,
) util.Action {
	return &StepActions{
		steps: []util.Step{
			&gcehelper.CreateWorkloadPolicyStep{
				Client:      client,
				ProjectID:   projectID,
				Region:      region,
				ClusterHash: clusterHash,
			},
			&gcehelper.CreateMMIGStep{
				Client:      client,
				ProjectID:   projectID,
				Region:      region,
				ClusterHash: clusterHash,
			},
		},
	}
}

// NewAttachMIGsAction creates an action to attach MIGs to an existing MMIG.
func NewAttachMIGsAction(
	client gcehelper.GCEClient,
	projectID, region, location, clusterName, clusterHash string,
	relationalCache cache.SliceRelationalCache,
) util.Action {
	return &StepActions{
		steps: []util.Step{
			&gcehelper.AttachMIGsToMMIGStep{
				Client:          client,
				ProjectID:       projectID,
				Region:          region,
				Location:        location,
				ClusterName:     clusterName,
				ClusterHash:     clusterHash,
				RelationalCache: relationalCache,
			},
		},
	}
}
