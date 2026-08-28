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
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/cache"
	"slice-controller/internal/util"
	betacompute "google.golang.org/api/compute/v0.beta"
	"k8s.io/klog/v2"
)

const (
	CreateWorkloadPolicyStepName   = "CreateWorkloadPolicy"
	DeleteWorkloadPolicyStepName   = "DeleteWorkloadPolicy"
	CreateMMIGStepName             = "CreateMMIG"
	DeleteMMIGStepName             = "DeleteMMIG"
	AttachMIGsToMMIGStepName       = "AttachMIGsToMMIG"
	TriggerMMIGDeformationStepName = "TriggerMMIGDeformation"
	DetachMIGsFromMMIGStepName     = "DetachMIGsFromMMIG"
	MIGAnnotationKey               = "node.gke.io/created-by-mig"
)

// CreateWorkloadPolicyStep creates a resource policy instance
type CreateWorkloadPolicyStep struct {
	Client      GCEClient
	ProjectID   string
	Region      string
	Slice       *slicev1beta1.Slice
	ClusterHash string
}

func (s *CreateWorkloadPolicyStep) Name() string {
	return CreateWorkloadPolicyStepName
}

func (s *CreateWorkloadPolicyStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", s.ProjectID, "region", s.Region)
	logger.Info("Creating WorkloadPolicy")
	return s.Client.CreateWorkloadPolicy(ctx, s.ProjectID, s.Region, slice, s.ClusterHash)
}

// DeleteWorkloadPolicyStep deletes a resource policy instance
type DeleteWorkloadPolicyStep struct {
	Client      GCEClient
	ProjectID   string
	Region      string
	ClusterHash string
}

func (s *DeleteWorkloadPolicyStep) Name() string {
	return DeleteWorkloadPolicyStepName
}

func (s *DeleteWorkloadPolicyStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", s.ProjectID, "region", s.Region)
	logger.Info("Deleting WorkloadPolicy")
	return s.Client.DeleteWorkloadPolicy(ctx, s.ProjectID, s.Region, slice.Name, s.ClusterHash)
}

// CreateMMIGStep creates a multimig instance.
type CreateMMIGStep struct {
	Client      GCEClient
	ProjectID   string
	Region      string
	ClusterHash string
}

func (s *CreateMMIGStep) Name() string {
	return CreateMMIGStepName
}

func (s *CreateMMIGStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", s.ProjectID, "region", s.Region)
	logger.Info("Creating MMIG")
	return s.Client.CreateMMIG(ctx, s.ProjectID, s.Region, slice, s.ClusterHash)
}

// DeleteMMIGStep deletes a multimig instance.
type DeleteMMIGStep struct {
	Client          GCEClient
	ProjectID       string
	Region          string
	ClusterHash     string
	RelationalCache cache.SliceRelationalCache
}

func (s *DeleteMMIGStep) Name() string {
	return DeleteMMIGStepName
}

func (s *DeleteMMIGStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", s.ProjectID, "region", s.Region)
	logger.Info("Deleting MMIG")

	// 1. Perform the GCE Deletion
	err := s.Client.DeleteMMIG(ctx, s.ProjectID, s.Region, slice.Name, s.ClusterHash)
	if err != nil {
		return err
	}

	// 2. Remove from Relational Cache immediately after success.
	// Since the GCE resource is now being deleted (or is already gone),
	// we must remove it from the cache to prevent the Health Monitor from
	// trying to poll a non-existent resource.
	if err := s.RelationalCache.DeleteSlice(slice.Name); err != nil {
		// We log the error but do NOT return it.
		// The primary goal (deleting infrastructure) succeeded. Failing here would
		// cause an unnecessary retry of the GCE deletion.
		logger.Error(err, "Failed to remove slice from relational cache", "slice", slice.Name)
	} else {
		logger.Info("Successfully removed slice from relational cache", "slice", slice.Name)
	}

	return nil
}

// AttachMIGsToMMIGStep attaches a MIG to a MMIG.
type AttachMIGsToMMIGStep struct {
	Client          GCEClient
	ProjectID       string
	Region          string
	Location        string
	ClusterName     string
	ClusterHash     string
	RelationalCache cache.SliceRelationalCache
}

func (s *AttachMIGsToMMIGStep) Name() string {
	return AttachMIGsToMMIGStepName
}

func (s *AttachMIGsToMMIGStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", s.ProjectID, "region", s.Region)
	logger.Info("Attaching MIGs to MMIG")
	cubes, err := getCubes(ctx, s.RelationalCache, slice)
	if err != nil {
		return err
	}
	if err := checkPartitionIds(cubes, slice.Spec.PartitionIds); err != nil {
		return err
	}

	err = s.Client.AttachMIGsToMMIG(ctx, s.ProjectID, s.Region, slice.Name, cubes, s.ClusterHash, slice.UID, slice.Status.ObservedGeneration, int64(util.GetRetryAttempt(slice)))
	return handlePartialFailure(ctx, s.Client, s.ProjectID, s.Region, slice.Name, s.ClusterHash, "Partial attachment details", err, cubes)
}

// TriggerMMIGDeformation trigger the mmig deactivation by detaching a pivot MIG
type TriggerMMIGDeformationStep struct {
	Client          GCEClient
	ProjectID       string
	Region          string
	Location        string
	ClusterName     string
	ClusterHash     string
	RelationalCache cache.SliceRelationalCache
	OpName          string
}

func (s *TriggerMMIGDeformationStep) Name() string {
	return TriggerMMIGDeformationStepName
}

func (s *TriggerMMIGDeformationStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", s.ProjectID, "region", s.Region)
	logger.Info("Trigger MMIG deformation")

	truthCubes, err := resolveTruthCubes(ctx, s.Client, s.ProjectID, s.Region, s.ClusterHash, s.RelationalCache, slice)
	if err != nil {
		return err
	}

	if len(truthCubes) == 0 {
		logger.Info("MMIG has no members, skipping deformation")
		return util.ErrDeformationSkipped
	}

	logger.Info("MIGs are attached to the MMIG", "MIGs", truthCubes, "mmigName", util.GetResourceName(slice.Name, s.ClusterHash))

	// Start the MMIG deformation by detaching the first MIG as pivot cube
	pivotCube := truthCubes[0]
	s.OpName, err = s.Client.DetachPivotMIGFromMMIG(ctx, s.ProjectID, s.Region, pivotCube, slice.UID, slice.Status.ObservedGeneration)
	if pErr := handlePartialFailure(ctx, s.Client, s.ProjectID, s.Region, slice.Name, s.ClusterHash, "Partial pivot detach details", err, []util.CubeInfo{pivotCube}); pErr != nil {
		return pErr
	}
	logger.Info("Detached pivot MIG from MMIG to trigger deformation", "pivotMig", pivotCube.MigName, "operation", s.OpName)
	return nil
}

// DetachMIGsFromMMIGStep detaches a MIG from a MMIG.
type DetachMIGsFromMMIGStep struct {
	Client          GCEClient
	ProjectID       string
	Region          string
	Location        string
	ClusterName     string
	ClusterHash     string
	RelationalCache cache.SliceRelationalCache
}

func (s *DetachMIGsFromMMIGStep) Name() string {
	return DetachMIGsFromMMIGStepName
}

func (s *DetachMIGsFromMMIGStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", s.ProjectID, "region", s.Region)
	logger.Info("Detaching MIGs from MMIG")

	truthCubes, err := resolveTruthCubes(ctx, s.Client, s.ProjectID, s.Region, s.ClusterHash, s.RelationalCache, slice)
	if err != nil {
		return err
	}
	if len(truthCubes) == 0 {
		logger.Info("MMIG has no members, skipping detach")
		return nil
	}

	logger.Info("MIGs are attached to the MMIG", "MIGs", truthCubes, "mmigName", util.GetResourceName(slice.Name, s.ClusterHash))

	err = s.Client.DetachMIGsFromMMIG(ctx, s.ProjectID, s.Region, slice.Name, truthCubes, s.ClusterHash, slice.UID, slice.Status.ObservedGeneration)
	return handlePartialFailure(ctx, s.Client, s.ProjectID, s.Region, slice.Name, s.ClusterHash, "Partial detach details", err, truthCubes)
}

// getCubes retrieves CubeInfo for each partition listed in the slice spec from the relational cache.
func getCubes(
	ctx context.Context,
	rc cache.SliceRelationalCache,
	slice *slicev1beta1.Slice,
) ([]util.CubeInfo, error) {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name)
	cubes := make([]util.CubeInfo, 0, len(slice.Spec.PartitionIds))
	for _, partitionId := range slice.Spec.PartitionIds {
		migInfo, found := rc.GetMIGByPartitionID(partitionId)
		if !found {
			logger.Info("MIG info not found in cache for partition ID", "partitionId", partitionId)
			continue
		}
		migUrl := migInfo.Name // "projects/project-name/zones/zone-name/instanceGroupManagers/mig-name"

		zone, migName, err := ParseMigUrl(migUrl)
		if err != nil {
			return nil, fmt.Errorf("failed to parse MIG name for zone %s: %w", migName, err)
		}

		cubes = append(cubes, util.CubeInfo{
			MigName:     migName,
			Zone:        zone,
			PartitionId: partitionId,
		})
	}
	return cubes, nil
}

// getAttachedPartitions queries GCE to find which of the specified cubes are currently attached to the MMIG.
func getAttachedPartitions(ctx context.Context, client GCEClient, projectID, region, mmigName string, cubes []util.CubeInfo) ([]string, []string, error) {
	members, err := client.ListMultiMigMembers(ctx, projectID, region, mmigName)
	if err != nil {
		return nil, nil, err
	}
	var attachedPartitions []string
	var logMigs []string
	for _, cube := range cubes {
		isAttached := false
		for _, m := range members {
			if m.Status != nil && strings.Contains(m.Status.InstanceGroupManager, cube.MigName) {
				isAttached = true
				break
			}
		}
		if isAttached {
			attachedPartitions = append(attachedPartitions, cube.PartitionId)
			logMigs = append(logMigs, fmt.Sprintf("%s(%s)", cube.MigName, cube.PartitionId))
		}
	}
	return attachedPartitions, logMigs, nil
}

// parseMigUrl extracts the zone and MIG name from the annotation string.
// The expected format is "projects/project-name/zones/zone-name/instanceGroupManagers/mig-name".
func ParseMigUrl(migUrl string) (zone, migName string, err error) {
	parts := strings.Split(migUrl, "/")
	for i, part := range parts {
		if part == "zones" && i+1 < len(parts) {
			zone = parts[i+1]
		}
		if part == "instanceGroupManagers" && i+1 < len(parts) {
			migName = parts[i+1]
		}
	}
	if zone == "" || migName == "" {
		return "", "", fmt.Errorf("could not parse zone or MIG name from migUrl: %s", migUrl)
	}
	return zone, migName, nil
}

// resolveTruthCubes fetches cached cubes and compares them against the MMIG API members.
// It returns a nil slice if the MMIG is completely empty, allowing callers to handle early returns.
func resolveTruthCubes(
	ctx context.Context,
	client GCEClient,
	projectID, region, clusterHash string,
	rc cache.SliceRelationalCache,
	slice *slicev1beta1.Slice,
) ([]util.CubeInfo, error) {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name, "project", projectID, "region", region)

	// 1. Get cached state
	cachedCubes, err := getCubes(ctx, rc, slice)
	if err != nil {
		return nil, err
	}

	// 2. Get Source of Truth from API
	mmigName := util.GetResourceName(slice.Name, clusterHash)
	attachedMigs, err := client.ListMultiMigMembers(ctx, projectID, region, mmigName)
	if err != nil {
		return nil, fmt.Errorf("failed to list MMIG members: %w", err)
	}

	// 3. Return early if MMIG is completely empty
	if len(attachedMigs) == 0 {
		return nil, nil // Return an empty/nil slice to indicate skipping
	}

	// 4. Compare and generate actual state
	truthCubes := syncCubesWithMMIGMembers(attachedMigs, cachedCubes, len(slice.Spec.PartitionIds) == 0, logger)

	if len(truthCubes) == 0 {
		return nil, fmt.Errorf("no valid cubes parsed from MMIG members for slice %s", slice.Name)
	}

	return truthCubes, nil
}

// syncCubesWithMMIGMembers compares the list of MIGs from the MMIG members API with the locally cached cubes.
// It returns a list of CubeInfo that represents the "truth" of what should be attached to the MMIG, while logging any discrepancies.
func syncCubesWithMMIGMembers(members []*betacompute.MultiMigMember, cachedCubes []util.CubeInfo, isPartitionEmpty bool, logger logr.Logger) []util.CubeInfo {
	cachedMap := make(map[string]util.CubeInfo)
	for _, c := range cachedCubes {
		cachedMap[c.MigName] = c
	}

	var truthCubes []util.CubeInfo
	actualMap := make(map[string]struct{}) // Idiomatic Go set using zero-byte empty struct

	for _, member := range members {
		if member.Status == nil || member.Status.InstanceGroupManager == "" {
			continue
		}

		igmURL := member.Status.InstanceGroupManager
		// Reuse the existing ParseMigUrl helper
		zone, migName, err := ParseMigUrl(igmURL)
		if err != nil {
			logger.Info("Warning: Skipping malformed InstanceGroupManager URL", "url", igmURL, "error", err)
			continue
		}

		actualMap[migName] = struct{}{}

		// Check if we already have this in our local cache
		if cachedCube, exists := cachedMap[migName]; exists {
			truthCubes = append(truthCubes, cachedCube)
		} else {
			// It exists in the API but not in our cache
			if !isPartitionEmpty {
				logger.Info("Warning: MIG found in MMIG API but missing from local cache", "mig", migName, "zone", zone)
			}
			truthCubes = append(truthCubes, util.CubeInfo{
				MigName: migName,
				Zone:    zone,
			})
		}
	}

	// Check for the reverse mismatch (in cache, but completely missing from the MMIG)
	for cachedName := range cachedMap {
		if _, exists := actualMap[cachedName]; !exists {
			logger.Info("Warning: MIG found in local cache but missing from actual MMIG members API", "mig", cachedName)
		}
	}
	return truthCubes
}

// checkPartitionIds verifies that the list of cubes matches the expected partition IDs in the spec.
func checkPartitionIds(cubes []util.CubeInfo, specPartitionIds []string) error {
	specIds := make(map[string]bool)
	for _, id := range specPartitionIds {
		specIds[id] = true
	}

	foundIds := make(map[string]bool)
	for _, c := range cubes {
		foundIds[c.PartitionId] = true
	}

	var missing []string
	for _, id := range specPartitionIds {
		if !foundIds[id] {
			missing = append(missing, id)
		}
	}

	var extra []string
	for _, c := range cubes {
		if !specIds[c.PartitionId] {
			extra = append(extra, c.PartitionId)
		}
	}

	if len(missing) > 0 || len(extra) > 0 {
		return fmt.Errorf("MIG mismatch in cache: missing partition IDs: [%s], extra partition IDs: [%s]", strings.Join(missing, ", "), strings.Join(extra, ", "))
	}
	return nil
}

// handlePartialFailure logs and returns a consolidated error when an operation on multiple cubes partially fails.
func handlePartialFailure(ctx context.Context, client GCEClient, projectID, region, sliceName, clusterHash, logMsg string, err error, cubes []util.CubeInfo) error {
	if err == nil {
		return nil
	}
	mmigName := util.GetResourceName(sliceName, clusterHash)
	attachedPartitions, logMigs, listErr := getAttachedPartitions(ctx, client, projectID, region, mmigName, cubes)
	if listErr == nil {
		klog.FromContext(ctx).Error(err, logMsg, "mmig", mmigName, "attached", strings.Join(logMigs, ", "))
		return fmt.Errorf("currently attached partition ID(s) %s: %w", strings.Join(attachedPartitions, ", "), err)
	}
	return err
}
