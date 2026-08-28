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

package kubehelper

import (
	"context"
	"fmt"
	"sync"
	"time"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/metrics"
	"slice-controller/internal/util"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

const (
	LabelNodeGroupStepName   = "LabelNodeGroup"
	UnlabelNodeGroupStepName = "UnlabelNodeGroup"

	// MaxConcurrentPartitions controls concurrency.
	// 32 workers means up to 32 partitions are processed simultaneously.
	MaxConcurrentPartitionsLabeling = 32
)

// LabelNodeGroupStep labels node group.
type LabelNodeGroupStep struct {
	Client                Client
	Labels                util.NodeLabels
	EnableHighConcurrency bool
}

func (s *LabelNodeGroupStep) Name() string {
	return LabelNodeGroupStepName
}

func (s *LabelNodeGroupStep) Do(ctx context.Context, slice *slicev1beta1.Slice) (err error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordK8sNodeOperation(metrics.OperationLabel, string(slice.Spec.Type), slice.Spec.Topology, startTime, err)
	}()

	logger := klog.FromContext(ctx).WithValues("slice", slice.Name)
	logger.Info("Labeling node groups")

	state := util.FindStateCondition(slice)
	if state == string(util.MMIGHealthStatusFailed) {
		logger.Info("Slice is in a failed state, skipping node labeling.")
		return nil
	}

	if !s.EnableHighConcurrency {
		for _, partitionId := range slice.Spec.PartitionIds {
			err = s.Client.LabelNodeGroup(ctx, s.Labels, partitionId)
			if err != nil {
				logger.Error(err, "Failed to label partition (sequential)", "partition", partitionId)
				return err
			}
		}
		return nil
	}

	logger.Info("Labeling node groups started", "partitions", len(slice.Spec.PartitionIds))

	var (
		errLock   sync.Mutex
		allErrors []error
		g         errgroup.Group
	)

	g.SetLimit(MaxConcurrentPartitionsLabeling)

	for _, partitionId := range slice.Spec.PartitionIds {
		pid := partitionId
		g.Go(func() error {
			// Use parent ctx so one failure doesn't cancel others
			err := s.Client.LabelNodeGroup(ctx, s.Labels, pid)
			if err != nil {
				logger.Error(err, "Failed to label partition", "partition", pid)
				errLock.Lock()
				allErrors = append(allErrors, err)
				errLock.Unlock()
			}
			return nil // Always return nil to keep the group running
		})
	}

	_ = g.Wait()

	if len(allErrors) > 0 {
		return utilerrors.NewAggregate(allErrors)
	}

	return nil
}

// UnlabelNodeGroupStep unlabels node group.
type UnlabelNodeGroupStep struct {
	Client                Client
	EnableHighConcurrency bool
	LabelKeys             util.NodeLabelKeys
	PartitionIDs          []string
}

func (s *UnlabelNodeGroupStep) Name() string {
	return UnlabelNodeGroupStepName
}

func (s *UnlabelNodeGroupStep) Do(ctx context.Context, slice *slicev1beta1.Slice) (err error) {
	startTime := time.Now()
	defer func() {
		// Note: usage of OperationLabel matches your snippet;
		// verify if metrics.OperationUnlabel exists/should be used here.
		metrics.RecordK8sNodeOperation(metrics.OperationLabel, string(slice.Spec.Type), slice.Spec.Topology, startTime, err)
	}()

	logger := klog.FromContext(ctx).WithValues("slice", slice.Name)
	logger.Info("Unlabeling node groups")

	partitions := s.PartitionIDs
	if len(partitions) == 0 {
		// By default, we use the provided partition IDs if present.
		// We fall back to the partitions in the slice spec only if the provided list is empty.
		// This is the default behavior for normal deletion. For N->0 updates,
		// partitions must be passed explicitly since the new spec will be empty.
		partitions = slice.Spec.PartitionIds
		logger.Info("Using partitions from slice spec for unlabeling", "partitions", partitions)
	}
	logger.Info("UnlabelNodeGroupStep: final partitions list for unlabeling", "partitions", partitions)

	if len(partitions) == 0 {
		logger.Info("Partitions list is empty, falling back to listing nodes by slice label")
		nodes, err := s.Client.ListNodesBySliceName(ctx, slice.Name)
		if err != nil {
			return fmt.Errorf("failed to list nodes by slice name: %w", err)
		}
		if len(nodes) == 0 {
			logger.Info("No nodes found for slice, skipping unlabeling", "slice", slice.Name)
			return nil
		}
		var nodeNames []string
		for _, node := range nodes {
			nodeNames = append(nodeNames, node.Name)
		}
		logger.Info("Found nodes to unlabel by slice label", "count", len(nodeNames), "nodes", nodeNames)
		return s.Client.UnlabelNodeGroupByNames(ctx, s.LabelKeys.RegularLabels, nodeNames)
	}

	if !s.EnableHighConcurrency {
		for _, partitionId := range partitions {
			err = s.Client.UnlabelNodeGroup(ctx, s.LabelKeys, partitionId)
			if err != nil {
				logger.Error(err, "Failed to unlabel partition (sequential)", "partition", partitionId)
				return err
			}
		}
		return nil
	}

	logger.Info("Unlabeling node groups started", "partitions", len(partitions))

	var (
		errLock   sync.Mutex
		allErrors []error
		g         errgroup.Group
	)

	g.SetLimit(MaxConcurrentPartitionsLabeling)

	for _, partitionId := range partitions {
		pid := partitionId
		g.Go(func() error {
			err := s.Client.UnlabelNodeGroup(ctx, s.LabelKeys, pid)
			if err != nil {
				logger.Error(err, "Failed to unlabel partition", "partition", pid)
				errLock.Lock()
				allErrors = append(allErrors, err)
				errLock.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()

	if len(allErrors) > 0 {
		return utilerrors.NewAggregate(allErrors)
	}

	return nil
}
