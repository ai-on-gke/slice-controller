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

package metrics

import (
	"context"
	"time"

	"k8s.io/klog/v2"
)

const (
	// UnknownNodePool is used as a fallback node pool name when the actual node pool is missing or unknown.
	UnknownNodePool = "unknown-pool"
)

// MetricsExporter holds the context required to export metrics.
type MetricsExporter struct {
	projectNumber string
	location      string
	clusterName   string
}

// NewMetricsExporter creates a new exporter instance.
func NewMetricsExporter(projectNumber, location, clusterName string) *MetricsExporter {
	klog.V(2).Infof("Metrics log server started.")
	return &MetricsExporter{
		projectNumber: projectNumber,
		location:      location,
		clusterName:   clusterName,
	}
}

// RecordSliceState records the current state of a slice.
func (m *MetricsExporter) RecordSliceState(ctx context.Context, sliceName, sliceNamespace, state, acceleratorType, topology, blockId, sliceId string) {
	klog.V(2).Infof("RecordSliceState called for slice %s/%s state %s", sliceNamespace, sliceName, state)
}

// RecordPartitionState records the current state of a partition.
func (m *MetricsExporter) RecordPartitionState(ctx context.Context, nodePoolName, state, acceleratorType, topology, partitionId, sliceId, blockId string) {
	klog.V(2).Infof("RecordPartitionState called for pool %s state %s", nodePoolName, state)
}

// RecordSliceFormLatency records the E2E latency of slice formation.
func (m *MetricsExporter) RecordSliceFormLatency(ctx context.Context, sliceName, sliceNamespace, endState, acceleratorType, topology, blockId, sliceId string, duration time.Duration) {
	klog.V(2).Infof("RecordSliceFormLatency called for slice %s/%s endState %s duration %f", sliceNamespace, sliceName, endState, duration.Seconds())
}

// RecordSliceDeformLatency records the E2E latency of slice deformation.
func (m *MetricsExporter) RecordSliceDeformLatency(ctx context.Context, sliceName, sliceNamespace, acceleratorType, topology, blockId, sliceId string, duration time.Duration) {
	klog.V(2).Infof("RecordSliceDeformLatency called for slice %s/%s duration %f", sliceNamespace, sliceName, duration.Seconds())
}

// GarbageCollectSlices removes slices from memory tracking.
func (m *MetricsExporter) GarbageCollectSlices(existingSliceIDs map[string]bool) {}

// GarbageCollectPartitions removes partitions from memory tracking.
func (m *MetricsExporter) GarbageCollectPartitions(existingPartitionIDs map[string]bool) {}

// RecordSliceStateTransition records the latency of a slice state transition.
func (m *MetricsExporter) RecordSliceStateTransition(ctx context.Context, sliceName, sliceNamespace, prevState, newState, acceleratorType, topology, sliceId string, duration time.Duration) {
	klog.V(2).Infof("RecordSliceStateTransition called for slice %s/%s from %s to %s duration %f", sliceNamespace, sliceName, prevState, newState, duration.Seconds())
}

// RecordSliceMetadata records metadata for a slice.
func (m *MetricsExporter) RecordSliceMetadata(ctx context.Context, sliceName, sliceNamespace, acceleratorType, topology, nodePoolName, sliceId, partitionId string, value int64) {
	klog.V(2).Infof("RecordSliceMetadata called for slice %s/%s value %d", sliceNamespace, sliceName, value)
}

// RecordSlicePartitionFailure records a partition failure.
func (m *MetricsExporter) RecordSlicePartitionFailure(ctx context.Context, sliceName, sliceNamespace, sliceId, partitionId, healthStatus, nodePoolName string) {
	klog.V(2).Infof("RecordSlicePartitionFailure called for pool %s partition %s status %s", nodePoolName, partitionId, healthStatus)
}

// ExportCumulativeFailures continuously exports the cumulative failure metrics.
func (m *MetricsExporter) ExportCumulativeFailures() {}
