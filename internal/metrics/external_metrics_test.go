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
	"testing"
	"time"
)

func TestMetricsExporterMethods(t *testing.T) {
	exporter := NewMetricsExporter("123456789", "us-central1-a", "test-cluster")
	if exporter == nil {
		t.Fatal("NewMetricsExporter returned nil")
	}

	ctx := context.Background()
	exporter.RecordSliceState(ctx, "test-slice", "test-ns", "ACTIVE", "tpu7x", "2x2x2", "block-1", "test-uid")
	exporter.RecordPartitionState(ctx, "pool-1", "HEALTHY", "tpu7x", "2x2x2", "p-1", "test-uid", "block-1")
	exporter.RecordSliceFormLatency(ctx, "test-slice", "test-ns", "ACTIVE", "tpu7x", "2x2x2", "block-1", "test-uid", 10*time.Second)
	exporter.RecordSliceDeformLatency(ctx, "test-slice", "test-ns", "tpu7x", "2x2x2", "block-1", "test-uid", 10*time.Second)
	exporter.RecordSliceStateTransition(ctx, "test-slice", "test-ns", "CREATING", "ACTIVE", "tpu7x", "2x2x2", "test-uid", 5*time.Second)
	exporter.RecordSliceMetadata(ctx, "test-slice", "test-ns", "tpu7x", "2x2x2", "pool-1", "test-uid", "p-1", 1)
	exporter.RecordSlicePartitionFailure(ctx, "test-slice", "test-ns", "test-uid", "p-1", "UNHEALTHY", "pool-1")
	exporter.ExportCumulativeFailures()
	exporter.GarbageCollectSlices(map[string]bool{"test-uid": true})
	exporter.GarbageCollectPartitions(map[string]bool{"p-1": true})
}
