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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/mocks"
	"slice-controller/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	testSlice = &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
		},
		Spec: slicev1beta1.SliceSpec{
			PartitionIds: []string{
				"p1",
				"p2",
			},
		},
	}
	testLabels    = util.NodeLabels{RegularLabels: map[string]string{"label": "value"}}
	testLabelKeys = util.NodeLabelKeys{RegularLabels: []string{"label"}}
)

func TestLabelNodeGroupStep(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockClient(ctrl)

	s := &LabelNodeGroupStep{
		Client:                m,
		Labels:                testLabels,
		EnableHighConcurrency: false,
	}

	if s.Name() != LabelNodeGroupStepName {
		t.Errorf("Name() = %s, want %s", s.Name(), LabelNodeGroupStepName)
	}

	m.EXPECT().LabelNodeGroup(gomock.Any(), testLabels, "p1").Return(nil)
	m.EXPECT().LabelNodeGroup(gomock.Any(), testLabels, "p2").Return(nil)
	if err := s.Do(context.Background(), testSlice); err != nil {
		t.Errorf("Do() error = %v, want nil", err)
	}
}

func TestUnlabelNodeGroupStep(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockClient(ctrl)
	s := &UnlabelNodeGroupStep{
		Client:                m,
		LabelKeys:             testLabelKeys,
		EnableHighConcurrency: false,
	}

	if s.Name() != UnlabelNodeGroupStepName {
		t.Errorf("Name() = %s, want %s", s.Name(), UnlabelNodeGroupStepName)
	}

	m.EXPECT().UnlabelNodeGroup(gomock.Any(), testLabelKeys, "p1").Return(nil)
	m.EXPECT().UnlabelNodeGroup(gomock.Any(), testLabelKeys, "p2").Return(nil)
	if err := s.Do(context.Background(), testSlice); err != nil {
		t.Errorf("Do() error = %v, want nil", err)
	}
}

func TestLabelNodeGroupStep_ConcurrencyLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockClient(ctrl)

	// 1. Create a massive slice with 100 partitions (Limit is 32)
	manyPartitions := make([]string, 100)
	for i := 0; i < 100; i++ {
		manyPartitions[i] = fmt.Sprintf("p-%d", i)
	}

	bigSlice := testSlice.DeepCopy()
	bigSlice.Spec.PartitionIds = manyPartitions

	s := &LabelNodeGroupStep{Client: m, EnableHighConcurrency: true}

	// 2. Track Concurrency safely
	var activeWorkers int32
	var maxSeenWorkers int32

	// 3. Setup Mock with a .Do() hook to simulate work and track active threads
	m.EXPECT().
		LabelNodeGroup(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(100).
		DoAndReturn(func(ctx context.Context, labels util.NodeLabels, pid string) error {
			// Increment active counter
			current := atomic.AddInt32(&activeWorkers, 1)
			// Update max seen if needed
			for {
				peak := atomic.LoadInt32(&maxSeenWorkers)
				if current <= peak {
					break
				}
				if atomic.CompareAndSwapInt32(&maxSeenWorkers, peak, current) {
					break
				}
			}

			// SIMULATE WORK: Sleep to force overlap
			time.Sleep(10 * time.Millisecond)

			// Decrement active counter
			atomic.AddInt32(&activeWorkers, -1)
			return nil
		})

	// 4. Run the Step
	start := time.Now()
	err := s.Do(context.Background(), bigSlice)
	duration := time.Since(start)

	// 5. Assertions
	if err != nil {
		t.Fatalf("Do() returned error: %v", err)
	}

	if maxSeenWorkers > MaxConcurrentPartitionsLabeling {
		t.Errorf("Concurrency limit exceeded! Max active workers: %d, Limit: %d", maxSeenWorkers, MaxConcurrentPartitionsLabeling)
	}

	// Sequential: 100 * 10ms = 1000ms
	// Parallel (32 workers): ~2 batches * 10ms = ~20ms (plus overhead)
	if duration > 500*time.Millisecond {
		t.Errorf("Test took too long (%v), implying sequential execution. Expected < 500ms", duration)
	}

	t.Logf("Success: Processed 100 partitions with peak concurrency %d in %v", maxSeenWorkers, duration)
}

func TestUnlabelNodeGroupStep_ConcurrencyLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockClient(ctrl)

	// 1. Create a massive slice with 100 partitions (Limit is 32)
	manyPartitions := make([]string, 100)
	for i := 0; i < 100; i++ {
		manyPartitions[i] = fmt.Sprintf("p-%d", i)
	}

	bigSlice := testSlice.DeepCopy()
	bigSlice.Spec.PartitionIds = manyPartitions

	s := &UnlabelNodeGroupStep{
		Client:                m,
		EnableHighConcurrency: true,
	}

	// 2. Track Concurrency safely
	var activeWorkers int32
	var maxSeenWorkers int32

	// 3. Setup Mock with a .Do() hook to simulate work and track active threads
	m.EXPECT().
		UnlabelNodeGroup(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(100).
		DoAndReturn(func(ctx context.Context, labelKeys util.NodeLabelKeys, pid string) error {
			// Increment active counter
			current := atomic.AddInt32(&activeWorkers, 1)

			// Update max seen if needed (CAS loop for thread safety)
			for {
				peak := atomic.LoadInt32(&maxSeenWorkers)
				if current <= peak {
					break
				}
				if atomic.CompareAndSwapInt32(&maxSeenWorkers, peak, current) {
					break
				}
			}

			// SIMULATE WORK: Sleep to force overlap
			time.Sleep(10 * time.Millisecond)

			// Decrement active counter
			atomic.AddInt32(&activeWorkers, -1)
			return nil
		})

	// 4. Run the Step
	start := time.Now()
	err := s.Do(context.Background(), bigSlice)
	duration := time.Since(start)

	// 5. Assertions
	if err != nil {
		t.Fatalf("Do() returned error: %v", err)
	}

	// Verify we didn't exceed the limit
	if maxSeenWorkers > MaxConcurrentPartitionsLabeling {
		t.Errorf("Concurrency limit exceeded! Max active workers: %d, Limit: %d", maxSeenWorkers, MaxConcurrentPartitionsLabeling)
	}

	// Verify we actually ran in parallel
	// Sequential: 100 * 10ms = 1000ms
	// Parallel (32 workers): ~4 batches * 10ms = ~40ms (plus overhead)
	if duration > 500*time.Millisecond {
		t.Errorf("Test took too long (%v), implying sequential execution. Expected < 500ms", duration)
	}

	t.Logf("Success: Processed 100 partitions with peak concurrency %d in %v", maxSeenWorkers, duration)
}

func TestUnlabelNodeGroupStep_CacheSufficient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockClient(ctrl)
	s := &UnlabelNodeGroupStep{
		Client:                m,
		LabelKeys:             testLabelKeys,
		EnableHighConcurrency: false,
		PartitionIDs:          []string{"p1", "p2"},
	}

	m.EXPECT().UnlabelNodeGroup(gomock.Any(), testLabelKeys, "p1").Return(nil)
	m.EXPECT().UnlabelNodeGroup(gomock.Any(), testLabelKeys, "p2").Return(nil)
	m.EXPECT().ListNodesBySliceName(gomock.Any(), gomock.Any()).Times(0)

	if err := s.Do(context.Background(), testSlice); err != nil {
		t.Errorf("Do() error = %v, want nil", err)
	}
}

func TestUnlabelNodeGroupStep_Fallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockClient(ctrl)
	s := &UnlabelNodeGroupStep{
		Client:                m,
		LabelKeys:             testLabelKeys,
		EnableHighConcurrency: false,
	}

	emptySlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "empty-slice"},
		Spec: slicev1beta1.SliceSpec{
			PartitionIds: []string{},
		},
	}

	m.EXPECT().ListNodesBySliceName(gomock.Any(), "empty-slice").Return([]corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}}, nil)
	m.EXPECT().UnlabelNodeGroupByNames(gomock.Any(), testLabelKeys.RegularLabels, []string{"node1"}).Return(nil)

	if err := s.Do(context.Background(), emptySlice); err != nil {
		t.Errorf("Do() error = %v, want nil", err)
	}
}
