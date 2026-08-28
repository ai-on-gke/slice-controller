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
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Helper to find a specific histogram metric
func findHistogramMetric(metricFamilies []*dto.MetricFamily, name string, labels map[string]string) (*dto.Metric, bool) {
	for _, mf := range metricFamilies {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				match := true
				actualLabels := make(map[string]string)
				for _, lp := range m.GetLabel() {
					actualLabels[lp.GetName()] = lp.GetValue()
				}

				if len(labels) != len(actualLabels) {
					match = false
				}

				for k, v := range labels {
					if actualLabels[k] != v {
						match = false
						break
					}
				}
				if match {
					return m, true
				}
			}
		}
	}
	return nil, false
}

// Helper to find a specific gauge metric

func TestRecordGceApiCall(t *testing.T) {
	registry := prometheus.NewRegistry()
	GceApiCall = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gce_api_call",
			Help:    "Distribution of GCE API call latencies by operation type and status.",
			Buckets: MillisecondBuckets,
		},
		[]string{"operation_type", "status"},
	)
	registry.MustRegister(GceApiCall)
	tests := []struct {
		name          string
		operationType string
		err           error
		expStatus     string
	}{
		{
			name:          "Successful API call",
			operationType: "test_api_success",
			err:           nil,
			expStatus:     ResultSuccess,
		},
		{
			name:          "Failed API call",
			operationType: "test_api_error",
			err:           errors.New("test error"),
			expStatus:     "error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := time.Now()
			time.Sleep(10 * time.Millisecond)
			RecordGceApiCall(tt.operationType, startTime, tt.err)
			metricFamilies, err := registry.Gather()
			if err != nil {
				t.Fatalf("Failed to gather metrics: %v", err)
			}
			labels := map[string]string{"operation_type": tt.operationType, "status": tt.expStatus}
			metric, found := findHistogramMetric(metricFamilies, "gce_api_call", labels)
			if !found {
				t.Errorf("GceApiCall metric not found for %s with operation_type=%s, status=%s", tt.name, tt.operationType, tt.expStatus)
				return
			}
			if metric.GetHistogram().GetSampleCount() != 1 {
				t.Errorf("Expected sample count 1 for %s, got %d", tt.name, metric.GetHistogram().GetSampleCount())
			}
			if metric.GetHistogram().GetSampleSum() <= 0 {
				t.Errorf("Expected positive duration sum for %s, got %f", tt.name, metric.GetHistogram().GetSampleSum())
			}
		})
	}
}

func TestRecordKubeApiCall(t *testing.T) {
	registry := prometheus.NewRegistry()
	KubeApiCall = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kube_api_call",
			Help:    "Distribution of Kube API call latencies by API name and status.",
			Buckets: MillisecondBuckets,
		},
		[]string{"api_name", "status"},
	)
	registry.MustRegister(KubeApiCall)
	tests := []struct {
		name      string
		apiName   string
		err       error
		expStatus string
	}{
		{
			name:      "Successful API call",
			apiName:   "node_patch_success",
			err:       nil,
			expStatus: ResultSuccess,
		},
		{
			name:      "Failed API call",
			apiName:   "node_patch_error",
			err:       errors.New("test error"),
			expStatus: "error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := time.Now()
			time.Sleep(10 * time.Millisecond)
			RecordKubeApiCall(tt.apiName, startTime, tt.err)
			metricFamilies, err := registry.Gather()
			if err != nil {
				t.Fatalf("Failed to gather metrics: %v", err)
			}
			labels := map[string]string{"api_name": tt.apiName, "status": tt.expStatus}
			metric, found := findHistogramMetric(metricFamilies, "kube_api_call", labels)
			if !found {
				t.Errorf("KubeApiCall metric not found for %s with api_name=%s, status=%s", tt.name, tt.apiName, tt.expStatus)
				return
			}
			if metric.GetHistogram().GetSampleCount() != 1 {
				t.Errorf("Expected sample count 1 for %s, got %d", tt.name, metric.GetHistogram().GetSampleCount())
			}
			if metric.GetHistogram().GetSampleSum() <= 0 {
				t.Errorf("Expected positive duration sum for %s, got %f", tt.name, metric.GetHistogram().GetSampleSum())
			}
		})
	}
}

func TestRecordK8sNodeOperation(t *testing.T) {
	registry := prometheus.NewRegistry()
	K8sNodeOperation = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "k8s_node_operation",
			Help:    "Distribution of k8s node operation latencies by operation type and success status.",
			Buckets: MillisecondBuckets,
		},
		[]string{"operation_type", "success", "accelerator_type", "slice_topology"},
	)
	registry.MustRegister(K8sNodeOperation)
	tests := []struct {
		name            string
		operationType   string
		acceleratorType string
		topology        string
		err             error
	}{
		{
			name:            "Successful Form Slice",
			operationType:   OperationFormSlice,
			acceleratorType: "tpu-v4",
			topology:        "2x2x2",
			err:             nil,
		},
		{
			name:            "Failed Deform Slice",
			operationType:   OperationDeformSlice,
			acceleratorType: "tpu-v5",
			topology:        "4x4x4",
			err:             errors.New("deform failed"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := time.Now()
			time.Sleep(10 * time.Millisecond)
			RecordK8sNodeOperation(tt.operationType, tt.acceleratorType, tt.topology, startTime, tt.err)
			expectedSuccess := strconv.FormatBool(tt.err == nil)
			metricFamilies, err := registry.Gather()
			if err != nil {
				t.Fatalf("Failed to gather metrics: %v", err)
			}
			labels := map[string]string{"operation_type": tt.operationType, "success": expectedSuccess, "accelerator_type": tt.acceleratorType, "slice_topology": tt.topology}
			metric, found := findHistogramMetric(metricFamilies, "k8s_node_operation", labels)
			if !found {
				t.Errorf("NodeLabelTaintOperation metric not found or not incremented for %s with operationType=%s, success=%s", tt.name, tt.operationType, expectedSuccess)
				return
			}
			if metric.GetHistogram().GetSampleCount() != 1 {
				t.Errorf("Expected sample count 1 for %s, got %d", tt.name, metric.GetHistogram().GetSampleCount())
			}
			if metric.GetHistogram().GetSampleSum() <= 0 {
				t.Errorf("Expected positive duration sum for %s, got %f", tt.name, metric.GetHistogram().GetSampleSum())
			}
		})
	}
}

func TestRecordSliceCRStateTransition(t *testing.T) {
	registry := prometheus.NewRegistry()
	SliceCRStateTransition = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "slice_cr_state_transition",
			Help:    "Distribution of slice state change duration by previous state and new state.",
			Buckets: SecondBuckets,
		},
		[]string{"prev_state", "new_state", "accelerator_type", "slice_topology"},
	)
	registry.MustRegister(SliceCRStateTransition)
	tests := []struct {
		name            string
		prevState       string
		newState        string
		acceleratorType string
		topology        string
		duration        time.Duration
	}{
		{
			name:            "State change from Creating to Created",
			prevState:       "Creating",
			newState:        "Created",
			acceleratorType: "tpu-v4", topology: "2x2x2",
			duration: 15 * time.Second,
		},
		{
			name:            "State change from Deleting to Deleted",
			prevState:       "Deleting",
			newState:        "Deleted",
			acceleratorType: "tpu-v5", topology: "4x4x4",
			duration: 30 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RecordSliceCRStateTransition(tt.prevState, tt.newState, tt.acceleratorType, tt.topology, tt.duration)
			metricFamilies, err := registry.Gather()
			if err != nil {
				t.Fatalf("Failed to gather metrics: %v", err)
			}
			labels := map[string]string{"prev_state": tt.prevState, "new_state": tt.newState, "accelerator_type": tt.acceleratorType, "slice_topology": tt.topology}
			metric, found := findHistogramMetric(metricFamilies, "slice_cr_state_transition", labels)
			if !found {
				t.Errorf("SliceStateTransition metric not found or not incremented for %s with prev_state=%s, new_state=%s", tt.name, tt.prevState, tt.newState)
				return
			}
			if metric.GetHistogram().GetSampleCount() != 1 {
				t.Errorf("Expected sample count 1 for %s, got %d", tt.name, metric.GetHistogram().GetSampleCount())
			}
			if metric.GetHistogram().GetSampleSum() != tt.duration.Seconds() {
				t.Errorf("Expected sample sum %f for %s, got %f", tt.duration.Seconds(), tt.name, metric.GetHistogram().GetSampleSum())
			}
		})
	}
}
