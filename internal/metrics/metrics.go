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
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// GCE API call results
	ResultSuccess = "success"
	ResultError   = "error"
	// Slice Operations
	OperationFormSlice   = "form_slice"
	OperationDeformSlice = "deform_slice"
	OperationLabel       = "node_label"
	OperationTaint       = "node_taint"
	// K8s node operation steps
	// TODO: record all following operations latency
	OperationCreateWP             = "CreateWP"
	OperationCreateMMIG           = "CreateMMIG"
	OperationAttachMIGs           = "AttachMIGs"
	OperationDetach1MIG           = "Detach1MIG"
	OperationDetachAllMIGs        = "DetachAllMIGs"
	OperationLabelNode            = "Label"
	OperationUnLabelNode          = "UnLabel"
	OperationDeleteWP             = "DeleteWP"
	OperationDeleteMMIG           = "DeleteMMIG"
	OperationGetMMIG              = "GetMMIG"
	OperationCreateWorkloadPolicy = "CreateWorkloadPolicy"
)

var MillisecondBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000}

var SecondBuckets = []float64{
	1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60,
	65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 120,
	125, 130, 135, 140, 145, 150, 155, 160, 165, 170, 175, 180,
	185, 190, 195, 200, 205, 210, 215, 220, 225, 230, 235, 240,
	245, 250, 255, 260, 265, 270, 275, 280, 285, 290, 295, 300,
	600, 900, 1200, 1800, 2400, 3000, 3600, 4200, 4800, 5400, 6000, 6600, 7200, 86400, 259200, 604800,
}

var (
	// GceApiCall is a Prometheus histogram to track the latency of GCE API calls,
	// TODO: add error response code to status. e.g. 404 403
	// TODO: add accelerator type and topology as attributes
	GceApiCall = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gce_api_call",
			Help:    "Distribution of GCE API call latencies by operation type and status.",
			Buckets: MillisecondBuckets,
		},
		[]string{"operation_type", "status"},
	)
	// k8s_node_operation is a Prometheus histogram to track the latency of k8s node operations,
	// Currently only used for record node label operation
	K8sNodeOperation = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "k8s_node_operation",
			Help:    "Distribution of k8s node operation latencies by operation type and success status.",
			Buckets: MillisecondBuckets,
		},
		[]string{"operation_type", "success", "accelerator_type", "slice_topology"},
	)
	// SliceCRStateTransition is a Prometheus histogram to track the latency of slice state changes,
	SliceCRStateTransition = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "slice_cr_state_transition",
			Help:    "Distribution of slice state change duration by previous state and new state.",
			Buckets: SecondBuckets,
		},
		[]string{"prev_state", "new_state", "accelerator_type", "slice_topology"},
	)
	// KubeApiCall is a Prometheus histogram to track the latency of Kube API calls,
	// TODO: add accelerator type and topology as attributes
	KubeApiCall = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kube_api_call",
			Help:    "Distribution of Kube API call latencies by API name and status.",
			Buckets: MillisecondBuckets,
		},
		[]string{"api_name", "status"},
	)
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(GceApiCall)
	metrics.Registry.MustRegister(K8sNodeOperation)
	metrics.Registry.MustRegister(SliceCRStateTransition)
	metrics.Registry.MustRegister(KubeApiCall)

}

// RecordGceApiCall records metrics for a GCE API call.
func RecordGceApiCall(operationType string, startTime time.Time, err error) {
	duration := float64(time.Since(startTime).Milliseconds())
	status := ResultSuccess
	if err != nil {
		status = ResultError
	}
	GceApiCall.WithLabelValues(operationType, status).Observe(duration)
	// klog.V(2).Infof("GCE API call metric collected. operation_type: %s, status: %s, duration_ms: %f", operationType, status, duration)
}

// RecordK8sNodeOperation records metrics for a node label and taint operation.
func RecordK8sNodeOperation(operationType string, acceleratorType, topology string, startTime time.Time, err error) {
	duration := float64(time.Since(startTime).Milliseconds())
	success := err == nil
	K8sNodeOperation.WithLabelValues(operationType, strconv.FormatBool(success), acceleratorType, topology).Observe(duration)
	klog.V(2).Infof("Node operation metric collected. operation_type: %s, success: %t, accelerator_type: %s, slice_topology: %s, duration_ms: %f", operationType, success, acceleratorType, topology, duration)
}

// RecordSliceCRStateTransition records metrics for a slice state change.
func RecordSliceCRStateTransition(prevState, newState, acceleratorType, topology string, duration time.Duration) {
	durationSeconds := duration.Seconds()
	SliceCRStateTransition.WithLabelValues(prevState, newState, acceleratorType, topology).Observe(durationSeconds)
	klog.V(2).Infof("Slice CR state transition metric collected. prev_state: %s, new_state: %s, accelerator_type: %s, slice_topology: %s, duration_s: %f", prevState, newState, acceleratorType, topology, durationSeconds)
}

// RecordKubeApiCall records metrics for a Kube API call.
func RecordKubeApiCall(apiName string, startTime time.Time, err error) {
	duration := float64(time.Since(startTime).Milliseconds())
	status := ResultSuccess
	if err != nil {
		status = ResultError
	}
	KubeApiCall.WithLabelValues(apiName, status).Observe(duration)
	klog.V(2).Infof("Kube API call metric collected. api_name: %s, status: %s, duration_ms: %f", apiName, status, duration)
}
