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

package util

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/metrics"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	TpuAcceleratorLabel = "cloud.google.com/gke-tpu-accelerator"

	TopologyLabelKey                           = "cloud.google.com/gke-tpu-topology"
	SliceLabelKey                              = "cloud.google.com/gke-tpu-slice"
	ReservationBlockIDLabel                    = "cloud.google.com/gce-topology-block"
	RetryAttemptAnnotationKey                  = "accelerator.gke.io/retry-attempt"
	LastReadyConditionTransitionTimeAnnotation = "accelerator.gke.io/LastReadyConditionTransitionTime"

	SubsliceLastOpAnnotation               = "accelerator.gke.io/last-operation-id"
	SubsliceProvisionedPartitionAnnotation = "accelerator.gke.io/provisioned-partition"

	Topology4x4x4 = "4x4x4"
	Topology2x4x4 = "2x4x4"
	Topology2x2x4 = "2x2x4"
	Topology2x2x2 = "2x2x2"
	Topology2x2x1 = "2x2x1"

	Topology2x4   = "2x4"
	Topology4x4   = "4x4"
	Topology4x8   = "4x8"
	Topology8x8   = "8x8"
	Topology8x16  = "8x16"
	Topology16x16 = "16x16"

	TopologyLabel2x2x1 = "cloud.google.com/gke-tpu-partition-2x2x1-id"
	TopologyLabel2x2x2 = "cloud.google.com/gke-tpu-partition-2x2x2-id"
	TopologyLabel2x2x4 = "cloud.google.com/gke-tpu-partition-2x2x4-id"
	TopologyLabel2x4x4 = "cloud.google.com/gke-tpu-partition-2x4x4-id"
	TopologyLabel4x4x4 = "cloud.google.com/gke-tpu-partition-4x4x4-id"

	TopologyLabel2x4   = "cloud.google.com/gke-tpu-slice-2x4-id"
	TopologyLabel4x4   = "cloud.google.com/gke-tpu-slice-4x4-id"
	TopologyLabel4x8   = "cloud.google.com/gke-tpu-slice-4x8-id"
	TopologyLabel8x8   = "cloud.google.com/gke-tpu-slice-8x8-id"
	TopologyLabel8x16  = "cloud.google.com/gke-tpu-slice-8x16-id"
	TopologyLabel16x16 = "cloud.google.com/gke-tpu-slice-16x16-id"
)

type NodeLabelKeys struct {
	RegularLabels      []string
	LinkedRunnerLabels []string
}

type NodeLabels struct {
	RegularLabels      map[string]string
	LinkedRunnerLabels map[string]string
}

type TpuTopologyDetails struct {
	NodeCount int
	LabelKey  string
}

var SupportedTopologiesInfo = map[string]TpuTopologyDetails{
	Topology4x4x4: {NodeCount: 16, LabelKey: TopologyLabel4x4x4},
	Topology2x4x4: {NodeCount: 8, LabelKey: TopologyLabel2x4x4},
	Topology2x2x4: {NodeCount: 4, LabelKey: TopologyLabel2x2x4},
	Topology2x2x2: {NodeCount: 2, LabelKey: TopologyLabel2x2x2},
	Topology2x2x1: {NodeCount: 1, LabelKey: TopologyLabel2x2x1},

	Topology2x4:   {NodeCount: 2, LabelKey: TopologyLabel2x4},
	Topology4x4:   {NodeCount: 4, LabelKey: TopologyLabel4x4},
	Topology4x8:   {NodeCount: 8, LabelKey: TopologyLabel4x8},
	Topology8x8:   {NodeCount: 16, LabelKey: TopologyLabel8x8},
	Topology8x16:  {NodeCount: 32, LabelKey: TopologyLabel8x16},
	Topology16x16: {NodeCount: 64, LabelKey: TopologyLabel16x16},
}

// ConvertReasonToStatus converts a reason string to a metav1.ConditionStatus.
func ConvertReasonToStatus(reason string) metav1.ConditionStatus {
	switch reason {
	case string(MMIGHealthStatusActive), string(MMIGHealthStatusActiveDegraded):
		return metav1.ConditionTrue
	case SliceHealthProbeFailed, string(MIGHealthStatusUnknown), SliceNotCreated:
		return metav1.ConditionUnknown
	default:
		return metav1.ConditionFalse
	}
}

// Find the condition of slice state
func FindStateCondition(slice *slicev1beta1.Slice) string {
	cond := meta.FindStatusCondition(slice.Status.Conditions, slicev1beta1.SliceStateConditionType)
	if cond == nil {
		return ""
	}
	return cond.Reason
}

// Find whether a condition exists in slice
func FindSliceCondition(slice *slicev1beta1.Slice, conditionName string) (metav1.Condition, bool) {
	cond := meta.FindStatusCondition(slice.Status.Conditions, conditionName)
	if cond == nil {
		return metav1.Condition{}, false
	}
	return *cond, true
}

// PatchSliceCondition updates the condition using Patch to avoid conflicts.
// If status or reason are empty, it attempts to preserve the current values from the existing condition.
func PatchSliceCondition(
	ctx context.Context,
	k8sClient client.Client,
	slice *slicev1beta1.Slice,
	state string,
	status metav1.ConditionStatus,
	reason string,
	message string,
	metricsExporter *metrics.MetricsExporter,
) (bool, error) {
	patchBase := client.MergeFrom(slice.DeepCopy())

	// If status or reason are empty, try to preserve current values
	currentCond, found := FindSliceCondition(slice, state)
	var prevState string

	if found {
		prevState = currentCond.Reason
		if status == "" {
			status = currentCond.Status
		}
		if reason == "" {
			reason = prevState
		}
	} else {
		if status == "" {
			status = metav1.ConditionUnknown
		}
	}

	reasonChanged := !found || prevState != reason

	// Calculate duration BEFORE updating the annotation
	var duration time.Duration
	var hasDuration bool
	if state == slicev1beta1.SliceStateConditionType && reasonChanged && found {
		if slice.Annotations != nil {
			if lastTransitionStr, ok := slice.Annotations[LastReadyConditionTransitionTimeAnnotation]; ok {
				if lastTransition, err := time.Parse(time.RFC3339, lastTransitionStr); err == nil {
					duration = time.Since(lastTransition)
					hasDuration = true
				}
			}
		}
	}

	// 2. Modify the object locally
	newCondition := metav1.Condition{
		Type:               state,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
	changed := meta.SetStatusCondition(&slice.Status.Conditions, newCondition)

	if !changed {
		return false, nil
	}

	// Execute the Patch
	if err := k8sClient.Status().Patch(ctx, slice, patchBase); err != nil {
		return changed, fmt.Errorf("failed to patch slice condition %s: %w", state, err)
	}

	// If this is the state condition, also update the transition time annotation
	if state == slicev1beta1.SliceStateConditionType && reasonChanged {
		patchBaseMeta := client.MergeFrom(slice.DeepCopy())
		if slice.Annotations == nil {
			slice.Annotations = make(map[string]string)
		}
		transitionTime := time.Now().Format(time.RFC3339)
		slice.Annotations[LastReadyConditionTransitionTimeAnnotation] = transitionTime
		klog.V(2).Infof("Updating slice annotation. sliceName: %s, reason: %s, transitionTime: %s", slice.Name, reason, transitionTime)
		if err := k8sClient.Patch(ctx, slice, patchBaseMeta); err != nil {
			return changed, fmt.Errorf("failed to patch slice annotation: %w, state: %s, reason: %s, message: %s", err, state, reason, message)
		}

		// Record metric AFTER successful patch and annotation update
		if hasDuration && metricsExporter != nil {
			newState := reason

			klog.V(2).Infof("Slice state changed. sliceName: %s, previous state: %s, new state: %s, duration_sec: %f",
				slice.Name, prevState, newState, duration.Seconds())

			metricsExporter.RecordSliceStateTransition(ctx, slice.Name, slice.Namespace, prevState, newState, string(slice.Spec.Type), slice.Spec.Topology, string(slice.UID), duration)
		}
	}

	return changed, nil
}

// DeleteSliceCondition remove a conditionType of the slice
func DeleteSliceCondition(
	slice *slicev1beta1.Slice,
	conditionType string,
	client client.Client,
	ctx context.Context,
) error {
	var newConditions []metav1.Condition
	for _, condition := range slice.Status.Conditions {
		if condition.Type != conditionType {
			newConditions = append(newConditions, condition)
		}
	}
	// Skip if no diff
	if len(newConditions) == len(slice.Status.Conditions) {
		return nil
	}
	slice.Status.Conditions = newConditions
	if err := client.Status().Update(ctx, slice); err != nil {
		// If update fails, requeue to retry
		return err
	}
	return nil
}

// Use the short hash (first 8 characters) of the full cluster hash with the
// slice name to form the resource name.
func GetResourceName(sliceName, clusterHash string) string {
	shortHash := clusterHash
	if len(clusterHash) > 8 {
		shortHash = clusterHash[:8]
	}
	if len(shortHash) == 0 {
		return sliceName
	}

	return fmt.Sprintf("%s-%s", sliceName, shortHash)
}

// ConstructMMIGURL constructs the MMIG URL from project number, region, and slice name.
func ConstructMMIGURL(projectNumber, region, sliceName, clusterHash string) string {
	MMIGName := GetResourceName(sliceName, clusterHash)
	policyURL := fmt.Sprintf("projects/%s/regions/%s/multiMigs/%s", projectNumber, region, MMIGName)
	return policyURL
}

// GetRegion extracts the region from a GCE location string.
func GetRegion(location string) (string, error) {
	parts := strings.Split(location, "-")
	if len(parts) > 3 || len(parts) < 2 {
		return "", fmt.Errorf("invalid location: %s", location)
	}
	return parts[0] + "-" + parts[1], nil
}

// ExtractSliceNameFromResourceName reverses GetResourceName.
// It requires the original clusterHash to accurately identify and remove the suffix.
func ExtractSliceNameFromResourceName(resourceName, clusterHash string) string {
	// 1. Re-calculate the short hash exactly as the original function did.
	shortHash := clusterHash
	if len(clusterHash) > 8 {
		shortHash = clusterHash[:8]
	}

	// 2. If the original hash was empty, no suffix was added.
	if len(shortHash) == 0 {
		return resourceName
	}
	// 3. Remove the suffix "-<shortHash>" from the resource name.
	suffix := "-" + shortHash
	return strings.TrimSuffix(resourceName, suffix)
}

// ValidateNodeCountForTopology checks if the provided node count matches the expected count for the given topology.
func ValidateNodeCountForTopology(topology string, nodeCount int) error {
	if topologyInfo, ok := SupportedTopologiesInfo[topology]; ok {
		if topologyInfo.NodeCount == nodeCount {
			return nil
		}
		return fmt.Errorf("node count mismatch for topology %s: expected %d, got %d", topology, topologyInfo.NodeCount, nodeCount)
	}
	return fmt.Errorf("unknown topology: %s", topology)
}

// IsSubslice checks if the slice spec is for a sub-slice request.
func IsSubslice(spec *slicev1beta1.SliceSpec) bool {
	// A sub-slice request must have at most one partition ID.
	if len(spec.PartitionIds) > 1 {
		return false
	}

	switch spec.Type {
	case slicev1beta1.TypeTpu7x:
		// For tpu7x, sub-slices have specific small topologies.
		allowedSmallTopologies := map[string]bool{
			"2x2x1": true,
			"2x2x2": true,
			"2x2x4": true,
			"2x4x4": true,
		}
		return allowedSmallTopologies[spec.Topology]
	}

	return false
}

// IsSubsliceTopology checks if a given topology string corresponds to a sub-slice.
func IsSubsliceTopology(topology string) bool {
	subsliceTopologies := map[string]bool{
		// v7x (Ironwood)
		"2x2x1": true,
		"2x2x2": true,
		"2x2x4": true,
		"2x4x4": true,
	}
	return subsliceTopologies[topology]
}

// IsFullTopology checks if a given topology string corresponds to a full partition (4x4x4 or 16x16).
func IsFullTopology(topology string) bool {
	return topology == Topology4x4x4 || topology == Topology16x16
}

// ParseMigDetails is a helper to extract resource names from a full GCE URL.
func ParseMigDetails(url string) (name, zone string, err error) {
	if url == "" {
		return "", "", fmt.Errorf("empty MIG URL")
	}
	parts := strings.Split(url, "/")

	for i, part := range parts {
		switch part {
		case "instanceGroupManagers":
			// The name of the MIG is the next part of the path.
			if i+1 < len(parts) {
				name = parts[i+1]
			}
		case "zones":
			// The zone is the next part of the path.
			if i+1 < len(parts) {
				zone = parts[i+1]
			}
		}
	}

	if name == "" || zone == "" {
		return "", "", fmt.Errorf("could not parse MIG name and/or zone from URL: %s", url)
	}

	return name, zone, nil
}

// GetRetryAttempt returns the retry attempt count from the slice annotations.
func GetRetryAttempt(slice *slicev1beta1.Slice) int {
	if slice.Annotations == nil {
		return 0
	}
	if val, ok := slice.Annotations[RetryAttemptAnnotationKey]; ok {
		if attempt, err := strconv.Atoi(val); err == nil {
			return attempt
		}
	}
	return 0
}

// IncrementRetryAttempt increments the retry attempt count in the slice annotations.
func IncrementRetryAttempt(slice *slicev1beta1.Slice) {
	attempt := GetRetryAttempt(slice)
	attempt++
	if slice.Annotations == nil {
		slice.Annotations = make(map[string]string)
	}
	slice.Annotations[RetryAttemptAnnotationKey] = strconv.Itoa(attempt)
}
