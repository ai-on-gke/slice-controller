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

package gce

import (
	"math/rand"
	"net/url"
	"strings"
	"time"

	alphacompute "google.golang.org/api/compute/v0.alpha"
)

// InjectedOperationStep defines the type for keys in the StepHttpFailures and StepTimeouts map.
type InjectedOperationStep string

// Constants for the different steps where an HTTP error can be injected.
const (
	CreateWorkloadPolicy    InjectedOperationStep = "CreateWorkloadPolicy"
	DeleteWorkloadPolicy    InjectedOperationStep = "DeleteWorkloadPolicy"
	CreateMMIG              InjectedOperationStep = "CreateMMIG"
	GetMMIG                 InjectedOperationStep = "GetMMIG"
	DeleteMMIG              InjectedOperationStep = "DeleteMMIG"
	GetInstanceGroupManager InjectedOperationStep = "GetInstanceGroupManager"
	AttachMIGToMMIG         InjectedOperationStep = "AttachMIGToMMIG"
	DetachMIGFromMMIG       InjectedOperationStep = "DetachMIGFromMMIG"
	GetAvailableTopologies  InjectedOperationStep = "GetAvailableTopologies"
)

// Constants for operation name prefixes.
const (
	OpPrefixCreatePolicy = "op-createpolicy-"
	OpPrefixDeletePolicy = "op-deletepolicy-"
	OpPrefixCreateMMIG   = "op-createmmig-"
	OpPrefixDeleteMMIG   = "op-deletemmig-"
	OpPrefixAttachMIGs   = "op-attachmigs-"
	OpPrefixDetachMIGs   = "op-detachmigs-"
	OpPrefixConfigure    = "op-configure-"
	defaultSliceName     = "default"
)

// MMIGHealthStatus defines the type for MMIG health status strings.
type MMIGHealthStatus string

// Constants for the different health statuses of an MMIG.
const (
	MMIGHealthStatusIncomplete     MMIGHealthStatus = "INCOMPLETE"
	MMIGHealthStatusActivating     MMIGHealthStatus = "ACTIVATING"
	MMIGHealthStatusActive         MMIGHealthStatus = "ACTIVE"
	MMIGHealthStatusActiveDegraded MMIGHealthStatus = "ACTIVE_DEGRADED"
	MMIGHealthStatusDeactivating   MMIGHealthStatus = "DEACTIVATING"
	MMIGHealthStatusFailed         MMIGHealthStatus = "FAILED"
	MMIGHealthStatusUnknown        MMIGHealthStatus = "UNKNOWN"
)

// allMMIGHealthStatuses contains all possible health status strings for an MMIG.
var allMMIGHealthStatuses = []MMIGHealthStatus{
	MMIGHealthStatusIncomplete,
	MMIGHealthStatusActivating,
	MMIGHealthStatusActive,
	MMIGHealthStatusActiveDegraded,
	MMIGHealthStatusDeactivating,
	MMIGHealthStatusFailed,
	MMIGHealthStatusUnknown,
}

// isValidMMIGHealthStatus checks if the provided string is a valid MMIGHealthStatus.
func isValidMMIGHealthStatus(status string) bool {
	for _, s := range allMMIGHealthStatuses {
		if MMIGHealthStatus(status) == s {
			return true
		}
	}
	return false
}

// AcceleratorTopologyHealthStatus defines the type for accelerator topology health status strings.
type AcceleratorTopologyHealthStatus string

const (
	AcceleratorTopologyHealthStatusHealthy   AcceleratorTopologyHealthStatus = "HEALTHY"
	AcceleratorTopologyHealthStatusDegraded  AcceleratorTopologyHealthStatus = "DEGRADED"
	AcceleratorTopologyHealthStatusUnhealthy AcceleratorTopologyHealthStatus = "UNHEALTHY"
)

var allAcceleratorTopologyHealthStatuses = []AcceleratorTopologyHealthStatus{
	AcceleratorTopologyHealthStatusHealthy,
	AcceleratorTopologyHealthStatusDegraded,
	AcceleratorTopologyHealthStatusUnhealthy,
}

func isValidAcceleratorTopologyHealthStatus(status string) bool {
	for _, s := range allAcceleratorTopologyHealthStatuses {
		if AcceleratorTopologyHealthStatus(status) == s {
			return true
		}
	}
	return false
}

// operationDone is a helper to create a GCE operation response,
// which is a common return type for many API calls. It builds a more
// complete Operation object to better mimic the real GCE API.
func operationDone(opName, opType, selfLink, targetLink, region, zone string) *alphacompute.Operation {
	now := time.Now().Format(time.RFC3339)
	return &alphacompute.Operation{
		Id:                  uint64(rand.Int63()),
		Name:                opName,
		Kind:                "compute#operation",
		Status:              "DONE",
		OperationType:       opType,
		Progress:            100,
		InsertTime:          now,
		StartTime:           now,
		EndTime:             now,
		Region:              region,
		Zone:                zone,
		SelfLink:            selfLink,
		TargetLink:          targetLink,
		HttpErrorStatusCode: 200,
		HttpErrorMessage:    "OK",
		StatusMessage:       "DONE",
	}
}

func getResourceName(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

// getBasePath returns the path without the final resource name or action.
func getBasePath(path string) string {
	parts := strings.Split(path, "/")
	return strings.Join(parts[:len(parts)-1], "/")
}

// getSliceAndOpNameFromURL extracts the slice name and operation name from an operation URL.
// e.g. /.../operations/op-createmmig-test-slice/slice/test-slice -> ("test-slice", "op-createmmig-test-slice")
func getSliceAndOpNameFromURL(path string) (string, string, bool) {
	decodedPath, err := url.PathUnescape(path)
	if err != nil {
		// Fallback to original path if decoding fails
		decodedPath = path
	}
	parts := strings.Split(decodedPath, "/")
	// Expected format: /.../operations/{opName}/slice/{sliceName}
	if len(parts) >= 4 && parts[len(parts)-2] == "slice" {
		opName := parts[len(parts)-3]
		sliceName := parts[len(parts)-1]
		return sliceName, opName, true
	}
	// Fallback for old format or other cases
	opName := getResourceName(decodedPath)
	sliceName, found := getSliceNameFromResource(opName)
	return sliceName, opName, found
}

// getSliceNameFromResource attempts to extract a slice name from a GCE resource name.
func getSliceNameFromResource(resourceName string) (string, bool) {
	if resourceName == "" {
		return defaultSliceName, false // Avoid panic on empty resource names
	}

	prefixes := []string{
		OpPrefixCreatePolicy,
		OpPrefixDeletePolicy,
		OpPrefixCreateMMIG,
		OpPrefixDeleteMMIG,
		OpPrefixAttachMIGs,
		OpPrefixDetachMIGs,
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(resourceName, prefix) {
			return strings.TrimPrefix(resourceName, prefix), true
		}
	}

	if strings.HasPrefix(resourceName, OpPrefixConfigure) {
		remainder := strings.TrimPrefix(resourceName, OpPrefixConfigure)
		lastDash := strings.LastIndex(remainder, "-")
		if lastDash != -1 {
			igmName := remainder[:lastDash]
			sliceName, err := findSliceForIGM(igmName)
			if err == nil {
				return sliceName, true
			}
		}
	}

	// Check if it's a known MIG and map to its slice
	if mig, ok := gceResources.Migs[resourceName]; ok {
		return mig.SliceName, true
	}

	return resourceName, false // Fallback, but indicate no prefix was matched.
}

// getSliceNameFromPolicyName attempts to extract a slice name from a GCE resource policy name.
// A policy name can be either the slice name itself or in the format "sliceName-clusterHash".
// This function iterates through existing slice names to find a match.
func getSliceNameFromPolicyName(policyName string) (string, bool) {
	lastDash := strings.LastIndex(policyName, "-")
	if lastDash != -1 && len(policyName)-lastDash-1 >= 8 {
		return policyName[:lastDash], true
	}
	// Fallback for un-hashed names if any known slice name is a prefix of policyName (backward compatibility)
	for sliceName := range sliceInjections {
		if strings.HasPrefix(policyName, sliceName) {
			return sliceName, true
		}
	}
	return "", false
}

// getResourceFromPath extracts a resource name from a URL path, like a region or zone.
func getResourceFromPath(path, resourceType string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	projectIndex := findString(parts, "projects")
	if projectIndex == -1 {
		return ""
	}
	for i, p := range parts {
		if p == resourceType && i+1 < len(parts) {
			// Return the resource URL relative to the project.
			// Example: projects/my-project/regions/us-central1
			return strings.Join(parts[projectIndex:i+2], "/")
		}
	}
	return ""
}

func findString(slice []string, val string) int {
	for i, item := range slice {
		if item == val {
			return i
		}
	}
	return -1
}

// isHashedVersionOf checks if nameA is a hashed version of nameB.
// e.g., isHashedVersionOf("slice-1-abcde", "slice-1") -> true
func isHashedVersionOf(nameA, nameB string) bool {
	if !strings.HasPrefix(nameA, nameB+"-") {
		return false
	}
	// Extract the supposed hash part that comes after "nameB-"
	hashPart := nameA[len(nameB)+1:]

	// The hash should not be empty, should be at most 8 characters long,
	// and should not contain another hyphen.
	if len(hashPart) > 0 && len(hashPart) <= 8 && !strings.Contains(hashPart, "-") {
		return true
	}
	return false
}
