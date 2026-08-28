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

package health

import (
	"fmt"
	"strings"

	"slice-controller/internal/cache"
	"slice-controller/internal/util"
	"google.golang.org/api/googleapi"
)

// isPermanentError checks if an error from GCE API is permanent.
func isPermanentError(err error) bool {
	gerr, ok := err.(*googleapi.Error)
	if !ok {
		return false
	}
	switch gerr.Code {
	case util.FailedPrecondition, util.ResourceNotFound, util.ResourceAlreadyExists, util.PermissionDenied, util.Unauthenticated:
		return true
	default:
		return false
	}
}

// isNotFoundError checks if an error from GCE API is a Not Found error.
func isNotFoundError(err error) bool {
	gerr, ok := err.(*googleapi.Error)
	if !ok {
		return false
	}
	return gerr.Code == util.ResourceNotFound
}

// parseMmigDetails extracts the MMIG name and region from its full GCE URL.
// parseMmigDetails extracts the project, region, and MMIG name from its full GCE URL.
func parseMmigDetails(url string) (project, region, name string, err error) {
	// e.g., "projects/p/regions/r/multiMigs/mmig-name"
	if url == "" {
		// Return 4 values to match signature
		return "", "", "", fmt.Errorf("empty MMIG URL")
	}
	parts := strings.Split(url, "/")

	var projectIdx, regionIdx, mmigIdx int = -1, -1, -1

	for i, part := range parts {
		switch part {
		case "projects": // Added this case
			if i+1 < len(parts) {
				project = parts[i+1]
				projectIdx = i
			}
		case "regions":
			if i+1 < len(parts) {
				region = parts[i+1]
				regionIdx = i
			}
		case "multiMigs":
			if i+1 < len(parts) {
				name = parts[i+1]
				mmigIdx = i
			}
		}
	}

	// Updated validation to check for all three parts
	if project == "" || region == "" || name == "" {
		return "", "", "", fmt.Errorf("could not parse project, region, and/or MMIG name from URL: %s", url)
	}

	// This single check ensures all parts were found and are in the correct order.
	// It replaces all the previous, separate index and 'Contains' checks.
	if !(projectIdx < regionIdx && regionIdx < mmigIdx) {
		return "", "", "", fmt.Errorf("invalid MMIG URL format, expected 'projects/.../regions/.../multiMigs/...': %s", url)
	}

	return project, region, name, nil
}

// aggregateTopologyHealth converts the detailed topology health map into a single,
// overall MIGHealthStatus.
func aggregateTopologyHealth(topologies *cache.AcceleratorTopologies) (cache.MIGHealthStatus, error) {
	if topologies == nil || len(topologies.TopologiesInfo) == 0 {
		return cache.MIGHealthStatusUnknown, fmt.Errorf("no topology health info")
	}

	hasFullTopology := false
	hasDegraded := false
	hasOnlyHealthy := true

	for _, topoInfo := range topologies.TopologiesInfo {
		if util.IsFullTopology(topoInfo.AcceleratorTopology) {
			hasFullTopology = true
		}

		status := topoInfo.AcceleratorTopologyHealthStatus

		switch status {
		case cache.MIGHealthStatusUnhealthy:
			// Fail fast if ANY topology is unhealthy
			return cache.MIGHealthStatusUnhealthy, nil

		case cache.MIGHealthStatusDegraded:
			hasDegraded = true
			hasOnlyHealthy = false

		case cache.MIGHealthStatusHealthy:
			// Healthy is good, continue checking others.
		default:
			// Unknown or other states mean it's not "Only Healthy"
			hasOnlyHealthy = false
		}
	}

	if !hasFullTopology {
		return cache.MIGHealthStatusUnknown, fmt.Errorf("missing expected full topology health info")
	}

	if hasDegraded {
		return cache.MIGHealthStatusDegraded, nil
	}

	if hasOnlyHealthy {
		return cache.MIGHealthStatusHealthy, nil
	}

	return cache.MIGHealthStatusUnknown, nil
}

// IsCreationFinalState checks if the given MMIG health state is a creation final state.
func IsFinalState(state string) bool {
	return state == string(cache.MMIGHealthStatusActive) ||
		state == string(cache.MMIGHealthStatusActiveDegraded) ||
		state == string(cache.MMIGHealthStatusFailed)
}
