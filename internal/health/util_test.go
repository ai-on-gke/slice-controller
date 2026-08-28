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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"slice-controller/internal/cache"
	"slice-controller/internal/util"
	"google.golang.org/api/googleapi"
)

func TestParseMmigDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		url           string
		expectProject string
		expectRegion  string
		expectName    string
		expectError   bool
		errorMsg      string
	}{
		{
			name:          "valid MMIG URL",
			url:           "projects/my-project/regions/us-central1/multiMigs/my-mmig",
			expectProject: "my-project",
			expectRegion:  "us-central1",
			expectName:    "my-mmig",
			expectError:   false,
		},
		{
			name:          "valid MMIG URL with other segments",
			url:           "foo/bar/projects/gke-prod/regions/europe-west4/multiMigs/main-cluster-mmig/something/else",
			expectProject: "gke-prod",
			expectRegion:  "europe-west4",
			expectName:    "main-cluster-mmig",
			expectError:   false,
		},
		{
			name:        "empty URL",
			url:         "",
			expectError: true,
			errorMsg:    "empty MMIG URL",
		},
		{
			name:        "missing region",
			url:         "projects/my-project/multiMigs/my-mmig",
			expectError: true,
			errorMsg:    "could not parse project, region, and/or MMIG name",
		},
		{
			name:        "missing multiMigs",
			url:         "projects/my-project/regions/us-central1/my-mmig",
			expectError: true,
			errorMsg:    "could not parse project, region, and/or MMIG name",
		},
		{
			name:        "missing projects segment",
			url:         "regions/us-central1/multiMigs/my-mmig",
			expectError: true,
			errorMsg:    "could not parse project, region, and/or MMIG name",
		},
		{
			name:        "regions after multiMigs (invalid order)",
			url:         "projects/my-project/multiMigs/my-mmig/regions/us-central1",
			expectError: true,
			errorMsg:    "invalid MMIG URL format", // Updated to match new error
		},
		{
			name:        "missing MMIG name",
			url:         "projects/my-project/regions/us-central1/multiMigs/",
			expectError: true,
			errorMsg:    "could not parse project, region, and/or MMIG name",
		},
		{
			name:        "missing region name",
			url:         "projects/my-project/regions//multiMigs/my-mmig",
			expectError: true,
			errorMsg:    "could not parse project, region, and/or MMIG name",
		},
		{
			name:        "missing project name",
			url:         "projects//regions/us-central1/multiMigs/my-mmig",
			expectError: true,
			errorMsg:    "could not parse project, region, and/or MMIG name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			project, region, name, err := parseMmigDetails(tt.url)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectProject, project)
				assert.Equal(t, tt.expectRegion, region)
				assert.Equal(t, tt.expectName, name)
			}
		})
	}
}

func TestParseMigDetails(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		expectName  string
		expectZone  string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid MIG URL",
			url:         "projects/my-project/zones/us-central1-a/instanceGroupManagers/my-mig",
			expectName:  "my-mig",
			expectZone:  "us-central1-a",
			expectError: false,
		},
		{
			name:        "valid MIG URL with trailing slash",
			url:         "projects/my-project/zones/us-west1-b/instanceGroupManagers/test-mig/",
			expectName:  "test-mig",
			expectZone:  "us-west1-b",
			expectError: false,
		},
		{
			name:        "empty URL",
			url:         "",
			expectError: true,
			errorMsg:    "empty MIG URL",
		},
		{
			name:        "missing zone",
			url:         "projects/my-project/instanceGroupManagers/my-mig",
			expectError: true,
			errorMsg:    "could not parse MIG name and/or zone",
		},
		{
			name:        "missing instanceGroupManagers",
			url:         "projects/my-project/zones/us-central1-a/my-mig",
			expectError: true,
			errorMsg:    "could not parse MIG name and/or zone",
		},
		{
			name:        "missing MIG name",
			url:         "projects/my-project/zones/us-central1-a/instanceGroupManagers/",
			expectError: true,
			errorMsg:    "could not parse MIG name and/or zone",
		},
		{
			name:        "missing zone name",
			url:         "projects/my-project/zones//instanceGroupManagers/my-mig",
			expectError: true,
			expectZone:  "could not parse MMIG name and/or zones",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, zone, err := util.ParseMigDetails(tt.url)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectName, name)
				assert.Equal(t, tt.expectZone, zone)
			}
		})
	}
}

func TestAggregateTopologyHealth(t *testing.T) {
	tests := []struct {
		name           string
		topologies     *cache.AcceleratorTopologies
		expectedStatus cache.MIGHealthStatus
		expectedErr    error
	}{
		{
			name:           "Nil topologies",
			topologies:     nil,
			expectedStatus: cache.MIGHealthStatusUnknown,
			expectedErr:    fmt.Errorf("no topology health info"),
		},
		{
			name: "Empty topologies info",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{},
			},
			expectedStatus: cache.MIGHealthStatusUnknown,
			expectedErr:    fmt.Errorf("no topology health info"),
		},
		{
			name: "Single Healthy 4x4x4",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"4x4x4": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
				},
			},
			expectedStatus: cache.MIGHealthStatusHealthy,
			expectedErr:    nil,
		},
		{
			name: "Missing full topology",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"2x2x1": {AcceleratorTopology: "2x2x1", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
				},
			},
			expectedStatus: cache.MIGHealthStatusUnknown,
			expectedErr:    fmt.Errorf("missing expected full topology health info"),
		},
		{
			name: "Any Unhealthy returns Unhealthy immediately",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"4x4x4": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
					"2x2x4": {AcceleratorTopology: "2x2x4", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusUnhealthy},
				},
			},
			expectedStatus: cache.MIGHealthStatusUnhealthy,
			expectedErr:    nil,
		},
		{
			name: "Degraded if any are Degraded and none Unhealthy",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"4x4x4": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
					"2x2x1": {AcceleratorTopology: "2x2x1", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusDegraded},
				},
			},
			expectedStatus: cache.MIGHealthStatusDegraded,
			expectedErr:    nil,
		},
		{
			name: "Unknown if 4x4x4 present but other statuses are unknown (and not Unhealthy/Degraded)",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"4x4x4": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
					"2x2x2": {AcceleratorTopology: "2x2x2", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusUnknown},
				},
			},
			expectedStatus: cache.MIGHealthStatusUnknown,
			expectedErr:    nil,
		},
		{
			name: "Multiple Healthy Topologies including 4x4x4",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"4x4x4": {AcceleratorTopology: "4x4x4", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
					"2x2x4": {AcceleratorTopology: "2x2x4", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
					"2x2x1": {AcceleratorTopology: "2x2x1", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
				},
			},
			expectedStatus: cache.MIGHealthStatusHealthy,
			expectedErr:    nil,
		},
		{
			name: "Single Healthy 16x16",
			topologies: &cache.AcceleratorTopologies{
				TopologiesInfo: map[string]cache.AcceleratorTopologyInfo{
					"16x16": {AcceleratorTopology: "16x16", AcceleratorTopologyHealthStatus: cache.MIGHealthStatusHealthy},
				},
			},
			expectedStatus: cache.MIGHealthStatusHealthy,
			expectedErr:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, err := aggregateTopologyHealth(tc.topologies)

			if status != tc.expectedStatus {
				t.Errorf("aggregateTopologyHealth() status = %v, want %v", status, tc.expectedStatus)
			}

			if (err != nil && tc.expectedErr == nil) || (err == nil && tc.expectedErr != nil) || (err != nil && tc.expectedErr != nil && err.Error() != tc.expectedErr.Error()) {
				t.Errorf("aggregateTopologyHealth() error = %v, want %v", err, tc.expectedErr)
			}
		})
	}
}

func TestIsPermanentError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "400 bad request (permanent)",
			err:      &googleapi.Error{Code: 400},
			expected: true,
		},
		{
			name:     "404 not found (permanent)",
			err:      &googleapi.Error{Code: 404},
			expected: true,
		},
		{
			name:     "409 already exists (permanent)",
			err:      &googleapi.Error{Code: 409},
			expected: true,
		},
		{
			name:     "403 forbidden (permanent)",
			err:      &googleapi.Error{Code: 403},
			expected: true,
		},
		{
			name:     "401 unauthorized (permanent)",
			err:      &googleapi.Error{Code: 401},
			expected: true,
		},
		{
			name:     "500 internal server error (transient)",
			err:      &googleapi.Error{Code: 500},
			expected: false,
		},
		{
			name:     "generic error (transient)",
			err:      fmt.Errorf("random error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isPermanentError(tt.err))
		})
	}
}

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "404 not found",
			err:      &googleapi.Error{Code: 404},
			expected: true,
		},
		{
			name:     "400 bad request",
			err:      &googleapi.Error{Code: 400},
			expected: false,
		},
		{
			name:     "generic error",
			err:      fmt.Errorf("not found"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isNotFoundError(tt.err))
		})
	}
}
