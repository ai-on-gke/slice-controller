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

	slicev1beta1 "slice-controller/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Condition list
	NodeLabeled = "NodeLabeled"

	// Reason list
	// Initial Reason of the slice
	SliceNotCreated = "SliceNotCreated"
	// Unable to call GCE API to fetch slice status
	SliceHealthProbeFailed = "SliceHealthProbeFailed"
	NodeLabelIsAdded       = "NodeLabelIsAdded"
	NodeLabelIsRemoved     = "NodeLabelIsRemoved"

	// Failure reason
	SliceCreationFailed = "SliceCreationFailed"
	// Failure message
	MMIGNotFound        = "MMIGNotFound"
	PreflightCheckError = "PreflightError"

	// State condition message
	SliceFormationInProgress = "SliceFormationInProgress"
	NodeLabelingCompleted    = "NodeLabelingCompleted"

	// Dynamic slice status states
	DynamicSliceActive       = "ACTIVE"
	DynamicSliceActivating   = "ACTIVATING"
	DynamicSliceFailed       = "FAILED"
	DynamicSliceDeactivating = "DEACTIVATING"
	DynamicSliceInactive     = "INACTIVE"
	// NumNodesInCube defines the number of nodes in a cube.
	NumNodesInCube = 16

	// RetryAnnotation is the annotation key used to indicate whether to retry slice formation on failure.
	RetryAnnotation       = "slice.gke.io/retry-on-failure"
	EnableRetryAnnotation = "true"
)

// Step represents a single step in an action.
type Step interface {
	// Do performs the action of the step.
	Do(ctx context.Context, slice *slicev1beta1.Slice) error
	// Name returns the name of the step for logging.
	Name() string
}

// Action represents a transactional operation composed of multiple steps.
type Action interface {
	// Steps returns the list of steps to be executed.
	Steps() []Step
}

type ActionType string

// Engine executes an action.
type Engine interface {
	// Execute the action steps
	Execute(ctx context.Context, client client.Client, slice *slicev1beta1.Slice, action Action) error
}

// CubeInfo holds information about a cube's MIG and zone.
type CubeInfo struct {
	MigName     string
	Zone        string
	PartitionId string
}
