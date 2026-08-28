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

package action

import (
	"context"
	"fmt"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/util"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// actionEngine implements the Engine interface.
type actionEngine struct{}

// NewEngine creates a new action engine.
func NewEngine() util.Engine {
	return &actionEngine{}
}

// Execute the action steps
func (e *actionEngine) Execute(ctx context.Context, c client.Client, slice *slicev1beta1.Slice, action util.Action) error {
	logger := klog.FromContext(ctx).WithValues("slice", slice.Name)
	steps := action.Steps()
	for _, step := range steps {
		logger.Info("Executing step", "step", step.Name())
		// not retrying in the execute, instead each step manages its own retry logic,
		// and return an error if all the retries up to a limit fail.
		err := step.Do(ctx, slice)
		if err != nil {
			logger.Error(err, "Failed to execute step", "step", step.Name())
			// Use %w so the Reconciler gets the step name, while preserving the types!
			return fmt.Errorf("step %s failed: %w", step.Name(), err)
		}
		logger.Info("Step executed successfully", "step", step.Name())
	}

	logger.Info("All steps completed successfully")
	return nil
}
