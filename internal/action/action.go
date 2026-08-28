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
	kubehelper "slice-controller/internal/action/kubehelper"
	"slice-controller/internal/util"
)

// StepActions is a generic action that executes a list of steps.
type StepActions struct {
	steps []util.Step
}

func (a *StepActions) Steps() []util.Step {
	return a.steps
}

// NewUnlabelNodesAction creates an action to unlabel nodes.
func NewUnlabelNodesAction(client kubehelper.Client, labelKeys util.NodeLabelKeys, enableHighConcurrency bool) util.Action {
	return &StepActions{
		steps: []util.Step{
			&kubehelper.UnlabelNodeGroupStep{Client: client, LabelKeys: labelKeys, EnableHighConcurrency: enableHighConcurrency},
		},
	}
}

// NewLabelNodesAction creates an action to label nodes.
func NewLabelNodesAction(client kubehelper.Client, labels util.NodeLabels, enableHighConcurrency bool) util.Action {
	return &StepActions{
		steps: []util.Step{
			&kubehelper.LabelNodeGroupStep{Client: client, Labels: labels, EnableHighConcurrency: enableHighConcurrency},
		},
	}
}
