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
	"testing"

	"slice-controller/internal/action/kubehelper"
	"slice-controller/internal/util"
)

func TestNewUnlabelNodesAction(t *testing.T) {
	action := NewUnlabelNodesAction(nil, util.NodeLabelKeys{RegularLabels: []string{"label1"}}, false)
	steps := action.Steps()
	if len(steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(steps))
	}
	if steps[0].Name() != kubehelper.UnlabelNodeGroupStepName {
		t.Errorf("Expected step %s, got %s", kubehelper.UnlabelNodeGroupStepName, steps[0].Name())
	}
}

func TestNewLabelNodesAction(t *testing.T) {
	action := NewLabelNodesAction(nil, util.NodeLabels{RegularLabels: map[string]string{"key": "val"}}, false)
	steps := action.Steps()
	if len(steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(steps))
	}
	if steps[0].Name() != kubehelper.LabelNodeGroupStepName {
		t.Errorf("Expected step %s, got %s", kubehelper.LabelNodeGroupStepName, steps[0].Name())
	}
}
