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
	"errors"
	"testing"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var (
	errFailed = errors.New("step failed")
)

// mockStep is a mock implementation of the Step interface for testing.
type mockStep struct {
	name  string
	doErr error
}

func (m *mockStep) Name() string {
	return m.name
}

func (m *mockStep) Do(ctx context.Context, slice *slicev1beta1.Slice) error {
	return m.doErr
}

// mockAction is a mock implementation of the Action interface for testing.
type mockAction struct {
	steps []util.Step
}

func (m *mockAction) Steps() []util.Step {
	return m.steps
}

func TestActionEngine_Execute(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slicev1beta1.AddToScheme(scheme)

	tests := []struct {
		name        string
		action      util.Action
		expectedErr error
	}{
		{
			name: "Execute all steps successfully",
			action: &mockAction{
				steps: []util.Step{
					&mockStep{name: "step1"},
					&mockStep{name: "step2"},
				},
			},
			expectedErr: nil,
		},
		{
			name: "Execute steps, fail on second step",
			action: &mockAction{
				steps: []util.Step{
					&mockStep{name: "step1"},
					&mockStep{name: "step2", doErr: errFailed},
					&mockStep{name: "step3"},
				},
			},
			expectedErr: errFailed,
		},
		{
			name: "No steps",
			action: &mockAction{
				steps: []util.Step{},
			},
			expectedErr: nil,
		},
	}

	engine := NewEngine()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-slice",
					Namespace: "default",
				},
			}

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(slice).Build()

			// Run Execute
			err := engine.Execute(context.Background(), fakeClient, slice, tt.action)

			// Verify Error
			if tt.expectedErr != nil {
				if !errors.Is(err, tt.expectedErr) {
					t.Errorf("%v: expected error '%v', got '%v'", tt.name, tt.expectedErr, err)
				}
			} else if err != nil {
				t.Errorf("%v: expected no error, got %v", tt.name, err)
			}
		})
	}
}
