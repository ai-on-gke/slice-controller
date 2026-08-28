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

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConditionsChanged(t *testing.T) {
	tests := []struct {
		name     string
		oldConds []metav1.Condition
		newConds []metav1.Condition
		expected bool
	}{
		{
			name:     "no conditions",
			oldConds: nil,
			newConds: nil,
			expected: false,
		},
		{
			name:     "condition added",
			oldConds: nil,
			newConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Init"},
			},
			expected: true,
		},
		{
			name: "condition removed",
			oldConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Init"},
			},
			newConds: nil,
			expected: true,
		},
		{
			name: "status changed",
			oldConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionFalse, Reason: "Init"},
			},
			newConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Init"},
			},
			expected: true,
		},
		{
			name: "reason changed",
			oldConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Creating"},
			},
			newConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Created"},
			},
			expected: true,
		},
		{
			name: "only message changed",
			oldConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Created", Message: "Old message"},
			},
			newConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Created", Message: "New message"},
			},
			expected: false,
		},
		{
			name: "only timestamp changed",
			oldConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Created", LastTransitionTime: metav1.Time{}},
			},
			newConds: []metav1.Condition{
				{Type: "TestCondition", Status: metav1.ConditionTrue, Reason: "Created", LastTransitionTime: metav1.Now()},
			},
			expected: false,
		},
		{
			name: "multiple conditions - no meaningful change",
			oldConds: []metav1.Condition{
				{Type: "Cond1", Status: metav1.ConditionTrue, Reason: "R1"},
				{Type: "Cond2", Status: metav1.ConditionFalse, Reason: "R2"},
			},
			newConds: []metav1.Condition{
				{Type: "Cond1", Status: metav1.ConditionTrue, Reason: "R1", Message: "updated"},
				{Type: "Cond2", Status: metav1.ConditionFalse, Reason: "R2", LastTransitionTime: metav1.Now()},
			},
			expected: false,
		},
		{
			name: "multiple conditions - meaningful change in one",
			oldConds: []metav1.Condition{
				{Type: "Cond1", Status: metav1.ConditionTrue, Reason: "R1"},
				{Type: "Cond2", Status: metav1.ConditionFalse, Reason: "R2"},
			},
			newConds: []metav1.Condition{
				{Type: "Cond1", Status: metav1.ConditionTrue, Reason: "R1"},
				{Type: "Cond2", Status: metav1.ConditionTrue, Reason: "R2_updated"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := conditionsChanged(tt.oldConds, tt.newConds)
			assert.Equal(t, tt.expected, result)
		})
	}
}
