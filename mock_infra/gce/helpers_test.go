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

import "testing"

func TestHelpers(t *testing.T) {
	t.Run("IsHashedVersionOf", testIsHashedVersionOf)
	t.Run("GetSliceNameFromPolicyName", testGetSliceNameFromPolicyName)
}

func testGetSliceNameFromPolicyName(t *testing.T) {
	testCases := []struct {
		name       string
		policyName string
		want       string
		found      bool
	}{
		{
			name:       "Hashed policy name - 8 chars",
			policyName: "my-slice-abcdefgh",
			want:       "my-slice",
			found:      true,
		},
		{
			name:       "Unhashed policy name (fallback)",
			policyName: "my-slice",
			want:       "my-slice",
			found:      true,
		},
	}

	mu.Lock()
	if sliceInjections == nil {
		sliceInjections = make(map[string]*SliceInjections)
	}
	sliceInjections["my-slice"] = &SliceInjections{}
	mu.Unlock()
	defer func() {
		mu.Lock()
		delete(sliceInjections, "my-slice")
		mu.Unlock()
	}()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, found := getSliceNameFromPolicyName(tc.policyName)
			if found != tc.found {
				t.Errorf("getSliceNameFromPolicyName(%q) found = %v, want %v", tc.policyName, found, tc.found)
			}
			if got != tc.want {
				t.Errorf("getSliceNameFromPolicyName(%q) = %q, want %q", tc.policyName, got, tc.want)
			}
		})
	}
}

func testIsHashedVersionOf(t *testing.T) {
	testCases := []struct {
		name  string
		nameA string
		nameB string
		want  bool
	}{
		{
			name:  "Valid hash - 5 chars",
			nameA: "slice-1-abcde",
			nameB: "slice-1",
			want:  true,
		},
		{
			name:  "Valid hash - 1 char",
			nameA: "slice-2-a",
			nameB: "slice-2",
			want:  true,
		},
		{
			name:  "Valid hash - 8 chars",
			nameA: "slice-3-abcdefgh",
			nameB: "slice-3",
			want:  true,
		},
		{
			name:  "Invalid hash - too long (9 chars)",
			nameA: "slice-4-abcdefghi",
			nameB: "slice-4",
			want:  false,
		},
		{
			name:  "Invalid hash - empty hash part",
			nameA: "slice-5-",
			nameB: "slice-5",
			want:  false,
		},
		{
			name:  "Not a hashed version - different prefix",
			nameA: "other-slice-1-abcde",
			nameB: "slice-1",
			want:  false,
		},
		{
			name:  "Not a hashed version - exact match",
			nameA: "slice-6",
			nameB: "slice-6",
			want:  false,
		},
		{
			name:  "Invalid hash - more hash part",
			nameA: "slice-7-0-abcde",
			nameB: "slice-7",
			want:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isHashedVersionOf(tc.nameA, tc.nameB)
			if got != tc.want {
				t.Errorf("isHashedVersionOf(%q, %q) = %v, want %v", tc.nameA, tc.nameB, got, tc.want)
			}
		})
	}
}
