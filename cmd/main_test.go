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

package main

import (
	"flag"
	"testing"
)

func TestKlogFlagsRegistered(t *testing.T) {
	// "v" and "logtostderr" are registered by klog.InitFlags.
	// Verify that they are present in the CommandLine flag set.
	vFlag := flag.Lookup("v")
	if vFlag == nil {
		t.Errorf("expected flag 'v' to be registered, but it was not")
	}

	logtostderrFlag := flag.Lookup("logtostderr")
	if logtostderrFlag == nil {
		t.Errorf("expected flag 'logtostderr' to be registered, but it was not")
	}
}
