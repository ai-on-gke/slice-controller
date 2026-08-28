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

// util/errors.go

package util

import (
	"errors"
	"fmt"
	"strings"
)

// APICallError indicates a failure when sending the initial HTTP request to GCE.
type APICallError struct {
	Err error
}

func (e *APICallError) Error() string {
	return fmt.Sprintf("GCE API call error: %v", e.Err)
}

func (e *APICallError) Unwrap() error {
	return e.Err
}

// TimeoutError indicates that the client context cancelled or timed out
// while waiting for an asynchronous operation to complete.
type TimeoutError struct {
	Message string
}

func (e *TimeoutError) Error() string {
	return e.Message
}

// OperationError indicates the GCE API call succeeded, but the resulting async operation failed.
type OperationError struct {
	Err error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("GCE operation error: %v", e.Err)
}

func (e *OperationError) Unwrap() error {
	return e.Err
}

// GCEErrorDetail holds the specific code and message from a failed GCE operation.
type GCEErrorDetail struct {
	Code    string
	Message string
}

// GCEOperationFailureError represents a completed GCE operation that finished with an error status.
type GCEOperationFailureError struct {
	OperationName string
	Details       []GCEErrorDetail
}

func (e *GCEOperationFailureError) Error() string {
	errStrs := make([]string, 0, len(e.Details))
	for _, d := range e.Details {
		errStrs = append(errStrs, fmt.Sprintf("{Code: %s, Message: %s}", d.Code, d.Message))
	}
	return fmt.Sprintf("operation %s failed: %s", e.OperationName, strings.Join(errStrs, ", "))
}

var ErrDeformationSkipped = errors.New("deformation skipped because MMIG has no members")
