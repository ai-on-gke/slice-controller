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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

// TestRetryGCECall_SuccessOnFirstAttempt verifies that the function returns successfully
// when the first API call succeeds.
func TestRetryGCECall_SuccessOnFirstAttempt(t *testing.T) {
	attempts := 0
	call := func() (string, error) {
		attempts++
		return "success", nil
	}

	result, err := RetryGCECall(context.Background(), call)

	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, 1, attempts, "The call should succeed on the first attempt")
}

// TestRetryGCECall_SuccessOnSecondAttempt verifies that the function retries and
// succeeds on the second attempt.
func TestRetryGCECall_SuccessOnSecondAttempt(t *testing.T) {
	attempts := 0
	call := func() (string, error) {
		attempts++
		if attempts < 2 {
			return "", &googleapi.Error{Code: 500, Message: "Temporary Server Error"}
		}
		return "success", nil
	}

	result, err := RetryGCECall(context.Background(), call)

	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, 2, attempts, "The call should succeed on the second attempt")
}

// TestRetryGCECall_FailureWithContextTimeout verifies that the retry loop is
// correctly terminated by context cancellation.
func TestRetryGCECall_FailureWithContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	attempts := 0
	call := func() (string, error) {
		attempts++
		// This select statement simulates work but respects the context deadline.
		select {
		case <-time.After(150 * time.Millisecond):
			// This will not be reached if the context times out correctly.
			return "should not reach here", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	result, err := RetryGCECall(ctx, call)

	assert.Error(t, err, "An error should be returned due to context timeout")
	// The error from backoff wraps the context error.
	assert.ErrorIs(t, err, context.DeadlineExceeded, "The error should be a context deadline exceeded error")
	var zero string
	assert.Equal(t, zero, result, "The result should be the zero value on failure")
	assert.Equal(t, 1, attempts, "The function should only be called once before timeout")
}

// TestRetryGCECall_IgnoredError verifies that if the API returns an error code
// that is in the ignore list, the function returns successfully with a zero value.
func TestRetryGCECall_IgnoredError(t *testing.T) {
	attempts := 0
	call := func() (*int, error) {
		attempts++
		return nil, &googleapi.Error{Code: ResourceNotFound, Message: "Not Found"}
	}

	result, err := RetryGCECall(context.Background(), call, ResourceNotFound)

	assert.NoError(t, err, "The ignored error should not be returned")
	assert.Nil(t, result, "The result should be nil for a pointer type on ignored error")
	assert.Equal(t, 1, attempts, "The call should not be retried for an ignored error")
}

// TestRetryGCECall_FailureAfterMaxRetries verifies that the function returns an
// error after exhausting all retry attempts.
func TestRetryGCECall_FailureAfterMaxRetries(t *testing.T) {
	attempts := 0
	call := func() (string, error) {
		attempts++
		return "", &googleapi.Error{Code: 503, Message: "Service Unavailable"}
	}

	result, err := RetryGCECall(context.Background(), call)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "gce api call failed after retries")
	assert.Contains(t, err.Error(), "Service Unavailable")
	var zero string
	assert.Equal(t, zero, result)
	// 1 initial call + GCERetryTimes
	assert.Equal(t, GCERetryTimes+1, attempts, fmt.Sprintf("The call should be attempted %v times in total", GCERetryTimes+1))
}

// TestRetryGCECall_NonGoogleAPIError verifies that non-googleapi errors are also retried.
func TestRetryGCECall_NonGoogleAPIError(t *testing.T) {
	attempts := 0
	call := func() (string, error) {
		attempts++
		return "", errors.New("a generic network error")
	}

	_, err := RetryGCECall(context.Background(), call)

	assert.Error(t, err)
	assert.Equal(t, GCERetryTimes+1, attempts, fmt.Sprintf("The call should be attempted %v times in total", GCERetryTimes+1))
}

// TestRetryGCECall_PermanentErrors verifies that specific permanent errors (400, 401, 403)
// are not retried and return immediately.
func TestRetryGCECall_PermanentErrors(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		message string
	}{
		{name: "FailedPrecondition", code: FailedPrecondition, message: "Failed Precondition"},
		{name: "Unauthenticated", code: Unauthenticated, message: "Unauthenticated"},
		{name: "PermissionDenied", code: PermissionDenied, message: "Permission Denied"},
	}

	for _, tt := range tests {
		tc := tt // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			gceErr := &googleapi.Error{Code: tc.code, Message: tc.message}
			call := func() (string, error) {
				attempts++
				return "", gceErr
			}

			_, err := RetryGCECall(context.Background(), call)

			assert.Error(t, err)
			assert.ErrorIs(t, err, gceErr, "The error should be the original GCE API error")
			assert.Equal(t, 1, attempts, "The call should not be retried for %s error", tc.name)
		})
	}
}

func TestRetryGCECall_DetailedAPIError(t *testing.T) {
	attempts := 0
	call := func() (string, error) {
		attempts++
		return "", &googleapi.Error{
			Code:    400,
			Message: "Bad Request",
			Errors: []googleapi.ErrorItem{
				{Reason: "invalidParameter", Message: "Parameter X is invalid"},
				{Reason: "missingRequiredField", Message: "Field Y is required"},
			},
		}
	}

	_, err := RetryGCECall(context.Background(), call)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "gce api call failed after retries")
	assert.Contains(t, err.Error(), "code: 400")
	assert.Contains(t, err.Error(), "message: Bad Request")
	assert.Contains(t, err.Error(), "details: [{Reason: invalidParameter, Message: Parameter X is invalid}, {Reason: missingRequiredField, Message: Field Y is required}]")
	assert.Equal(t, 1, attempts, "Permanent error (400) should not be retried")
}
