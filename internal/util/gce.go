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
	"crypto/md5"
	"errors"
	"fmt"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/api/googleapi"
	"k8s.io/apimachinery/pkg/types"
)

// MMIGHealthStatus defines the health of an MMIG.
type MMIGHealthStatus string

// MIGHealthStatus defines the health of a MIG.
type MIGHealthStatus string

type OperationType int

const (

	// Op status
	OperationDone = "DONE"

	// API error code
	ResourceAlreadyExists = 409
	ResourceNotFound      = 404
	FailedPrecondition    = 400
	PermissionDenied      = 403
	Unauthenticated       = 401

	// param
	GCERetryTimes = 2

	// MMIGHealthStatusIncomplete indicates the MMIG is incomplete.
	MMIGHealthStatusIncomplete MMIGHealthStatus = "INCOMPLETE"
	// MMIGHealthStatusActivating indicates the MMIG is activating.
	MMIGHealthStatusActivating MMIGHealthStatus = "ACTIVATING"
	// MMIGHealthStatusActive indicates the MMIG is active.
	MMIGHealthStatusActive MMIGHealthStatus = "ACTIVE"
	// MMIGHealthStatusActiveDegraded indicates the MMIG is active but degraded.
	MMIGHealthStatusActiveDegraded MMIGHealthStatus = "ACTIVE_DEGRADED"
	// MMIGHealthStatusDeactivating indicates the MMIG is deactivating.
	MMIGHealthStatusDeactivating MMIGHealthStatus = "DEACTIVATING"
	// MMIGHealthStatusFailed indicates the MMIG has failed.
	MMIGHealthStatusFailed MMIGHealthStatus = "FAILED"
	// MMIGHealthStatusUnknown indicates the MMIG health is unknown.
	MMIGHealthStatusUnknown MMIGHealthStatus = "UNKNOWN"
	// MIGHealthStatusUnknown indicates the MIG health is unknown.
	MIGHealthStatusUnknown MIGHealthStatus = "UNKNOWN"

	// Patch operation type
	AttachOperation     OperationType = 0
	DetachOperation     OperationType = 1
	ActivateOperation   OperationType = 2
	DeactivateOperation OperationType = 3
)

// GCEAPIFunc is a function that makes a GCE API call and returns a result and an error.
type GCEAPIFunc[T any] func() (T, error)

// RetryGCECall retries a GCE API call with exponential backoff.
// It can ignore certain error codes, in which case it returns a nil error and a zero value for the result.
// TODO: set appropriate gce op timeout
func RetryGCECall[T any](ctx context.Context, call GCEAPIFunc[T], ignoreErrorCodes ...int) (T, error) {
	var result T
	var zero T

	operation := func() error {
		var err error
		result, err = call()
		if err != nil {
			var gerr *googleapi.Error
			if errors.As(err, &gerr) {
				for _, code := range ignoreErrorCodes {
					if gerr.Code == code {
						result = zero
						return nil // Stop retrying, return success.
					}
				}
				if gerr.Code == FailedPrecondition || gerr.Code == Unauthenticated || gerr.Code == PermissionDenied {
					return backoff.Permanent(err)
				}
			}
		}
		return err // Let backoff decide if it's retryable.
	}

	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0 // No time limit for retries, relies on context for timeout.

	// Using WithMaxRetries to limit the number of attempts.
	err := backoff.Retry(operation, backoff.WithContext(backoff.WithMaxRetries(bo, GCERetryTimes), ctx))

	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok {
			var errStrs []string
			for _, e := range gerr.Errors {
				errStrs = append(errStrs, fmt.Sprintf("{Reason: %s, Message: %s}", e.Reason, e.Message))
			}
			return zero, fmt.Errorf("gce api call failed after retries: code: %d, message: %s, details: [%s]: %w", gerr.Code, gerr.Message, strings.Join(errStrs, ", "), err)
		}
		return zero, fmt.Errorf("gce api call failed after retries: %w", err)
	}

	return result, nil
}

// GenerateRequestID creates a unique, deterministic, RFC 4122 compliant UUID (Version 3).
// It safely combines the Slice UID, generation, operation type, resource name, and retry attempt.
func GenerateRequestID(uid types.UID, generation int64, resourceName string, opType OperationType, retryAttempt int) string {
	// 1. Create a unique, deterministic seed string
	seed := fmt.Sprintf("%s-%d-%d-%s-%d", uid, generation, opType, resourceName, retryAttempt)

	// 2. Hash the seed using MD5 to get exactly 16 bytes
	hash := md5.Sum([]byte(seed))

	// 3. Set the UUID Version (3) and Variant (RFC 4122) bits
	hash[6] = (hash[6] & 0x0f) | 0x30 // Set version to 3 (Name-based MD5)
	hash[8] = (hash[8] & 0x3f) | 0x80 // Set variant to RFC 4122

	// 4. Format as standard UUID: 8-4-4-4-12
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		hash[0:4],
		hash[4:6],
		hash[6:8],
		hash[8:10],
		hash[10:16],
	)
}
