/*
Copyright 2025 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/util"
)

// nolint:unused
// log is for logging in this package.
var (
	slicelog = logf.Log.WithName("slice-resource")
)

// SetupSliceWebhookWithManager registers the webhook for Slice in the manager.
func SetupSliceWebhookWithManager(mgr ctrl.Manager, enableSliceUpdate, supportSubsliceUpdate bool) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&slicev1beta1.Slice{}).
		WithValidator(&SliceCustomValidator{
			EnableSliceUpdate:     enableSliceUpdate,
			SupportSubsliceUpdate: supportSubsliceUpdate,
		}).
		WithDefaulter(&SliceCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-accelerator-gke-io-v1beta1-slice,mutating=true,failurePolicy=fail,sideEffects=None,groups=accelerator.gke.io,resources=slices,verbs=create;update,versions=v1beta1,name=mslice-v1beta1.kb.io,admissionReviewVersions=v1

// SliceCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Slice when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type SliceCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &SliceCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Slice.
func (d *SliceCustomDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	slice, ok := obj.(*slicev1beta1.Slice)

	if !ok {
		return fmt.Errorf("expected an Slice object but got %T", obj)
	}
	slicelog.Info("Defaulting for Slice", "name", slice.GetName())

	// TODO(user): fill in your defaulting logic.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-accelerator-gke-io-v1beta1-slice,mutating=false,failurePolicy=fail,sideEffects=None,groups=accelerator.gke.io,resources=slices,verbs=create;update,versions=v1beta1,name=vslice-v1beta1.kb.io,admissionReviewVersions=v1

// SliceCustomValidator struct is responsible for validating the Slice resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type SliceCustomValidator struct {
	EnableSliceUpdate     bool
	SupportSubsliceUpdate bool
}

var _ webhook.CustomValidator = &SliceCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Slice.
func (v *SliceCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	slice, ok := obj.(*slicev1beta1.Slice)
	if !ok {
		return nil, fmt.Errorf("expected a Slice object but got %T", obj)
	}
	slicelog.Info("Validation for Slice upon creation", "name", slice.GetName())

	return nil, validateSlice(slice)
}

// validateCronJob validates the fields of a CronJob object.
func validateSlice(slice *slicev1beta1.Slice) error {
	var allErrs field.ErrorList
	if err := validateSliceAccelerator(slice); err != nil {
		allErrs = append(allErrs, err)
	}
	if err := validatePartitionIds(slice); err != nil {
		allErrs = append(allErrs, err)
	}
	if err := validateSliceName(slice); err != nil {
		allErrs = append(allErrs, err)
	}

	if len(allErrs) == 0 {
		return nil
	}
	return allErrs.ToAggregate()
}

func validateSliceAccelerator(slice *slicev1beta1.Slice) *field.Error {
	acceleratorType := slice.Spec.Type
	topology := slice.Spec.Topology
	partitionIdLength := len(slice.Spec.PartitionIds)

	var allowedAcceleratorTypes = map[string]bool{
		string(slicev1beta1.TypeTpu7x): true,
	}

	if _, ok := allowedAcceleratorTypes[string(acceleratorType)]; !ok {
		return field.Invalid(field.NewPath("spec", "type"), acceleratorType, fmt.Sprintf("%s is not supported", acceleratorType))
	}

	// Validation logic based on type.
	switch acceleratorType {
	case "tpu7x":
		if err := validateTpu7x(topology, partitionIdLength, string(acceleratorType)); err != nil {
			return field.Invalid(field.NewPath("spec", "topology"), topology, err.Error())
		}
		return nil
	default:
		return field.Invalid(field.NewPath("spec", "type"), acceleratorType, "Unknown accelerator type")
	}
}

func validatePartitionIds(slice *slicev1beta1.Slice) *field.Error {
	seen := make(map[string]bool)
	for i, partitionId := range slice.Spec.PartitionIds {
		if partitionId == "" {
			return field.Invalid(field.NewPath("spec", "partitionIds").Index(i), partitionId, "partition ID cannot be empty")
		}
		if _, ok := seen[partitionId]; ok {
			return field.Duplicate(field.NewPath("spec", "partitionIds"), partitionId)
		}
		seen[partitionId] = true
	}
	return nil
}

// Validate that the slice name cannot be more than 49 characters long.
// This is because, for Linked Runners, we will add the label gke-tpu-slice-<slice-name>
// to the control node (63-14=49). For non-linked VM cases, we append a short hash of 8
// characters, so the maximum length could be 63-9=54. However, since there isn't a way
// to distinguish between linked runner vs non-linked runner in the slice CR, we simply
// enforce a limit of 49 in both cases.
// The name must also match the regular expression [a-z]([-a-z0-9]*[a-z0-9])?
// which means the first character must be a lowercase letter, and all following
// characters must be a dash, lowercase letter, or digit, except the last
// character, which cannot be a dash.
func validateSliceName(slice *slicev1beta1.Slice) *field.Error {
	if len(slice.Name) > 49 {
		return field.Invalid(
			field.NewPath("metadata", "name"),
			slice.Name, "must be no more than 49 characters")
	}
	re := regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)
	if !re.MatchString(slice.Name) {
		return field.Invalid(
			field.NewPath("metadata", "name"),
			slice.Name, `must match the regex [a-z]([-a-z0-9]*[a-z0-9])?`)
	}

	return nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Slice.
func (v *SliceCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	slice, ok := newObj.(*slicev1beta1.Slice)
	if !ok {
		return nil, fmt.Errorf("expected a Slice object for the newObj but got %T", newObj)
	}
	slicelog.Info("Validation for Slice upon update", "name", slice.GetName())

	oldSlice, ok := oldObj.(*slicev1beta1.Slice)
	if !ok {
		return nil, fmt.Errorf("expected an old Slice object but got %T", oldObj)
	}

	// If slice update is disabled, block any change to PartitionIds
	if !v.EnableSliceUpdate {
		if !reflect.DeepEqual(oldSlice.Spec.PartitionIds, slice.Spec.PartitionIds) {
			return nil, fmt.Errorf("slice update is disabled; cannot update partitionIds")
		}
	}

	// Reject sub-slice updates
	if util.IsSubslice(&slice.Spec) {
		if !v.SupportSubsliceUpdate {
			if !reflect.DeepEqual(oldSlice.Spec.PartitionIds, slice.Spec.PartitionIds) {
				return nil, fmt.Errorf("updating partitions for sub-slice is not supported for now")
			}
		}
	}

	// Enforce immutability of specific spec fields
	if slice.Spec.Topology != oldSlice.Spec.Topology {
		return nil, fmt.Errorf("topology is immutable, cannot change from %s to %s", oldSlice.Spec.Topology, slice.Spec.Topology)
	}
	if slice.Spec.Type != oldSlice.Spec.Type {
		return nil, fmt.Errorf("type is immutable, cannot change from %s to %s", oldSlice.Spec.Type, slice.Spec.Type)
	}

	oldCount := len(oldSlice.Spec.PartitionIds)
	newCount := len(slice.Spec.PartitionIds)

	// Enforce 0->N or N->0 transitions for partitionIds
	if oldCount > 0 && newCount > 0 {
		if !reflect.DeepEqual(oldSlice.Spec.PartitionIds, slice.Spec.PartitionIds) {
			if !(v.SupportSubsliceUpdate && util.IsSubslice(&slice.Spec)) {
				return nil, fmt.Errorf("updating partitions from N to M is not supported; only 0 to N or N to 0 transitions are allowed")
			}
		}
	}

	// Sub-slice specific validations
	if util.IsSubslice(&slice.Spec) {
		if newCount > 1 {
			return nil, fmt.Errorf("sub-slice cannot have more than 1 partition")
		}
	}

	return nil, validateSlice(slice)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Slice.
func (v *SliceCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	slice, ok := obj.(*slicev1beta1.Slice)
	if !ok {
		return nil, fmt.Errorf("expected a Slice object but got %T", obj)
	}
	slicelog.Info("Validation for Slice upon deletion", "name", slice.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}

// validateTpu7x handles all validation rules for the 'tpu7x' type.
func validateTpu7x(topology string, partitionIdLength int, tpuType string) error {
	// Parse the topology string (e.g., "4x4x4") into integer dimensions.
	dims, err := parseDimensions(topology)
	if err != nil {
		return err // Pass up parsing errors (e.g., non-positive numbers)
	}

	// Allow specific small topologies that don't follow the general rules.
	allowedSmallTopologies := map[string]bool{
		"2x2x1": true,
		"2x2x2": true,
		"2x2x4": true,
		"2x4x4": true,
	}

	if allowedSmallTopologies[topology] {
		// Check if it is an empty slice creation, which is allowed for small topologies.
		if partitionIdLength == 0 {
			return nil
		}

		// Normal subslicing expects exactly 1 partition.
		if partitionIdLength != 1 {
			return fmt.Errorf("there should be exactly 1 partition, but found: %d", partitionIdLength)
		}
		return nil
	}

	// Check: tpu must be AxBxC (3 dimensions)
	if len(dims) != 3 {
		return fmt.Errorf("%s topology format must be AxBxC (e.g., '4x4x4'), got '%s'", tpuType, topology)
	}

	x, y, z := dims[0], dims[1], dims[2]

	// Check: X, Y, Z should be 4N (multiples of 4).
	if x%4 != 0 || y%4 != 0 || z%4 != 0 {
		return fmt.Errorf("%s dimensions must be multiples of 4, got %d_x_%d_x_%d", tpuType, x, y, z)
	}

	// Check: X <= Y <= Z
	if !(x <= y && y <= z) {
		return fmt.Errorf("%s dimensions must satisfy X <= Y <= Z, got %d_x_%d_x_%d", tpuType, x, y, z)
	}

	numChips := x * y * z

	maxChips := 9216

	// Check: X x Y x Z <= maxChips
	if numChips > maxChips {
		return fmt.Errorf("%s topology product (%s) exceeds maximum of %d", tpuType, topology, maxChips)
	}

	// Check how many cubes corresponding to the given topology. Each cube has 64 chips.
	numPartitions := numChips / 64
	if partitionIdLength > 0 && numPartitions != partitionIdLength {
		return fmt.Errorf("the topology %s indicates %d partitions, but found %d partitions in the spec", topology, numPartitions, partitionIdLength)
	}

	// All tpu7x checks passed.
	return nil
}

// parseDimensions is a helper function to split a topology string "AxBxC"
// into a slice of positive integers.
func parseDimensions(topology string) ([]int, error) {
	parts := strings.Split(topology, "x")

	dims := make([]int, len(parts))
	for i, part := range parts {
		// format is already validated from regex.
		val := 0
		for _, char := range part {
			// The regex ^\d+x... guarantees char is '0'-'9'
			digit := int(char - '0')
			val = val*10 + digit
		}

		if val <= 0 {
			// This check is still needed as regex allows "0x..."
			return nil, fmt.Errorf("invalid dimension '%s': must be a positive number", part)
		}
		dims[i] = val
	}
	return dims, nil
}
