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

package v1beta1_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/webhook/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Slice Webhook", func() {
	const topology4x8x8 = "4x8x8"
	const topology4x4x4 = "4x4x4"
	const topology2x2x1 = "2x2x1"
	var (
		obj    *slicev1beta1.Slice
		oldObj *slicev1beta1.Slice
		v      *v1beta1.SliceCustomValidator
	)

	BeforeEach(func() {
		obj = &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: "test-slice"}}
		oldObj = &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: "test-slice"}}
		v = &v1beta1.SliceCustomValidator{EnableSliceUpdate: true}
		Expect(v).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating Slice under Defaulting Webhook", func() {
		// TODO (user): Add logic for defaulting webhooks
		// Example:
		// It("Should apply defaults when a required field is empty", func() {
		//     By("simulating a scenario where defaults should be applied")
		//     obj.SomeFieldWithDefault = ""
		//     By("calling the Default method to apply defaults")
		//     defaulter.Default(ctx, obj)
		//     By("checking that the default values are set")
		//     Expect(obj.SomeFieldWithDefault).To(Equal("default_value"))
		// })
	})

	Context("When updating Slice under Validating Webhook", func() {
		BeforeEach(func() {
			obj.Spec.Type = slicev1beta1.TypeTpu7x
			obj.Spec.Topology = topology4x4x4
			obj.Spec.PartitionIds = []string{"p1"}

			oldObj.Spec.Type = slicev1beta1.TypeTpu7x
			oldObj.Spec.Topology = topology4x4x4
			oldObj.Spec.PartitionIds = []string{"p1"}
		})

		It("Should admit valid update", func() {
			_, err := v.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should deny update if partition count does not match topology", func() {
			oldObj.Spec.PartitionIds = []string{}        // Start from 0 to allow update
			obj.Spec.PartitionIds = []string{"p1", "p2"} // Mismatch for 4x4x4
			_, err := v.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("the topology " + topology4x4x4 + " indicates 1 partitions, but found 2 partitions"))
		})

		It("Should deny update if topology is changed", func() {
			obj.Spec.Topology = topology4x8x8
			_, err := v.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("topology is immutable, cannot change from " + topology4x4x4 + " to " + topology4x8x8))
		})

		It("Should admit update to empty partitions", func() {
			obj.Spec.PartitionIds = []string{} // Allowed
			_, err := v.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should deny update to partitions when slice update is disabled", func() {
			v.EnableSliceUpdate = false
			obj.Spec.PartitionIds = []string{"p2"} // Changed from p1
			_, err := v.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("slice update is disabled; cannot update partitionIds"))
		})

		It("Should admit update to metadata (finalizers) when slice update is disabled", func() {
			v.EnableSliceUpdate = false
			obj.ObjectMeta.Finalizers = []string{"another-finalizer"}
			_, err := v.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
		})

	})

	Context("when validating Slice accelerator spec", func() {
		It("should return an error for an unsupported accelerator type", func() {
			obj.Spec = slicev1beta1.SliceSpec{
				Type: "unsupported-type",
			}
			_, err := v.ValidateCreate(context.Background(), obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported-type is not supported"))
		})

		Context("for tpu7x accelerator type", func() {
			BeforeEach(func() {
				obj.Spec.Type = "tpu7x"
			})

			It("should succeed for a valid topology", func() {
				obj.Spec.Topology = topology4x8x8 // 4*8*8/64 = 4 partitions
				obj.Spec.PartitionIds = []string{"p1", "p2", "p3", "p4"}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should succeed for an empty partition list with valid topology", func() {
				obj.Spec.Topology = topology4x8x8
				obj.Spec.PartitionIds = []string{}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should succeed for an empty partition list with small topology", func() {
				obj.Spec.Topology = "2x2x4"
				obj.Spec.PartitionIds = []string{}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should succeed for special small topologies", func() {
				obj.Spec.PartitionIds = []string{"p1"}
				topologies := []string{topology2x2x1, "2x2x2", "2x2x4", "2x4x4"}
				for _, topology := range topologies {
					By("checking topology " + topology)
					obj.Spec.Topology = topology
					_, err := v.ValidateCreate(context.Background(), obj)
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("should succeed for empty partitionIds on special small topologies", func() {
				obj.Spec.PartitionIds = []string{}
				topologies := []string{topology2x2x1, "2x2x2", "2x2x4", "2x4x4"}
				for _, topology := range topologies {
					By("checking topology " + topology)
					obj.Spec.Topology = topology
					_, err := v.ValidateCreate(context.Background(), obj)
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("should fail for special small topologies with incorrect partition count", func() {
				obj.Spec.Topology = topology2x2x1
				obj.Spec.PartitionIds = []string{"p1", "p2"}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("there should be exactly 1 partition, but found: 2"))
			})

			It("should fail if partition IDs contain empty string", func() {
				obj.Spec.Topology = topology4x8x8
				obj.Spec.PartitionIds = []string{"p1", "", "p3", "p4"}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("partition ID cannot be empty"))
			})

			It("should fail for a valid topology with incorrect partition count", func() {
				obj.Spec.Topology = topology4x8x8 // Expects 4 partitions
				obj.Spec.PartitionIds = []string{"p1", "p2"}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("the topology 4x8x8 indicates 4 partitions, but found 2 partitions in the spec"))
			})

			It("should fail for superslicing topology with incorrect partition count", func() {
				obj.Spec.Topology = topology4x8x8 // Expects 4 partitions
				obj.Spec.PartitionIds = []string{"p1"}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("the topology 4x8x8 indicates 4 partitions, but found 1 partitions in the spec"))
			})

			It("should fail for a 2-dimensional topology", func() {
				obj.Spec.Topology = "4x4"
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("tpu7x topology format must be AxBxC"))
			})

			It("should fail if dimensions are not multiples of 4", func() {
				obj.Spec.Topology = "4x4x6"
				obj.Spec.PartitionIds = []string{"p1", "p2"} // 4*4*6/64 is not integer
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("dimensions must be multiples of 4"))
			})

			It("should fail if dimensions are not sorted (X <= Y <= Z)", func() {
				By("checking Y > Z")
				obj.Spec.Topology = "4x8x4"
				obj.Spec.PartitionIds = []string{"p1", "p2"}
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("dimensions must satisfy X <= Y <= Z"))

				By("checking X > Y")
				obj.Spec.Topology = "8x4x8"
				obj.Spec.PartitionIds = []string{"p1", "p2", "p3", "p4"}
				_, err = v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("dimensions must satisfy X <= Y <= Z"))
			})

			It("should fail if the product of dimensions exceeds the maximum", func() {
				obj.Spec.Topology = "20x20x24"              // 9600 > 9216
				obj.Spec.PartitionIds = make([]string, 150) // 20*20*24/64 = 150
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("exceeds maximum of 9216"))
			})
		})

	})

	Context("when validating Slice name", func() {
		BeforeEach(func() {
			// Common spec for name validation tests
			obj.Spec = slicev1beta1.SliceSpec{
				Type:     "tpu7x",
				Topology: "2x2x1",
			}
		})

		It("should succeed if the name is 49 characters or less", func() {
			By("having a name with 49 characters")
			obj.ObjectMeta.Name = "verylong-slice-name-that-is-exactly-49-characters"
			Expect(obj.ObjectMeta.Name).To(HaveLen(49))
			_, err := v.ValidateCreate(context.Background(), obj)
			Expect(err).NotTo(HaveOccurred())

			By("having a name with less than 49 characters")
			obj.ObjectMeta.Name = "short-name"
			_, err = v.ValidateCreate(context.Background(), obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should fail if the name is longer than 49 characters", func() {
			obj.ObjectMeta.Name = "a-super-duper-extremely-long-slice-name-that-is-over-49-chars"
			_, err := v.ValidateCreate(context.Background(), obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must be no more than 49 characters"))
		})

		It("should succeed for valid names matching the regex", func() {
			validNames := []string{"a", "slice-1", "another-slice-name-with-digits-123"}
			for _, name := range validNames {
				By("testing a valid name: " + name)
				obj.ObjectMeta.Name = name
				_, err := v.ValidateCreate(context.Background(), obj)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("should fail for invalid names not matching the regex", func() {
			invalidNames := map[string]string{
				"must start with a lowercase letter": "1-slice",
				"must not start with a dash":         "-slice",
				"must not contain uppercase letters": "Slice",
				"must not end with a dash":           "slice-",
				"must not contain special chars":     "slice_1",
			}

			for reason, name := range invalidNames {
				By("testing an invalid name that "+reason+": "+name, func() {
					obj.ObjectMeta.Name = name
					_, err := v.ValidateCreate(context.Background(), obj)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring(`must match the regex [a-z]([-a-z0-9]*[a-z0-9])?`))
				})
			}
		})

		It("should fail if the name starts with an uppercase letter", func() {
			obj.ObjectMeta.Name = "My-slice"
			_, err := v.ValidateCreate(context.Background(), obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`must match the regex [a-z]([-a-z0-9]*[a-z0-9])?`))
		})
	})
})
