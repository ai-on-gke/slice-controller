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

package cache

// MMIGHealthStatus defines the health of an MMIG.
type MMIGHealthStatus string

// MIGHealthStatus defines the health of a MIG.
type MIGHealthStatus string

// MIGPartitionState defines the logical state of a MIG partition.
type MIGPartitionState string

// MIGInstancesHealth defines the health status of instances in a MIG.
type MIGInstancesHealth string

const (
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
	// Fetching MMIG health status failed.
	MMIGHealthStatusProbeFailed MMIGHealthStatus = "SliceHealthProbeFailed"
	// MMIG not found
	MMIGHealthStatusNotFound MMIGHealthStatus = "MMIGHealthNotFound"

	// MIGHealthStatusHealthy indicates the MIG is healthy.
	MIGHealthStatusHealthy MIGHealthStatus = "HEALTHY"
	// MIGHealthStatusIncomplete indicates the MIG is incomplete.
	MIGHealthStatusIncomplete MIGHealthStatus = "INCOMPLETE"
	// MIGHealthStatusDegraded indicates the MIG is degraded.
	MIGHealthStatusDegraded MIGHealthStatus = "DEGRADED"
	// MIGHealthStatusUnhealthy indicates the MIG is unhealthy.
	MIGHealthStatusUnhealthy MIGHealthStatus = "UNHEALTHY"
	// MIGHealthStatusUnknown indicates the MIG health is unknown.
	MIGHealthStatusUnknown MIGHealthStatus = "UNKNOWN"

	// MIGInstancesHealthUnhealthyOrMissing indicates some VMs are in another state than RUNNING or they are missing.
	MIGInstancesHealthUnhealthyOrMissing MIGInstancesHealth = "UNHEALTHY_OR_MISSING"

	// MIGPartitionStateActive indicates the MIG partition is active.
	MIGPartitionStateActive MIGPartitionState = "ACTIVE"
	// MIGPartitionStateFailed indicates the MIG partition has failed.
	MIGPartitionStateFailed MIGPartitionState = "FAILED"
	// MIGPartitionStateIncomplete indicates the MIG partition is incomplete.
	MIGPartitionStateIncomplete MIGPartitionState = "INCOMPLETE"
	// MIGPartitionStateActivating indicates the MIG partition is activating.
	MIGPartitionStateActivating MIGPartitionState = "ACTIVATING"
	// MIGPartitionStateDeactivating indicates the MIG partition is deactivating.
	MIGPartitionStateDeactivating MIGPartitionState = "DEACTIVATING"
	// MIGPartitionStateInactive indicates the MIG partition is inactive.
	MIGPartitionStateInactive MIGPartitionState = "INACTIVE"
)

type SliceRelationalCache interface {
	AddSlice(sliceName string, partitionIDs []string, topology string) error
	DeleteSlice(sliceName string) error
	GetSliceInfo(sliceName string) (SliceInfo, bool)
	GetBlockID(partitionID string) (string, bool)
	GetMIGByPartitionID(partitionID string) (MIGInfo, bool)
	GetAllNodesFromMIG(migName string) ([]string, error)
	GetSliceNameByPartitionID(partitionID string) (string, bool)
}

// NodeInformerRelationalCacheInterface defines the methods we need from the relational cache.
// This allows for mocking in tests.
type NodeInformerRelationalCacheInterface interface {
	UpdateNodeInfo(migURL, nodeName, reservationName, reservationBlockID, nodePoolName string, partitions map[string][]string, isLinkedRunnerControlNode bool, acceleratorType string) error
	DeleteNodeAndCleanUpMIG(nodeName string) (bool, error)
}

type MonitorRelationalCacheInterface interface {
	GetAllMIGNames() []string
	GetAllSliceNames() []string
	GetMIG(migURL string) (MIGInfo, bool)
	GetSliceInfo(mmigURL string) (SliceInfo, bool)
	GetSliceNameByPartitionID(partitionID string) (string, bool)
}

type MonitorHealthCacheInterface interface {
	SetMIGHealth(migURL string, status MIGHealthStatus, errMsg string)
	SetMIGPartitionsStatus(migURL string, topologies map[string]AcceleratorTopologyInfo)
	SetMMIGHealth(mmigURL string, status MMIGHealthStatus, errMsg string)
	CleanupMIGHealth(migURL string)
	CleanupMIGPartitionsStatus(migURL string)
	CleanupMMIGHealth(mmigURL string)
	GetAllMIGKeys() map[string]struct{}
	GetAllMMIGKeys() map[string]struct{}
}

// AcceleratorTopologyError represents an error associated with an accelerator topology.
type AcceleratorTopologyError struct {
	Code    string
	Message string
}

// AcceleratorTopologyInfo represents the health and state status of a slice in a cube.
type AcceleratorTopologyInfo struct {
	AcceleratorTopology             string
	AcceleratorTopologyHealthStatus MIGHealthStatus
	AcceleratorTopologyState        MIGPartitionState
	SliceID                         string
	InstancesHealth                 string
	Errors                          []AcceleratorTopologyError
}

// AcceleratorTopologies represents the health status of a set of accelerator topologies.
type AcceleratorTopologies struct {
	MigName        string
	TopologiesInfo map[string]AcceleratorTopologyInfo
}

// SuperSliceTopologyHealth represents the health status of a single super slice topology.
type SuperSliceTopologyHealth struct {
	AcceleratorTopology    string
	HealthStatus           MMIGHealthStatus
	LastCheckErrorMessages []string
	LastCheckTime          string
}

// SuperSliceHealthStatus represents the overall health status of a super slice,
// composed of health statuses from all its constituent topologies.
type SuperSliceHealthStatus struct {
	MMIGName        string
	TopologyHealths []*SuperSliceTopologyHealth // Renamed from SuperSliceHealthInfo
}

// ReconcilerRelationalCacheInterface defines the methods we need from the relational cache.
// This allows for mocking in tests.
type ReconcilerRelationalCacheInterface interface {
	GetMIG(migURL string) (MIGInfo, bool)
	GetSliceInfo(name string) (SliceInfo, bool)
	GetAllMIGNames() []string
	GetAllSliceNames() []string
	GetBlockID(partitionID string) (string, bool)
	GetMIGByPartitionID(partitionID string) (MIGInfo, bool)
	GetSliceNameByPartitionID(partitionID string) (string, bool)
}

type ReconcilerHealthCacheInterface interface {
	GetMIGHealth(url string) (TimestampedHealth[MIGHealthStatus], bool)
	GetMIGPartitionsStatus(url string) (map[string]AcceleratorTopologyInfo, bool)
	GetMMIGHealth(url string) (TimestampedHealth[MMIGHealthStatus], bool)
}
