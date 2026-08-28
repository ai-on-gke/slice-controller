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

// This package is for mocking out services that the controller depends on
// allowing for easier testing.
package services

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"slice-controller/internal/metrics"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	alphacompute "google.golang.org/api/compute/v0.alpha"
	betacompute "google.golang.org/api/compute/v0.beta"
	v1compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	gcfg "gopkg.in/gcfg.v1"
	"k8s.io/cloud-provider-gcp/providers/gce"
	ctrl "sigs.k8s.io/controller-runtime"
)

// ComputeCall is an interface for mocking GCE v1 API calls in tests.
type ComputeCall interface {
	Do(opts ...googleapi.CallOption) (*v1compute.Operation, error)
}

// ComputeCallAlpha is an interface for mocking GCE alpha API calls in tests.
type ComputeCallAlpha interface {
	Do(opts ...googleapi.CallOption) (*alphacompute.Operation, error)
}

// ComputeService is a wrapper around the GCE API clients.
type ComputeService struct {
	V1Service    *v1compute.Service
	AlphaService *alphacompute.Service
	BetaService  *betacompute.Service
}

// ComputeServiceInterface defines the interface for ComputeService.
type ComputeServiceInterface interface {
	CreateResourcePolicy(
		projectID string, region string, policy *betacompute.ResourcePolicy,
	) (*betacompute.Operation, error)
	DeleteResourcePolicy(projectID, region, policyName string) (*betacompute.Operation, error)
	GetInstanceGroupManager(projectID, region, igmName string) (*betacompute.InstanceGroupManager, error)
	PatchInstanceGroupManager(
		ctx context.Context,
		projectID, zone, igmName string,
		igmResource *betacompute.InstanceGroupManager,
		requestID string,
	) (*betacompute.Operation, error)
	CreateMMIG(project string, region string, mmig *betacompute.MultiMig) (*betacompute.Operation, error)
	DeleteMMIG(project, region, mmigName string) (*betacompute.Operation, error)
	GetMMIG(project, region, mmigName string) (*betacompute.MultiMig, error)
	GetRegionalOperation(ctx context.Context, project, region, operationName string) (*betacompute.Operation, error)
	GetZonalOperation(ctx context.Context, project, zone, operationName string) (*betacompute.Operation, error)
	GetAvailableAcceleratorTopologies(
		projectID, zone, igmName string,
	) (*betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse, error)
	ListMultiMigMembers(ctx context.Context,
		project string,
		region string,
		mmigName string,
		pageToken string) (*betacompute.MultiMigMemberList, error)
	ConfigureAcceleratorTopology(
		ctx context.Context,
		projectID, zone, igmName string, req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest,
		requestId string,
	) (*betacompute.Operation, error)
}

// InitComputeService initializes the compute service.
// If configPath is "local", it will initialize the service with built-in
// credentials, intended for local development and testing.
// Otherwise, it attempts to initialize the service with credentials from the
// specified config file or from the default credentials chain (e.g., environment
// variables or metadata server).
func InitComputeService(configPath string, mockGceEndpoint string) (*ComputeService, error) {
	ctx := context.Background()
	setupLog := ctrl.Log.WithName("gceServiceInit")

	if mockGceEndpoint != "" {
		setupLog.Info(fmt.Sprintf("Using mock GCE endpoint: %s", mockGceEndpoint))
		v1Compute, err := v1compute.NewService(ctx, option.WithEndpoint(mockGceEndpoint), option.WithoutAuthentication())
		if err != nil {
			setupLog.Error(err, "failed to create v1 compute service with mock GCE endpoint")
			return nil, err
		}
		alphaCompute, err := alphacompute.NewService(
			ctx, option.WithEndpoint(mockGceEndpoint), option.WithoutAuthentication())
		if err != nil {
			setupLog.Error(err, "failed to create alpha compute service with mock GCE endpoint")
			return nil, err
		}
		betaCompute, err := betacompute.NewService(
			ctx, option.WithEndpoint(mockGceEndpoint), option.WithoutAuthentication())
		if err != nil {
			setupLog.Error(err, "failed to create beta compute service with mock GCE endpoint")
			return nil, err
		}
		return &ComputeService{
			V1Service:    v1Compute,
			AlphaService: alphaCompute,
			BetaService:  betaCompute,
		}, nil
	}

	if configPath == "local" {
		setupLog.Info("Running locally using built in credentials")
		v1Compute, err := v1compute.NewService(context.Background())
		if err != nil {
			return &ComputeService{}, err
		}
		alphaCompute, err := alphacompute.NewService(context.Background())
		if err != nil {
			return &ComputeService{}, err
		}
		betaCompute, err := betacompute.NewService(context.Background())
		if err != nil {
			return &ComputeService{}, err
		}
		return &ComputeService{V1Service: v1Compute, AlphaService: alphaCompute, BetaService: betaCompute}, nil
	}

	setupLog.Info(fmt.Sprintf("ConfigPath: %v", configPath))
	var config io.ReadCloser
	if configPath != "" {
		var err error
		config, err = os.Open(configPath)
		if err != nil {
			setupLog.Error(err, "Couldn't open cloud provider configuration: %s", configPath)
		}
		defer func() {
			if err := config.Close(); err != nil {
				setupLog.Error(err, "Failed to close config")
			}
		}()
	}
	// Create Google Compute Engine token.
	var err error
	tokenSource := google.ComputeTokenSource("")
	if len(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")) > 0 {
		tokenSource, err = google.DefaultTokenSource(context.Background(), v1compute.ComputeScope)
		if err != nil {
			return nil, err
		}
	}

	if config != nil {
		var cfg gce.ConfigFile
		if err := gcfg.ReadInto(&cfg, config); err != nil {
			setupLog.Error(err, "Couldn't read config")
			return nil, err
		}
		setupLog.Info(fmt.Sprintf("config: %+v", cfg))
		if cfg.Global.TokenURL == "" {
			setupLog.Info("Empty tokenUrl in cloud config")
		} else {
			tokenSource = gce.NewAltTokenSource(cfg.Global.TokenURL, cfg.Global.TokenBody)
			setupLog.Info(fmt.Sprintf("Using TokenSource from config %#v", tokenSource))
		}
	} else {
		setupLog.Info(fmt.Sprintf("Using default TokenSource %#v", tokenSource))
	}

	setupLog.Info(fmt.Sprintf("token: %+v", tokenSource))

	client := oauth2.NewClient(context.Background(), tokenSource)
	client.Timeout = 30 * time.Second
	v1Compute, err := v1compute.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	alphaCompute, err := alphacompute.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	betaCompute, err := betacompute.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}

	return &ComputeService{V1Service: v1Compute, AlphaService: alphaCompute, BetaService: betaCompute}, nil
}

// GetOperation retrieves the status of a GCE region operation.
func (s *ComputeService) GetOperation(project, region, operationID string) ComputeCall {
	return s.V1Service.ZoneOperations.Get(project, region, operationID)
}

// GetMMIG retrieves the status of a MMIG.
func (s *ComputeService) GetMMIG(project, region, mmigName string) (*betacompute.MultiMig, error) {
	startTime := time.Now()
	resp, err := s.BetaService.RegionMultiMigs.Get(project, region, mmigName).Do()
	metrics.RecordGceApiCall("mmig_get_health", startTime, err)
	return resp, err
}

// CreateResourcePolicy creates a new GCE resource policy.
func (s *ComputeService) CreateResourcePolicy(
	projectID string, region string, policyConfig *betacompute.ResourcePolicy,
) (*betacompute.Operation, error) {
	startTime := time.Now()
	op, err := s.BetaService.ResourcePolicies.Insert(projectID, region, policyConfig).Do()
	metrics.RecordGceApiCall("resource_policy_create", startTime, err)
	return op, err
}

// DeleteResourcePolicy deletes a GCE resource policy.
func (s *ComputeService) DeleteResourcePolicy(projectID, region, policyName string) (*betacompute.Operation, error) {
	startTime := time.Now()
	op, err := s.BetaService.ResourcePolicies.Delete(projectID, region, policyName).Do()
	metrics.RecordGceApiCall("resource_policy_delete", startTime, err)
	return op, err
}

// GetInstanceGroupManager retrieves a GCE Instance Group Manager.
func (s *ComputeService) GetInstanceGroupManager(
	projectID, region, igmName string,
) (*betacompute.InstanceGroupManager, error) {
	startTime := time.Now()
	resp, err := s.BetaService.InstanceGroupManagers.Get(projectID, region, igmName).Do()
	metrics.RecordGceApiCall("instance_group_manager_get", startTime, err)
	return resp, err
}

// PatchInstanceGroupManager applies a partial update to a GCE Instance Group Manager.
func (s *ComputeService) PatchInstanceGroupManager(
	ctx context.Context,
	projectID, zone, igmName string,
	igmResource *betacompute.InstanceGroupManager,
	requestID string,
) (*betacompute.Operation, error) {
	op, err := s.BetaService.InstanceGroupManagers.Patch(
		projectID,
		zone,
		igmName,
		igmResource).RequestId(requestID).Context(ctx).Do()
	return op, err
}

// CreateMMIG creates a new GCE Multi-Instance-Group-Manager (MMIG).
func (s *ComputeService) CreateMMIG(
	project string, region string, mmig *betacompute.MultiMig,
) (*betacompute.Operation, error) {
	startTime := time.Now()
	op, err := s.BetaService.RegionMultiMigs.Insert(project, region, mmig).Do()
	metrics.RecordGceApiCall("mmig_create", startTime, err)
	return op, err
}

// DeleteMMIG deletes a GCE Multi-Instance-Group-Manager (MMIG).
func (s *ComputeService) DeleteMMIG(project, region, mmigName string) (*betacompute.Operation, error) {
	startTime := time.Now()
	op, err := s.BetaService.RegionMultiMigs.Delete(project, region, mmigName).Do()
	metrics.RecordGceApiCall("mmig_delete", startTime, err)
	return op, err
}

// GetRegionalOperation retrieves a regional operation.
func (s *ComputeService) GetRegionalOperation(
	ctx context.Context, project, region, operationName string,
) (*betacompute.Operation, error) {
	return s.BetaService.RegionOperations.Get(project, region, operationName).Context(ctx).Do()
}

// GetZonalOperation retrieves a zonal operation.
func (s *ComputeService) GetZonalOperation(
	ctx context.Context, project, zone, operationName string,
) (*betacompute.Operation, error) {
	return s.BetaService.ZoneOperations.Get(project, zone, operationName).Context(ctx).Do()
}

// GetAvailableAcceleratorTopologies retrieves the available accelerator topologies for a given MIG.
func (s *ComputeService) GetAvailableAcceleratorTopologies(
	projectID, zone, igmName string,
) (*betacompute.InstanceGroupManagersGetAvailableAcceleratorTopologiesResponse, error) {
	return s.BetaService.InstanceGroupManagers.GetAvailableAcceleratorTopologies(projectID, zone, igmName).Do()
}

// ListMultiMigMembers lists a page of members of a given MMIG.
func (s *ComputeService) ListMultiMigMembers(ctx context.Context,
	project string,
	region string,
	mmigName string,
	pageToken string) (*betacompute.MultiMigMemberList, error) {
	call := s.BetaService.RegionMultiMigMembers.List(project, region, mmigName).Context(ctx)

	// Apply the page token if fetching a subsequent page
	if pageToken != "" {
		call = call.PageToken(pageToken)
	}
	return call.Do()
}

// ConfigureAcceleratorTopology configures the accelerator topologies for a given MIG.
func (s *ComputeService) ConfigureAcceleratorTopology(
	ctx context.Context,
	projectID, zone, igmName string,
	req *betacompute.InstanceGroupManagersConfigureAcceleratorTopologiesRequest,
	requestId string,
) (*betacompute.Operation, error) {
	call := s.BetaService.InstanceGroupManagers.ConfigureAcceleratorTopologies(projectID, zone, igmName, req)
	if requestId != "" {
		call.RequestId(requestId)
	}
	return call.Context(ctx).Do()
}
