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

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	betacontainer "google.golang.org/api/container/v1beta1"
	"google.golang.org/api/option"
	gcfg "gopkg.in/gcfg.v1"
	"k8s.io/cloud-provider-gcp/providers/gce"
	ctrl "sigs.k8s.io/controller-runtime"
)

type NodePoolDataService interface {
	GetNodePool(project, location, clusterName, nodePoolName string) (*betacontainer.NodePool, error)
	ListNodePools(project, location, clusterName string) ([]*betacontainer.NodePool, error)
}

type ContainerService struct {
	ContainerService *betacontainer.Service
}

func (s *ContainerService) GetNodePool(project, location, clusterName,
	nodePoolName string) (*betacontainer.NodePool, error) {
	name := "projects/" + project + "/locations/" + location + "/clusters/" + clusterName + "/nodePools/" + nodePoolName
	return s.ContainerService.Projects.Locations.Clusters.NodePools.Get(name).Do()
}

func (s *ContainerService) ListNodePools(project, location, clusterName string) ([]*betacontainer.NodePool, error) {
	name := "projects/" + project + "/locations/" + location + "/clusters/" + clusterName
	resp, err := s.ContainerService.Projects.Locations.Clusters.NodePools.List(name).Do()
	if err != nil {
		return nil, err
	}
	return resp.NodePools, err
}

func InitContainerService(configPath string) (*ContainerService, error) {
	ctx := context.Background()
	setupLog := ctrl.Log.WithName("gkeServiceInit")

	if configPath == "" {
		setupLog.Info("Running local test using WithoutAuthentication")
		containerService, err := betacontainer.NewService(ctx, option.WithoutAuthentication())
		if err != nil {
			return &ContainerService{}, err
		}

		return &ContainerService{ContainerService: containerService}, nil
	}

	if configPath == "local" {
		setupLog.Info("Running locally using built in credentials")
		containerService, err := betacontainer.NewService(ctx)
		if err != nil {
			return &ContainerService{}, err
		}

		return &ContainerService{ContainerService: containerService}, nil
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
		tokenSource, err = google.DefaultTokenSource(ctx, betacontainer.CloudPlatformScope)
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

	client := oauth2.NewClient(ctx, tokenSource)
	client.Timeout = 30 * time.Second
	containerService, err := betacontainer.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}

	return &ContainerService{ContainerService: containerService}, nil
}
