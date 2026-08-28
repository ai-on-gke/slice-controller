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
	"context"
	"time"

	"github.com/go-logr/logr"
	"slice-controller/internal/metrics"
)

// SetupMetrics initializes and returns a MetricsExporter for pushing metrics.
func SetupMetrics(ctx context.Context, projectID, location, clusterName, configPath string,
	flushInterval time.Duration, logger logr.Logger) (*metrics.MetricsExporter, func(), error) {
	logger.Info("Metrics exporter initialized.")
	return metrics.NewMetricsExporter(projectID, location, clusterName), func() {}, nil
}
