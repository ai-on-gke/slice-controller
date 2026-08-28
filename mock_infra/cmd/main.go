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
	"log"
	"net/http"
	"os"

	"slice-controller/mock_infra/gce"
)

func main() {
	// Use MOCK_GCE_PORT to determine the listening port, defaulting to 8080.
	// Listen on all interfaces.
	port := os.Getenv("MOCK_GCE_PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port
	log.Printf("Starting mock GCE server on %s", addr)
	mux := gce.CreateMux()
	err := http.ListenAndServe(addr, mux)
	if err != nil {
		log.Fatalf("Failed to start mock GCE server: %v", err)
	}
}
