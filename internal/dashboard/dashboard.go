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

package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"slice-controller/internal/informer"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// topologyInformer is an interface to abstract the TpuTopologyInformer.
// It's useful for testing the dashboard package in isolation.
type topologyInformer interface {
	GetTopologyRepoSnapshot() informer.RepoSnapshot
}

// Dashboard serves a UI for visualizing slice topology.
type Dashboard struct {
	informer topologyInformer
	port     int
}

// New creates a new Dashboard.
func New(informer topologyInformer, port int) *Dashboard {
	return &Dashboard{
		informer: informer,
		port:     port,
	}
}

// Start starts the dashboard's HTTP server.
// It's a runnable that can be added to the controller-runtime manager.
func (d *Dashboard) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("dashboard")

	mux := http.NewServeMux()
	mux.HandleFunc("/", d.serveUI)
	mux.HandleFunc("/data", d.serveData)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", d.port),
		Handler: mux,
	}

	go func() {
		logger.Info("Starting dashboard server", "port", d.port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(err, "dashboard server failed")
		}
	}()

	<-ctx.Done()
	logger.Info("Shutting down dashboard server")
	return server.Shutdown(context.Background())
}

func (d *Dashboard) serveUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	if _, err := fmt.Fprint(w, uiHTML); err != nil {
		log.Log.WithName("dashboard").Error(err, "failed to write UI response")
	}
}

func (d *Dashboard) serveData(w http.ResponseWriter, r *http.Request) {
	repoSnapshot := d.informer.GetTopologyRepoSnapshot()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(repoSnapshot); err != nil {
		http.Error(w, "Failed to encode topology data", http.StatusInternalServerError)
	}
}

const uiHTML = `
<!DOCTYPE html>
<html>
<head>
    <title>Slice Controller Dashboard</title>
    <style>
        body { font-family: sans-serif; margin: 2em; background-color: #f4f4f4; }
        h1, h2, h3 { color: #333; }
        #legend { margin-bottom: 2em; }
        .state-box { display: inline-block; width: 12px; height: 12px; border: 1px solid #ccc; margin-right: 5px; vertical-align: middle; }
        .legend-item { margin-right: 20px; }
        .topology-table {
            border-collapse: collapse;
            margin-bottom: 2em;
            width: 100%;
        }
        .topology-table th, .topology-table td {
            border: 1px solid #ccc;
            padding: 10px;
            vertical-align: top;
            text-align: left;
            min-width: 150px;
        }
        .topology-table thead th {
            background-color: #e9ecef;
            text-align: center;
        }
        .topology-table .main-header {
            font-size: 1.2em;
        }

        /* Cell coloring */
        td.not-ready { background-color: #f8d7da; } /* Broken */
        td.ready.state-Allocatable { background-color: #d4edda; } /* Allocatable */
        td.ready.state-Allocated { background-color: #ffe8d6; } /* Allocated */
        td.ready.state-PartiallyAllocated { background-color: #fff3cd; } /* Partially Allocated */
        td.ready.state-undefined { background-color: #f8f9fa; }

        strong { color: #555; }
    </style>
</head>
<body>
    <h1>Slice Topology</h1>

    <div id="legend">
        <h3>Legend</h3>
        <span class="legend-item"><span class="state-box" style="background-color: #d4edda;"></span>Allocatable</span>
        <span class="legend-item"><span class="state-box" style="background-color: #fff3cd;"></span>Partially Allocated</span>
        <span class="legend-item"><span class="state-box" style="background-color: #ffe8d6;"></span>Allocated</span>
        <span class="legend-item"><span class="state-box" style="background-color: #f8d7da;"></span>Broken</span>
    </div>

    <div id="topology-container"></div>

    <script>
        const MAX_DEPTH = 7;

        function generateCellContent(node) {
            if (!node) return '';
            let content = '<strong>UID:</strong> ' + node.UID;
            if (node.ClaimBySliceID) {
                content += '<br><strong>Claimed By:</strong> ' + node.ClaimBySliceID;
            }
            if (!node.Children || node.Children.length === 0) {
                if (node.NodeName) {
                    content += '<br><strong>Node:</strong> ' + node.NodeName;
                }
            }
            return content;
        }

        function generateRowsRecursive(node, path, rows) {
            if (!node) return;

            const newPath = [...path, node];

            if (!node.Children || node.Children.length === 0 || newPath.length >= MAX_DEPTH) {
                const row = new Array(MAX_DEPTH).fill(null);
                for (let i = 0; i < newPath.length; i++) {
                    row[i] = newPath[i];
                }
                rows.push(row);
                return;
            }

            node.Children.forEach(child => {
                generateRowsRecursive(child, newPath, rows);
            });
        }

        function renderTopology(data) {
            const container = document.getElementById('topology-container');
            container.innerHTML = '';
            if (!data) {
                container.innerHTML = '<p>No topology data available.</p>';
                return;
            }

            // --- Summary Table ---
            const allTopologies = ["16x16", "8x16", "8x8", "4x8", "4x4", "2x4", "2x2"];
            const summary = {};
            allTopologies.forEach(t => {
                summary[t] = {
                    broken: 0,
                    allocated: 0,
                    allocatable: 0,
                    partiallyAllocated: 0,
                    total: 0,
                };
            });

            const processedUIDs = new Set();
            if (Object.keys(data).length > 0) {
                for (const type in data) {
                    for (const topology of allTopologies) {
                        if (data[type][topology]) {
                            for (const uid in data[type][topology]) {
                                const rootNode = data[type][topology][uid];
                                function traverseAndSummarize(node) {
                                    if (!node || processedUIDs.has(node.UID)) return;
                                    processedUIDs.add(node.UID);

                                    const s = summary[node.Topology];
                                    if (s) {
                                        s.total++;
                                        if (!node.IsReady) {
                                            s.broken++;
                                        } else {
                                            if (node.AllocationState === 'Allocated') {
                                                s.allocated++;
                                            } else if (node.AllocationState === 'Allocatable') {
                                                s.allocatable++;
                                            } else if (node.AllocationState === 'PartiallyAllocated') {
                                                s.partiallyAllocated++;
                                            }
                                        }
                                    }

                                    if (node.Children) {
                                        node.Children.forEach(traverseAndSummarize);
                                    }
                                }
                                traverseAndSummarize(rootNode);
                            }
                        }
                    }
                }
            }

            const summaryTitle = document.createElement('h2');
            summaryTitle.textContent = 'Topology Summary';
            container.appendChild(summaryTitle);

            const summaryTable = document.createElement('table');
            summaryTable.className = 'topology-table';
            const summaryThead = document.createElement('thead');
            summaryThead.innerHTML = '<tr><th>Topology</th><th>Allocatable</th><th>Partially Allocated</th><th>Allocated</th><th>Broken</th><th>Total</th></tr>';
            summaryTable.appendChild(summaryThead);
            const summaryTbody = document.createElement('tbody');
            for (const topology of allTopologies) {
                const stats = summary[topology];
                const tr = document.createElement('tr');
                tr.innerHTML = '<td>' + topology + '</td>' +
                    '<td>' + stats.allocatable + '</td>' +
                    '<td>' + stats.partiallyAllocated + '</td>' +
                    '<td>' + stats.allocated + '</td>' +
                    '<td>' + stats.broken + '</td>' +
                    '<td>' + stats.total + '</td>';
                summaryTbody.appendChild(tr);
            }
            summaryTable.appendChild(summaryTbody);
            container.appendChild(summaryTable);


            // --- Detailed Topology View ---
            if (Object.keys(data).length > 0) {
                const detailedTitle = document.createElement('h2');
                detailedTitle.textContent = 'Topology Details';
                container.appendChild(detailedTitle);

                for (const type in data) {
                    for (const topology of allTopologies) {
                         if (!data[type][topology]) continue;

                        const uids = Object.keys(data[type][topology]);
                        if (uids.length === 0) continue;

                        for (const uid of uids) {
                            const rootNode = data[type][topology][uid];
                            
                            const table = document.createElement('table');
                            table.className = 'topology-table';
                            
                            const thead = document.createElement('thead');
                            const topologyHeaderValues = allTopologies.slice(allTopologies.indexOf(topology));
                            let headerRow = '<tr>';
                            for (let i = 0; i < MAX_DEPTH; i++) {
                                headerRow += '<th>' + (topologyHeaderValues[i] || '') + '</th>';
                            }
                            headerRow += '</tr>';
                            thead.innerHTML = '<tr><th class="main-header" colspan="' + MAX_DEPTH + '">Topology: ' + topology + ' (Type: ' + type + ', UID: ' + uid + ')</th></tr>' + headerRow;
                            table.appendChild(thead);

                            const tbody = document.createElement('tbody');
                            table.appendChild(tbody);

                            const tableRows = [];
                            generateRowsRecursive(rootNode, [], tableRows);

                            if (tableRows.length === 0) {
                                const row = new Array(MAX_DEPTH).fill(null);
                                row[0] = rootNode;
                                tableRows.push(row);
                            }

                            for (let r = 0; r < tableRows.length; r++) {
                                const tr = document.createElement('tr');
                                for (let c = 0; c < MAX_DEPTH; c++) {
                                    const node = tableRows[r][c];

                                    if (r > 0 && node && node === tableRows[r-1][c]) {
                                        continue;
                                    }

                                    const td = document.createElement('td');
                                    if (node) {
                                        td.className = (node.IsReady ? 'ready' : 'not-ready') + ' state-' + (node.AllocationState || 'undefined');
                                        let rowSpan = 1;
                                        for (let i = r + 1; i < tableRows.length; i++) {
                                            if (tableRows[i][c] === node) {
                                                rowSpan++;
                                            } else {
                                                break;
                                            }
                                        }
                                        td.rowSpan = rowSpan;
                                        td.innerHTML = generateCellContent(node);
                                    }
                                    tr.appendChild(td);
                                }
                                tbody.appendChild(tr);
                            }
                            container.appendChild(table);
                        }
                    }
                }
            }
        }

        async function fetchData() {
            try {
                const response = await fetch('/data');
                if (!response.ok) {
                    throw new Error('Network response was not ok ' + response.statusText);
                }
                const data = await response.json();
                renderTopology(data);
            } catch (error) {
                console.error('Error fetching topology data:', error);
                document.getElementById('topology-container').innerHTML = '<p>Error loading data. Check console.</p>';
            }
        }

        setInterval(fetchData, 5000);
        fetchData();
    </script>
</body>
</html>
`
