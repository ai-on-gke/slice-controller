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

package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"os"
	"path/filepath"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	slicev1beta1 "slice-controller/api/v1beta1"
	"slice-controller/internal/action"
	"slice-controller/internal/action/gcehelper"
	"slice-controller/internal/action/kubehelper"
	"slice-controller/internal/cache"
	"slice-controller/internal/controller"
	"slice-controller/internal/health"
	"slice-controller/internal/metrics"
	"slice-controller/internal/util"
	webhookslicev1beta1 "slice-controller/internal/webhook/v1beta1"
	"slice-controller/services"
	// +kubebuilder:scaffold:imports
)

const (
	mgrQPS   = 300
	mgrBurst = 400
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	klogFlags                   flag.FlagSet
	klogRegisteredToCommandLine bool
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(slicev1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme

	// Register klog flags with the standard flag library if not already registered
	if flag.Lookup("v") == nil {
		klog.InitFlags(nil)
		klogRegisteredToCommandLine = true
	} else {
		klog.InitFlags(&klogFlags)
		// Register non-colliding klog flags to the standard flag library so they can be parsed.
		klogFlags.VisitAll(func(f *flag.Flag) {
			if flag.Lookup(f.Name) == nil {
				flag.Var(f.Value, f.Name, f.Usage)
			}
		})
	}
}

// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var webhookServerCert, webhookServerKey string
	var webhookServerPort int
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var projectNumber string
	var clusterName string
	var clusterHash string
	var location string
	var nodeHealthAggregationInterval time.Duration
	var topologyWorkerCount int
	var tlsOpts []func(*tls.Config)
	var enableDashboard bool
	var useMockingGceEndpoint bool
	var nodeInformerEnabled bool
	var qpsLimit int
	var healthMonitorWorkerCount int
	var healthMonitorEnabled bool
	var healthReconcilerWokerCount int
	var healthReconcilerEnabled bool
	var enableSubSlicing bool
	var projectID string
	var sliceReconcilerWorkerCount int
	var enableHighGCEConcurrency bool
	var enableHighNodeLablingConcurrency bool
	var metricsFlushInterval time.Duration
	var enableLinkedRunner bool
	var minRetryDelay time.Duration
	var maxRetryDelay time.Duration
	var workQueueRateLimit float64
	var workQueueBurst int
	var enableSliceUpdate bool
	var supportSubsliceUpdate bool

	cloudConfig := flag.String("cloud-config", "/etc/gce.conf",
		"The path to the cloud provider configuration file. Empty string for no configuration file.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0",
		"The address the metrics endpoint binds to. "+
			"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":10080", "The address the probe endpoint binds to.")
	flag.StringVar(&projectNumber, "project-number", "",
		"The project number of the cluster the controller is operating on")
	flag.StringVar(&clusterName, "cluster-name", "", "The name of the cluster the controller is operating on")
	flag.StringVar(&clusterHash, "cluster-hash", "", "The hash of the cluster the controller is operating on")
	flag.StringVar(&location, "location", "",
		"The location (zone or region) of the cluster the controller is operating on")
	flag.StringVar(&projectID, "project-id", "", "GCP Project ID")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&webhookServerCert, "webhook-server-cert", "", "Base64 encoded cert used by the webhook server.")
	flag.StringVar(&webhookServerKey, "webhook-server-key", "", "Base64 encoded key used by the webhook server.")
	flag.IntVar(&webhookServerPort, "webhook-server-port", 9443, "The port used by the webhook server.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.DurationVar(&nodeHealthAggregationInterval, "node-health-aggregation-interval",
		5*time.Second, "The interval at which to aggregate node health.")
	flag.IntVar(&topologyWorkerCount, "topology-worker-count", 1, "The number of workers for the topology informer.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.BoolVar(&enableDashboard, "enable-dashboard", false, "Enable the dashboard UI for visualizing topologies.")
	flag.BoolVar(&nodeInformerEnabled, "enable-node-informer", true, "Enable the Node informer for health monitoring.")
	flag.IntVar(&qpsLimit, "qps-limit", 300, "The QPS limit for the API client.")
	flag.BoolVar(&healthMonitorEnabled, "health-monitor-enabled", true,
		"Enable the health monitor to monitor health of MIGs and MMIGs.")
	flag.IntVar(&healthMonitorWorkerCount, "health-monitor-worker-count", 25,
		"The number of workers for the health monitor.")
	flag.IntVar(&healthReconcilerWokerCount, "health-reconciler-worker-count", 1,
		"The number of workers for the health reconciler.")
	flag.BoolVar(&healthReconcilerEnabled, "health-reconciler-enabled", true,
		"Enable the health reconciler to reconcile health status.")
	flag.BoolVar(&enableSubSlicing, "enable-subslicing", true,
		"Enable subslicing on slice controller.")
	flag.IntVar(&sliceReconcilerWorkerCount, "slice-reconciler-worker-count", 128,
		"The number of workers for the slice reconciler.")
	flag.BoolVar(&enableHighGCEConcurrency, "enable-high-gce-concurrency", true,
		"If set, enables high parallelism for slice formation/deformation which may increase resource consumption.")
	flag.BoolVar(&enableHighNodeLablingConcurrency, "enable-high-node-labeling-concurrency", true,
		"If set, enables high parallelism for node labeling/unlabeling which may increase resource consumption.")
	flag.BoolVar(&useMockingGceEndpoint, "use-mocking-gce-endpoint", false,
		"Use mocking GCE endpoint for local testing only. Enable by setting OS ENV `MOCKING_GCE_API_ENDPOINT`.")
	flag.DurationVar(&metricsFlushInterval, "metrics-flush-interval", 15*time.Second,
		"The interval at which to flush metrics.")
	flag.BoolVar(&enableLinkedRunner, "enable-linked-runner", true,
		"Enable Linked Runner logic in the slice controller.")
	flag.DurationVar(&minRetryDelay, "min-retry-delay", 5*time.Second,
		"The min time before a retrying on error.")
	flag.DurationVar(&maxRetryDelay, "max-retry-delay", 60*time.Second,
		"The max time before a retrying on error.")
	flag.Float64Var(&workQueueRateLimit, "workqueue-rate-limit", 200.0,
		"The rate limit for the workqueue.")
	flag.IntVar(&workQueueBurst, "workqueue-burst", 500,
		"The burst size for the workqueue.")
	flag.BoolVar(&enableSliceUpdate, "enable-slice-update", false,
		"Enable slice update logic.")
	flag.BoolVar(&supportSubsliceUpdate, "support-subslice-update", false,
		"Enable sub-slice update logic.")

	if err := flag.Set("logtostderr", "true"); err != nil {
		setupLog.Error(err, "failed to set logtostderr flag")
	}
	// Parse command-line flags
	flag.Parse()

	if !klogRegisteredToCommandLine {
		setupLog.Info("Syncing glog flags to klog")
		flag.CommandLine.VisitAll(func(f *flag.Flag) {
			if klogFlag := klogFlags.Lookup(f.Name); klogFlag != nil {
				// Avoid redundant sync for non-colliding flags that we registered
				if f.Value == klogFlag.Value {
					return
				}
				val := f.Value.String()
				if err := klogFlags.Set(f.Name, val); err != nil {
					setupLog.Error(err, "failed to sync flag to klog", "name", f.Name, "value", val)
				}
			}
		})
	}

	ctrl.SetLogger(klog.NewKlogr())

	mockGceEndpoint := os.Getenv("MOCKING_GCE_API_ENDPOINT")
	if mockGceEndpoint != "" {
		useMockingGceEndpoint = true
		*cloudConfig = ""
		setupLog.Info("Using mocking GCE endpoint", "endpoint", mockGceEndpoint)
	}

	if metricsAddr != "0" {
		setupLog.Info("metrics server is starting to collect.", "addr", metricsAddr)
	}
	ctx := context.Background()

	var metricsExporter *metrics.MetricsExporter
	// ENABLE_EXTERNAL_METRICS defaults to true. To disable, set it to "false".
	if os.Getenv("ENABLE_EXTERNAL_METRICS") != "false" && !useMockingGceEndpoint {
		// If SetupMetrics returns an error, metricsExporter will be nil, but the controller will not crash
		// because the metricsShutdown call is guarded by this error check in main.go and will just print
		// a setLog.Error in log explorer, and all internal components perform explicit nil checks before
		// recording metrics, so it would be a safe and graceful degradation.
		var metricsShutdown func()
		var err error
		metricsExporter, metricsShutdown, err = SetupMetrics(ctx, projectID, location, clusterName,
			*cloudConfig, metricsFlushInterval, setupLog)
		if err != nil {
			setupLog.Error(err, "unable to setup metrics exporter")
		} else {
			defer metricsShutdown()
		}
	} else {
		setupLog.Info("External metrics are disabled via ENABLE_EXTERNAL_METRICS env var")
	}

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher *certwatcher.CertWatcher
	var webhookCertWatcher *certwatcher.CertWatcher

	var webhookOpts []func(*tls.Config)

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookOpts = append(webhookOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	if webhookServerCert != "" && webhookServerKey != "" {
		certPEM, err := base64.StdEncoding.DecodeString(webhookServerCert)
		if err != nil {
			setupLog.Error(err, "failed to decode base64 encoded webhook server cert")
		}
		keyPEM, err := base64.StdEncoding.DecodeString(webhookServerKey)
		if err != nil {
			setupLog.Error(err, "failed to decode base64 encoded webhook server key")
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			setupLog.Error(err, "failed to create x509 key pair")
			os.Exit(1)
		}
		webhookOpts = append(webhookOpts, func(config *tls.Config) {
			config.GetCertificate = func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return &cert, nil
			}
		})
	}

	var webhookServer webhook.Server
	if webhookOpts != nil {
		webhookOpts = append(webhookOpts, tlsOpts...)
		webhookServer = webhook.NewServer(webhook.Options{
			TLSOpts: webhookOpts,
			Port:    webhookServerPort,
		})
	} else {
		setupLog.Info("Webhook server is not initialized because no certificate information was provided.")
	}

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	// TODO: adjust the qps as needed
	// Please be aware that this qps will be shared by both main slice-controller
	// And the health monitoring pieces.
	mgrConfig := ctrl.GetConfigOrDie()
	mgrConfig.QPS = mgrQPS
	mgrConfig.Burst = mgrBurst

	mgr, err := ctrl.NewManager(mgrConfig, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsServerOptions,
		WebhookServer:           webhookServer,
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionNamespace: "kube-system",
		LeaderElectionID:        "slice-controller",
		// LeaderElectionID: "956c81bc.accelerator.gke.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	relationalCache := cache.NewRelationalCache()
	setupLog.Info("Relational cache initialized")
	healthCache := cache.NewHealthCache()
	setupLog.Info("Health cache initialized")

	region, err := util.GetRegion(location)
	if err != nil {
		setupLog.Error(err, "Failed to get region from location", "Location", location)
		os.Exit(1)
	}
	if nodeInformerEnabled {
		nodeinformer, err := health.NewNodeInformer(mgr, relationalCache, enableLinkedRunner)
		if err != nil {
			setupLog.Error(err, "unable to create Node informer")
			os.Exit(1)
		}
		if err := mgr.Add(nodeinformer); err != nil {
			setupLog.Error(err, "unable to add Node informer to manager")
			os.Exit(1)
		}
		setupLog.Info("Node informer added to manager")
	}

	mgrClient := mgr.GetClient()
	kubeHelper := kubehelper.NewClient(mgrClient, relationalCache, enableLinkedRunner)

	// Initialize the compute service.
	computeService, err := services.InitComputeService(*cloudConfig, mockGceEndpoint)
	if err != nil {
		setupLog.Error(err, "unable to initialize the compute service")
		os.Exit(1)
	}

	gceHelper := gcehelper.NewClient(computeService, enableHighGCEConcurrency, enableSubSlicing)

	if healthMonitorEnabled {
		monitor, err := health.NewMonitor(
			mgrClient,
			relationalCache,
			healthCache,
			computeService,
			projectNumber,
			location,
			qpsLimit,
			healthMonitorWorkerCount,
			clusterHash,
			enableHighGCEConcurrency,
			enableLinkedRunner,
			enableSubSlicing,
		)
		if err != nil {
			setupLog.Error(err, "unable to create health monitor")
			os.Exit(1)
		}

		if err := mgr.Add(monitor); err != nil {
			setupLog.Error(err, "unable to add health monitor to manager")
			os.Exit(1)
		}
		setupLog.Info("Health monitor added to manager")
	}

	if healthReconcilerEnabled {
		reconciler, err := health.NewHealthReconciler(
			mgr,
			relationalCache,
			healthCache,
			healthReconcilerWokerCount,
			clusterHash,
			metricsExporter,
			enableLinkedRunner,
			enableSubSlicing,
		)
		if err != nil {
			setupLog.Error(err, "unable to create health reconciler")
			os.Exit(1)
		}
		healthCache.RegisterListener(reconciler)
		if err := mgr.Add(reconciler); err != nil {
			setupLog.Error(err, "unable to add health reconciler to manager")
			os.Exit(1)
		}
		setupLog.Info("Health reconciler added to manager")
	}

	if err = (&controller.SliceReconciler{
		Client:                            mgrClient,
		Scheme:                            mgr.GetScheme(),
		ProjectNumber:                     projectNumber,
		Location:                          location,
		Region:                            region,
		ClusterName:                       clusterName,
		ClusterHash:                       clusterHash,
		ComputeService:                    computeService,
		KubeHelper:                        kubeHelper,
		GCEHelper:                         gceHelper,
		SliceEngine:                       action.NewEngine(),
		RelationalCache:                   relationalCache,
		EnableHighGCEConcurrency:          enableHighGCEConcurrency,
		EnableHighNodeLabelingConcurrency: enableHighNodeLablingConcurrency,
		MetricsExporter:                   metricsExporter,
		EnableLinkedRunner:                enableLinkedRunner,
		EnableSliceUpdate:                 enableSliceUpdate,
		MinRetryDelay:                     minRetryDelay,
		MaxRetryDelay:                     maxRetryDelay,
		WorkQueueRateLimit:                workQueueRateLimit,
		WorkQueueBurst:                    workQueueBurst,
		EnableSubSlicing:                  enableSubSlicing,
		SupportSubsliceUpdate:             supportSubsliceUpdate,
	}).SetupWithManager(mgr, sliceReconcilerWorkerCount); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Slice")
		os.Exit(1)
	}

	if webhookServer != nil {
		if err = webhookslicev1beta1.SetupSliceWebhookWithManager(mgr, enableSliceUpdate, supportSubsliceUpdate); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "Slice")
			os.Exit(1)
		}
	}

	// +kubebuilder:scaffold:builder

	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
