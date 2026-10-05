package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	meshv1alpha1 "github.com/itaischwarz/agent-mesh/api/v1alpha1"
	"github.com/itaischwarz/agent-mesh/internal/controller"
	"github.com/itaischwarz/agent-mesh/internal/meshserver"
	"github.com/itaischwarz/agent-mesh/pkg/mesh"
)

var scheme = newScheme()

func newScheme() *runtime.Scheme {
	s := clientgoscheme.Scheme
	utilruntime.Must(meshv1alpha1.AddToScheme(s))
	return s
}

func main() {
	var metricsAddr string
	var probeAddr string
	var meshAddr, meshNamespace, meshPublicURL string
	var enableLeaderElection bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.StringVar(&meshAddr, "mesh-bind-address", mesh.DefaultAddr, "mesh A2A (JSON-RPC) bind address")
	flag.StringVar(&meshNamespace, "mesh-namespace", "default", "namespace the mesh delegates to unless a request overrides it")
	flag.StringVar(&meshPublicURL, "mesh-public-url", "http://localhost"+mesh.DefaultAddr, "URL the mesh advertises in its agent card")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "enable leader election for HA")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "agent-mesh-leader.mesh.itai.dev",
	})
	if err != nil {
		ctrl.Log.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.AgentReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to create controller", "controller", "Agent")
		os.Exit(1)
	}

	if err := (&controller.TaskReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to create controller", "controller", "Task")
		os.Exit(1)
	}

	meshExec := &meshserver.Executor{Client: mgr.GetClient(), Namespace: meshNamespace, PublicURL: meshPublicURL}
	if err := mgr.Add(meshserver.NewRunnable(meshAddr, meshExec)); err != nil {
		ctrl.Log.Error(err, "unable to add mesh A2A server")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	ctrl.Log.Info("starting agent-mesh manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "problem running manager")
		os.Exit(1)
	}
}
