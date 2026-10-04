package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
	"github.com/itaic/agent-mesh/internal/agentclient"
)

// AgentReconciler reconciles an Agent object: it owns a Deployment+Service
// pair that runs the agent's A2A server, then fetches the agent card that
// server publishes at /.well-known/agent-card.json to confirm it's live.
type AgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mesh.itai.dev,resources=agents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mesh.itai.dev,resources=agents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *AgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var agent meshv1alpha1.Agent
	if err := r.Get(ctx, req.NamespacedName, &agent); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	dep := r.desiredDeployment(&agent)
	if err := controllerutil.SetControllerReference(&agent, dep, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyDeployment(ctx, dep); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling deployment: %w", err)
	}

	svc := r.desiredService(&agent)
	if err := controllerutil.SetControllerReference(&agent, svc, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyService(ctx, svc); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling service: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, agent.Spec.Port)

	var currentDep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}, &currentDep); err != nil {
		return ctrl.Result{}, err
	}
	available := currentDep.Status.AvailableReplicas > 0

	agent.Status.ObservedGeneration = agent.Generation
	agent.Status.Endpoint = endpoint

	if !available {
		agent.Status.Ready = false
		meta_SetCondition(&agent.Status.Conditions, metav1.Condition{
			Type:    "Available",
			Status:  metav1.ConditionFalse,
			Reason:  "DeploymentNotReady",
			Message: "waiting for at least one available replica",
		})
		if err := r.Status().Update(ctx, &agent); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Deployment has ready pods — verify the live agent card.
	card, err := agentclient.ResolveCard(ctx, endpoint)
	if err != nil {
		logger.Error(err, "fetching agent card", "endpoint", endpoint)
		agent.Status.Ready = false
		meta_SetCondition(&agent.Status.Conditions, metav1.Condition{
			Type: "CardVerified", Status: metav1.ConditionFalse,
			Reason: "AgentCardUnavailable", Message: err.Error(),
		})
		_ = r.Status().Update(ctx, &agent)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	observed := make([]string, 0, len(card.Skills))
	for _, s := range card.Skills {
		observed = append(observed, s.ID)
	}
	agent.Status.ObservedCapabilities = observed
	agent.Status.Ready = true
	meta_SetCondition(&agent.Status.Conditions, metav1.Condition{
		Type: "CardVerified", Status: metav1.ConditionTrue,
		Reason: "CardMatchesLiveAgent", Message: "AgentCard fetched successfully",
	})
	meta_SetCondition(&agent.Status.Conditions, metav1.Condition{
		Type: "Available", Status: metav1.ConditionTrue,
		Reason: "DeploymentReady", Message: "deployment has available replicas",
	})

	if err := r.Status().Update(ctx, &agent); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueSteadyState}, nil
}

func (r *AgentReconciler) desiredDeployment(a *meshv1alpha1.Agent) *appsv1.Deployment {
	labels := map[string]string{"mesh.itai.dev/agent": a.Name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: a.Name, Namespace: a.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: a.Spec.Replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:      "a2a-server",
						Image:     a.Spec.Image,
						Ports:     []corev1.ContainerPort{{ContainerPort: a.Spec.Port, Name: "a2a"}},
						Env:       a.Spec.Env,
						Resources: a.Spec.Resources,
					}},
				},
			},
		},
	}
}

func (r *AgentReconciler) desiredService(a *meshv1alpha1.Agent) *corev1.Service {
	labels := map[string]string{"mesh.itai.dev/agent": a.Name}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: a.Name, Namespace: a.Namespace},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: "a2a", Port: a.Spec.Port, TargetPort: intstrFromInt32(a.Spec.Port),
			}},
		},
	}
}

func (r *AgentReconciler) applyDeployment(ctx context.Context, dep *appsv1.Deployment) error {
	var existing appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, dep)
	} else if err != nil {
		return err
	}
	existing.Spec = dep.Spec
	return r.Update(ctx, &existing)
}

func (r *AgentReconciler) applyService(ctx context.Context, svc *corev1.Service) error {
	var existing corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, svc)
	} else if err != nil {
		return err
	}
	existing.Spec.Ports = svc.Spec.Ports
	existing.Spec.Selector = svc.Spec.Selector
	return r.Update(ctx, &existing)
}

func (r *AgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&meshv1alpha1.Agent{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
