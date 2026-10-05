package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meshv1alpha1 "github.com/itaischwarz/agent-mesh/api/v1alpha1"
	"github.com/itaischwarz/agent-mesh/internal/a2aconv"
	"github.com/itaischwarz/agent-mesh/internal/agentclient"
	"github.com/itaischwarz/agent-mesh/pkg/mesh"
)

const taskFinalizer = "mesh.itai.dev/task-cleanup"

// TaskReconciler resolves a Task's target Agent, dispatches the task with
// A2A SendMessage on first sight, then subscribes to the agent's task stream
// until the task reaches a terminal state, mirroring status/artifacts back
// onto the CR.
type TaskReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=mesh.itai.dev,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mesh.itai.dev,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mesh.itai.dev,resources=agents,verbs=get;list;watch

func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var task meshv1alpha1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !task.ObjectMeta.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, &task)
	}

	if !controllerutil.ContainsFinalizer(&task, taskFinalizer) {
		controllerutil.AddFinalizer(&task, taskFinalizer)
		if err := r.Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Checked before dispatch: an agent may answer with a direct Message and
	// no task, leaving TaskID empty on a finished Task.
	if task.Status.State.IsTerminal() {
		return ctrl.Result{}, nil
	}

	endpoint, err := r.resolveAgentEndpoint(ctx, &task)
	if err != nil {
		meta_SetCondition(&task.Status.Conditions, metav1.Condition{
			Type: "Dispatched", Status: metav1.ConditionFalse,
			Reason: "AgentNotReady", Message: err.Error(),
		})
		if task.Status.TaskID == "" {
			task.Status.State = meshv1alpha1.TaskStateUnknown
		}
		_ = r.Status().Update(ctx, &task)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	cl, err := agentclient.New(ctx, endpoint)
	if err != nil {
		meta_SetCondition(&task.Status.Conditions, metav1.Condition{
			Type: "Dispatched", Status: metav1.ConditionFalse,
			Reason: "AgentUnreachable", Message: err.Error(),
		})
		_ = r.Status().Update(ctx, &task)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	defer cl.Destroy()

	if task.Status.TaskID == "" {
		if res, err := r.dispatch(ctx, cl, &task); err != nil || res.RequeueAfter > 0 || task.Status.State.IsTerminal() {
			return res, err
		}
	}

	return r.consumeStream(ctx, cl, &task)
}

// dispatch sends the Task's message to the agent. The A2A messageId is the
// Task's UID, so a send retried after a crash is recognizable as the same
// message rather than a new request.
func (r *TaskReconciler) dispatch(ctx context.Context, cl *a2aclient.Client, task *meshv1alpha1.Task) (ctrl.Result, error) {
	req := &a2a.SendMessageRequest{
		Message: a2aconv.ToA2AMessage(task.Spec.Message, string(task.UID), task.Spec.ContextID),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}
	if task.Spec.SkillID != "" {
		req.Metadata = map[string]any{mesh.MetaSkillID: task.Spec.SkillID}
	}

	result, err := cl.SendMessage(ctx, req)
	if err != nil {
		log.FromContext(ctx).Error(err, "dispatching task", "agent", task.Spec.AgentRef)
		meta_SetCondition(&task.Status.Conditions, metav1.Condition{
			Type: "Dispatched", Status: metav1.ConditionFalse,
			Reason: "SendMessageFailed", Message: err.Error(),
		})
		_ = r.Status().Update(ctx, task)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	switch res := result.(type) {
	case *a2a.Task:
		applyEvent(task, res)
	case *a2a.Message:
		// The agent answered directly without creating a task.
		task.Status.StatusMessage = a2aconv.FromA2AMessage(res)
		task.Status.State = meshv1alpha1.TaskStateCompleted
		task.Status.LastTransitionTime = metav1.Now()
	}
	task.Status.ObservedGeneration = task.Generation
	meta_SetCondition(&task.Status.Conditions, metav1.Condition{
		Type: "Dispatched", Status: metav1.ConditionTrue,
		Reason: "SendMessageSucceeded", Message: "message accepted by agent",
	})
	return ctrl.Result{}, r.Status().Update(ctx, task)
}

// consumeStream subscribes to the agent's task and folds each event onto the
// Task's status until it goes terminal. Agents that don't stream, or streams
// that end early, fall back to a single GetTask poll.
func (r *TaskReconciler) consumeStream(ctx context.Context, cl *a2aclient.Client, task *meshv1alpha1.Task) (ctrl.Result, error) {
	sub := &a2a.SubscribeToTaskRequest{ID: a2a.TaskID(task.Status.TaskID)}
	for ev, err := range cl.SubscribeToTask(ctx, sub) {
		if err != nil {
			if errors.Is(err, a2a.ErrTaskNotFound) {
				return r.markGone(ctx, task, err)
			}
			log.FromContext(ctx).Info("task stream unavailable, polling instead", "taskID", task.Status.TaskID, "err", err.Error())
			return r.poll(ctx, cl, task)
		}
		applyEvent(task, ev)
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if task.Status.State.IsTerminal() {
			return ctrl.Result{}, nil
		}
	}
	return r.poll(ctx, cl, task)
}

func (r *TaskReconciler) poll(ctx context.Context, cl *a2aclient.Client, task *meshv1alpha1.Task) (ctrl.Result, error) {
	remote, err := cl.GetTask(ctx, &a2a.GetTaskRequest{ID: a2a.TaskID(task.Status.TaskID)})
	if errors.Is(err, a2a.ErrTaskNotFound) {
		return r.markGone(ctx, task, err)
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "polling task", "taskID", task.Status.TaskID)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	applyEvent(task, remote)
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}
	if task.Status.State.IsTerminal() {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: requeuePolling}, nil
}

func (r *TaskReconciler) markGone(ctx context.Context, task *meshv1alpha1.Task, cause error) (ctrl.Result, error) {
	task.Status.State = meshv1alpha1.TaskStateUnknown
	meta_SetCondition(&task.Status.Conditions, metav1.Condition{
		Type: "Dispatched", Status: metav1.ConditionFalse,
		Reason: "TaskGone", Message: cause.Error(),
	})
	return ctrl.Result{}, r.Status().Update(ctx, task)
}

// finalize asks the agent to cancel an in-flight task before releasing the CR.
// Cancellation is best-effort: an unreachable agent must not block deletion.
func (r *TaskReconciler) finalize(ctx context.Context, task *meshv1alpha1.Task) error {
	if !controllerutil.ContainsFinalizer(task, taskFinalizer) {
		return nil
	}
	if task.Status.TaskID != "" && !task.Status.State.IsTerminal() {
		if endpoint, err := r.resolveAgentEndpoint(ctx, task); err == nil {
			if cl, err := agentclient.New(ctx, endpoint); err == nil {
				_, _ = cl.CancelTask(ctx, &a2a.CancelTaskRequest{ID: a2a.TaskID(task.Status.TaskID)})
				_ = cl.Destroy()
			}
		}
	}
	controllerutil.RemoveFinalizer(task, taskFinalizer)
	return r.Update(ctx, task)
}

// applyEvent folds one A2A event onto the Task's status.
func applyEvent(task *meshv1alpha1.Task, ev a2a.Event) {
	prev := task.Status.State
	switch e := ev.(type) {
	case *a2a.Task:
		a2aconv.ApplyTask(&task.Status, e)
	case *a2a.TaskStatusUpdateEvent:
		a2aconv.ApplyStatus(&task.Status, e.Status)
	case *a2a.TaskArtifactUpdateEvent:
		a2aconv.UpsertArtifact(&task.Status, e)
	case *a2a.Message:
		task.Status.StatusMessage = a2aconv.FromA2AMessage(e)
	}
	if task.Status.State != prev {
		task.Status.LastTransitionTime = metav1.Now()
	}
}

func (r *TaskReconciler) resolveAgentEndpoint(ctx context.Context, task *meshv1alpha1.Task) (string, error) {
	var agent meshv1alpha1.Agent
	err := r.Get(ctx, types.NamespacedName{Name: task.Spec.AgentRef, Namespace: task.Namespace}, &agent)
	if apierrors.IsNotFound(err) {
		return "", fmt.Errorf("agent %q not found", task.Spec.AgentRef)
	} else if err != nil {
		return "", err
	}
	if !agent.Status.Ready || agent.Status.Endpoint == "" {
		return "", fmt.Errorf("agent %q not ready yet", task.Spec.AgentRef)
	}
	return agent.Status.Endpoint, nil
}

func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&meshv1alpha1.Task{}).
		Complete(r)
}
