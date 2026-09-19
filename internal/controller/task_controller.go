package controller

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
	"github.com/itaic/agent-mesh/internal/a2aconv"
	a2aclient "github.com/itaic/agent-mesh/pkg/a2a/client"
	a2apb "github.com/itaic/agent-mesh/pkg/a2a/pb"
)

const taskFinalizer = "mesh.itai.dev/task-cleanup"

// TaskReconciler resolves a Task's target Agent, dispatches the task over
// gRPC on first sight, then subscribes to the remote agent's update stream
// until the task reaches a terminal state, mirroring status/artifacts back
// onto the CR.
type TaskReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=mesh.itai.dev,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mesh.itai.dev,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mesh.itai.dev,resources=agents,verbs=get;list;watch

func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var task meshv1alpha1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion: try to cancel the remote task before letting go.
	if !task.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&task, taskFinalizer) {
			if task.Status.TaskID != "" {
				if endpoint, err := r.resolveAgentEndpoint(ctx, &task); err == nil {
					if conn, err := a2aclient.Dial(ctx, endpoint); err == nil {
						_, _ = conn.CancelTask(ctx, task.Status.TaskID)
						_ = conn.Close()
					}
				}
			}
			controllerutil.RemoveFinalizer(&task, taskFinalizer)
			if err := r.Update(ctx, &task); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&task, taskFinalizer) {
		controllerutil.AddFinalizer(&task, taskFinalizer)
		if err := r.Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
	}

	endpoint, err := r.resolveAgentEndpoint(ctx, &task)
	if err != nil {
		meta_SetCondition(&task.Status.Conditions, metav1.Condition{
			Type: "Dispatched", Status: metav1.ConditionFalse,
			Reason: "AgentNotReady", Message: err.Error(),
		})
		task.Status.State = meshv1alpha1.TaskStateUnknown
		_ = r.Status().Update(ctx, &task)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	conn, err := a2aclient.Dial(ctx, endpoint)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, fmt.Errorf("dialing agent %s: %w", endpoint, err)
	}
	defer conn.Close()

	// First reconcile after creation: no TaskID yet, so dispatch.
	if task.Status.TaskID == "" {
		remoteTask, err := conn.SendMessage(ctx, &a2apb.SendMessageRequest{
			Message:   a2aconv.ToProtoMessage(task.Spec.Message),
			ContextId: task.Spec.ContextID,
			SkillId:   task.Spec.SkillID,
		})
		if err != nil {
			logger.Error(err, "dispatching task", "agent", task.Spec.AgentRef)
			meta_SetCondition(&task.Status.Conditions, metav1.Condition{
				Type: "Dispatched", Status: metav1.ConditionFalse,
				Reason: "SendMessageFailed", Message: err.Error(),
			})
			_ = r.Status().Update(ctx, &task)
			return ctrl.Result{RequeueAfter: requeueShort}, nil
		}
		applyRemoteTask(&task, remoteTask)
		meta_SetCondition(&task.Status.Conditions, metav1.Condition{
			Type: "Dispatched", Status: metav1.ConditionTrue,
			Reason: "SendMessageSucceeded", Message: "task accepted by agent",
		})
		if err := r.Status().Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeuePolling}, nil
	}

	// Already dispatched and terminal: nothing left to do.
	if task.Status.State.IsTerminal() {
		return ctrl.Result{}, nil
	}

	// Subscribe to the agent's update stream and mirror events onto status
	// until the task goes terminal, the stream ends, or it breaks.
	return r.consumeStream(ctx, conn, &task)
}

// consumeStream drains streamAgent's channels, folding each event onto the
// Task's status, until a terminal state / final frame / stream end / error.
func (r *TaskReconciler) consumeStream(
	ctx context.Context, conn *a2aclient.Client, task *meshv1alpha1.Task,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	out, errc := r.streamAgent(ctx, conn, task.Status.TaskID)

	for {
		select {
		case <-ctx.Done():
			return ctrl.Result{}, ctx.Err()

		case err := <-errc:
			if status.Code(err) == codes.NotFound {
				// Task was GC'd on the agent side: record it and stop.
				task.Status.State = meshv1alpha1.TaskStateUnknown
				meta_SetCondition(&task.Status.Conditions, metav1.Condition{
					Type: "Dispatched", Status: metav1.ConditionFalse,
					Reason: "TaskGone", Message: err.Error(),
				})
				_ = r.Status().Update(ctx, task)
				return ctrl.Result{}, nil
			}
			logger.Error(err, "task stream broke", "taskID", task.Status.TaskID)
			return ctrl.Result{RequeueAfter: requeueShort}, nil

		case update, ok := <-out:
			if !ok {
				// Clean EOF: persist whatever we last observed, then requeue
				// so a fresh reconcile can re-check / re-subscribe.
				if err := r.Status().Update(ctx, task); err != nil {
					return ctrl.Result{}, err
				}
				if task.Status.State.IsTerminal() {
					return ctrl.Result{}, nil
				}
				return ctrl.Result{RequeueAfter: requeuePolling}, nil
			}

			switch {
			case update.GetStatusUpdate() != nil:
				// A status-only frame may carry no artifacts; don't let it
				// wipe the ones accumulated from ArtifactUpdate frames.
				prevArtifacts := task.Status.Artifacts
				applyRemoteTask(task, update.GetStatusUpdate())
				if len(task.Status.Artifacts) == 0 {
					task.Status.Artifacts = prevArtifacts
				}
			case update.GetArtifactUpdate() != nil:
				upsertArtifact(&task.Status.Artifacts, update.GetArtifactUpdate())
			}

			if err := r.Status().Update(ctx, task); err != nil {
				return ctrl.Result{}, err
			}
			if update.GetFinal() || task.Status.State.IsTerminal() {
				return ctrl.Result{}, nil
			}
		}
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

// streamAgent subscribes to one task's update stream and fans it onto two
// channels: out for TaskUpdate events, errc for a single terminal error.
// out is closed when the stream ends cleanly (io.EOF) or ctx is canceled;
// errc receives exactly one value iff the stream broke abnormally. errc is
// intentionally left open — a nil read would be indistinguishable from "no
// error", so the consumer must treat closed(out) as the end signal.
func (r *TaskReconciler) streamAgent(
	ctx context.Context, conn *a2aclient.Client, taskID string,
) (<-chan *a2apb.TaskUpdate, <-chan error) {
	out := make(chan *a2apb.TaskUpdate, 64)
	errc := make(chan error, 1)

	go func() {
		defer close(out)

		stream, err := conn.SubscribeToTask(ctx, taskID)
		if err != nil {
			errc <- fmt.Errorf("subscribing to task %s: %w", taskID, err)
			return
		}

		for {
			update, err := stream.Recv()
			switch {
			case err == io.EOF:
				return // server closed cleanly; close(out) via defer
			case status.Code(err) == codes.Canceled, errors.Is(err, context.Canceled):
				return // our ctx died (reconciler returned / shutdown); not an error
			case status.Code(err) == codes.NotFound:
				errc <- fmt.Errorf("task %s gone from agent: %w", taskID, err)
				return
			case err != nil:
				errc <- fmt.Errorf("recv on task %s stream: %w", taskID, err)
				return
			}

			select {
			case out <- update:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errc
}


func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&meshv1alpha1.Task{}).
		Complete(r)
}
