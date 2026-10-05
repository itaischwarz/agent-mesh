// Package meshserver makes agent-mesh itself an A2A v1.0 agent. A caller
// sends it an ordinary A2A message naming a target Agent in the request
// metadata (see pkg/mesh); the mesh records the delegation as a Task
// resource, which TaskReconciler dispatches to the target, and relays the
// Task's progress back to the caller as standard A2A task events.
package meshserver

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	meshv1alpha1 "github.com/itaischwarz/agent-mesh/api/v1alpha1"
	"github.com/itaischwarz/agent-mesh/internal/a2aconv"
	"github.com/itaischwarz/agent-mesh/pkg/mesh"
)

// labelA2ATaskID ties a Task resource to the A2A task id the mesh gave its
// caller, so Cancel can find the resource to delete.
const labelA2ATaskID = "mesh.itai.dev/a2a-task-id"

// relayPollInterval is how often the mesh re-reads a delegated Task resource
// to relay its progress. A watch would remove this latency later.
const relayPollInterval = time.Second

// Executor implements a2asrv.AgentExecutor (handling requests sent to the
// mesh) and a2asrv.AgentCardProducer (the mesh's own agent card).
type Executor struct {
	client.Client
	// Namespace is where target Agents are looked up and Tasks created,
	// unless the caller overrides it with mesh.MetaNamespace.
	Namespace string
	// PublicURL is the URL the mesh advertises in its agent card.
	PublicURL string
}

var (
	_ a2asrv.AgentExecutor     = (*Executor)(nil)
	_ a2asrv.AgentCardProducer = (*Executor)(nil)
)

// Execute delegates the incoming message to the target agent by creating a
// Task resource, then relays that resource's progress as A2A events.
func (e *Executor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		if execCtx.StoredTask != nil {
			yield(nil, fmt.Errorf("%w: follow-up messages to an existing mesh task are not supported yet", a2a.ErrUnsupportedOperation))
			return
		}
		target, ns := metaString(execCtx, mesh.MetaTargetAgent), metaString(execCtx, mesh.MetaNamespace)
		if ns == "" {
			ns = e.Namespace
		}
		if target == "" {
			yield(nil, fmt.Errorf("%w: request metadata must set %q to the agent to delegate to", a2a.ErrInvalidParams, mesh.MetaTargetAgent))
			return
		}
		var agent meshv1alpha1.Agent
		if err := e.Get(ctx, types.NamespacedName{Name: target, Namespace: ns}, &agent); err != nil {
			if apierrors.IsNotFound(err) {
				err = fmt.Errorf("%w: agent %q not found in namespace %q", a2a.ErrInvalidParams, target, ns)
			}
			yield(nil, err)
			return
		}

		cr := &meshv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: target + "-",
				Namespace:    ns,
				Labels:       map[string]string{labelA2ATaskID: string(execCtx.TaskID)},
			},
			Spec: meshv1alpha1.TaskSpec{
				AgentRef:  target,
				SkillID:   metaString(execCtx, mesh.MetaSkillID),
				Message:   *a2aconv.FromA2AMessage(execCtx.Message),
				ContextID: execCtx.ContextID,
			},
		}
		if err := e.Create(ctx, cr); err != nil {
			yield(nil, fmt.Errorf("creating task resource: %w", err))
			return
		}

		submitted := a2a.NewSubmittedTask(execCtx, execCtx.Message)
		submitted.Metadata = map[string]any{mesh.MetaTaskName: cr.Name, mesh.MetaNamespace: ns}
		if !yield(submitted, nil) {
			return
		}
		e.relay(ctx, execCtx, client.ObjectKeyFromObject(cr), yield)
	}
}

// relay re-reads the Task resource until it's terminal, yielding an artifact
// event for each new or changed artifact and a status event whenever the
// state or status message changes. Artifacts go first so a terminal status
// arrives after the deliverables from the same read.
func (e *Executor) relay(ctx context.Context, execCtx *a2asrv.ExecutorContext, key client.ObjectKey, yield func(a2a.Event, error) bool) {
	sentParts := map[string]int{} // artifact id -> parts already relayed
	var lastState a2a.TaskState = a2a.TaskStateSubmitted
	var lastMsg string

	ticker := time.NewTicker(relayPollInterval)
	defer ticker.Stop()
	for {
		var cr meshv1alpha1.Task
		if err := e.Get(ctx, key, &cr); err != nil {
			if apierrors.IsNotFound(err) {
				yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, agentText("delegated task was deleted")), nil)
			} else {
				yield(nil, err)
			}
			return
		}

		for _, art := range cr.Status.Artifacts {
			if sentParts[art.ID] == len(art.Parts) {
				continue
			}
			sentParts[art.ID] = len(art.Parts)
			ev := &a2a.TaskArtifactUpdateEvent{
				TaskID: execCtx.TaskID, ContextID: execCtx.ContextID,
				Artifact: a2aconv.ToA2AArtifact(art),
			}
			if !yield(ev, nil) {
				return
			}
		}

		state := relayState(&cr)
		msg := cr.Status.StatusMessage
		msgKey := fmt.Sprint(msg)
		if state != a2a.TaskStateUnspecified && (state != lastState || msgKey != lastMsg) {
			lastState, lastMsg = state, msgKey
			var a2aMsg *a2a.Message
			if msg != nil {
				a2aMsg = a2aconv.ToA2AMessage(*msg, a2a.NewMessageID(), execCtx.ContextID)
				a2aMsg.TaskID = execCtx.TaskID
			}
			if !yield(a2a.NewStatusUpdateEvent(execCtx, state, a2aMsg), nil) {
				return
			}
			if state.Terminal() {
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// relayState maps the Task resource's state to what the mesh's caller sees.
// "unknown" means either the target isn't ready yet (stay submitted) or the
// target lost the task (report failure).
func relayState(cr *meshv1alpha1.Task) a2a.TaskState {
	if cr.Status.State != meshv1alpha1.TaskStateUnknown {
		return a2aconv.ToA2ATaskState(cr.Status.State)
	}
	for _, c := range cr.Status.Conditions {
		if c.Type == "Dispatched" && c.Reason == "TaskGone" {
			return a2a.TaskStateFailed
		}
	}
	return a2a.TaskStateUnspecified
}

// Cancel deletes the delegated Task resource; its finalizer asks the target
// agent to cancel before the resource goes away.
func (e *Executor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		var list meshv1alpha1.TaskList
		if err := e.List(ctx, &list, client.MatchingLabels{labelA2ATaskID: string(execCtx.TaskID)}); err != nil {
			yield(nil, err)
			return
		}
		for i := range list.Items {
			if err := e.Delete(ctx, &list.Items[i]); client.IgnoreNotFound(err) != nil {
				yield(nil, err)
				return
			}
		}
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

// Card builds the mesh's agent card. Each ready Agent in the mesh's
// namespace is listed as a skill whose id is the value to put in
// mesh.MetaTargetAgent, so the card doubles as the mesh's directory.
func (e *Executor) Card(ctx context.Context) (*a2a.AgentCard, error) {
	var agents meshv1alpha1.AgentList
	if err := e.List(ctx, &agents, client.InNamespace(e.Namespace)); err != nil {
		return nil, err
	}
	skills := make([]a2a.AgentSkill, 0, len(agents.Items))
	for _, ag := range agents.Items {
		if !ag.Status.Ready {
			continue
		}
		tags := ag.Status.ObservedCapabilities
		if tags == nil {
			tags = []string{}
		}
		skills = append(skills, a2a.AgentSkill{
			ID:          ag.Name,
			Name:        ag.Name,
			Description: ag.Spec.Description,
			Tags:        tags,
		})
	}
	return &a2a.AgentCard{
		Name: "agent-mesh",
		Description: fmt.Sprintf("Delegates tasks to agents running in the mesh. Set request metadata %q "+
			"to one of this card's skill ids.", mesh.MetaTargetAgent),
		Version:             "0.1.0",
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(e.PublicURL, a2a.TransportProtocolJSONRPC)},
		Capabilities:        a2a.AgentCapabilities{Streaming: true},
		DefaultInputModes:   []string{"text/plain"},
		DefaultOutputModes:  []string{"text/plain"},
		Skills:              skills,
	}, nil
}

// metaString reads a string metadata value from the request, falling back
// to the message's own metadata.
func metaString(execCtx *a2asrv.ExecutorContext, key string) string {
	if v, ok := execCtx.Metadata[key].(string); ok && v != "" {
		return v
	}
	if execCtx.Message != nil {
		if v, ok := execCtx.Message.Metadata[key].(string); ok {
			return v
		}
	}
	return ""
}

func agentText(text string) *a2a.Message {
	return a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(text))
}

// httpRunnable serves the mesh's A2A endpoint as a manager.Runnable so it
// shares the manager's startup and graceful shutdown.
type httpRunnable struct {
	srv *http.Server
}

// NewRunnable serves exec over A2A JSON-RPC at addr, with its agent card at
// the standard well-known path.
func NewRunnable(addr string, exec *Executor) manager.Runnable {
	mux := http.NewServeMux()
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewAgentCardHandler(exec))
	mux.Handle("/", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	return &httpRunnable{srv: &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}}
}

func (h *httpRunnable) Start(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() { errc <- h.srv.ListenAndServe() }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("mesh A2A server: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return h.srv.Shutdown(shutdownCtx)
	}
}

// NeedLeaderElection is false: every manager replica can accept A2A
// requests, since all state lives in Task resources rather than in memory.
func (h *httpRunnable) NeedLeaderElection() bool { return false }
