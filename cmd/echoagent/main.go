// Command echoagent is a minimal reference implementation of A2AServiceServer
// — the part of the A2A protocol agent-mesh never implements itself, since a
// real agent is meant to be an arbitrary external image (Agent.Spec.Image).
// It exists purely for local testing: point an Agent CR at it and there's
// something real for TaskReconciler to dial, dispatch to, and stream from.
//
// Behavior: SendMessage accepts a task, narrates two fake "thinking" steps
// via status_message, then completes with an artifact that echoes the input
// text back. Not for production use.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	a2apb "github.com/itaic/agent-mesh/pkg/a2a/pb"
)

const (
	processingDelay       = 2 * time.Second
	subscribePollInterval = 300 * time.Millisecond
)

func main() {
	var addr string
	flag.StringVar(&addr, "bind-address", ":50051", "A2A gRPC bind address")
	flag.Parse()

	agent := newEchoAgent()

	srv := grpc.NewServer()
	a2apb.RegisterA2AServiceServer(srv, agent)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", addr, err)
	}

	log.Printf("echoagent listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// echoAgent implements a2apb.A2AServiceServer over an in-memory task store.
type echoAgent struct {
	a2apb.UnimplementedA2AServiceServer

	idSeq uint64

	mu    sync.Mutex
	tasks map[string]*taskRecord
}

func newEchoAgent() *echoAgent {
	return &echoAgent{tasks: make(map[string]*taskRecord)}
}

func (a *echoAgent) nextID() string {
	return fmt.Sprintf("echo-%d", atomic.AddUint64(&a.idSeq, 1))
}

func (a *echoAgent) lookup(id string) (*taskRecord, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.tasks[id]
	return t, ok
}

func (a *echoAgent) GetAgentCard(ctx context.Context, req *a2apb.GetAgentCardRequest) (*a2apb.AgentCard, error) {
	return &a2apb.AgentCard{
		Name:        "echo-agent",
		Description: "Reference A2A agent for local agent-mesh testing: echoes its input back as an artifact after two simulated thinking steps.",
		Version:     "0.1.0",
		Skills: []*a2apb.Skill{{
			Id:          "echo",
			Name:        "Echo",
			Description: "Repeats back whatever text it's given",
		}},
		PreferredTransport: []string{"grpc"},
	}, nil
}

func (a *echoAgent) SendMessage(ctx context.Context, req *a2apb.SendMessageRequest) (*a2apb.Task, error) {
	id := a.nextID()
	t := &taskRecord{id: id, contextID: req.GetContextId(), state: a2apb.TaskState_WORKING}

	a.mu.Lock()
	a.tasks[id] = t
	a.mu.Unlock()

	go a.run(t, req.GetMessage())

	return t.snapshot(), nil
}

// run simulates work: two narrated "thinking" steps, then completion with an
// artifact that echoes the input text.
func (a *echoAgent) run(t *taskRecord, in *a2apb.Message) {
	text := extractText(in)

	t.setStatusMessage(agentMessage(fmt.Sprintf("received %q, thinking...", text)))
	time.Sleep(processingDelay)

	t.setStatusMessage(agentMessage("calling downstream tool..."))
	time.Sleep(processingDelay)

	t.complete(&a2apb.Artifact{
		Name:  "echo-response",
		Parts: []*a2apb.Part{{Content: &a2apb.Part_Text{Text: "echo: " + text}}},
	})
}

func (a *echoAgent) GetTask(ctx context.Context, req *a2apb.GetTaskRequest) (*a2apb.Task, error) {
	t, ok := a.lookup(req.GetId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetId())
	}
	return t.snapshot(), nil
}

func (a *echoAgent) CancelTask(ctx context.Context, req *a2apb.CancelTaskRequest) (*a2apb.Task, error) {
	t, ok := a.lookup(req.GetId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetId())
	}
	t.cancel()
	return t.snapshot(), nil
}

// SubscribeToTask streams status/artifact updates until the task goes
// terminal — same poll-and-diff shape as meshserver's pollSubTask, just one
// layer further down the chain (this is the thing TaskReconciler's
// streamAgent dials).
func (a *echoAgent) SubscribeToTask(req *a2apb.GetTaskRequest, stream a2apb.A2AService_SubscribeToTaskServer) error {
	t, ok := a.lookup(req.GetId())
	if !ok {
		return status.Errorf(codes.NotFound, "task %q not found", req.GetId())
	}
	ctx := stream.Context()

	var lastState a2apb.TaskState = -1
	var lastMessage string
	sentArtifact := false

	ticker := time.NewTicker(subscribePollInterval)
	defer ticker.Stop()

	for {
		snap := t.snapshot()
		msgText := extractText(snap.GetStatusMessage())

		if len(snap.GetArtifacts()) > 0 && !sentArtifact {
			sentArtifact = true
			update := &a2apb.TaskUpdate{
				Event: &a2apb.TaskUpdate_ArtifactUpdate{ArtifactUpdate: snap.Artifacts[0]},
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		}

		if snap.State != lastState || msgText != lastMessage {
			lastState, lastMessage = snap.State, msgText
			final := isTerminal(snap.State)
			update := &a2apb.TaskUpdate{
				Event: &a2apb.TaskUpdate_StatusUpdate{StatusUpdate: snap},
				Final: final,
			}
			if err := stream.Send(update); err != nil {
				return err
			}
			if final {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// taskRecord is one in-flight (or completed) task, guarded by its own mutex
// since run() mutates it from a goroutine while RPC handlers read it
// concurrently.
type taskRecord struct {
	mu            sync.Mutex
	id            string
	contextID     string
	state         a2apb.TaskState
	statusMessage *a2apb.Message
	artifact      *a2apb.Artifact
}

func (t *taskRecord) setStatusMessage(m *a2apb.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.statusMessage = m
}

func (t *taskRecord) complete(artifact *a2apb.Artifact) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.artifact = artifact
	t.state = a2apb.TaskState_COMPLETED
}

func (t *taskRecord) cancel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !isTerminal(t.state) {
		t.state = a2apb.TaskState_CANCELED
	}
}

func (t *taskRecord) snapshot() *a2apb.Task {
	t.mu.Lock()
	defer t.mu.Unlock()
	task := &a2apb.Task{
		Id:            t.id,
		ContextId:     t.contextID,
		State:         t.state,
		StatusMessage: t.statusMessage,
	}
	if t.artifact != nil {
		task.Artifacts = []*a2apb.Artifact{t.artifact}
	}
	return task
}

func isTerminal(s a2apb.TaskState) bool {
	switch s {
	case a2apb.TaskState_COMPLETED, a2apb.TaskState_FAILED,
		a2apb.TaskState_CANCELED, a2apb.TaskState_REJECTED:
		return true
	default:
		return false
	}
}

func extractText(m *a2apb.Message) string {
	if m == nil {
		return ""
	}
	var parts []string
	for _, p := range m.GetParts() {
		if t, ok := p.GetContent().(*a2apb.Part_Text); ok {
			parts = append(parts, t.Text)
		}
	}
	return strings.Join(parts, " ")
}

func agentMessage(text string) *a2apb.Message {
	return &a2apb.Message{
		Role:  a2apb.Message_AGENT,
		Parts: []*a2apb.Part{{Content: &a2apb.Part_Text{Text: text}}},
	}
}
