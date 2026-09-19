package meshserver

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
	"github.com/itaic/agent-mesh/internal/a2aconv"
	meshpb "github.com/itaic/agent-mesh/pkg/mesh/pb"
)

// watchPollInterval is how often WatchSubTask re-Gets the Task CR. This is a
// poll under the hood, not a real k8s Watch — same tradeoff TaskReconciler
// itself had before streamAgent existed. A real Watch (client.WithWatch, or
// hooking the manager's informer) would remove the latency later; polling a
// single object is cheap enough to start with and needs no new client type.
const watchPollInterval = 2 * time.Second

// Server implements meshpb.MeshServiceServer: the inbound side agent pods
// call to reach the rest of the mesh (the inverse of pkg/a2a/client, which
// is agent-mesh calling out to an agent). It holds the same controller-runtime
// client the reconcilers use, so CreateSubTask/GetAgentInfo just Create/Get
// CRs like any reconciler would.
type Server struct {
	meshpb.UnimplementedMeshServiceServer
	client.Client
}

// grpcRunnable adapts a *grpc.Server into a manager.Runnable (Start(ctx)
// error) so main.go can hand it to mgr.Add and get the same startup/graceful
// -shutdown lifecycle the reconcilers already get, instead of a bare goroutine.
type grpcRunnable struct {
	srv  *grpc.Server
	addr string
}

// NewRunnable builds a *grpc.Server, registers meshSrv as the MeshService
// implementation, and returns it wrapped as a manager.Runnable ready for
// mgr.Add. grpc.NewServer()'s return value is what satisfies
// grpc.ServiceRegistrar — RegisterMeshServiceServer's first argument —
// which is why registration happens here rather than inside Start.
func NewRunnable(addr string, meshSrv meshpb.MeshServiceServer) manager.Runnable {
	srv := grpc.NewServer()
	meshpb.RegisterMeshServiceServer(srv, meshSrv)
	return &grpcRunnable{srv: srv, addr: addr}
}

// Start implements manager.Runnable. It binds addr, serves until ctx is
// canceled, then drains in-flight RPCs (GracefulStop) before returning.
func (g *grpcRunnable) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", g.addr)
	if err != nil {
		return fmt.Errorf("binding mesh grpc server on %s: %w", g.addr, err)
	}

	go func() {
		<-ctx.Done()
		g.srv.GracefulStop()
	}()

	if err := g.srv.Serve(lis); err != nil {
		return fmt.Errorf("mesh grpc server: %w", err)
	}
	return nil
}

// CreateSubTask creates a Task CR on the caller's behalf, targeting
// req.TargetAgentRef. It does not dial the target agent itself — writing
// the CR is the entire job here. TaskReconciler is the only thing that ever
// calls SendMessage (task_controller.go), and it owns dispatch, streaming,
// and status from that point on. See below for why that split matters.
func (s *Server) CreateSubTask(ctx context.Context, req *meshpb.CreateSubTaskRequest) (*meshpb.CreateSubTaskResponse, error) {
	switch {
	case req.GetTargetAgentRef() == "":
		return nil, status.Error(codes.InvalidArgument, "target_agent_ref is required")
	case req.GetNamespace() == "":
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	case req.GetMessage() == nil:
		return nil, status.Error(codes.InvalidArgument, "message is required")
	}

	msg := a2aconv.FromProtoMessage(req.GetMessage())
	task := &meshv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: req.GetTargetAgentRef() + "-",
			Namespace:    req.GetNamespace(),
		},
		Spec: meshv1alpha1.TaskSpec{
			AgentRef:  req.GetTargetAgentRef(),
			SkillID:   req.GetSkillId(),
			Message:   *msg,
			ContextID: req.GetContextId(),
		},
	}
	if err := s.Create(ctx, task); err != nil {
		return nil, status.Errorf(codes.Internal, "creating subtask: %v", err)
	}
	return &meshpb.CreateSubTaskResponse{TaskId: task.Name}, nil
}

// GetAgentInfo reports a named Agent's declared skills and readiness.
func (s *Server) GetAgentInfo(ctx context.Context, req *meshpb.GetAgentInfoRequest) (*meshpb.GetAgentInfoResponse, error) {
	var agent meshv1alpha1.Agent
	err := s.Get(ctx, types.NamespacedName{Name: req.AgentRef, Namespace: req.Namespace}, &agent)
	if apierrors.IsNotFound(err) {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("agent %q not found", req.AgentRef))
	} else if err != nil {
		return nil, err
	}


	return &meshpb.GetAgentInfoResponse{
		Name:        agent.Name,
		Ready:       agent.Status.Ready,
		SkillIds:    agent.Status.ObservedCapabilities,
		Description: agent.Spec.Description,
	}, nil
}



// WatchSubTask streams SubTaskUpdates for one Task CR until it reaches a
// terminal state. It's the consumer half: pollSubTask does the actual work
// and hands back a channel of already-built *meshpb.SubTaskUpdate, this just
// loops and Sends each one — same producer/consumer split as
// TaskReconciler's streamAgent/consumeStream, one layer further out.
func (s *Server) WatchSubTask(req *meshpb.WatchSubTaskRequest, stream meshpb.MeshService_WatchSubTaskServer) error {
	if req.GetTaskId() == "" || req.GetNamespace() == "" {
		return status.Error(codes.InvalidArgument, "task_id and namespace are required")
	}
	ctx := stream.Context()
	key := types.NamespacedName{Name: req.GetTaskId(), Namespace: req.GetNamespace()}

	out, errc := s.pollSubTask(ctx, key)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-errc:
			return err

		case update, ok := <-out:
			if !ok {
				return nil
			}
			if err := stream.Send(update); err != nil {
				return err
			}
			if update.GetFinal() {
				return nil
			}
		}
	}
}

// pollSubTask re-Gets the Task CR every watchPollInterval and, whenever
// something changed since the last tick, emits one fully-built
// *meshpb.SubTaskUpdate onto out — in the order artifacts, then thought,
// then status, so a terminal status update (Final: true) arrives after any
// deliverables/narration from the same tick, not before. out is closed when
// the task goes terminal or ctx is canceled; errc receives at most one value
// (Get failing) and is left open, same convention as streamAgent's errc.
func (s *Server) pollSubTask(ctx context.Context, key types.NamespacedName) (<-chan *meshpb.SubTaskUpdate, <-chan error) {
	out := make(chan *meshpb.SubTaskUpdate, 16)
	errc := make(chan error, 1)

	go func() {
		defer close(out)

		var lastState meshv1alpha1.TaskState
		var lastThought string
		var sentArtifacts int
		var seq uint64

		ticker := time.NewTicker(watchPollInterval)
		defer ticker.Stop()

		for {
			var task meshv1alpha1.Task
			err := s.Get(ctx, key, &task)
			switch {
			case apierrors.IsNotFound(err):
				errc <- status.Errorf(codes.NotFound, "subtask %q not found", key.Name)
				return
			case err != nil:
				errc <- err
				return
			}

			// Artifacts: the actual deliverables, flush any new ones first.
			for sentArtifacts < len(task.Status.Artifacts) {
				update := &meshpb.SubTaskUpdate{
					Event: &meshpb.SubTaskUpdate_ArtifactUpdate{
						ArtifactUpdate: a2aconv.ToProtoArtifact(task.Status.Artifacts[sentArtifacts]),
					},
				}
				select {
				case out <- update:
				case <-ctx.Done():
					return
				}
				sentArtifacts++
			}

			// Thoughts: the agent's own narration while still working. Only
			// emitted on change, and only pre-terminal — once done, the
			// artifacts are the answer, not the play-by-play.
			if thought := messageText(task.Status.StatusMessage); thought != "" &&
				thought != lastThought && !task.Status.State.IsTerminal() {
				lastThought = thought
				seq++
				update := &meshpb.SubTaskUpdate{
					Event: &meshpb.SubTaskUpdate_ThoughtEvent{
						ThoughtEvent: &meshpb.ThoughtEvent{
							AgentId: task.Spec.AgentRef,
							TaskId:  task.Name,
							Seq:     seq,
							Ts:      timestamppb.Now(),
							Content: thought,
						},
					},
				}
				select {
				case out <- update:
				case <-ctx.Done():
					return
				}
			}

			// Status: the lifecycle signal, sent last so Final arrives after
			// anything else from this tick.
			if task.Status.State != lastState {
				lastState = task.Status.State
				final := task.Status.State.IsTerminal()
				update := &meshpb.SubTaskUpdate{
					Event: &meshpb.SubTaskUpdate_StatusUpdate{
						StatusUpdate: &meshpb.SubTaskStatus{State: a2aconv.ToProtoTaskState(task.Status.State)},
					},
					Final: final,
				}
				select {
				case out <- update:
				case <-ctx.Done():
					return
				}
				if final {
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return out, errc
}

// messageText flattens a Message's text Parts for ThoughtEvent.Content.
// Data/file parts carry no narration, so they're skipped rather than erroring.
func messageText(m *meshv1alpha1.Message) string {
	if m == nil {
		return ""
	}
	var parts []string
	for _, p := range m.Parts {
		if p.Kind == "text" && p.Text != "" {
			parts = append(parts, p.Text)
		}
	}
	return strings.Join(parts, " ")
}