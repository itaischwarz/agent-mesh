package meshserver

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"


	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
	meshpb "github.com/itaic/agent-mesh/pkg/mesh/pb"
)

// Server implements meshpb.MeshServiceServer: the inbound side agent pods
// call to reach the rest of the mesh (the inverse of pkg/a2a/client, which
// is agent-mesh calling out to an agent). It holds the same controller-runtime
// client the reconcilers use, so CreateSubTask/GetAgentInfo just Create/Get
// CRs like any reconciler would.
type Server struct {
	meshpb.UnimplementedMeshServiceServer
	client.Client
}

type grpcRunnable struct{
	srv *grpc.Server 
	addr string 
}


 

func Start(meshAddr string) Server{
	grpServer := grpc.NewServer()
	meshpb.RegisterMeshServiceServer()
	
}
// CreateSubTask creates a Task CR on the caller's behalf, targeting
// req.TargetAgentRef. TaskReconciler owns everything after that (dispatch,
// streaming, status) — this just writes the CR and returns its name.
func (s *Server) CreateSubTask(ctx context.Context, req *meshpb.CreateSubTaskRequest) (*meshpb.CreateSubTaskResponse, error) {
	// TODO: validate req, build a meshv1alpha1.Task from it, s.Create(ctx, task),
	// return &meshpb.CreateSubTaskResponse{TaskName: task.Name}.z

	return nil, status.Error(codes.Unimplemented, "CreateSubTask not implemented")
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
	// TODO: populate &meshpb.GetAgentInfoResponse{...} from agent.Spec/agent.Status.
	return nil, status.Error(codes.Unimplemented, "GetAgentInfo not implemented")
}
