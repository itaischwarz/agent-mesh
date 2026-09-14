// Package client wraps the generated A2A gRPC stubs (google.golang.org/grpc
// + protoc-gen-go) behind a small interface the controllers depend on. It
// imports github.com/itaic/agent-mesh/pkg/a2a/pb, which is generated from
// pkg/a2a/proto/a2a.proto via:
//
//	make proto
//
// (see the Makefile at the repo root). Until that's run, this package won't
// compile — that's expected for a fresh checkout of generated-code-backed
// projects and is called out in the README.
package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	a2apb "github.com/itaic/agent-mesh/pkg/a2a/pb"
)

// Client is a thin, mockable wrapper so controllers don't take a direct
// dependency on grpc.ClientConn plumbing.
type Client struct {
	conn *grpc.ClientConn
	rpc  a2apb.A2AServiceClient
}

// Dial opens a gRPC connection to an agent's in-cluster Service endpoint,
// e.g. "my-agent.default.svc.cluster.local:50051".
//
// This uses insecure transport credentials because it's talking to sibling
// pods over the cluster-internal network; swap in TLS creds (mTLS per the
// AgentCard's declared auth scheme) before this ever leaves a dev cluster.
func Dial(ctx context.Context, target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialing agent at %s: %w", target, err)
	}
	return &Client{conn: conn, rpc: a2apb.NewA2AServiceClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// GetAgentCard fetches the live capability declaration — used by
// AgentReconciler to reconcile spec.skills against reality.
func (c *Client) GetAgentCard(ctx context.Context) (*a2apb.AgentCard, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.rpc.GetAgentCard(ctx, &a2apb.GetAgentCardRequest{})
}

// SendMessage dispatches a new task and returns its initial state
// (typically SUBMITTED or WORKING). TaskReconciler stores the returned
// Task.Id in status.taskID for subsequent polling.
func (c *Client) SendMessage(ctx context.Context, req *a2apb.SendMessageRequest) (*a2apb.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.rpc.SendMessage(ctx, req)
}

// GetTask polls current task state.
func (c *Client) GetTask(ctx context.Context, id string) (*a2apb.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.rpc.GetTask(ctx, &a2apb.GetTaskRequest{Id: id})
}


// CancelTask requests cooperative cancellation, used when a Task CR is deleted
// mid-flight (see TaskReconciler's finalizer).
func (c *Client) CancelTask(ctx context.Context, id string) (*a2apb.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.rpc.CancelTask(ctx, &a2apb.CancelTaskRequest{Id: id})
}

// SubscribeToTask returns a stream of updates; callers range over it until
// the channel closes or an update arrives with Final=true. Not yet wired
// into TaskReconciler (which currently polls) — see README "Next steps".
func (c *Client) SubscribeToTask(ctx context.Context, id string) (a2apb.A2AService_SubscribeToTaskClient, error) {
	return c.rpc.SubscribeToTask(ctx, &a2apb.GetTaskRequest{Id: id})
}
