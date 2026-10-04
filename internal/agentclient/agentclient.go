// Package agentclient connects the mesh to a deployed agent over A2A v1.0
// using the official a2a-go SDK. The agent's card is fetched from its
// in-cluster Service URL, and the SDK picks whichever binding (JSON-RPC,
// HTTP+JSON, or gRPC) the agent advertises.
package agentclient

import (
	"context"
	"fmt"
	"net/url"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ResolveCard fetches the agent card served at baseURL's
// /.well-known/agent-card.json.
func ResolveCard(ctx context.Context, baseURL string) (*a2a.AgentCard, error) {
	card, err := agentcard.DefaultResolver.Resolve(ctx, baseURL)
	if err != nil {
		return nil, fmt.Errorf("resolving agent card at %s: %w", baseURL, err)
	}
	return card, nil
}

// New resolves the agent's card from baseURL and returns a client for it.
// Callers must Destroy the client when done.
//
// Agents advertise the URL they bound to (often loopback), not their
// in-cluster Service address, so every advertised interface is re-pointed at
// baseURL's host before a transport is chosen. The Service only exposes the
// agent's single port, so an interface on a different port wouldn't be
// reachable anyway.
func New(ctx context.Context, baseURL string) (*a2aclient.Client, error) {
	card, err := ResolveCard(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing agent base URL %q: %w", baseURL, err)
	}
	for _, iface := range card.SupportedInterfaces {
		if iface != nil {
			iface.URL = repoint(iface, base)
		}
	}
	// TLS/mTLS between mesh and agents is not configured yet.
	return a2aclient.NewFromCard(ctx, card,
		a2agrpc.WithGRPCTransport(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
}

func repoint(iface *a2a.AgentInterface, base *url.URL) string {
	if iface.ProtocolBinding == a2a.TransportProtocolGRPC {
		return base.Host // gRPC targets are host:port, no scheme or path
	}
	u, err := url.Parse(iface.URL)
	if err != nil || u.Host == "" {
		return base.String()
	}
	u.Scheme, u.Host = base.Scheme, base.Host
	return u.String()
}
