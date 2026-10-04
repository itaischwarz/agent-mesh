package agentclient

import (
	"net/url"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestRepoint(t *testing.T) {
	base, _ := url.Parse("http://echo-agent.default.svc.cluster.local:50051")
	cases := []struct {
		name  string
		iface a2a.AgentInterface
		want  string
	}{
		{"loopback jsonrpc", a2a.AgentInterface{URL: "http://127.0.0.1:9999", ProtocolBinding: a2a.TransportProtocolJSONRPC},
			"http://echo-agent.default.svc.cluster.local:50051"},
		{"keeps path", a2a.AgentInterface{URL: "http://localhost:8080/a2a/v1", ProtocolBinding: a2a.TransportProtocolHTTPJSON},
			"http://echo-agent.default.svc.cluster.local:50051/a2a/v1"},
		{"grpc is host:port", a2a.AgentInterface{URL: "localhost:9000", ProtocolBinding: a2a.TransportProtocolGRPC},
			"echo-agent.default.svc.cluster.local:50051"},
		{"unparseable falls back to base", a2a.AgentInterface{URL: "", ProtocolBinding: a2a.TransportProtocolJSONRPC},
			"http://echo-agent.default.svc.cluster.local:50051"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := repoint(&tc.iface, base); got != tc.want {
				t.Errorf("repoint = %q, want %q", got, tc.want)
			}
		})
	}
}
