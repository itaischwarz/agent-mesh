// Command echoagent is a minimal A2A v1.0 agent for testing agent-mesh, built
// on the official a2a-go server SDK. It narrates two fake "thinking" steps,
// then completes with an artifact that echoes the input text back. The SDK
// handles task storage, GetTask, and SubscribeToTask; this file only
// describes what the agent does.
package main

import (
	"context"
	"flag"
	"iter"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

const stepDelay = 2 * time.Second

func main() {
	var addr string
	flag.StringVar(&addr, "bind-address", ":50051", "A2A (JSON-RPC over HTTP) bind address")
	flag.Parse()

	// The URL this agent advertises. agent-mesh re-points it at the agent's
	// Service, so the default only matters for direct local testing.
	publicURL := os.Getenv("A2A_PUBLIC_URL")
	if publicURL == "" {
		publicURL = "http://localhost" + addr
	}

	card := &a2a.AgentCard{
		Name:                "echo-agent",
		Description:         "Reference A2A agent for testing agent-mesh: echoes its input back after two simulated thinking steps.",
		Version:             "0.2.0",
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(publicURL, a2a.TransportProtocolJSONRPC)},
		Capabilities:        a2a.AgentCapabilities{Streaming: true},
		DefaultInputModes:   []string{"text/plain"},
		DefaultOutputModes:  []string{"text/plain"},
		Skills: []a2a.AgentSkill{{
			ID:          "echo",
			Name:        "Echo",
			Description: "Repeats back whatever text it's given",
			Tags:        []string{"test"},
		}},
	}

	mux := http.NewServeMux()
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	mux.Handle("/", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(echoExecutor{})))

	log.Printf("echoagent serving A2A on %s (card advertises %s)", addr, publicURL)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

type echoExecutor struct{}

var _ a2asrv.AgentExecutor = echoExecutor{}

func (echoExecutor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		if execCtx.StoredTask == nil {
			if !yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
		}
		input := textOf(execCtx.Message)

		for _, step := range []string{"received \"" + input + "\", thinking...", "calling downstream tool..."} {
			if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, agentText(step)), nil) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(stepDelay):
			}
		}

		artifact := a2a.NewArtifactEvent(execCtx, a2a.NewTextPart("echo: "+input))
		artifact.Artifact.Name = "echo-response"
		if !yield(artifact, nil) {
			return
		}
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
	}
}

func (echoExecutor) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

func textOf(m *a2a.Message) string {
	if m == nil {
		return ""
	}
	var parts []string
	for _, p := range m.Parts {
		if t := p.Text(); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, " ")
}

func agentText(text string) *a2a.Message {
	return a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(text))
}
