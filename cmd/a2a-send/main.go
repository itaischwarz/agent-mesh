// Command a2a-send sends one message to any A2A v1.0 agent and prints the
// streamed task events. Point it at an agent directly, or at agent-mesh with
// -target to delegate through the mesh the way a calling agent would.
//
//	go run ./cmd/a2a-send -url http://localhost:50051 -text hello
//	go run ./cmd/a2a-send -url http://localhost:50052 -target echo-agent -text hello
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/itaischwarz/agent-mesh/internal/agentclient"
	"github.com/itaischwarz/agent-mesh/pkg/mesh"
)

func main() {
	url := flag.String("url", "http://localhost:50051", "base URL of the A2A agent (or the mesh)")
	text := flag.String("text", "hello from a2a-send", "message text to send")
	target := flag.String("target", "", "when calling the mesh: the Agent to delegate to")
	namespace := flag.String("namespace", "", "when calling the mesh: the target Agent's namespace (default: the mesh's)")
	timeout := flag.Duration("timeout", 2*time.Minute, "give up after this long")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, *url, *text, *target, *namespace); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, url, text, target, namespace string) error {
	card, err := agentclient.ResolveCard(ctx, url)
	if err != nil {
		return err
	}
	fmt.Printf("agent card: %s (%d skills)\n", card.Name, len(card.Skills))
	for _, s := range card.Skills {
		fmt.Printf("  skill %q: %s\n", s.ID, s.Description)
	}

	cl, err := agentclient.New(ctx, url)
	if err != nil {
		return err
	}
	defer cl.Destroy()

	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text))}
	if target != "" {
		req.Metadata = map[string]any{mesh.MetaTargetAgent: target}
		if namespace != "" {
			req.Metadata[mesh.MetaNamespace] = namespace
		}
	}

	var final a2a.TaskState
	for ev, err := range cl.SendStreamingMessage(ctx, req) {
		if err != nil {
			return err
		}
		switch e := ev.(type) {
		case *a2a.Task:
			final = e.Status.State
			fmt.Printf("task     %s  %s%s\n", e.ID, e.Status.State, metaSuffix(e.Metadata))
		case *a2a.TaskStatusUpdateEvent:
			final = e.Status.State
			fmt.Printf("status   %s  %s\n", e.Status.State, textOf(e.Status.Message))
		case *a2a.TaskArtifactUpdateEvent:
			fmt.Printf("artifact %s  %s\n", e.Artifact.Name, textOf(&a2a.Message{Parts: e.Artifact.Parts}))
		case *a2a.Message:
			final = a2a.TaskStateCompleted
			fmt.Printf("message  %s\n", textOf(e))
		}
	}
	if final != a2a.TaskStateCompleted {
		return fmt.Errorf("stream ended in state %q, not completed", final)
	}
	fmt.Println("OK: completed")
	return nil
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

func metaSuffix(md map[string]any) string {
	if name, ok := md[mesh.MetaTaskName].(string); ok {
		return "  (Task resource: " + name + ")"
	}
	return ""
}
