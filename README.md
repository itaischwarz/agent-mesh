# agent-mesh

A Kubernetes-native control plane for [A2A v1.0](https://a2a-protocol.org)
agents (the Agent2Agent protocol, governed by the Linux Foundation's Agentic
AI Foundation). Agents are first-class CRDs; delegating work to one is a Task
CRD. agent-mesh speaks standard A2A on both sides, built on the official
[a2a-go](https://github.com/a2aproject/a2a-go) SDK, so any A2A agent can be
deployed into it and any A2A client can delegate through it.

## Architecture

```
caller (any A2A client)
   │  A2A SendMessage, metadata mesh.itai.dev/target-agent=<agent>
   ▼
agent-mesh (itself an A2A agent)  ──creates──>  Task (CRD)
   ▲  relays Task progress as A2A events            │
   │                                                ▼
   │                                       TaskReconciler
   │                                                │  A2A SendMessage / SubscribeToTask / CancelTask
   │                                                ▼  (JSON-RPC, HTTP+JSON, or gRPC — whatever the card advertises)
Agent (CRD) ──owns──> Deployment + Service  ──>  your A2A agent image
   AgentReconciler fetches /.well-known/agent-card.json,
   writes status.ready / status.observedCapabilities
```

- **`api/v1alpha1`** — `Agent` and `Task` CRD types. `Task.status` tracks the
  A2A task lifecycle (`submitted → working → {input-required} →
  completed/failed/canceled/rejected`) plus streamed artifacts.
- **`internal/controller`** — `AgentReconciler` (deploy + verify the live agent
  card) and `TaskReconciler` (dispatch, stream, and a finalizer that cancels
  the remote task on delete).
- **`internal/meshserver`** — the mesh's own A2A endpoint. Its agent card lists
  every ready Agent as a skill, so it doubles as the mesh's directory.
- **`internal/agentclient`** — resolves an agent's card from its Service and
  builds an a2a-go client, re-pointing advertised URLs at the Service.
- **`internal/a2aconv`** — conversions between CRD types and a2a-go types.
- **`pkg/mesh`** — the public contract for calling the mesh (metadata keys).
- **`cmd/echoagent`** — a minimal A2A agent for testing.
- **`cmd/a2a-send`** — sends one message to any A2A agent (or the mesh) and
  prints the streamed events.

## Writing an agent for the mesh

Any A2A v1.0 agent works, in any language. It must:

1. Serve its agent card at `/.well-known/agent-card.json`.
2. Listen on `0.0.0.0:<Agent.spec.port>` — not loopback, or the Service can't
   reach it. (The URL it advertises in its card doesn't matter; the mesh
   re-points it at the Service.)

## Running it

Local checks, no cluster needed:

```sh
make test          # unit tests
make test-a2a      # builds the echoagent image, runs it, round-trips a task
```

On a kind cluster named `agent-mesh`:

```sh
kind create cluster --name agent-mesh
make deploy-local                                   # build + load images, apply CRDs/RBAC/manager
kubectl apply -f config/samples/echoagent_agent.yaml
kubectl get agents -w                               # wait for READY=true

kubectl -n agent-mesh-system port-forward deploy/agent-mesh-controller-manager 50052:50052 &
go run ./cmd/a2a-send -url http://localhost:50052 -target echo-agent -text "hello mesh"
kubectl get tasks                                   # the delegation, recorded as a Task
```

`make generate` and `make manifests` regenerate deepcopy code, CRDs, and RBAC
from the `+kubebuilder` markers (requires `controller-gen`).

## Known gaps

- **Mesh task state is in memory.** The mesh's A2A task store is the SDK's
  in-memory default; if the manager restarts, callers lose their mesh task
  ids (the Task resources survive). A Task-backed task store would fix this.
- **No multi-turn through the mesh.** Follow-up messages to an existing mesh
  task (e.g. answering `input-required`) are rejected.
- **No in-cluster Service for the mesh port.** Reach it via port-forward for
  now; set `--mesh-public-url` once it has a stable address.
- **Polling relay.** The mesh re-reads each Task resource every second to
  relay progress; a watch would remove the latency.
- **No TLS or auth.** Mesh-to-agent traffic is plaintext, and agent card
  signatures aren't verified yet.
- **Routing is by name.** Callers name the target agent; choosing an agent by
  skill is future work.
