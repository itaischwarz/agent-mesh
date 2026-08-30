# agent-mesh

A Kubernetes-native implementation of Google's [A2A protocol](https://a2a-protocol.org)
(now under the Linux Foundation's Agentic AI Foundation). Agents are
first-class CRDs; dispatching work to them is a Task CRD. The controller
handles deployment, capability verification, and task lifecycle so an
operator interacts with the mesh through `kubectl` instead of hand-rolled
gRPC calls.

## Architecture

```
Agent (CRD)  ──owns──>  Deployment + Service  (runs your A2A server image)
     │                          │
     │ AgentReconciler dials the Service, calls GetAgentCard,
     │ writes status.ready / status.observedCapabilities
     ▼
status.endpoint  (ClusterIP DNS:port)
     ▲
     │ resolved by
     │
Task (CRD)  ──> TaskReconciler ──gRPC(A2AService)──> Agent's Service
     spec.agentRef                 SendMessage / GetTask / CancelTask
     spec.message                  status.state, status.artifacts mirrored back
```

- **`api/v1alpha1`** — `Agent` and `Task` CRD types. `Agent.spec` describes a
  deployable A2A server plus the `AgentCard` it should present (skills,
  description). `Task.spec` describes one unit of work (target agent,
  message) and `Task.status` tracks it through the A2A `TaskState`
  lifecycle (`submitted → working → {input-required} → completed/failed/canceled/rejected`).
- **`pkg/a2a/proto/a2a.proto`** — the gRPC contract, trimmed from the
  official `a2a.v1` package to the RPCs this mesh drives: `GetAgentCard`,
  `SendMessage`, `GetTask`, `CancelTask`, `SubscribeToTask` (streaming).
- **`pkg/a2a/client`** — thin Go wrapper around the generated stubs.
- **`internal/controller`** — `AgentReconciler` (deploy + verify) and
  `TaskReconciler` (dispatch + poll, with a finalizer that best-effort
  cancels the remote task on delete).

## What's generated vs. hand-written

This sandbox has no network access, so I couldn't run `protoc`,
`controller-gen`, or `go mod tidy`. Concretely, that means:

- **`pkg/a2a/pb/`** doesn't exist yet — it's the output of `make proto`
  against `pkg/a2a/proto/a2a.proto`. `client.go` and `helpers.go` import it
  as `a2apb` and won't compile until you generate it.
- **`api/v1alpha1/zz_generated.deepcopy.go`** is hand-written to match what
  `controller-gen object:generate` would produce (it's what makes `Agent`/
  `Task` satisfy `runtime.Object`). Regenerate for real with `make generate`
  once `controller-gen` is on your PATH — it should come out equivalent.
- **`config/crd/bases/*.yaml`** and **`config/rbac/role.yaml`** are
  hand-written to match the `+kubebuilder:` markers in the Go source.
  `make manifests` regenerates them from the markers directly, which is the
  source of truth going forward.
- **`go.sum`** doesn't exist — `go mod tidy` needs to fetch the dependency
  graph, which needs network.

## Getting it running

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

make proto        # generates pkg/a2a/pb
make generate      # regenerates zz_generated.deepcopy.go (optional, already hand-matched)
make manifests     # regenerates config/crd + rbac from markers (optional, already hand-matched)
go mod tidy
make build

kubectl apply -f config/crd/bases/
make deploy         # or: go run main.go  against your kubeconfig for local dev

kubectl apply -f config/samples/mesh_v1alpha1_agent.yaml
kubectl apply -f config/samples/mesh_v1alpha1_task.yaml
kubectl get tasks -w
```

You'll need a container image that actually implements the `A2AService`
server side (`GetAgentCard`/`SendMessage`/`GetTask`/`CancelTask`/
`SubscribeToTask`) for `config/samples/mesh_v1alpha1_agent.yaml` to go
`Ready`. If you don't have one handy, the A2A Python or JS SDKs (referenced
from a2a-protocol.org) can stand one up quickly for testing — point
`spec.image` at that.

## Next steps / known gaps

- **Streaming**: `TaskReconciler` currently polls `GetTask` every
  `requeuePolling` (3s). `SubscribeToTask` is defined in the proto and
  wrapped in the client, just not wired into the reconcile loop yet — that's
  the natural next cut (a long-lived goroutine per active Task feeding
  results back through the work queue, rather than poll-driven
  `Requeue`s).
- **mTLS**: `client.Dial` uses insecure transport credentials, fine for
  same-cluster pods during development. Before this leaves a dev cluster,
  wire up mTLS per the `AgentCard`'s declared auth scheme — the signed
  Agent Card fields (`signature`) are in the proto but not yet verified.
- **Multi-agent routing**: `Task.spec.agentRef` is a direct reference.
  Discovery-by-skill (asking "which Agent in this namespace can do X"
  instead of naming one) would turn this from a dispatch layer into an
  actual mesh — a good candidate for a `TaskTemplate` or `Route` CRD later.
- **Webhooks**: no validating/mutating webhooks yet (e.g. rejecting a Task
  whose `skillID` doesn't match any skill the target Agent declares).
