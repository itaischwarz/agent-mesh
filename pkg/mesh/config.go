// Package mesh holds the public contract for calling agent-mesh. The mesh is
// itself an A2A v1.0 agent: callers use any A2A client to SendMessage to it,
// naming the agent to delegate to in the request metadata, then follow the
// returned task with GetTask/SubscribeToTask as they would any other agent.
package mesh

// DefaultAddr is the mesh's default A2A (JSON-RPC over HTTP) bind address.
const DefaultAddr = ":50052"

// Request metadata keys understood by the mesh. Set them on
// SendMessageRequest.metadata (or the message's own metadata).
const (
	// MetaTargetAgent names the Agent resource the mesh should delegate to.
	// Required. The mesh's agent card lists each ready agent as a skill
	// whose id is the value to use here.
	MetaTargetAgent = "mesh.itai.dev/target-agent"

	// MetaNamespace selects the namespace of the target Agent. Optional;
	// defaults to the mesh's configured namespace.
	MetaNamespace = "mesh.itai.dev/namespace"

	// MetaSkillID optionally names one of the target agent's skills. A2A
	// v1.0 has no per-request skill selector, so it's forwarded to the
	// target agent as request metadata and is advisory only.
	MetaSkillID = "mesh.itai.dev/skill-id"

	// MetaTaskName is set by the mesh on the tasks it returns: the name of
	// the Task resource tracking the delegation, for kubectl inspection.
	MetaTaskName = "mesh.itai.dev/task-name"
)
