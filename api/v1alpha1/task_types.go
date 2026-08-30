package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TaskState mirrors the A2A protocol's TaskState enum (spec section on Task
// lifecycle): submitted -> working -> {input-required <-> working} ->
// {completed | failed | canceled | rejected}. auth-required covers agents
// that need an out-of-band credential mid-task.
// +kubebuilder:validation:Enum=submitted;working;input-required;auth-required;completed;canceled;failed;rejected;unknown
type TaskState string

const (
	TaskStateSubmitted     TaskState = "submitted"
	TaskStateWorking       TaskState = "working"
	TaskStateInputRequired TaskState = "input-required"
	TaskStateAuthRequired  TaskState = "auth-required"
	TaskStateCompleted     TaskState = "completed"
	TaskStateCanceled      TaskState = "canceled"
	TaskStateFailed        TaskState = "failed"
	TaskStateRejected      TaskState = "rejected"
	TaskStateUnknown       TaskState = "unknown"
)

// Part is a discriminated union matching A2A's Part type (TextPart /
// DataPart / FilePart collapsed into one struct with a kind discriminator,
// since CRDs don't support proto oneof directly).
type Part struct {
	// +kubebuilder:validation:Enum=text;data;file
	Kind string `json:"kind"`
	Text string `json:"text,omitempty"`
	// Data holds arbitrary structured JSON for kind=data.
	Data *runtime.RawExtension `json:"data,omitempty"`
	// FileURI is used for kind=file (agent-mesh resolves it against an
	// in-cluster artifact store rather than embedding bytes in the CRD).
	FileURI  string `json:"fileURI,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
}

// Message mirrors A2A's Message object: a role plus one or more Parts.
type Message struct {
	// +kubebuilder:validation:Enum=user;agent
	Role  string `json:"role"`
	Parts []Part `json:"parts"`
}

// TaskSpec defines the work to dispatch and where to dispatch it.
type TaskSpec struct {
	// AgentRef names the Agent resource (in this namespace) that should
	// receive the task. TaskReconciler resolves this to status.endpoint.
	// +kubebuilder:validation:Required
	AgentRef string `json:"agentRef"`

	// SkillID optionally pins the task to one of the target Agent's
	// declared skills; left empty, the agent chooses how to route it.
	SkillID string `json:"skillID,omitempty"`

	// Message is the initial user message sent via A2A SendMessage.
	// +kubebuilder:validation:Required
	Message Message `json:"message"`

	// ContextID lets multiple Task resources share one A2A conversation
	// context, mirroring the spec's contextId field for multi-turn tasks.
	ContextID string `json:"contextID,omitempty"`
}

// Artifact mirrors A2A's Artifact object — a named bundle of output Parts
// produced by the agent while working a task.
type Artifact struct {
	Name  string `json:"name,omitempty"`
	Parts []Part `json:"parts,omitempty"`
}

// TaskStatus reflects what the remote agent has reported back.
type TaskStatus struct {
	// TaskID is the identifier the remote agent assigned on SendMessage;
	// used for all subsequent GetTask/CancelTask/SubscribeToTask calls.
	TaskID string `json:"taskID,omitempty"`

	State TaskState `json:"state,omitempty"`

	// StatusMessage carries the agent's latest status Message, e.g. an
	// input-required prompt or a failure reason.
	StatusMessage *Message `json:"statusMessage,omitempty"`

	Artifacts []Artifact `json:"artifacts,omitempty"`

	// LastTransitionTime is when State last changed, for backoff/timeout logic.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`

	ObservedGeneration int64               `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition  `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=`.spec.agentRef`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Task is the Schema for the tasks API — one unit of A2A work dispatched
// from the mesh to a target Agent, with status kept in sync by polling
// (or, once wired up, subscribing to) the agent's task stream.
type Task struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskSpec   `json:"spec,omitempty"`
	Status TaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TaskList contains a list of Task.
type TaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Task `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Task{}, &TaskList{})
}
