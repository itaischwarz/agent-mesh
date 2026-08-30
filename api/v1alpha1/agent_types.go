package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Skill mirrors an entry in an A2A AgentCard's skills list — a capability
// the agent advertises, matching the AgentCard.skills[] shape in the A2A
// spec (id, name, description, tags, examples).
type Skill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Examples    []string `json:"examples,omitempty"`
}

// AgentSpec defines the desired state of an Agent: what to run, and the
// AgentCard it should present to the mesh. The controller deploys the
// workload and verifies at runtime that the live AgentCard (fetched over
// gRPC) matches what's declared here.
type AgentSpec struct {
	// Image is the container image implementing the A2A server for this agent.
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Port is the gRPC port the agent's A2AService listens on.
	// +kubebuilder:default=50051
	Port int32 `json:"port,omitempty"`

	// Replicas controls how many pods back this agent. Task dispatch load
	// balances across them via the Service the controller creates.
	// +kubebuilder:default=1
	Replicas *int32 `json:"replicas,omitempty"`

	// Name is the agent's identity string, surfaced in its AgentCard.name.
	Name string `json:"name"`

	// Description is surfaced in the agent's AgentCard.description.
	Description string `json:"description,omitempty"`

	// Skills declares the capabilities this agent should expose. The
	// reconciler cross-checks these against the live AgentCard the running
	// pod returns and flags drift in status.conditions.
	Skills []Skill `json:"skills,omitempty"`

	// Env passes through extra environment variables to the agent container
	// (model endpoint, API keys via secretRef, etc).
	Env []corev1.EnvVar `json:"env,omitempty"`

	// Resources sets compute requests/limits on the agent container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// AgentStatus reflects the observed state of the deployed agent.
type AgentStatus struct {
	// Ready is true once the backing Deployment has available replicas AND
	// the controller has successfully fetched a live AgentCard from it.
	Ready bool `json:"ready,omitempty"`

	// ObservedCapabilities lists skill IDs the live AgentCard reported,
	// which may lag or differ from spec.skills if the agent's binary is
	// out of sync with the CRD (e.g. a rollout in progress).
	ObservedCapabilities []string `json:"observedCapabilities,omitempty"`

	// Endpoint is the in-cluster address (ClusterIP Service DNS name and
	// port) other agents/controllers use to reach this agent's A2AService.
	Endpoint string `json:"endpoint,omitempty"`

	// ObservedGeneration lets clients detect whether status reflects the
	// most recent spec.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions follows the standard Kubernetes condition pattern
	// (e.g. type=Ready, type=CardVerified, type=Available).
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Agent is the Schema for the agents API. Each Agent resource is a node in
// the mesh: a deployed workload plus the AgentCard it advertises for
// discovery by other agents and by TaskReconciler.
type Agent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentSpec   `json:"spec,omitempty"`
	Status AgentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AgentList contains a list of Agent.
type AgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Agent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Agent{}, &AgentList{})
}
