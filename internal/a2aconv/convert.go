// Package a2aconv converts between the CRD's Message/Part/Artifact/TaskState
// shapes (api/v1alpha1) and the official A2A v1.0 Go types from
// github.com/a2aproject/a2a-go/v2/a2a. Both internal/controller (mirroring a
// remote agent's task onto Task.status) and internal/meshserver (serving
// that status back out over A2A) need the same translation, so it lives here.
package a2aconv

import (
	"encoding/json"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"k8s.io/apimachinery/pkg/runtime"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
)

// ToA2AMessage converts the CRD's Message into an A2A message with the given
// messageId. Callers should pass a stable id (e.g. the Task's UID) so a
// retried send is recognizable as the same message.
func ToA2AMessage(m meshv1alpha1.Message, id, contextID string) *a2a.Message {
	role := a2a.MessageRoleUser
	if m.Role == "agent" {
		role = a2a.MessageRoleAgent
	}
	return &a2a.Message{
		ID:        id,
		ContextID: contextID,
		Role:      role,
		Parts:     ToA2AParts(m.Parts),
	}
}

// FromA2AMessage converts an A2A message back into the CRD's shape.
func FromA2AMessage(m *a2a.Message) *meshv1alpha1.Message {
	if m == nil {
		return nil
	}
	role := "user"
	if m.Role == a2a.MessageRoleAgent {
		role = "agent"
	}
	return &meshv1alpha1.Message{Role: role, Parts: FromA2AParts(m.Parts)}
}

// ToA2AParts converts CRD parts into A2A parts.
func ToA2AParts(parts []meshv1alpha1.Part) a2a.ContentParts {
	out := make(a2a.ContentParts, 0, len(parts))
	for _, p := range parts {
		var part *a2a.Part
		switch p.Kind {
		case "text":
			part = a2a.NewTextPart(p.Text)
		case "file":
			part = a2a.NewFileURLPart(a2a.URL(p.FileURI), p.MimeType)
		case "raw":
			part = a2a.NewRawPart(p.Raw)
		case "data":
			part = a2a.NewDataPart(anyFromRawExtension(p.Data))
		default:
			continue
		}
		part.MediaType = p.MimeType
		part.Filename = p.Filename
		out = append(out, part)
	}
	return out
}

// FromA2AParts converts A2A parts into CRD parts.
func FromA2AParts(parts a2a.ContentParts) []meshv1alpha1.Part {
	out := make([]meshv1alpha1.Part, 0, len(parts))
	for _, p := range parts {
		if p == nil {
			continue
		}
		cp := meshv1alpha1.Part{MimeType: p.MediaType, Filename: p.Filename}
		switch c := p.Content.(type) {
		case a2a.Text:
			cp.Kind, cp.Text = "text", string(c)
		case a2a.URL:
			cp.Kind, cp.FileURI = "file", string(c)
		case a2a.Raw:
			cp.Kind, cp.Raw = "raw", []byte(c)
		case a2a.Data:
			cp.Kind, cp.Data = "data", rawExtensionFromAny(c.Value)
		default:
			continue
		}
		out = append(out, cp)
	}
	return out
}

// FromA2AArtifact converts an A2A artifact into the CRD's shape.
func FromA2AArtifact(a *a2a.Artifact) meshv1alpha1.Artifact {
	return meshv1alpha1.Artifact{ID: string(a.ID), Name: a.Name, Parts: FromA2AParts(a.Parts)}
}

// ToA2AArtifact converts a CRD artifact into an A2A artifact.
func ToA2AArtifact(a meshv1alpha1.Artifact) *a2a.Artifact {
	return &a2a.Artifact{ID: a2a.ArtifactID(a.ID), Name: a.Name, Parts: ToA2AParts(a.Parts)}
}

var toCRDState = map[a2a.TaskState]meshv1alpha1.TaskState{
	a2a.TaskStateSubmitted:     meshv1alpha1.TaskStateSubmitted,
	a2a.TaskStateWorking:       meshv1alpha1.TaskStateWorking,
	a2a.TaskStateInputRequired: meshv1alpha1.TaskStateInputRequired,
	a2a.TaskStateAuthRequired:  meshv1alpha1.TaskStateAuthRequired,
	a2a.TaskStateCompleted:     meshv1alpha1.TaskStateCompleted,
	a2a.TaskStateCanceled:      meshv1alpha1.TaskStateCanceled,
	a2a.TaskStateFailed:        meshv1alpha1.TaskStateFailed,
	a2a.TaskStateRejected:      meshv1alpha1.TaskStateRejected,
}

// FromA2ATaskState maps an A2A task state onto the CRD's state enum.
func FromA2ATaskState(s a2a.TaskState) meshv1alpha1.TaskState {
	if st, ok := toCRDState[s]; ok {
		return st
	}
	return meshv1alpha1.TaskStateUnknown
}

// ToA2ATaskState is FromA2ATaskState's inverse.
func ToA2ATaskState(s meshv1alpha1.TaskState) a2a.TaskState {
	for a2aState, crdState := range toCRDState {
		if crdState == s {
			return a2aState
		}
	}
	return a2a.TaskStateUnspecified
}

// ApplyTask copies a full A2A task snapshot onto Task.status.
func ApplyTask(status *meshv1alpha1.TaskStatus, remote *a2a.Task) {
	status.TaskID = string(remote.ID)
	ApplyStatus(status, remote.Status)
	status.Artifacts = make([]meshv1alpha1.Artifact, 0, len(remote.Artifacts))
	for _, a := range remote.Artifacts {
		if a != nil {
			status.Artifacts = append(status.Artifacts, FromA2AArtifact(a))
		}
	}
}

// ApplyStatus copies an A2A task status (state + message) onto Task.status.
// A status update with no message keeps the previous one, so a bare state
// transition doesn't wipe the agent's last narration.
func ApplyStatus(status *meshv1alpha1.TaskStatus, s a2a.TaskStatus) {
	status.State = FromA2ATaskState(s.State)
	if s.Message != nil {
		status.StatusMessage = FromA2AMessage(s.Message)
	}
}

// UpsertArtifact folds a streamed artifact update into Task.status.artifacts:
// append=true extends the parts of the artifact with the same ID, otherwise
// the artifact replaces any existing one with that ID or is added.
func UpsertArtifact(status *meshv1alpha1.TaskStatus, ev *a2a.TaskArtifactUpdateEvent) {
	if ev == nil || ev.Artifact == nil {
		return
	}
	incoming := FromA2AArtifact(ev.Artifact)
	for i := range status.Artifacts {
		if status.Artifacts[i].ID != incoming.ID {
			continue
		}
		if ev.Append {
			status.Artifacts[i].Parts = append(status.Artifacts[i].Parts, incoming.Parts...)
		} else {
			status.Artifacts[i] = incoming
		}
		return
	}
	status.Artifacts = append(status.Artifacts, incoming)
}

func anyFromRawExtension(raw *runtime.RawExtension) any {
	if raw == nil || len(raw.Raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw.Raw, &v); err != nil {
		return nil
	}
	return v
}

func rawExtensionFromAny(v any) *runtime.RawExtension {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return &runtime.RawExtension{Raw: b}
}
