// Package a2aconv converts between the CRD's Message/Part/Artifact shapes
// (api/v1alpha1, which can't express a proto oneof directly) and the A2A
// wire types (pkg/a2a/pb). Both internal/controller (mirroring remote task
// state onto status) and internal/meshserver (building a Task from an
// inbound CreateSubTaskRequest) need this same translation, so it lives
// here rather than being owned by — and duplicated for — either one.
package a2aconv

import (
	"encoding/json"

	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/runtime"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
	a2apb "github.com/itaic/agent-mesh/pkg/a2a/pb"
)

// ToProtoMessage converts the CRD's Message into the wire Message the
// generated client expects.
func ToProtoMessage(m meshv1alpha1.Message) *a2apb.Message {
	role := a2apb.Message_USER
	if m.Role == "agent" {
		role = a2apb.Message_AGENT
	}
	parts := make([]*a2apb.Part, 0, len(m.Parts))
	for _, p := range m.Parts {
		part := &a2apb.Part{MimeType: p.MimeType}
		switch p.Kind {
		case "text":
			part.Content = &a2apb.Part_Text{Text: p.Text}
		case "file":
			part.Content = &a2apb.Part_FileUri{FileUri: p.FileURI}
		case "data":
			if p.Data != nil {
				part.Content = &a2apb.Part_Data{Data: StructFromRawExtension(p.Data)}
			}
		}
		parts = append(parts, part)
	}
	return &a2apb.Message{Role: role, Parts: parts}
}

// FromProtoMessage converts a wire Message back into the CRD-friendly shape.
func FromProtoMessage(m *a2apb.Message) *meshv1alpha1.Message {
	if m == nil {
		return nil
	}
	role := "user"
	if m.Role == a2apb.Message_AGENT {
		role = "agent"
	}
	parts := make([]meshv1alpha1.Part, 0, len(m.Parts))
	for _, p := range m.Parts {
		out := meshv1alpha1.Part{MimeType: p.MimeType}
		switch c := p.Content.(type) {
		case *a2apb.Part_Text:
			out.Kind = "text"
			out.Text = c.Text
		case *a2apb.Part_FileUri:
			out.Kind = "file"
			out.FileURI = c.FileUri
		case *a2apb.Part_Data:
			out.Kind = "data"
			out.Data = RawExtensionFromStruct(c.Data)
		}
		parts = append(parts, out)
	}
	return &meshv1alpha1.Message{Role: role, Parts: parts}
}

// FromProtoArtifact converts a wire Artifact into the CRD-friendly shape.
func FromProtoArtifact(a *a2apb.Artifact) meshv1alpha1.Artifact {
	parts := make([]meshv1alpha1.Part, 0, len(a.Parts))
	for _, p := range a.Parts {
		cp := meshv1alpha1.Part{MimeType: p.MimeType}
		switch c := p.Content.(type) {
		case *a2apb.Part_Text:
			cp.Kind, cp.Text = "text", c.Text
		case *a2apb.Part_FileUri:
			cp.Kind, cp.FileURI = "file", c.FileUri
		case *a2apb.Part_Data:
			cp.Kind, cp.Data = "data", RawExtensionFromStruct(c.Data)
		}
		parts = append(parts, cp)
	}
	return meshv1alpha1.Artifact{Name: a.Name, Parts: parts}
}

// ToProtoArtifact converts the CRD's Artifact into the wire shape, for
// re-streaming a Task's accumulated artifacts back out (WatchSubTask).
func ToProtoArtifact(a meshv1alpha1.Artifact) *a2apb.Artifact {
	parts := make([]*a2apb.Part, 0, len(a.Parts))
	for _, p := range a.Parts {
		part := &a2apb.Part{MimeType: p.MimeType}
		switch p.Kind {
		case "text":
			part.Content = &a2apb.Part_Text{Text: p.Text}
		case "file":
			part.Content = &a2apb.Part_FileUri{FileUri: p.FileURI}
		case "data":
			if p.Data != nil {
				part.Content = &a2apb.Part_Data{Data: StructFromRawExtension(p.Data)}
			}
		}
		parts = append(parts, part)
	}
	return &a2apb.Artifact{Name: a.Name, Parts: parts}
}

// FromProtoTaskState maps the wire TaskState enum onto the CRD's string enum.
func FromProtoTaskState(s a2apb.TaskState) meshv1alpha1.TaskState {
	switch s {
	case a2apb.TaskState_SUBMITTED:
		return meshv1alpha1.TaskStateSubmitted
	case a2apb.TaskState_WORKING:
		return meshv1alpha1.TaskStateWorking
	case a2apb.TaskState_INPUT_REQUIRED:
		return meshv1alpha1.TaskStateInputRequired
	case a2apb.TaskState_AUTH_REQUIRED:
		return meshv1alpha1.TaskStateAuthRequired
	case a2apb.TaskState_COMPLETED:
		return meshv1alpha1.TaskStateCompleted
	case a2apb.TaskState_CANCELED:
		return meshv1alpha1.TaskStateCanceled
	case a2apb.TaskState_FAILED:
		return meshv1alpha1.TaskStateFailed
	case a2apb.TaskState_REJECTED:
		return meshv1alpha1.TaskStateRejected
	default:
		return meshv1alpha1.TaskStateUnknown
	}
}

// ToProtoTaskState is FromProtoTaskState's inverse, for re-streaming a
// Task CR's status.state back out over the wire (WatchSubTask).
func ToProtoTaskState(s meshv1alpha1.TaskState) a2apb.TaskState {
	switch s {
	case meshv1alpha1.TaskStateSubmitted:
		return a2apb.TaskState_SUBMITTED
	case meshv1alpha1.TaskStateWorking:
		return a2apb.TaskState_WORKING
	case meshv1alpha1.TaskStateInputRequired:
		return a2apb.TaskState_INPUT_REQUIRED
	case meshv1alpha1.TaskStateAuthRequired:
		return a2apb.TaskState_AUTH_REQUIRED
	case meshv1alpha1.TaskStateCompleted:
		return a2apb.TaskState_COMPLETED
	case meshv1alpha1.TaskStateCanceled:
		return a2apb.TaskState_CANCELED
	case meshv1alpha1.TaskStateFailed:
		return a2apb.TaskState_FAILED
	case meshv1alpha1.TaskStateRejected:
		return a2apb.TaskState_REJECTED
	default:
		return a2apb.TaskState_TASK_STATE_UNSPECIFIED
	}
}

// StructFromRawExtension converts a CRD's arbitrary JSON blob (a data Part)
// into a protobuf Struct for the wire. Errors are swallowed into an empty
// struct rather than failing the whole call over a malformed data part —
// callers that need stricter behavior should validate upstream.
func StructFromRawExtension(raw *runtime.RawExtension) *structpb.Struct {
	if raw == nil || len(raw.Raw) == 0 {
		return &structpb.Struct{}
	}
	var m map[string]any
	if err := json.Unmarshal(raw.Raw, &m); err != nil {
		return &structpb.Struct{}
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		return &structpb.Struct{}
	}
	return s
}

// RawExtensionFromStruct converts a wire Struct back into the CRD's
// arbitrary-JSON-blob shape.
func RawExtensionFromStruct(s *structpb.Struct) *runtime.RawExtension {
	if s == nil {
		return nil
	}
	b, err := json.Marshal(s.AsMap())
	if err != nil {
		return nil
	}
	return &runtime.RawExtension{Raw: b}
}
