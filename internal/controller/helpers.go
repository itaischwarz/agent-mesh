package controller

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
	a2apb "github.com/itaic/agent-mesh/pkg/a2a/pb"
)

const (
	requeueShort       = 5 * time.Second  // transient errors: dial/rpc failures
	requeuePolling     = 3 * time.Second  // active task, not yet terminal
	requeueSteadyState = 30 * time.Second // healthy agent, periodic re-verification
)

// meta_SetCondition upserts a condition by Type, bumping LastTransitionTime
// only when Status actually changes. Named to match the k8s.io/apimachinery
// meta.SetStatusCondition helper it stands in for (avoiding the extra
// import here keeps this file dependency-light for the scaffold).
func meta_SetCondition(conditions *[]metav1.Condition, newCond metav1.Condition) {
	newCond.LastTransitionTime = metav1.Now()
	for i, existing := range *conditions {
		if existing.Type == newCond.Type {
			if existing.Status == newCond.Status {
				newCond.LastTransitionTime = existing.LastTransitionTime
			}
			(*conditions)[i] = newCond
			return
		}
	}
	*conditions = append(*conditions, newCond)
}

func intstrFromInt32(i int32) intstr.IntOrString {
	return intstr.FromInt32(i)
}

// toProtoMessage converts the CRD's Message (used because CRDs can't
// express proto oneof directly) into the wire Message the generated client
// expects.
func toProtoMessage(m meshv1alpha1.Message) *a2apb.Message {
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
				part.Content = &a2apb.Part_Data{Data: structFromRawExtension(p.Data)}
			}
		}
		parts = append(parts, part)
	}
	return &a2apb.Message{Role: role, Parts: parts}
}

// fromProtoMessage converts a wire Message back into the CRD-friendly shape
// for storing in Task.status.statusMessage.
func fromProtoMessage(m *a2apb.Message) *meshv1alpha1.Message {
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
			out.Data = rawExtensionFromStruct(c.Data)
		}
		parts = append(parts, out)
	}
	return &meshv1alpha1.Message{Role: role, Parts: parts}
}

// applyRemoteTask copies a proto Task response onto Task.status.
func applyRemoteTask(task *meshv1alpha1.Task, remote *a2apb.Task) {
	task.Status.TaskID = remote.Id
	task.Status.State = protoStateToTaskState(remote.State)
	task.Status.StatusMessage = fromProtoMessage(remote.StatusMessage)
	task.Status.LastTransitionTime = metav1.Now()

	artifacts := make([]meshv1alpha1.Artifact, 0, len(remote.Artifacts))
	for _, a := range remote.Artifacts {
		parts := make([]meshv1alpha1.Part, 0, len(a.Parts))
		for _, p := range a.Parts {
			cp := meshv1alpha1.Part{MimeType: p.MimeType}
			switch c := p.Content.(type) {
			case *a2apb.Part_Text:
				cp.Kind, cp.Text = "text", c.Text
			case *a2apb.Part_FileUri:
				cp.Kind, cp.FileURI = "file", c.FileUri
			case *a2apb.Part_Data:
				cp.Kind, cp.Data = "data", rawExtensionFromStruct(c.Data)
			}
			parts = append(parts, cp)
		}
		artifacts = append(artifacts, meshv1alpha1.Artifact{Name: a.Name, Parts: parts})
	}
	task.Status.Artifacts = artifacts
}

// upsertArtifact merges a streamed ArtifactUpdate into the status list,
// replacing any existing entry with the same Name and appending otherwise.
// (A2A artifact updates are keyed by name; a re-sent name is a revision.)
func upsertArtifact(dst *[]meshv1alpha1.Artifact, a *a2apb.Artifact) {
	if a == nil {
		return
	}
	parts := make([]meshv1alpha1.Part, 0, len(a.Parts))
	for _, p := range a.Parts {
		cp := meshv1alpha1.Part{MimeType: p.MimeType}
		switch c := p.Content.(type) {
		case *a2apb.Part_Text:
			cp.Kind, cp.Text = "text", c.Text
		case *a2apb.Part_FileUri:
			cp.Kind, cp.FileURI = "file", c.FileUri
		case *a2apb.Part_Data:
			cp.Kind, cp.Data = "data", rawExtensionFromStruct(c.Data)
		}
		parts = append(parts, cp)
	}
	merged := meshv1alpha1.Artifact{Name: a.Name, Parts: parts}

	for i := range *dst {
		if (*dst)[i].Name == a.Name {
			(*dst)[i] = merged
			return
		}
	}
	*dst = append(*dst, merged)
}

func protoStateToTaskState(s a2apb.TaskState) meshv1alpha1.TaskState {
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
