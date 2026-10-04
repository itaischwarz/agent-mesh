package a2aconv

import (
	"reflect"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"k8s.io/apimachinery/pkg/runtime"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
)

func TestPartsRoundTrip(t *testing.T) {
	in := []meshv1alpha1.Part{
		{Kind: "text", Text: "hello", MimeType: "text/plain"},
		{Kind: "file", FileURI: "https://example.com/a.pdf", MimeType: "application/pdf", Filename: "a.pdf"},
		{Kind: "raw", Raw: []byte{0x01, 0x02}, MimeType: "application/octet-stream"},
		{Kind: "data", Data: &runtime.RawExtension{Raw: []byte(`{"k":"v"}`)}},
	}
	out := FromA2AParts(ToA2AParts(in))
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in: %+v\nout: %+v", in, out)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	in := meshv1alpha1.Message{Role: "agent", Parts: []meshv1alpha1.Part{{Kind: "text", Text: "hi"}}}
	msg := ToA2AMessage(in, "msg-1", "ctx-1")
	if msg.ID != "msg-1" || msg.ContextID != "ctx-1" || msg.Role != a2a.MessageRoleAgent {
		t.Fatalf("unexpected a2a message: %+v", msg)
	}
	if got := FromA2AMessage(msg); !reflect.DeepEqual(&in, got) {
		t.Fatalf("round trip mismatch: %+v vs %+v", in, got)
	}
}

func TestTaskStateMappingIsBijective(t *testing.T) {
	for a2aState, crdState := range toCRDState {
		if got := ToA2ATaskState(crdState); got != a2aState {
			t.Errorf("ToA2ATaskState(%q) = %q, want %q", crdState, got, a2aState)
		}
		if a2aState.Terminal() != crdState.IsTerminal() {
			t.Errorf("terminal disagreement for %q: a2a=%v crd=%v", a2aState, a2aState.Terminal(), crdState.IsTerminal())
		}
	}
	if got := FromA2ATaskState("TASK_STATE_SOMETHING_NEW"); got != meshv1alpha1.TaskStateUnknown {
		t.Errorf("unrecognized state mapped to %q, want unknown", got)
	}
}

func TestApplyStatusKeepsMessageOnBareTransition(t *testing.T) {
	var st meshv1alpha1.TaskStatus
	ApplyStatus(&st, a2a.TaskStatus{State: a2a.TaskStateWorking, Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("thinking"))})
	ApplyStatus(&st, a2a.TaskStatus{State: a2a.TaskStateCompleted})
	if st.State != meshv1alpha1.TaskStateCompleted {
		t.Fatalf("state = %q, want completed", st.State)
	}
	if st.StatusMessage == nil || st.StatusMessage.Parts[0].Text != "thinking" {
		t.Fatalf("status message lost on bare transition: %+v", st.StatusMessage)
	}
}

func TestUpsertArtifact(t *testing.T) {
	var st meshv1alpha1.TaskStatus
	chunk := func(id, text string, appendTo bool) *a2a.TaskArtifactUpdateEvent {
		return &a2a.TaskArtifactUpdateEvent{
			Append:   appendTo,
			Artifact: &a2a.Artifact{ID: a2a.ArtifactID(id), Parts: a2a.ContentParts{a2a.NewTextPart(text)}},
		}
	}

	UpsertArtifact(&st, chunk("a", "one", false))
	UpsertArtifact(&st, chunk("a", "two", true))
	UpsertArtifact(&st, chunk("b", "other", false))
	if len(st.Artifacts) != 2 || len(st.Artifacts[0].Parts) != 2 {
		t.Fatalf("append did not extend artifact a: %+v", st.Artifacts)
	}

	UpsertArtifact(&st, chunk("a", "replaced", false))
	if parts := st.Artifacts[0].Parts; len(parts) != 1 || parts[0].Text != "replaced" {
		t.Fatalf("non-append update did not replace artifact a: %+v", parts)
	}
}
