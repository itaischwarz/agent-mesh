package controller

import (
	"encoding/json"

	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/runtime"
)

// structFromRawExtension converts a CRD's arbitrary JSON blob (data.Part
// with kind=data) into a protobuf Struct for the wire. Errors are swallowed
// into an empty struct rather than failing the whole reconcile over a
// malformed data part — a real implementation should surface this as a
// condition instead.
func structFromRawExtension(raw *runtime.RawExtension) *structpb.Struct {
	if raw == nil || len(raw.Raw) == 0 {
		return &structpb.Struct{}
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw.Raw, &m); err != nil {
		return &structpb.Struct{}
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		return &structpb.Struct{}
	}
	return s
}

func rawExtensionFromStruct(s *structpb.Struct) *runtime.RawExtension {
	if s == nil {
		return nil
	}
	b, err := json.Marshal(s.AsMap())
	if err != nil {
		return nil
	}
	return &runtime.RawExtension{Raw: b}
}
