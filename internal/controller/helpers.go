package controller

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	meshv1alpha1 "github.com/itaic/agent-mesh/api/v1alpha1"
	"github.com/itaic/agent-mesh/internal/a2aconv"
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

// applyRemoteTask copies a proto Task response onto Task.status.
func applyRemoteTask(task *meshv1alpha1.Task, remote *a2apb.Task) {
	task.Status.TaskID = remote.Id
	task.Status.State = a2aconv.FromProtoTaskState(remote.State)
	task.Status.StatusMessage = a2aconv.FromProtoMessage(remote.StatusMessage)
	task.Status.LastTransitionTime = metav1.Now()

	artifacts := make([]meshv1alpha1.Artifact, 0, len(remote.Artifacts))
	for _, a := range remote.Artifacts {
		artifacts = append(artifacts, a2aconv.FromProtoArtifact(a))
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
	merged := a2aconv.FromProtoArtifact(a)

	for i := range *dst {
		if (*dst)[i].Name == a.Name {
			(*dst)[i] = merged
			return
		}
	}
	*dst = append(*dst, merged)
}
