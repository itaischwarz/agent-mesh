package controller

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	requeueShort       = 5 * time.Second  // transient errors: agent unreachable, rpc failures
	requeuePolling     = 3 * time.Second  // active task on an agent that doesn't stream
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
