package util

import (
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// HadoopJobCreatedReason is added in a mpijob when it is created.
	HadoopJobCreatedReason = "HadoopJobCreated"
	// HadoopJobSubmittedReason is added in a mpijob when it is created.
	HadoopJobSubmittedReason = "HadoopJobSubmittedReason"
	// HadoopJobRunningReason is added in a mpijob when it is running.
	HadoopJobRunningReason = "HadoopJobRunning"
	// HadoopJobSucceededReason is added in a mpijob when it is succeeded.
	HadoopJobSucceededReason = "HadoopJobSucceeded"
	// HadoopJobFailedReason is added in a mpijob when it is failed.
	HadoopJobFailedReason = "HadoopJobFailed"
)

// UpdateJobConditions updates the conditions of the given HadoopJob.
func UpdateJobConditions(status *v1alpha1.HadoopJobStatus, conditionType v1alpha1.JobConditionType, reason, message string) error {
	condition := newJobCondition(conditionType, reason, message)
	setCondition(status, condition)
	return nil
}

// newJobCondition creates a new HadoopJob condition.
func newJobCondition(conditionType v1alpha1.JobConditionType, reason, message string) v1alpha1.JobCondition {
	return v1alpha1.JobCondition{
		Type:               conditionType,
		Status:             corev1.ConditionTrue,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getJobCondition returns the condition with the provided type.
func getJobCondition(status v1alpha1.HadoopJobStatus, condType v1alpha1.JobConditionType) *v1alpha1.JobCondition {
	for _, condition := range status.Conditions {
		if condition.Type == condType {
			return &condition
		}
	}
	return nil
}

// setClusterCondition updates the HadoopJob to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func setCondition(status *v1alpha1.HadoopJobStatus, condition v1alpha1.JobCondition) {

	currentCond := getJobCondition(*status, condition.Type)

	// Do nothing if condition doesn't change
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}

	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	// Append the updated condition
	newConditions := filterOutJobCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// filterOutJobCondition returns a new slice of HadoopJob conditions without conditions with the provided type.
func filterOutJobCondition(conditions []v1alpha1.JobCondition, condType v1alpha1.JobConditionType) []v1alpha1.JobCondition {
	var newConditions []v1alpha1.JobCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}

		c.Status = corev1.ConditionFalse
		newConditions = append(newConditions, c)
	}
	return newConditions
}
