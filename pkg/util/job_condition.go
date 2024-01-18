package util

import (
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// HadoopApplicationCreatedReason is added in a mpiapplication when it is created.
	HadoopApplicationCreatedReason = "HadoopApplicationCreated"
	// HadoopApplicationSubmittedReason is added in a mpiapplication when it is created.
	HadoopApplicationSubmittedReason = "HadoopApplicationSubmittedReason"
	// HadoopApplicationRunningReason is added in a mpiapplication when it is running.
	HadoopApplicationRunningReason = "HadoopApplicationRunning"
	// HadoopApplicationSucceededReason is added in a mpiapplication when it is succeeded.
	HadoopApplicationSucceededReason = "HadoopApplicationSucceeded"
	// HadoopApplicationFailedReason is added in a mpiapplication when it is failed.
	HadoopApplicationFailedReason = "HadoopApplicationFailed"
)

// UpdateApplicationConditions updates the conditions of the given HadoopApplication.
func UpdateApplicationConditions(status *v1alpha1.HadoopApplicationStatus, conditionType v1alpha1.ApplicationConditionType, reason, message string) error {
	condition := newApplicationCondition(conditionType, reason, message)
	setCondition(status, condition)
	return nil
}

// newApplicationCondition creates a new HadoopApplication condition.
func newApplicationCondition(conditionType v1alpha1.ApplicationConditionType, reason, message string) v1alpha1.ApplicationCondition {
	return v1alpha1.ApplicationCondition{
		Type:               conditionType,
		Status:             corev1.ConditionTrue,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getApplicationCondition returns the condition with the provided type.
func getApplicationCondition(status v1alpha1.HadoopApplicationStatus, condType v1alpha1.ApplicationConditionType) *v1alpha1.ApplicationCondition {
	for _, condition := range status.Conditions {
		if condition.Type == condType {
			return &condition
		}
	}
	return nil
}

// setClusterCondition updates the HadoopApplication to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func setCondition(status *v1alpha1.HadoopApplicationStatus, condition v1alpha1.ApplicationCondition) {

	currentCond := getApplicationCondition(*status, condition.Type)

	// Do nothing if condition doesn't change
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}

	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	// Append the updated condition
	newConditions := filterOutApplicationCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// filterOutApplicationCondition returns a new slice of HadoopApplication conditions without conditions with the provided type.
func filterOutApplicationCondition(conditions []v1alpha1.ApplicationCondition, condType v1alpha1.ApplicationConditionType) []v1alpha1.ApplicationCondition {
	var newConditions []v1alpha1.ApplicationCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}

		c.Status = corev1.ConditionFalse
		newConditions = append(newConditions, c)
	}
	return newConditions
}
