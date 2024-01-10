package util

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// HadoopclusterCreatedReason is added in a mpijob when it is created.
	HadoopclusterCreatedReason = "HadoopCLusterCreated"
	// HadoopclusterRunningReason is added in a mpijob when it is running.
	HadoopclusterRunningReason = "HadoopClusterRunning"
)

// InitializeClusterStatuses initializes the ReplicaStatuses for MPIJob.
func InitializeClusterStatuses(status *v1alpha1.HadoopClusterStatus, replicaType v1alpha1.ReplicaType) {
	if status.ReplicaStatuses == nil {
		status.ReplicaStatuses = make(map[v1alpha1.ReplicaType]*v1alpha1.ReplicaStatus)
	}

	status.ReplicaStatuses[replicaType] = &v1alpha1.ReplicaStatus{}
}

// UpdateClusterReplicaStatuses updates the JobReplicaStatuses according to the pod.
// originally from pkg/controller.v1/tensorflow/status.go (deleted)
func UpdateClusterReplicaStatuses(status *v1alpha1.HadoopClusterStatus, replicaType v1alpha1.ReplicaType, pod *corev1.Pod) {
	switch pod.Status.Phase {
	case corev1.PodRunning:
		status.ReplicaStatuses[replicaType].Active++
	}
}

// UpdateClusterConditions updates the conditions of the given Hadoopcluster.
func UpdateClusterConditions(status *v1alpha1.HadoopClusterStatus, conditionType v1alpha1.ClusterConditionType, reason, message string) error {
	condition := newCondition(conditionType, reason, message)
	setCondition(status, condition)
	return nil
}

// newCondition creates a new Hadoopcluster condition.
func newCondition(conditionType v1alpha1.ClusterConditionType, reason, message string) v1alpha1.ClusterCondition {
	return v1alpha1.ClusterCondition{
		Type:               conditionType,
		Status:             corev1.ConditionTrue,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getCondition returns the condition with the provided type.
func getCondition(status v1alpha1.HadoopClusterStatus, condType v1alpha1.ClusterConditionType) *v1alpha1.ClusterCondition {
	for _, condition := range status.Conditions {
		if condition.Type == condType {
			return &condition
		}
	}
	return nil
}

// setCondition updates the Hadoopcluster to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func setCondition(status *v1alpha1.HadoopClusterStatus, condition v1alpha1.ClusterCondition) {

	currentCond := getCondition(*status, condition.Type)

	// Do nothing if condition doesn't change
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}

	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	// Append the updated condition
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// filterOutCondition returns a new slice of Hadoopcluster conditions without conditions with the provided type.
func filterOutCondition(conditions []v1alpha1.ClusterCondition, condType v1alpha1.ClusterConditionType) []v1alpha1.ClusterCondition {
	var newConditions []v1alpha1.ClusterCondition
	for _, c := range conditions {
		if condType == v1alpha1.ClusterRestarting && c.Type == v1alpha1.ClusterRunning {
			continue
		}

		if c.Type == condType {
			continue
		}

		newConditions = append(newConditions, c)
	}
	return newConditions
}
