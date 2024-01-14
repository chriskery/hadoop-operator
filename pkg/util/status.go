package util

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// IsClusterRunning checks whether the cluster is running.
func IsClusterRunning(cluster *v1alpha1.HadoopCluster) bool {
	if cluster.Status.Conditions == nil {
		return false
	}

	for _, condition := range cluster.Status.Conditions {
		if condition.Type == v1alpha1.ClusterRunning && condition.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

// IsJobFinished checks whether the cluster is finished.
func IsJobFinished(job *v1alpha1.HadoopJob) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == v1alpha1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
		if condition.Type == v1alpha1.JobSucceeded && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsJobRunning checks whether the job is running.
func IsJobRunning(job *v1alpha1.HadoopJob) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == v1alpha1.JobRunning && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
