package util

import (
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
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

// IsApplicationFinished checks whether the cluster is finished.
func IsApplicationFinished(application *v1alpha1.HadoopApplication) bool {
	for _, condition := range application.Status.Conditions {
		if condition.Type == v1alpha1.ApplicationFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
		if condition.Type == v1alpha1.ApplicationSucceeded && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsApplicationRunning checks whether the application is running.
func IsApplicationRunning(application *v1alpha1.HadoopApplication) bool {
	for _, condition := range application.Status.Conditions {
		if condition.Type == v1alpha1.ApplicationRunning && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsApplicationCreated checks whether the application is running.
func IsApplicationCreated(application *v1alpha1.HadoopApplication) bool {
	for _, condition := range application.Status.Conditions {
		if condition.Type == v1alpha1.ApplicationCreated && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsApplicationSubmitted checks whether the application is running.
func IsApplicationSubmitted(application *v1alpha1.HadoopApplication) bool {
	for _, condition := range application.Status.Conditions {
		if condition.Type == v1alpha1.ApplicationSubmitted && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
