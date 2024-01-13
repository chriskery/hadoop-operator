package util

import (
	"testing"

	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestClusterRunningCondition(t *testing.T) {
	cluster := &v1alpha1.HadoopCluster{
		Status: v1alpha1.HadoopClusterStatus{
			Conditions: []v1alpha1.ClusterCondition{
				{
					Type:   v1alpha1.ClusterRunning,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	assert.True(t, IsClusterRunning(cluster))

	cluster.Status.Conditions[0].Status = corev1.ConditionFalse
	assert.False(t, IsClusterRunning(cluster))

	cluster.Status.Conditions = nil
	assert.False(t, IsClusterRunning(cluster))
}

func TestJobFinishedCondition(t *testing.T) {
	job := &v1alpha1.HadoopJob{
		Status: v1alpha1.HadoopJobStatus{
			Conditions: []v1alpha1.JobCondition{
				{
					Type:   v1alpha1.JobFailed,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	assert.True(t, IsJobFinished(job))

	job.Status.Conditions[0].Type = v1alpha1.JobSucceeded
	assert.True(t, IsJobFinished(job))

	job.Status.Conditions[0].Status = corev1.ConditionFalse
	assert.False(t, IsJobFinished(job))

	job.Status.Conditions = nil
	assert.False(t, IsJobFinished(job))
}
