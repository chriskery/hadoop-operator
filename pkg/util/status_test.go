package util

import (
	"testing"

	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
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

func TestApplicationFinishedCondition(t *testing.T) {
	application := &v1alpha1.HadoopApplication{
		Status: v1alpha1.HadoopApplicationStatus{
			Conditions: []v1alpha1.ApplicationCondition{
				{
					Type:   v1alpha1.ApplicationFailed,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	assert.True(t, IsApplicationFinished(application))

	application.Status.Conditions[0].Type = v1alpha1.ApplicationSucceeded
	assert.True(t, IsApplicationFinished(application))

	application.Status.Conditions[0].Status = corev1.ConditionFalse
	assert.False(t, IsApplicationFinished(application))

	application.Status.Conditions = nil
	assert.False(t, IsApplicationFinished(application))
}
