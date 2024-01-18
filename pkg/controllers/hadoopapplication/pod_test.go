package hadoopapplication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestIsPodReady(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Ready: true},
			},
		},
	}
	assert.True(t, isPodReady(pod))

	pod.Status.Phase = corev1.PodPending
	assert.False(t, isPodReady(pod))

	pod.Status.ContainerStatuses[0].Ready = false
	assert.False(t, isPodReady(pod))
}

func TestIsPodFailed(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
		},
	}
	assert.True(t, isPodFailed(pod))

	pod.Status.Phase = corev1.PodRunning
	assert.False(t, isPodFailed(pod))
}

func TestIsPodSucceeded(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}
	assert.True(t, isPodSucceeded(pod))

	pod.Status.Phase = corev1.PodRunning
	assert.False(t, isPodSucceeded(pod))
}

func TestIsPodFinished(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}
	assert.True(t, isPodFinished(pod))

	pod.Status.Phase = corev1.PodFailed
	assert.True(t, isPodFinished(pod))

	pod.Status.Phase = corev1.PodRunning
	assert.False(t, isPodFinished(pod))
}
