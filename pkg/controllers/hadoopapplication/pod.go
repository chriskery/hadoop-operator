package hadoopapplication

import corev1 "k8s.io/api/core/v1"

// isPodReady returns true if the pod is ready
func isPodReady(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodRunning &&
		p.Status.ContainerStatuses[0].Ready
}

// isPodFailed returns true if the pod is failed
func isPodFailed(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed
}

// isPodSucceeded returns true if the pod is succeeded
func isPodSucceeded(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded
}

// isPodRunning returns true if the pod is running
func isPodFinished(p *corev1.Pod) bool {
	return isPodSucceeded(p) || isPodFailed(p)
}
