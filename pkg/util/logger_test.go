package util

import (
	"github.com/stretchr/testify/assert"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

func TestLoggerForCluster(t *testing.T) {
	cluster := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}
	logger := LoggerForCluster(cluster)
	assert.Equal(t, "test-namespace.test-cluster", logger.Data["cluster"])
	assert.Equal(t, types.UID("test-uid"), logger.Data["uid"])
}

func TestLoggerForStatefulSet(t *testing.T) {
	statefulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-statefulset",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}
	logger := LoggerForStatefulSet(statefulSet, "StatefulSet")
	assert.Equal(t, "test-namespace.test-statefulset", logger.Data["statefulSet"])
	assert.Equal(t, types.UID("test-uid"), logger.Data["uid"])
}

func TestLoggerForDeploy(t *testing.T) {
	deployment := &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}
	logger := LoggerForDeploy(deployment, "Deployment")
	assert.Equal(t, "test-namespace.test-deployment", logger.Data["deploy"])
	assert.Equal(t, types.UID("test-uid"), logger.Data["uid"])
}

func TestLoggerForPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}
	logger := LoggerForPod(pod, "Pod")
	assert.Equal(t, "test-namespace.test-pod", logger.Data["pod"])
	assert.Equal(t, types.UID("test-uid"), logger.Data["uid"])
}

func TestLoggerForService(t *testing.T) {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}
	logger := LoggerForService(service, "Service")
	assert.Equal(t, "test-namespace.test-service", logger.Data["service"])
	assert.Equal(t, types.UID("test-uid"), logger.Data["uid"])
}

func TestLoggerForConfigMap(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-configmap",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}
	logger := LoggerForConfigMap(configMap, "ConfigMap")
	assert.Equal(t, "test-namespace.test-configmap", logger.Data["configMap"])
	assert.Equal(t, types.UID("test-uid"), logger.Data["uid"])
}

func TestLoggerForGenericKind(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}
	logger := LoggerForGenericKind(pod, "Pod")
	assert.Equal(t, "test-namespace.test-pod", logger.Data["Pod"])
	assert.Equal(t, types.UID("test-uid"), logger.Data["uid"])
}
