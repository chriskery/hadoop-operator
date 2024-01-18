// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	log "github.com/sirupsen/logrus"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func getClusterName(controllerRef *metav1.OwnerReference, namespace string, kind string) string {
	if controllerRef.Kind == kind {
		return namespace + "." + controllerRef.Name
	}
	return ""
}

func LoggerForCluster(cluster metav1.Object) *log.Entry {
	return log.WithFields(log.Fields{
		// We use cluster to match the key used in controller.go
		// Its more common in K8s to use a period to indicate namespace.name. So that's what we use.
		"cluster": cluster.GetNamespace() + "." + cluster.GetName(),
		"uid":     cluster.GetUID(),
	})
}

func LoggerForStatefulSet(statefulSet *appv1.StatefulSet, kind string) *log.Entry {
	cluster := ""
	if controllerRef := metav1.GetControllerOf(statefulSet); controllerRef != nil {
		if controllerRef.Kind == kind {
			cluster = getClusterName(controllerRef, statefulSet.GetNamespace(), kind)
		}
	}
	return log.WithFields(log.Fields{
		// We use cluster to match the key used in controller.go
		// Its more common in K8s to use a period to indicate namespace.name. So that's what we use.
		"cluster":     cluster,
		"statefulSet": statefulSet.GetNamespace() + "." + statefulSet.GetName(),
		"uid":         statefulSet.GetUID(),
	})
}

func LoggerForDeploy(deployment *appv1.Deployment, kind string) *log.Entry {
	cluster := ""
	if controllerRef := metav1.GetControllerOf(deployment); controllerRef != nil {
		if controllerRef.Kind == kind {
			cluster = getClusterName(controllerRef, deployment.GetNamespace(), kind)
		}
	}
	return log.WithFields(log.Fields{
		// We use cluster to match the key used in controller.go
		// Its more common in K8s to use a period to indicate namespace.name. So that's what we use.
		"cluster": cluster,
		"deploy":  deployment.GetNamespace() + "." + deployment.GetName(),
		"uid":     deployment.GetUID(),
	})
}

func LoggerForPod(pod *corev1.Pod, kind string) *log.Entry {
	cluster := ""
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		if controllerRef.Kind == kind {
			cluster = getClusterName(controllerRef, pod.GetNamespace(), kind)
		}
	}
	return log.WithFields(log.Fields{
		// We use cluster to match the key used in controller.go
		// In controller.go we log the key used with the workqueue.
		"cluster": cluster,
		"pod":     pod.Namespace + "." + pod.Name,
		"uid":     pod.ObjectMeta.UID,
	})
}

func LoggerForService(svc *corev1.Service, kind string) *log.Entry {
	cluster := ""
	if controllerRef := metav1.GetControllerOf(svc); controllerRef != nil {
		if controllerRef.Kind == kind {
			cluster = getClusterName(controllerRef, svc.GetNamespace(), kind)
		}
	}
	return log.WithFields(log.Fields{
		// We use cluster to match the key used in controller.go
		// In controller.go we log the key used with the workqueue.
		"cluster": cluster,
		"service": svc.Namespace + "." + svc.Name,
		"uid":     svc.ObjectMeta.UID,
	})
}

func LoggerForConfigMap(cm *corev1.ConfigMap, kind string) *log.Entry {
	cluster := ""
	if controllerRef := metav1.GetControllerOf(cm); controllerRef != nil {
		if controllerRef.Kind == kind {
			cluster = getClusterName(controllerRef, cm.GetNamespace(), kind)
		}
	}
	return log.WithFields(log.Fields{
		// We use cluster to match the key used in controller.go
		// In controller.go we log the key used with the workqueue.
		"kcluster":  cluster,
		"configMap": cm.Namespace + "." + cm.Name,
		"uid":       cm.ObjectMeta.UID,
	})
}

// LoggerForGenericKind generates log entry for generic Kubernetes resource Kind
func LoggerForGenericKind(obj metav1.Object, kind string) *log.Entry {
	cluster := ""
	if controllerRef := metav1.GetControllerOf(obj); controllerRef != nil {
		if controllerRef.Kind == kind {
			cluster = getClusterName(controllerRef, obj.GetNamespace(), kind)
		}
	}
	return log.WithFields(log.Fields{
		// We use application to match the key used in controller.go
		// In controller.go we log the key used with the workqueue.
		"cluster": cluster,
		kind:      obj.GetNamespace() + "." + obj.GetName(),
		"uid":     obj.GetUID(),
	})
}
