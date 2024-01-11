package builder

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/control"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/json"
)

const (
	DefaultHadoopOperatorConfPath       = "/etc/hadoop-operator"
	DefaultHadoopOperatorConfVolumeName = "hadoop-config"

	entrypointPath = DefaultHadoopOperatorConfPath + "/entrypoint"

	EnvHadoopRole     = "HADOOP_ROLE"
	EnvNameNodeFormat = "NAME_NODE_FORMAT"
)

func appendHadoopConfigMapVolume(volumes []corev1.Volume, configMapName string) []corev1.Volume {
	if volumes == nil {
		volumes = make([]corev1.Volume, 0)
	}
	volumes = append(volumes, corev1.Volume{
		Name: DefaultHadoopOperatorConfVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: configMapName,
				},
			},
		},
	})
	return volumes
}

func appendHadoopConfigMapVolumeMount(volumeMounts []corev1.VolumeMount) []corev1.VolumeMount {
	if volumeMounts == nil {
		volumeMounts = make([]corev1.VolumeMount, 0)
	}
	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      DefaultHadoopOperatorConfVolumeName,
		MountPath: DefaultHadoopOperatorConfPath,
	})
	return volumeMounts
}

func setPodEnv(podTemplateSpec *corev1.PodTemplateSpec, replicaType v1alpha1.ReplicaType) {
	for i := range podTemplateSpec.Spec.Containers {
		replicaTypeExist := false
		for _, envVar := range podTemplateSpec.Spec.Containers[i].Env {
			if envVar.Name == EnvHadoopRole {
				replicaTypeExist = true
				break
			}
		}
		if !replicaTypeExist {
			podTemplateSpec.Spec.Containers[i].Env = append(podTemplateSpec.Spec.Containers[i].Env, corev1.EnvVar{
				Name:  EnvHadoopRole,
				Value: string(replicaType),
			})
		}

		if replicaType == v1alpha1.ReplicaTypeNameNode {
			podTemplateSpec.Spec.Containers[i].Env = append(podTemplateSpec.Spec.Containers[i].Env, corev1.EnvVar{
				Name:  EnvNameNodeFormat,
				Value: "true",
			})
		}

	}
}

func reconcileStatefulSetHPA(statefulSetControl control.StatefulSetControlInterface, object metav1.Object, replicas int32) error {
	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"replicas": replicas,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return err
	}

	return statefulSetControl.PatchStatefulSet(object.GetNamespace(), object.GetName(), patchBytes)
}
