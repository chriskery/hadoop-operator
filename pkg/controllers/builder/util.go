package builder

import (
	corev1 "k8s.io/api/core/v1"
)

const (
	DefaultHadoopOperatorConfPath       = "/etc/hadoop-operator"
	DefaultHadoopOperatorConfVolumeName = "hadoop-config"

	entrypointPath = DefaultHadoopOperatorConfPath + "/entrypoint"

	EnvHadoopRole = "HADOOP_ROLE"
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

func setPodEnv(podTemplateSpec *corev1.PodTemplateSpec, replicaType string) {
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
				Value: replicaType,
			})
		}
	}
}
