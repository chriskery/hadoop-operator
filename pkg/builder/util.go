package builder

import (
	"fmt"
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/control"
	"github.com/chriskery/hadoop-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"strconv"
)

const (
	DefaultHadoopOperatorConfPath       = "/etc/hadoop-operator"
	DefaultHadoopOperatorConfVolumeName = "hadoop-config"

	entrypointPath = DefaultHadoopOperatorConfPath + "/entrypoint"

	EnvHadoopRole          = "HADOOP_ROLE"
	EnvNameNodeFormat      = "NAME_NODE_FORMAT"
	EnvNameNodeAddr        = "HADOOP_NAME_NODE_ADDR"
	EnvResourceManagerAddr = "HADOOP_RESOURCE_MANAGER_ADDR"
)

var entrypointCmd = fmt.Sprintf("cp %s /tmp/entrypoint && chmod +x /tmp/entrypoint && /tmp/entrypoint", entrypointPath)

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

func setPodEnv(hadoopCluster *v1alpha1.HadoopCluster, containers []corev1.Container, replicaType v1alpha1.ReplicaType) {
	nameNodeAddr := util.GetReplicaName(hadoopCluster, v1alpha1.ReplicaTypeNameNode)
	resourceManagerAddr := util.GetReplicaName(hadoopCluster, v1alpha1.ReplicaTypeResourcemanager)

	for i := range containers {
		replicaTypeExist := false
		for _, envVar := range containers[i].Env {
			if envVar.Name == EnvHadoopRole {
				replicaTypeExist = true
				break
			}
		}
		if !replicaTypeExist {
			containers[i].Env = append(containers[i].Env, corev1.EnvVar{
				Name:  EnvHadoopRole,
				Value: string(replicaType),
			})
		}

		containers[i].Env = append(containers[i].Env, corev1.EnvVar{
			Name:  EnvNameNodeAddr,
			Value: nameNodeAddr,
		})
		containers[i].Env = append(containers[i].Env, corev1.EnvVar{
			Name:  EnvResourceManagerAddr,
			Value: resourceManagerAddr,
		})

		if replicaType == v1alpha1.ReplicaTypeNameNode {
			containers[i].Env = append(containers[i].Env, corev1.EnvVar{
				Name:  EnvNameNodeFormat,
				Value: strconv.FormatBool(hadoopCluster.Spec.HDFS.NameNode.Format),
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

// isServiceNodePortExpose check if expose type is service and type is nodeport
func isServiceNodePortExpose(expose v1alpha1.ExposeSpec) bool {
	return expose.ExposeType == v1alpha1.ExposeTypeNodePort
}

func isIngressExpose(expose v1alpha1.ExposeSpec) bool {
	return expose.ExposeType == v1alpha1.ExposeTypeIngress
}
