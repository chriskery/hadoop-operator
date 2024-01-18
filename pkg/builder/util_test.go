package builder

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestAppendHadoopConfigMapVolume(t *testing.T) {
	volumes := make([]corev1.Volume, 0)
	configMapName := "test-configmap"
	volumes = appendHadoopConfigMapVolume(volumes, configMapName)
	assert.Equal(t, 1, len(volumes))
	assert.Equal(t, DefaultHadoopOperatorConfVolumeName, volumes[0].Name)
	assert.Equal(t, configMapName, volumes[0].VolumeSource.ConfigMap.LocalObjectReference.Name)
}

func TestAppendHadoopConfigMapVolumeMount(t *testing.T) {
	volumeMounts := make([]corev1.VolumeMount, 0)
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)
	assert.Equal(t, 1, len(volumeMounts))
	assert.Equal(t, DefaultHadoopOperatorConfVolumeName, volumeMounts[0].Name)
	assert.Equal(t, DefaultHadoopOperatorConfPath, volumeMounts[0].MountPath)
}

func TestSetPodEnv(t *testing.T) {
	cluster := testutil.NewHadoopCluster()
	podTemplateSpec := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Env: []corev1.EnvVar{},
				},
			},
		},
	}
	replicaType := v1alpha1.ReplicaTypeDataNode
	setPodEnv(cluster, podTemplateSpec.Spec.Containers, replicaType)

	envNameNodeAddrExist := false
	envHadoopRoleExist := false
	envResourceManagerAddrExist := false

	envHadoopRoleValue := ""
	for _, env := range podTemplateSpec.Spec.Containers[0].Env {
		if env.Name == EnvNameNodeAddr {
			envNameNodeAddrExist = true
		}
		if env.Name == EnvHadoopRole {
			envHadoopRoleExist = true
			envHadoopRoleValue = env.Value
		}
		if env.Name == EnvResourceManagerAddr {
			envResourceManagerAddrExist = true
		}
	}
	assert.Equal(t, true, envNameNodeAddrExist)
	assert.Equal(t, true, envHadoopRoleExist)
	assert.Equal(t, true, envResourceManagerAddrExist)
	assert.Equal(t, EnvHadoopRole, envHadoopRoleValue)
}
