package builder

import (
	"fmt"
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/util"
	utillabels "github.com/chriskery/hadoop-operator/pkg/util/labels"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// getLabels returns labels for object
func getLabels(object metav1.Object, replicaType v1alpha1.ReplicaType) map[string]string {
	labels := utillabels.GenLabels(object.GetName(), replicaType)
	util.MergeMap(labels, object.GetLabels())
	return labels
}

// genPodSpec generates driver pod spec for hadoop job
func getPodSpec(cluster *v1alpha1.HadoopCluster, replicaType v1alpha1.ReplicaType) (*corev1.PodTemplateSpec, error) {
	labels := getLabels(cluster, replicaType)

	var hadoopNodeSpec *v1alpha1.HadoopNodeSpec
	var volumeMounts []corev1.VolumeMount
	switch replicaType {
	case v1alpha1.ReplicaTypeDataNode:
		hadoopNodeSpec = &cluster.Spec.HDFS.DataNode.HadoopNodeSpec
		volumeMounts = cluster.Spec.HDFS.DataNode.VolumeMounts
	case v1alpha1.ReplicaTypeNameNode:
		hadoopNodeSpec = &cluster.Spec.HDFS.NameNode.HadoopNodeSpec
		volumeMounts = cluster.Spec.HDFS.NameNode.VolumeMounts
	case v1alpha1.ReplicaTypeResourcemanager:
		hadoopNodeSpec = &cluster.Spec.Yarn.ResourceManager.HadoopNodeSpec
		volumeMounts = cluster.Spec.Yarn.ResourceManager.VolumeMounts
	case v1alpha1.ReplicaTypeNodemanager:
		hadoopNodeSpec = &cluster.Spec.Yarn.NodeManager.HadoopNodeSpec
		volumeMounts = cluster.Spec.Yarn.NodeManager.VolumeMounts
	default:
		return nil, fmt.Errorf("unknown replica type %s", replicaType)
	}

	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			Name:   util.GetReplicaName(cluster, replicaType),
		},
		Spec: corev1.PodSpec{
			Volumes:          hadoopNodeSpec.Volumes,
			RestartPolicy:    corev1.RestartPolicyAlways,
			DNSPolicy:        corev1.DNSClusterFirstWithHostNet,
			ImagePullSecrets: hadoopNodeSpec.ImagePullSecrets,
			HostNetwork:      hadoopNodeSpec.HostNetwork,
		},
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(
		podTemplateSpec.Spec.Volumes,
		util.GetReplicaName(cluster, v1alpha1.ReplicaTypeConfigMap),
	)

	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nameNodeCmd := []string{"sh", "-c", entrypointCmd}

	containers := []corev1.Container{{
		Name:            string(replicaType),
		Image:           hadoopNodeSpec.Image,
		Command:         nameNodeCmd,
		Resources:       hadoopNodeSpec.Resources,
		VolumeMounts:    volumeMounts,
		Env:             hadoopNodeSpec.Env,
		ImagePullPolicy: hadoopNodeSpec.ImagePullPolicy,
		SecurityContext: hadoopNodeSpec.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(cluster, podTemplateSpec.Spec.Containers, replicaType)
	if err := setInitContainer(cluster, replicaType, podTemplateSpec); err != nil {
		return nil, err
	}
	return podTemplateSpec, nil
}

func getHeadLessNodeServiceSpec(cluster *v1alpha1.HadoopCluster, serviceName string, replicaType v1alpha1.ReplicaType) *corev1.Service {
	labels := getLabels(cluster, replicaType)
	replicaService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
		},
	}

	return replicaService
}

func getNodePodServiceSpec(
	cluster *v1alpha1.HadoopCluster,
	serviceName string,
	replicaType v1alpha1.ReplicaType,
	ports []corev1.ServicePort,
) *corev1.Service {
	labels := getLabels(cluster, replicaType)
	replicaService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
		},
	}

	return replicaService
}
