package util

import (
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"strconv"
)

func GetResourceManagerName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-resourcemanager"
}

func GetNameNodeName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-namenode"
}

func GetConfigMapName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-configmap"
}

func GetDataNodeServiceName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster, replica int) string {
	return GetDataNodeStatefulSetName(hadoopCluster) + "-" + strconv.Itoa(replica)
}

func GetNodeManagerServiceName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster, replica int) string {
	return GetNodeManagerStatefulSetName(hadoopCluster) + "-" + strconv.Itoa(replica)
}

func GetNodeManagerStatefulSetName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-nodemanager"
}

func GetDataNodeStatefulSetName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-datanode"
}

func GenOwnerReference(obj metav1.Object) *metav1.OwnerReference {
	controllerRef := &metav1.OwnerReference{
		APIVersion:         hadoopclusterorgv1alpha1.GroupVersion.String(),
		Kind:               hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind,
		Name:               obj.GetName(),
		UID:                obj.GetUID(),
		BlockOwnerDeletion: ptr.To(true),
		Controller:         ptr.To(true),
	}
	return controllerRef
}
