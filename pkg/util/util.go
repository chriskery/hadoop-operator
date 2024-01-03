package util

import (
	kubeclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func GetResourceManagerName(hadoopCluster *kubeclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-resource-manager"
}

func GetNameNodeName(hadoopCluster *kubeclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-namenode"
}

func GetConfigMapName(hadoopCluster *kubeclusterorgv1alpha1.HadoopCluster) string {
	return hadoopCluster.GetName() + "-configmap"
}

func GenOwnerReference(obj metav1.Object) *metav1.OwnerReference {
	controllerRef := &metav1.OwnerReference{
		APIVersion:         kubeclusterorgv1alpha1.GroupVersion.String(),
		Kind:               kubeclusterorgv1alpha1.GroupVersion.WithKind(kubeclusterorgv1alpha1.HadoopClusterKind).Kind,
		Name:               obj.GetName(),
		UID:                obj.GetUID(),
		BlockOwnerDeletion: ptr.To(true),
		Controller:         ptr.To(true),
	}
	return controllerRef
}
