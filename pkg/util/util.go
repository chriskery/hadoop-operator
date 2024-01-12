package util

import (
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"strconv"
)

func GetReplicaName(hadoopCluster metav1.Object, replicaType hadoopclusterorgv1alpha1.ReplicaType) string {
	return hadoopCluster.GetName() + "-" + string(replicaType)
}

func GetDataNodeServiceName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster, replica int) string {
	return GetReplicaName(hadoopCluster, hadoopclusterorgv1alpha1.ReplicaTypeDataNode) + "-" + strconv.Itoa(replica)
}

func GetNodeManagerServiceName(hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster, replica int) string {
	return GetReplicaName(hadoopCluster, hadoopclusterorgv1alpha1.ReplicaTypeNodemanager) + "-" + strconv.Itoa(replica)
}

func GenOwnerReference(obj metav1.Object, kind string) *metav1.OwnerReference {
	controllerRef := &metav1.OwnerReference{
		APIVersion:         hadoopclusterorgv1alpha1.GroupVersion.String(),
		Kind:               kind,
		Name:               obj.GetName(),
		UID:                obj.GetUID(),
		BlockOwnerDeletion: ptr.To(true),
		Controller:         ptr.To(true),
	}
	return controllerRef
}

type ObjectFilterFunction func(obj metav1.Object) bool

// ConvertPodListWithFilter converts pod list to pod pointer list with ObjectFilterFunction
func ConvertPodListWithFilter(list []corev1.Pod, pass ObjectFilterFunction) []*corev1.Pod {
	if list == nil {
		return nil
	}
	ret := make([]*corev1.Pod, 0, len(list))
	for i := range list {
		obj := &list[i]
		if pass != nil {
			if pass(obj) {
				ret = append(ret, obj)
			}
		} else {
			ret = append(ret, obj)
		}
	}
	return ret
}
