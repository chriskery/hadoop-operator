package labels

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"strings"
)

func GenLabels(clusterName string, replicaType v1alpha1.ReplicaType) map[string]string {
	replicaTypeStr := strings.Replace(string(replicaType), "/", "-", -1)
	return map[string]string{
		v1alpha1.ClusterNameLabel: clusterName,
		v1alpha1.ReplicaTypeLabel: replicaTypeStr,
	}
}
