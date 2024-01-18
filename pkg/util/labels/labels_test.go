package labels

import (
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/util/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenLabels(t *testing.T) {
	type args struct {
		clusterName string
		replicaType v1alpha1.ReplicaType
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"test-labels", args{replicaType: v1alpha1.ReplicaTypeNodemanager, clusterName: testutil.TestHadoopClusterName},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GenLabels(tt.args.clusterName, tt.args.replicaType)
			assert.NotNil(t, got[v1alpha1.ClusterNameLabel])
			assert.NotNil(t, got[v1alpha1.ReplicaTypeLabel])
		})
	}
}
