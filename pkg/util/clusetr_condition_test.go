package util

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestInitializeClusterStatuses(t *testing.T) {
	hadoopCluster := testutil.NewHadoopCluster()
	type args struct {
		status      *v1alpha1.HadoopClusterStatus
		replicaType v1alpha1.ReplicaType
	}
	tests := []struct {
		name string
		args args
	}{
		{"test-resourcemanager", args{&hadoopCluster.Status, v1alpha1.ReplicaTypeResourcemanager}},
		{"test-nodemanager", args{&hadoopCluster.Status, v1alpha1.ReplicaTypeNodemanager}},
		{"test-namenode", args{&hadoopCluster.Status, v1alpha1.ReplicaTypeNameNode}},
		{"test-datanode", args{&hadoopCluster.Status, v1alpha1.ReplicaTypeDataNode}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitializeClusterStatuses(tt.args.status, tt.args.replicaType)
			replicaStatus := hadoopCluster.Status.ReplicaStatuses[tt.args.replicaType]
			assert.NotNilf(t, replicaStatus, "replica status is nil: %s", tt.args.replicaType)
		})
	}
}

func TestUpdateClusterConditions(t *testing.T) {
	hadoopCluster := testutil.NewHadoopCluster()

	type args struct {
		status        *v1alpha1.HadoopClusterStatus
		conditionType v1alpha1.ClusterConditionType
		reason        string
		message       string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test-running",
			args{
				status:        &hadoopCluster.Status,
				conditionType: v1alpha1.ClusterRunning,
				reason:        "test running",
				message:       "test running",
			},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := UpdateClusterConditions(tt.args.status, tt.args.conditionType, tt.args.reason, tt.args.message)
			assert.Equal(t, err != nil, tt.wantErr, "UpdateClusterConditions() error = %v, wantErr %v", err, tt.wantErr)

			condition := getCLusterCondition(*tt.args.status, tt.args.conditionType)
			assert.NotNilf(t, condition, "getCLusterCondition() = %v, want %v", condition, tt.args.conditionType)
			assert.Equal(t, condition.Reason, tt.args.reason, "getCLusterCondition() = %v, want %v", condition.Reason, tt.args.reason)
			assert.Equal(t, condition.Message, tt.args.message, "getCLusterCondition() = %v, want %v", condition.Message, tt.args.message)
			assert.Equal(t, condition.Status, corev1.ConditionTrue, "getCLusterCondition() = %v, want %v", condition.Status, corev1.ConditionTrue)
		})
	}
}

func TestUpdateClusterReplicaStatuses(t *testing.T) {
	hadoopCluster := testutil.NewHadoopCluster()
	replicaType := v1alpha1.ReplicaTypeResourcemanager

	type args struct {
		status      *v1alpha1.HadoopClusterStatus
		replicaType v1alpha1.ReplicaType
		pod         *corev1.Pod
	}
	tests := []struct {
		name       string
		args       args
		wantActive int32
	}{
		{"test-failed", args{
			status:      &hadoopCluster.Status,
			replicaType: replicaType,
			pod:         &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
		}, 0},
		{"test-running", args{
			status:      &hadoopCluster.Status,
			replicaType: replicaType,
			pod: &corev1.Pod{Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			}},
		}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitializeClusterStatuses(tt.args.status, tt.args.replicaType)
			UpdateClusterReplicaStatuses(tt.args.status, tt.args.replicaType, tt.args.pod)
			replicaStatus := hadoopCluster.Status.ReplicaStatuses[tt.args.replicaType]
			assert.NotNilf(t, replicaStatus, "replica status is nil: %s", tt.args.replicaType)

			assert.Equal(t, replicaStatus.Active, tt.wantActive)
		})
	}
}

func Test_filterOutCondition(t *testing.T) {
	initConditions := []v1alpha1.ClusterCondition{
		{
			Type:   v1alpha1.ClusterCreated,
			Status: corev1.ConditionTrue,
		},
		{
			Type:   v1alpha1.ClusterRunning,
			Status: corev1.ConditionTrue,
		},
		{
			Type:   v1alpha1.ClusterRestarting,
			Status: corev1.ConditionTrue,
		},
	}
	type args struct {
		conditions []v1alpha1.ClusterCondition
		condType   v1alpha1.ClusterConditionType
	}
	tests := []struct {
		name string
		args args
		want []v1alpha1.ClusterCondition
	}{
		{
			"test-running", args{conditions: initConditions, condType: v1alpha1.ClusterRunning}, []v1alpha1.ClusterCondition{
				{
					Type:   v1alpha1.ClusterCreated,
					Status: corev1.ConditionTrue,
				},
				{
					Type:   v1alpha1.ClusterRestarting,
					Status: corev1.ConditionTrue,
				},
			},
		},
		{
			"test-created", args{conditions: initConditions, condType: v1alpha1.ClusterCreated}, []v1alpha1.ClusterCondition{
				{
					Type:   v1alpha1.ClusterRunning,
					Status: corev1.ConditionTrue,
				},
				{
					Type:   v1alpha1.ClusterRestarting,
					Status: corev1.ConditionTrue,
				},
			},
		},
		{
			"test-restarted", args{conditions: initConditions, condType: v1alpha1.ClusterRestarting}, []v1alpha1.ClusterCondition{
				{
					Type:   v1alpha1.ClusterCreated,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterOutClusterCondition(tt.args.conditions, tt.args.condType)
			assert.Equal(t, tt.want, got, "filterOutClusterCondition() = %v, want %v", got, tt.want)
		})
	}
}
