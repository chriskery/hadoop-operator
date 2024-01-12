package util

import (
	"fmt"
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestGenOwnerReference(t *testing.T) {
	hadoopCluster := testutil.NewHadoopCluster()

	type args struct {
		obj metav1.Object
	}
	tests := []struct {
		name string
		args args
		want *metav1.OwnerReference
	}{
		{
			"test-ownerReference",
			args{hadoopCluster},
			&metav1.OwnerReference{
				APIVersion: hadoopCluster.APIVersion,
				Kind:       hadoopCluster.Kind,
				Name:       hadoopCluster.GetName(),
				UID:        hadoopCluster.GetUID(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ownerReference := GenOwnerReference(tt.args.obj, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
			assert.Equalf(t, tt.want.APIVersion, ownerReference.APIVersion, "GenOwnerReference, want %s, got %s", tt.want.APIVersion, ownerReference.APIVersion)
			assert.Equalf(t, tt.want.Kind, ownerReference.Kind, "GenOwnerReference, want %s, got %s", tt.want.Kind, ownerReference.Kind)
			assert.Equalf(t, tt.want.Name, ownerReference.Name, "GenOwnerReference, want %s, got %s", tt.want.Name, ownerReference.Name)
			assert.Equalf(t, tt.want.UID, ownerReference.UID, "GenOwnerReference, want %s, got %s", tt.want.UID, ownerReference.UID)
		})
	}
}

func TestConvertPodListWithFilter(t *testing.T) {
	hadoopCluster := testutil.NewHadoopCluster()
	ownerReference := GenOwnerReference(hadoopCluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)

	var pods []corev1.Pod
	for i := 0; i < 10; i++ {
		pods = append(pods, *testutil.NewBasePod(fmt.Sprintf("pods-%d", i), hadoopCluster, []metav1.OwnerReference{*ownerReference}))
	}

	type args struct {
		list []corev1.Pod
		pass ObjectFilterFunction
	}
	tests := []struct {
		name string
		args args
		want []*corev1.Pod
	}{
		{
			"test-empty",
			args{pods, ObjectFilterFunction(func(obj metav1.Object) bool {
				return false
			})}, []*corev1.Pod{},
		},
		{
			"test-all",
			args{pods, ObjectFilterFunction(func(obj metav1.Object) bool {
				return true
			})}, convertPodsToPointer(pods),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, len(tt.want), len(ConvertPodListWithFilter(tt.args.list, tt.args.pass)), "ConvertPodListWithFilter(%v, %v)", tt.args.list, tt.args.pass)
		})
	}
}

func convertPodsToPointer(pods []corev1.Pod) []*corev1.Pod {
	var pointerPods []*corev1.Pod
	for _, pod := range pods {
		pointerPods = append(pointerPods, &pod)
	}
	return pointerPods
}
