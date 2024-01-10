package testutil

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/google/uuid"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	"testing"
)

func NewHadoopCluster() *v1alpha1.HadoopCluster {
	hadoopCluster := &v1alpha1.HadoopCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestHadoopClusterName,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(uuid.New().String()),
		},
		Spec: v1alpha1.HadoopClusterSpec{},
	}
	hadoopCluster.Default()

	return hadoopCluster
}

const (
	DummyContainerName  = "dummy"
	DummyContainerImage = "dummy/dummy:latest"
)

func NewTestReplicaSpecTemplate() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  DummyContainerName,
					Image: DummyContainerImage,
				},
			},
		},
	}
}

func NewBasePod(name string, cluster metav1.Object, refs []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Labels:          map[string]string{},
			Namespace:       cluster.GetNamespace(),
			OwnerReferences: refs,
		},
		Spec: NewTestReplicaSpecTemplate().Spec,
	}
}

func NewBaseService(name string, testJob *v1alpha1.HadoopCluster, t *testing.T) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       testJob.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(testJob, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind))},
		},
	}
}

func NewBaseStatefulSet(name string, testJob *v1alpha1.HadoopCluster, labels map[string]string) *appv1.StatefulSet {
	podTemplate := NewTestReplicaSpecTemplate()
	podTemplate.Labels = labels

	return &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       testJob.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(testJob, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind))},
		},
		Spec: appv1.StatefulSetSpec{
			Replicas: pointer.Int32(1),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: podTemplate,
		},
	}
}
