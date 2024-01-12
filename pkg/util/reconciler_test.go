package util

import (
	"testing"

	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestOnDependentCreateFunc(t *testing.T) {
	e := event.CreateEvent{
		Object: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha1.ReplicaTypeLabel: "test",
				},
			},
		},
	}
	assert.True(t, OnDependentCreateFunc()(e))
}

func TestOnDependentUpdateFunc(t *testing.T) {
	ctrlManager, envTest := testutil.NewCtrlManager(t)
	defer envTest.Stop()
	e := event.UpdateEvent{
		ObjectNew: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha1.ReplicaTypeLabel: "test",
				},
			},
		},
		ObjectOld: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha1.ReplicaTypeLabel: "test",
				},
			},
		},
	}
	assert.False(t, OnDependentUpdateFunc(ctrlManager.GetClient())(e))
}

func TestResolveControllerRef(t *testing.T) {
	ctrlManager, envTest := testutil.NewCtrlManager(t)
	defer envTest.Stop()

	controllerRef := &metav1.OwnerReference{
		Kind: "HadoopCluster",
		Name: "test-cluster",
		UID:  "test-uid",
	}
	assert.Nil(t, resolveControllerRef(
		"HadoopCluster",
		"test-namespace",
		controllerRef,
		ctrlManager.GetClient(),
	))
}

func TestOnDependentDeleteFunc(t *testing.T) {
	e := event.DeleteEvent{
		Object: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha1.ReplicaTypeLabel: "test",
				},
			},
		},
	}
	assert.True(t, OnDependentDeleteFunc()(e))
}
