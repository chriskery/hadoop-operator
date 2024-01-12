package util

import (
	"testing"

	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestOnDependentCreateFunc(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	defer ginkgo.GinkgoRecover()

	e := event.CreateEvent{
		Object: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha1.ReplicaTypeLabel: "test",
				},
			},
		},
	}
	gomega.NewWithT(t).Expect(OnDependentCreateFunc()(e)).To(gomega.BeTrue())
}

func TestOnDependentUpdateFunc(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	defer ginkgo.GinkgoRecover()

	ctrlManager := testutil.NewCtrlManager()
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
	gomega.Expect(OnDependentUpdateFunc(ctrlManager.GetClient())(e)).To(gomega.BeFalse())
}

func TestResolveControllerRef(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)

	ctrlManager := testutil.NewCtrlManager()
	controllerRef := &metav1.OwnerReference{
		Kind: "HadoopCluster",
		Name: "test-cluster",
		UID:  "test-uid",
	}

	gomega.Expect(resolveControllerRef(
		"HadoopCluster",
		"test-namespace",
		controllerRef,
		ctrlManager.GetClient(),
	)).To(gomega.BeNil())
}
