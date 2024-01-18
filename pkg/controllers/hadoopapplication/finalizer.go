package hadoopapplication

import (
	"context"
	"fmt"
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const DeletionFinalizer = "deletion.finalizers.hadoopapplications.kubecluster.org"

var _ util.Finalizer = &HadoopApplicationReconciler{}

func (r *HadoopApplicationReconciler) Clean(ctx context.Context, obj interface{}) error {
	hadoopApplication, ok := obj.(*v1alpha1.HadoopApplication)
	if !ok {
		return fmt.Errorf("expected a HadoopCluster but got a %T", obj)
	}

	// Add label on all Pods to be picked up in pre-stop hook via Downward API
	if err := r.addHadoopClusterDeletionLabel(ctx, hadoopApplication); err != nil {
		return fmt.Errorf("failed to add deletion markers to HadoopCluster Pods: %w", err)
	}

	err := r.driverBuilder.Clean(hadoopApplication)
	if err != nil {
		return err
	}
	return nil
}

// removeFinalizer removes the deletion finalizer from the HadoopCluster
func (r *HadoopApplicationReconciler) addHadoopClusterDeletionLabel(ctx context.Context, hadoopApplication *v1alpha1.HadoopApplication) error {
	// Create selector.
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{v1alpha1.ApplicationNameLabel: hadoopApplication.Name},
	})
	if err != nil {
		return err
	}

	podList := &corev1.PodList{}
	err = r.List(
		context.Background(),
		podList,
		client.InNamespace(hadoopApplication.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	)
	if err != nil {
		return err
	}

	for i := 0; i < len(podList.Items); i++ {
		pod := &podList.Items[i]
		pod.Labels[v1alpha1.DeletionLabel] = "true"
		if err = r.Client.Update(ctx, pod); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("cannot Update Pod %s in Namespace %s: %w", pod.Name, pod.Namespace, err)
		}
	}

	return nil
}
