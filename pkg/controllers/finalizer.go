package controllers

import (
	"context"
	"fmt"
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientretry "k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const DeletionFinalizer = "deletion.finalizers.hadoopclusters.kubeclusetr.org"

// addFinalizerIfNeeded adds a deletion finalizer if the HadoopCluster does not have one yet and is not marked for deletion
func (r *HadoopClusterReconciler) addFinalizerIfNeeded(ctx context.Context, hadoopCluster *v1alpha1.HadoopCluster) error {
	if hadoopCluster.ObjectMeta.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(hadoopCluster, DeletionFinalizer) {
		controllerutil.AddFinalizer(hadoopCluster, DeletionFinalizer)
		if err := r.Client.Update(ctx, hadoopCluster); err != nil {
			return err
		}
	}
	return nil
}

func (r *HadoopClusterReconciler) removeFinalizer(ctx context.Context, hadoopCluster *v1alpha1.HadoopCluster) error {
	controllerutil.RemoveFinalizer(hadoopCluster, DeletionFinalizer)
	return r.Client.Update(ctx, hadoopCluster)
}

func (r *HadoopClusterReconciler) prepareForDeletion(ctx context.Context, hadoopCluster *v1alpha1.HadoopCluster) error {
	if !controllerutil.ContainsFinalizer(hadoopCluster, DeletionFinalizer) {
		return nil
	}
	if err := clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
		// Add label on all Pods to be picked up in pre-stop hook via Downward API
		if err := r.addHadoopClusterDeletionLabel(ctx, hadoopCluster); err != nil {
			return fmt.Errorf("failed to add deletion markers to HadoopCluster Pods: %w", err)
		}
		for _, builder := range r.builders {
			if err := builder.Clean(hadoopCluster); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "HadoopCluster deletion")
	}

	if err := r.removeFinalizer(ctx, hadoopCluster); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Failed to remove finalizer for deletion")
		return err
	}
	return nil
}

func (r *HadoopClusterReconciler) addHadoopClusterDeletionLabel(ctx context.Context, hadoopCluster *v1alpha1.HadoopCluster) error {
	// Create selector.
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{v1alpha1.ClusterNameLabel: hadoopCluster.Name},
	})
	if err != nil {
		return err
	}

	podList := &corev1.PodList{}
	err = r.List(
		context.Background(),
		podList,
		client.InNamespace(hadoopCluster.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	)
	for i := 0; i < len(podList.Items); i++ {
		pod := &podList.Items[i]
		pod.Labels[v1alpha1.ReplicaTypeLabel] = "true"
		if err = r.Client.Update(ctx, pod); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("cannot Update Pod %s in Namespace %s: %w", pod.Name, pod.Namespace, err)
		}
	}

	return nil
}
