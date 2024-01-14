package util

import (
	"context"
	clientretry "k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type Finalizer interface {
	Clean(ctx context.Context, obj interface{}) error
}

// AddFinalizerIfNeeded adds a deletion finalizer if the HadoopCluster does not have one yet and is not marked for deletion
func AddFinalizerIfNeeded(ctx context.Context, client client.Client, object client.Object, deletionFinalizer string) error {
	if !object.GetDeletionTimestamp().IsZero() || controllerutil.ContainsFinalizer(object, deletionFinalizer) {
		return nil
	}

	controllerutil.AddFinalizer(object, deletionFinalizer)
	if err := client.Update(ctx, object); err != nil {
		return err
	}
	return nil
}

// RemoveFinalizer removes the deletion finalizer from the HadoopCluster
func RemoveFinalizer(ctx context.Context, client client.Client, object client.Object, deletionFinalizer string) error {
	controllerutil.RemoveFinalizer(object, deletionFinalizer)
	return client.Update(ctx, object)
}

// PrepareForDeletion prepares the HadoopCluster for deletion
func PrepareForDeletion(
	ctx context.Context,
	client client.Client,
	object client.Object,
	deletionFinalizer string,
	finalizer Finalizer,
) error {
	if !controllerutil.ContainsFinalizer(object, deletionFinalizer) {
		return nil
	}
	if err := clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
		return finalizer.Clean(ctx, object)
	}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "HadoopCluster deletion")
		return err
	}

	if err := RemoveFinalizer(ctx, client, object, deletionFinalizer); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Failed to remove finalizer for deletion")
		return err
	}
	return nil
}
