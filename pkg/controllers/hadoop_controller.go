/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	v1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/builder"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	"github.com/go-logr/logr"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const controllerName = "hadoop-cluster-operator"

func NewReconciler(mgr manager.Manager) *HadoopClusterReconciler {
	recorder := mgr.GetEventRecorderFor(controllerName)
	r := &HadoopClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		apiReader: mgr.GetAPIReader(),
		builders:  builder.ResourceBuilders(mgr, recorder),
		Log:       log.Log,
		Recorder:  recorder,
	}

	return r
}

// HadoopClusterReconciler reconciles a hadoopCluster object
type HadoopClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	apiReader client.Reader
	Recorder  record.EventRecorder
	Log       logr.Logger

	builders []builder.Builder
}

// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=pods,verbs=update;get;list;watch;create
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=events,verbs=get;create;patch
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *HadoopClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	logger := r.Log.WithValues(v1alpha1.HadoopClusterSingular, req.NamespacedName)

	hadoopCluster := &v1alpha1.HadoopCluster{}
	err := r.Get(ctx, req.NamespacedName, hadoopCluster)
	if err != nil {
		logger.Info(err.Error(), "unable to fetch hadoop cluster", req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// skip for HadoopCluster that is being deleted
	if !hadoopCluster.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, r.prepareForDeletion(ctx, hadoopCluster)
	}

	// Ensure the resource have a deletion marker
	if err = r.addFinalizerIfNeeded(ctx, hadoopCluster); err != nil {
		return ctrl.Result{}, err
	}

	oldStatus := hadoopCluster.Status.DeepCopy()
	for _, builder := range r.builders {
		if err = builder.Build(hadoopCluster, oldStatus); err != nil {
			klog.Warningf("Reconcile Hadoop Cluster error %v", err)
			return ctrl.Result{}, err
		}
	}

	if err = r.UpdateClusterStatus(hadoopCluster, oldStatus); err != nil {
		return ctrl.Result{}, err
	}

	// No need to update the cluster status if the status hasn't changed since last time.
	if !reflect.DeepEqual(hadoopCluster.Status, oldStatus) {
		if err = r.UpdateClusterStatusInApiServer(hadoopCluster, oldStatus); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// UpdateClusterStatusInApiServer updates the status of the given MXJob.
func (r *HadoopClusterReconciler) UpdateClusterStatusInApiServer(cluster *v1alpha1.HadoopCluster, status *v1alpha1.HadoopClusterStatus) error {
	if status.ReplicaStatuses == nil {
		status.ReplicaStatuses = map[v1alpha1.ReplicaType]*v1alpha1.ReplicaStatus{}
	}

	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status = *status.DeepCopy()

	if err := r.Status().Update(context.Background(), clusterCopy); err != nil {
		klog.Error(err, " failed to update HadoopCluster conditions in the API server")
		return err
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HadoopClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := controller.New(controllerName, mgr, controller.Options{
		Reconciler: r,
	})

	if err != nil {
		return err
	}

	// using onOwnerCreateFunc is easier to set defaults
	if err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.HadoopCluster{}), &handler.EnqueueRequestForObject{},
		predicate.Funcs{CreateFunc: r.onOwnerCreateFunc()},
	); err != nil {
		return err
	}

	// inject watching for job related pod
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &corev1.Pod{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopCluster{}, handler.OnlyControllerOwner()),
		predicate.Funcs{
			CreateFunc: util.OnDependentCreateFunc(),
			UpdateFunc: util.OnDependentUpdateFunc(r.Client),
			DeleteFunc: util.OnDependentDeleteFunc(),
		},
	); err != nil {
		return err
	}

	// inject watching for job related service
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &corev1.Service{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopCluster{}, handler.OnlyControllerOwner()),
		predicate.Funcs{
			CreateFunc: util.OnDependentCreateFunc(),
			UpdateFunc: util.OnDependentUpdateFunc(r.Client),
			DeleteFunc: util.OnDependentDeleteFunc(),
		},
	); err != nil {
		return err
	}

	// inject watching for job related service
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &corev1.ConfigMap{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopCluster{}, handler.OnlyControllerOwner()),
		predicate.Funcs{
			CreateFunc: util.OnDependentCreateFunc(),
			UpdateFunc: util.OnDependentUpdateFunc(r.Client),
			DeleteFunc: util.OnDependentDeleteFunc(),
		},
	); err != nil {
		return err
	}

	// inject watching for job related service
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &appv1.StatefulSet{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopCluster{}, handler.OnlyControllerOwner()),
		predicate.Funcs{
			CreateFunc: util.OnDependentCreateFunc(),
			UpdateFunc: util.OnDependentUpdateFunc(r.Client),
			DeleteFunc: util.OnDependentDeleteFunc(),
		},
	); err != nil {
		return err
	}

	return nil
}

func (r *HadoopClusterReconciler) UpdateClusterStatus(cluster *v1alpha1.HadoopCluster, status *v1alpha1.HadoopClusterStatus) error {
	if len(status.Conditions) == 0 {
		msg := fmt.Sprintf("HadoopCluster %s is created.", cluster.GetName())
		if err := util.UpdateClusterConditions(status, v1alpha1.ClusterCreated, util.HadoopclusterCreatedReason, msg); err != nil {
			return err
		}
	}

	if status.StartTime == nil {
		now := metav1.Now()
		status.StartTime = &now
	}

	clusetrRunning := true
	nameNodeStatus, ok := status.ReplicaStatuses[v1alpha1.ReplicaTypeNameNode]
	if ok && util.ReplicaReady(cluster.Spec.HDFS.NameNode.Replicas, 1, nameNodeStatus.Active) {
		clusetrRunning = false
	}
	dataNodeStatus, ok := status.ReplicaStatuses[v1alpha1.ReplicaTypeDataNode]
	if ok && util.ReplicaReady(cluster.Spec.HDFS.DataNode.Replicas, 1, dataNodeStatus.Active) {
		clusetrRunning = false
	}
	resourcemanagerStatus, ok := status.ReplicaStatuses[v1alpha1.ReplicaTypeResourcemanager]
	if ok && util.ReplicaReady(cluster.Spec.Yarn.ResourceManager.Replicas, 1, resourcemanagerStatus.Active) {
		clusetrRunning = false
	}
	nodemanagerStatus, ok := status.ReplicaStatuses[v1alpha1.ReplicaTypeNodemanager]
	if ok && util.ReplicaReady(cluster.Spec.Yarn.NodeManager.Replicas, 1, nodemanagerStatus.Active) {
		clusetrRunning = false
	}

	if clusetrRunning {
		msg := fmt.Sprintf("HadoopCluster %s/%s is running.", cluster.Namespace, cluster.Name)
		err := util.UpdateClusterConditions(status, v1alpha1.ClusterRunning, util.HadoopclusterRunningReason, msg)
		if err != nil {
			return err
		}
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "HadoopClusterRunning", msg)
	}

	return nil
}

// onOwnerCreateFunc modify creation condition.
func (r *HadoopClusterReconciler) onOwnerCreateFunc() func(event.CreateEvent) bool {
	return func(e event.CreateEvent) bool {
		hadoopClusetr, ok := e.Object.(*v1alpha1.HadoopCluster)
		if !ok {
			return true
		}
		msg := fmt.Sprintf("HadoopCluster %s is created.", e.Object.GetName())
		klog.Info(msg)

		if err := util.UpdateClusterConditions(&hadoopClusetr.Status, v1alpha1.ClusterCreated, util.HadoopclusterCreatedReason, msg); err != nil {
			klog.Error(msg)
			return false
		}
		return true
	}
}
