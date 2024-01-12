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

package hadoopjob

import (
	"context"
	"fmt"
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/builder"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

const controllerName = "hadoop-job-operator"

func NewReconciler(mgr manager.Manager) *HadoopJobReconciler {
	recorder := mgr.GetEventRecorderFor(controllerName)
	r := &HadoopJobReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		apiReader: mgr.GetAPIReader(),
		Log:       log.Log,
		Recorder:  recorder,
	}

	r.driverBuilder = &builder.DriverBuilder{}
	r.driverBuilder.SetupWithManager(mgr, recorder)
	return r
}

// HadoopJobReconciler reconciles a hadoopCluster object
type HadoopJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	apiReader client.Reader
	Recorder  record.EventRecorder
	Log       logr.Logger

	driverBuilder builder.Builder
}

// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=pods,verbs=update;get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=get;create;patch
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopjobs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *HadoopJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	logger := r.Log.WithValues(v1alpha1.HadoopJobSingular, req.NamespacedName)

	hadoopJob := &v1alpha1.HadoopJob{}
	err := r.Get(ctx, req.NamespacedName, hadoopJob)
	if err != nil {
		logger.Info(err.Error(), "unable to fetch hadoop cluster", req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// skip for HadoopJob that is being deleted
	if !hadoopJob.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, nil
	}

	oldStatus := hadoopJob.Status.DeepCopy()
	// Use common to reconcile the job related pod and service
	err = r.ReconcileJobs(hadoopJob, oldStatus)
	if err != nil {
		logger.Error(err, "Reconcile PyTorchJob error")
		return ctrl.Result{}, err
	}

	// No need to update the cluster status if the status hasn't changed since last time.
	if !reflect.DeepEqual(hadoopJob.Status, oldStatus) {
		if err = r.UpdateClusterStatusInApiServer(hadoopJob, oldStatus); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// UpdateClusterStatusInApiServer updates the status of the given MXJob.
func (r *HadoopJobReconciler) UpdateClusterStatusInApiServer(cluster *v1alpha1.HadoopJob, status *v1alpha1.HadoopJobStatus) error {
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status = *status.DeepCopy()

	if err := r.Status().Update(context.Background(), clusterCopy); err != nil {
		klog.Error(err, " failed to update HadoopJob conditions in the API server")
		return err
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HadoopJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := controller.New(controllerName, mgr, controller.Options{
		Reconciler: r,
	})

	if err != nil {
		return err
	}

	// using onOwnerCreateFunc is easier to set defaults
	if err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.HadoopJob{}), &handler.EnqueueRequestForObject{},
		predicate.Funcs{CreateFunc: r.onOwnerCreateFunc()},
	); err != nil {
		return err
	}

	// inject watching for job related pod
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &v1alpha1.HadoopCluster{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopJob{}, handler.OnlyControllerOwner()),
		predicate.Funcs{
			CreateFunc: util.OnDependentCreateFunc(),
			UpdateFunc: util.OnDependentUpdateFunc(r.Client),
			DeleteFunc: util.OnDependentDeleteFunc(),
		},
	); err != nil {
		return err
	}

	// inject watching for job related pod
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &corev1.Pod{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopJob{}, handler.OnlyControllerOwner()),
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

// onOwnerCreateFunc modify creation condition.
func (r *HadoopJobReconciler) onOwnerCreateFunc() func(event.CreateEvent) bool {
	return func(e event.CreateEvent) bool {
		job, ok := e.Object.(*v1alpha1.HadoopJob)
		if !ok {
			return true
		}
		msg := fmt.Sprintf("HadoopJob job %s created", job.GetName())
		klog.Info(msg)
		err := util.UpdateJobConditions(&job.Status, v1alpha1.JobCreated, util.HadoopJobCreatedReason, msg)
		if err != nil {
			return false
		}
		return true
	}
}

func (r *HadoopJobReconciler) ReconcileJobs(job *v1alpha1.HadoopJob, status *v1alpha1.HadoopJobStatus) error {
	hadoopCluster, err := r.GetHadoopClusterForJob(job)
	if err != nil {
		klog.Warningf("GetPodsForJob error %v", err)
		return err
	}

	err = r.ReconcileHadoopCluster(job, status, hadoopCluster)
	if err != nil {
		klog.Warningf("ReconcilePods error %v", err)
		return err
	}

	err = r.ReconcileDriver(job, status, hadoopCluster)
	if err != nil {
		klog.Warningf("ReconcileServices error %v", err)
		return err
	}

	return nil
}

func (r *HadoopJobReconciler) GetHadoopClusterForJob(job *v1alpha1.HadoopJob) (*v1alpha1.HadoopCluster, error) {
	hadoopCluster := &v1alpha1.HadoopCluster{}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: job.GetNamespace(), Name: job.GetName()}, hadoopCluster)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	} else {
		return hadoopCluster, nil
	}
}

func (r *HadoopJobReconciler) ReconcileHadoopCluster(job *v1alpha1.HadoopJob, status *v1alpha1.HadoopJobStatus, cluster *v1alpha1.HadoopCluster) error {
	if cluster != nil {
		return nil
	}

	labels := map[string]string{
		v1alpha1.JobNameLabel: job.GetName(),
	}
	cluster = &v1alpha1.HadoopCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.GetName(),
			Namespace: job.GetNamespace(),
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, v1alpha1.SchemeGroupVersion.WithKind(v1alpha1.HadoopJobKind)),
			},
		},
		Spec: v1alpha1.HadoopClusterSpec{
			Yarn: v1alpha1.YarnSpec{
				NodeManager: v1alpha1.YarnNodeManagerSpecTemplate{
					HadoopNodeSpec: job.Spec.ExecutorSpec,
					ServiceType:    corev1.ServiceTypeNodePort,
				},
				ResourceManager: v1alpha1.YarnResourceManagerSpecTemplate{
					HadoopNodeSpec: job.Spec.ExecutorSpec,
				},
			},
			HDFS: v1alpha1.HDFSSpec{
				NameNode: v1alpha1.HDFSNameNodeSpecTemplate{HadoopNodeSpec: job.Spec.ExecutorSpec, Format: true},
				DataNode: v1alpha1.HDFSDataNodeSpecTemplate{HadoopNodeSpec: job.Spec.ExecutorSpec},
			},
		},
	}

	cluster.Spec.HDFS.DataNode.Resources = corev1.ResourceRequirements{}
	cluster.Spec.HDFS.NameNode.Resources = corev1.ResourceRequirements{}
	cluster.Spec.Yarn.ResourceManager.Resources = corev1.ResourceRequirements{}

	if err := r.Create(context.Background(), cluster); err != nil {
		return err
	}

	msg := fmt.Sprintf("HadoopCluster %s/%s is created.", cluster.GetNamespace(), cluster.GetName())
	r.Recorder.Eventf(job, corev1.EventTypeNormal, "HadoopClusterCreated", msg)
	return nil
}

func (r *HadoopJobReconciler) ReconcileDriver(job *v1alpha1.HadoopJob, status *v1alpha1.HadoopJobStatus, cluster *v1alpha1.HadoopCluster) error {
	if cluster == nil || !util.IsClusterRunning(cluster) {
		return nil
	}

	err := r.driverBuilder.Build(job, status)
	if err != nil {
		return err
	}

	driverPodName := util.GetReplicaName(job, v1alpha1.ReplicaTypeDriver)
	driverPod := &corev1.Pod{}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: job.GetNamespace(), Name: driverPodName}, driverPod)
	if err != nil {
		return err
	}

	if isPodReady(driverPod) {
		msg := fmt.Sprintf("Driver %s/%s is ready.", driverPod.GetNamespace(), driverPod.GetName())
		r.Recorder.Eventf(driverPod, corev1.EventTypeNormal, "DriverReady", msg)
		err = util.UpdateJobConditions(status, v1alpha1.JobRunning, util.HadoopJobRunningReason, msg)
		if err != nil {
			return err
		}
	} else if isPodSucceeded(driverPod) {
		msg := fmt.Sprintf("Driver %s/%s is succeeded.", driverPod.GetNamespace(), driverPod.GetName())
		r.Recorder.Eventf(driverPod, corev1.EventTypeNormal, "DriverSucceeded", msg)
		err = util.UpdateJobConditions(status, v1alpha1.JobSucceeded, util.HadoopJobSucceededReason, msg)
		if err != nil {
			return err
		}
	} else if isPodFailed(driverPod) {
		msg := fmt.Sprintf("Driver %s/%s is failed.", driverPod.GetNamespace(), driverPod.GetName())
		r.Recorder.Eventf(driverPod, corev1.EventTypeNormal, "DriverFailed", msg)
		err = util.UpdateJobConditions(status, v1alpha1.JobFailed, util.HadoopJobFailedReason, msg)
		if err != nil {
			return err
		}
	}

	return nil
}
