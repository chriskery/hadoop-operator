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

package hadoopapplication

import (
	"context"
	"fmt"
	"reflect"

	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/builder"
	"github.com/chriskery/hadoop-operator/pkg/util"
	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const controllerName = "hadoop-application-operator"

func NewReconciler(mgr manager.Manager) *HadoopApplicationReconciler {
	recorder := mgr.GetEventRecorderFor(controllerName)
	r := &HadoopApplicationReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Log:      log.Log,
		Recorder: recorder,
	}

	r.driverBuilder = &builder.DriverBuilder{}
	r.driverBuilder.SetupWithManager(mgr, recorder)

	r.dataLoaderBuilder = &builder.DataloaderBuilder{}
	r.dataLoaderBuilder.SetupWithManager(mgr, recorder)
	return r
}

// HadoopApplicationReconciler reconciles a hadoopCluster object
type HadoopApplicationReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	Recorder record.EventRecorder
	Log      logr.Logger

	driverBuilder builder.Builder

	dataLoaderBuilder builder.Builder
}

// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=pods,verbs=update;get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=get;create;patch
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopapplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopapplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubecluster.org,resources=hadoopapplications/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *HadoopApplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	logger := r.Log.WithValues(v1alpha1.HadoopApplicationSingular, req.NamespacedName)

	hadoopApplication := &v1alpha1.HadoopApplication{}
	err := r.Get(ctx, req.NamespacedName, hadoopApplication)
	if err != nil {
		logger.Info(err.Error(), "unable to fetch hadoop cluster", req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// skip for HadoopApplication that is being deleted
	if !hadoopApplication.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, util.PrepareForDeletion(ctx, r.Client, hadoopApplication, DeletionFinalizer, r)
	}

	// Ensure the resource have a deletion marker
	if err = util.AddFinalizerIfNeeded(ctx, r.Client, hadoopApplication, DeletionFinalizer); err != nil {
		return ctrl.Result{}, err
	}

	oldStatus := hadoopApplication.Status.DeepCopy()
	// Use common to reconcile the application related pod and service
	err = r.ReconcileApplications(hadoopApplication, oldStatus)
	if err != nil {
		logger.Error(err, "Reconcile HadoopApplications error")
		return ctrl.Result{}, err
	}

	// No need to update the cluster status if the status hasn't changed since last time.
	if !reflect.DeepEqual(hadoopApplication.Status, oldStatus) {
		if err = r.UpdateClusterStatusInApiServer(hadoopApplication, oldStatus); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// UpdateClusterStatusInApiServer updates the status of the given MXApplication.
func (r *HadoopApplicationReconciler) UpdateClusterStatusInApiServer(cluster *v1alpha1.HadoopApplication, status *v1alpha1.HadoopApplicationStatus) error {
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status = *status.DeepCopy()

	if err := r.Status().Update(context.Background(), clusterCopy); err != nil {
		logrus.Error(err, " failed to update HadoopApplication conditions in the API server")
		return err
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HadoopApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := controller.New(controllerName, mgr, controller.Options{
		Reconciler: r,
	})

	if err != nil {
		return err
	}

	// using onOwnerCreateFunc is easier to set defaults
	if err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.HadoopApplication{}), &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	// inject watching for application related pod
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &v1alpha1.HadoopCluster{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopApplication{}, handler.OnlyControllerOwner()),
		predicate.Funcs{
			CreateFunc: util.OnDependentCreateFunc(),
			UpdateFunc: util.OnDependentUpdateFunc(r.Client),
			DeleteFunc: util.OnDependentDeleteFunc(),
		},
	); err != nil {
		return err
	}

	// inject watching for application related pod
	if err = c.Watch(
		source.Kind(mgr.GetCache(), &corev1.Pod{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &v1alpha1.HadoopApplication{}, handler.OnlyControllerOwner()),
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

// ReconcileApplications reconciles the application
func (r *HadoopApplicationReconciler) ReconcileApplications(application *v1alpha1.HadoopApplication, status *v1alpha1.HadoopApplicationStatus) error {
	hadoopCluster, err := r.GetHadoopClusterForApplication(application)
	if err != nil {
		return err
	}

	if util.IsApplicationFinished(application) {
		if hadoopCluster != nil {
			// If the Application is succeed or failed, delete all pods and services.
			return r.DeleteHadoopCluster(application, hadoopCluster)
		} else {
			return nil
		}
	}

	if status.Conditions == nil {
		msg := fmt.Sprintf("HadoopApplication application %s created", application.GetName())
		if err = util.UpdateApplicationConditions(status, v1alpha1.ApplicationCreated, util.HadoopApplicationCreatedReason, msg); err != nil {
			return err
		}
	}

	if status.StartTime == nil {
		status.StartTime = ptr.To(metav1.Now())
	}

	if hadoopCluster == nil {
		err = r.CreateHadoopCluster(application, status, hadoopCluster)
		if err != nil {
			logrus.Warningf("ReconcilePods error %v", err)
			return err
		}
	}

	if hadoopCluster == nil || !util.IsClusterRunning(hadoopCluster) || !hadoopCluster.DeletionTimestamp.IsZero() {
		logrus.Infof("HadoopApplication %s/%s hadoop cluster not ready yet.", application.GetNamespace(), application.GetName())
		return nil
	} else if util.IsApplicationCreated(application) {
		r.Recorder.Eventf(application, corev1.EventTypeNormal, "HadoopClusterReady", "HadoopCluster is ready.")
	}

	if application.Spec.DataLoaderSpec != nil {
		dataLoaded, err := r.ReconcileDataLoader(application, status)
		if err != nil {
			logrus.Warningf("ReconcileServices error %v", err)
			return err
		}

		if !dataLoaded {
			logrus.Infof("HadoopApplication %s/%s data not loaded yet.", application.GetNamespace(), application.GetName())
			return nil
		}
	}

	err = r.ReconcileDriver(application, status, hadoopCluster)
	if err != nil {
		logrus.Warningf("ReconcileServices error %v", err)
		return err
	}

	return nil
}

func (r *HadoopApplicationReconciler) GetHadoopClusterForApplication(application *v1alpha1.HadoopApplication) (*v1alpha1.HadoopCluster, error) {
	hadoopCluster := &v1alpha1.HadoopCluster{}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: application.GetNamespace(), Name: application.GetName()}, hadoopCluster)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	} else {
		return hadoopCluster, nil
	}
}

func (r *HadoopApplicationReconciler) CreateHadoopCluster(application *v1alpha1.HadoopApplication, _ *v1alpha1.HadoopApplicationStatus, cluster *v1alpha1.HadoopCluster) error {
	if cluster != nil {
		return nil
	}

	labels := map[string]string{
		v1alpha1.ApplicationNameLabel: application.GetName(),
	}
	cluster = &v1alpha1.HadoopCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      application.GetName(),
			Namespace: application.GetNamespace(),
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(application, v1alpha1.SchemeGroupVersion.WithKind(v1alpha1.HadoopApplicationKind)),
			},
		},
		Spec: v1alpha1.HadoopClusterSpec{
			Yarn: v1alpha1.YarnSpec{
				NodeManager: v1alpha1.YarnNodeManagerSpecTemplate{
					HadoopNodeSpec: application.Spec.ExecutorSpec,
				},
				ResourceManager: v1alpha1.YarnResourceManagerSpecTemplate{
					HadoopNodeSpec: application.Spec.ExecutorSpec,
					Expose:         v1alpha1.ExposeSpec{ExposeType: v1alpha1.ExposeTypeNodePort},
				},
			},
			HDFS: v1alpha1.HDFSSpec{
				NameNode: v1alpha1.HDFSNameNodeSpecTemplate{
					HadoopNodeSpec: application.Spec.ExecutorSpec,
					Format:         application.Spec.NameNodeDirFormat,
					Expose:         v1alpha1.ExposeSpec{ExposeType: v1alpha1.ExposeTypeNodePort},
				},
				DataNode: v1alpha1.HDFSDataNodeSpecTemplate{
					HadoopNodeSpec: application.Spec.ExecutorSpec,
				},
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
	r.Recorder.Eventf(application, corev1.EventTypeNormal, "HadoopClusterCreated", msg)
	return nil
}

func (r *HadoopApplicationReconciler) ReconcileDriver(application *v1alpha1.HadoopApplication, status *v1alpha1.HadoopApplicationStatus, cluster *v1alpha1.HadoopCluster) error {
	continuerBuild, err := r.driverBuilder.Build(application, status)
	if err != nil || continuerBuild {
		return err
	}

	driverPodName := util.GetReplicaName(application, v1alpha1.ReplicaTypeDriver)
	driverPod := &corev1.Pod{}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: application.GetNamespace(), Name: driverPodName}, driverPod)
	if err != nil {
		return client.IgnoreNotFound(err)
	}

	if isPodReady(driverPod) && !util.IsApplicationRunning(application) {
		msg := fmt.Sprintf("Driver %s/%s is ready.", driverPod.GetNamespace(), driverPod.GetName())
		r.Recorder.Eventf(driverPod, corev1.EventTypeNormal, "DriverReady", msg)
		r.Recorder.Eventf(application, corev1.EventTypeNormal, "DriverReady", msg)
		err = util.UpdateApplicationConditions(status, v1alpha1.ApplicationRunning, util.HadoopApplicationRunningReason, msg)
		if err != nil {
			return err
		}
	} else if isPodSucceeded(driverPod) {
		msg := fmt.Sprintf("Driver %s/%s is succeeded.", driverPod.GetNamespace(), driverPod.GetName())
		r.Recorder.Eventf(driverPod, corev1.EventTypeNormal, "DriverSucceeded", msg)
		r.Recorder.Eventf(application, corev1.EventTypeNormal, "DriverSucceeded", msg)
		err = util.UpdateApplicationConditions(status, v1alpha1.ApplicationSucceeded, util.HadoopApplicationSucceededReason, msg)
		if err != nil {
			return err
		}
	} else if isPodFailed(driverPod) {
		msg := fmt.Sprintf("Driver %s/%s is failed.", driverPod.GetNamespace(), driverPod.GetName())
		r.Recorder.Eventf(driverPod, corev1.EventTypeNormal, "DriverFailed", msg)
		r.Recorder.Eventf(application, corev1.EventTypeNormal, "DriverFailed", msg)
		err = util.UpdateApplicationConditions(status, v1alpha1.ApplicationFailed, util.HadoopApplicationFailedReason, msg)
		if err != nil {
			return err
		}
	}

	if status.CompletionTime == nil && isPodFinished(driverPod) {
		status.CompletionTime = ptr.To(metav1.Now())
	}

	return nil
}

func (r *HadoopApplicationReconciler) DeleteHadoopCluster(application *v1alpha1.HadoopApplication, cluster *v1alpha1.HadoopCluster) error {
	if application.Spec.ExecutorSpec.DeleteOnTermination != nil && !*application.Spec.ExecutorSpec.DeleteOnTermination {
		r.Recorder.Event(application, corev1.EventTypeNormal, "CleanHadoopCluster", "No Need to delete HadoopCluster because DeleteOnTermination is true")
		return nil
	}
	err := r.Delete(context.Background(), cluster)
	if err != nil {
		return err
	}
	r.Recorder.Event(application, corev1.EventTypeNormal, "CleanHadoopCluster", "Delete HadoopCluster because application finished")
	return nil
}

func (r *HadoopApplicationReconciler) ReconcileDataLoader(
	application *v1alpha1.HadoopApplication,
	status *v1alpha1.HadoopApplicationStatus,
) (bool, error) {
	continuerBuild, err := r.dataLoaderBuilder.Build(application, status)
	if err != nil || !continuerBuild {
		return continuerBuild, err
	}

	dataLoaderPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      util.GetReplicaName(application, v1alpha1.ReplicaTypeDataloader),
		Namespace: application.GetNamespace(),
	}}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: application.GetNamespace(), Name: dataLoaderPod.GetName()}, dataLoaderPod)
	if err != nil {
		return false, err
	}

	if isPodReady(dataLoaderPod) && (!util.IsApplicationSubmitted(application) || !util.IsApplicationRunning(application)) {
		msg := fmt.Sprintf("Dataloader %s/%s is loading.", dataLoaderPod.GetNamespace(), dataLoaderPod.GetName())
		r.Recorder.Eventf(application, corev1.EventTypeNormal, "DataloaderLoading", msg)
		err = util.UpdateApplicationConditions(status, v1alpha1.ApplicationDataLoading, util.HadoopApplicationRunningReason, msg)
		return false, err
	} else if isPodSucceeded(dataLoaderPod) {
		msg := fmt.Sprintf("Dataloader %s/%s is succeeded.", dataLoaderPod.GetNamespace(), dataLoaderPod.GetName())
		r.Recorder.Eventf(application, corev1.EventTypeNormal, "DataLoaderSucceeded", msg)
		return true, err
	} else if isPodFailed(dataLoaderPod) {
		msg := fmt.Sprintf("Dataloader %s/%s is failed.", dataLoaderPod.GetNamespace(), dataLoaderPod.GetName())
		r.Recorder.Eventf(application, corev1.EventTypeNormal, "DataloaderFailed", msg)
		err = util.UpdateApplicationConditions(status, v1alpha1.ApplicationFailed, util.HadoopApplicationFailedReason, msg)
		return false, err
	}

	return false, nil
}
