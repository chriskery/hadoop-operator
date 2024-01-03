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
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/builder"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
)

const controllerName = "hadoop-cluster-operator"

func NewReconciler(mgr manager.Manager) *HadoopClusterReconciler {
	r := &HadoopClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		apiReader: mgr.GetAPIReader(),
		builders:  builder.ResourceBuilders(mgr, mgr.GetEventRecorderFor(controllerName)),
		Log:       log.Log,
	}

	r.WorkQueue = &util.FakeWorkQueue{}
	return r
}

// HadoopClusterReconciler reconciles a hadoopCluster object
type HadoopClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	apiReader client.Reader
	Log       logr.Logger

	builders []builder.Builder

	// WorkQueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	WorkQueue workqueue.RateLimitingInterface

	// Recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=pods,verbs=update;get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=events,verbs=get;create;patch
// +kubebuilder:rbac:groups=kubecluster.org.kubecluster.org,resources=hadoopclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubecluster.org.kubecluster.org,resources=hadoopclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubecluster.org.kubecluster.org,resources=hadoopclusters/finalizers,verbs=update

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

	oldStatus := hadoopCluster.Status.DeepCopy()

	for _, builder := range r.builders {
		if err = builder.Build(hadoopCluster, oldStatus); err != nil {
			logrus.Warnf("Reconcile Hadoop Cluster error %v", err)
			return ctrl.Result{}, err
		}
	}
	// No need to update the cluster status if the status hasn't changed since last time.
	if !reflect.DeepEqual(hadoopCluster.Status, oldStatus) {
		if err = r.Status().Update(ctx, hadoopCluster); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HadoopClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.HadoopCluster{}).
		Complete(r)
}
