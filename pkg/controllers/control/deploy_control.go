package control

import (
	"context"
	"fmt"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// Reasons for Deploy events
const (
	// FailedCreateDeployReason is added in an event and in a cluster condition
	// when a Deploy for a replica set is failed to be created.
	FailedCreateDeployReason = "FailedCreateDeploy"
	// SuccessfulCreateDeployReason is added in an event when a Deploy for a cluster
	// is successfully created.
	SuccessfulCreateDeployReason = "SuccessfulCreateDeploy"
	// FailedDeleteDeployReason is added in an event and in a cluster condition
	// when a Deploy for a replica set is failed to be deleted.
	FailedDeleteDeployReason = "FailedDeleteDeploy"
	// SuccessfulDeleteDeployReason is added in an event when a Deploy for a cluster
	// is successfully deleted.
	SuccessfulDeleteDeployReason = "SuccessfulDeleteDeploy"
)

// DeployControlInterface is an interface that knows how to add or delete Deploys
// created as an interface to allow testing.
type DeployControlInterface interface {
	// CreateDeploys creates new Deploys according to the spec.
	CreateDeploys(namespace string, deploy *appv1.Deployment, object runtime.Object) error
	// CreateDeploysWithControllerRef creates new Deploys according to the spec, and sets object as the Deploy's controller.
	CreateDeploysWithControllerRef(namespace string, deploy *appv1.Deployment, object runtime.Object, controllerRef *metav1.OwnerReference) error
	// DeleteDeploy deletes the Deploy identified by DeployID.
	DeleteDeploy(namespace string, deployID string, object runtime.Object) error
	// PatchDeploy patches the Deploy.
	PatchDeploy(namespace, name string, data []byte) error
}

// RealDeployControl is the default implementation of DeployControlInterface.
type RealDeployControl struct {
	KubeClient clientset.Interface
	Recorder   record.EventRecorder
}

var _ DeployControlInterface = &RealDeployControl{}

func getDeploysLabelSet(spec *appv1.Deployment) labels.Set {
	desiredLabels := make(labels.Set)
	for k, v := range spec.Labels {
		desiredLabels[k] = v
	}
	return desiredLabels
}

func getDeploysFinalizers(spec *appv1.Deployment) []string {
	desiredFinalizers := make([]string, len(spec.Finalizers))
	copy(desiredFinalizers, spec.Finalizers)
	return desiredFinalizers
}

func getDeploysAnnotationSet(spec *appv1.Deployment) labels.Set {
	desiredAnnotations := make(labels.Set)
	for k, v := range spec.Annotations {
		desiredAnnotations[k] = v
	}
	return desiredAnnotations
}

func (r RealDeployControl) CreateDeploys(namespace string, deploy *appv1.Deployment, object runtime.Object) error {
	return r.createDeploys(namespace, deploy, object, nil)
}

func (r RealDeployControl) CreateDeploysWithControllerRef(namespace string, deploy *appv1.Deployment, controllerObject runtime.Object, controllerRef *metav1.OwnerReference) error {
	if err := ValidateControllerRef(controllerRef); err != nil {
		return err
	}
	return r.createDeploys(namespace, deploy, controllerObject, controllerRef)
}

func (r RealDeployControl) CreateDeploysOnNode(namespace string, deploy *appv1.Deployment, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	if err := ValidateControllerRef(controllerRef); err != nil {
		return err
	}
	return r.createDeploys(namespace, deploy, object, controllerRef)
}

func (r RealDeployControl) PatchDeploy(namespace, name string, data []byte) error {
	_, err := r.KubeClient.AppsV1().Deployments(namespace).Patch(context.TODO(), name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
	return err
}

func GetCloneDeploy(deploy *appv1.Deployment, parentObject runtime.Object, controllerRef *metav1.OwnerReference) (*appv1.Deployment, error) {
	desiredLabels := getDeploysLabelSet(deploy)
	desiredFinalizers := getDeploysFinalizers(deploy)
	desiredAnnotations := getDeploysAnnotationSet(deploy)

	cloneSatefulSet := &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      desiredLabels,
			Annotations: desiredAnnotations,
			Name:        deploy.Name,
			Namespace:   deploy.Namespace,
			Finalizers:  desiredFinalizers,
		},
	}
	if controllerRef != nil {
		cloneSatefulSet.OwnerReferences = append(cloneSatefulSet.OwnerReferences, *controllerRef)
	}
	cloneSatefulSet.Spec = *deploy.Spec.DeepCopy()
	return cloneSatefulSet, nil
}

func (r RealDeployControl) createDeploys(
	namespace string,
	deploy *appv1.Deployment,
	object runtime.Object,
	controllerRef *metav1.OwnerReference,
) error {
	cloneDeploy, err := GetCloneDeploy(deploy, object, controllerRef)
	if err != nil {
		return err
	}
	if labels.Set(cloneDeploy.Labels).AsSelectorPreValidated().Empty() {
		return fmt.Errorf("unable to create Deploys, no labels")
	}
	logger := util.LoggerForDeploy(cloneDeploy, object.GetObjectKind().GroupVersionKind().Kind)
	if newDeploy, err := r.KubeClient.AppsV1().Deployments(namespace).Create(context.TODO(), cloneDeploy, metav1.CreateOptions{}); err != nil {
		r.Recorder.Eventf(object, corev1.EventTypeWarning, FailedCreateDeployReason, "Error creating: %v", err)
		return err
	} else {
		accessor, err := meta.Accessor(object)
		if err != nil {
			logger.Errorf("parentObject does not have ObjectMeta, %v", err)
			return nil
		}
		logger.Infof("Controller %v created Deploy %v", accessor.GetName(), newDeploy.Name)
		r.Recorder.Eventf(object, corev1.EventTypeNormal, SuccessfulCreateDeployReason, "Created Deploy: %v", newDeploy.Name)
	}
	return nil
}

func (r RealDeployControl) DeleteDeploy(namespace string, DeployID string, object runtime.Object) error {
	accessor, err := meta.Accessor(object)
	if err != nil {
		return fmt.Errorf("object does not have ObjectMeta, %v", err)
	}
	logger := util.LoggerForCluster(accessor)
	Deploy, err := r.KubeClient.AppsV1().Deployments(namespace).Get(context.TODO(), DeployID, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if Deploy.DeletionTimestamp != nil {
		logger.Infof("Deploy %s/%s is terminating, skip deleting", Deploy.Namespace, Deploy.Name)
		return nil
	}
	logger.Infof("Controller %v deleting Deploy %v/%v", accessor.GetName(), namespace, DeployID)
	// delete options
	if err := r.KubeClient.AppsV1().Deployments(namespace).Delete(context.TODO(), DeployID, metav1.DeleteOptions{}); err != nil {
		r.Recorder.Eventf(object, corev1.EventTypeWarning, FailedDeleteDeployReason, "Error deleting: %v", err)
		return fmt.Errorf("unable to delete Deploys: %v", err)
	} else {
		r.Recorder.Eventf(object, corev1.EventTypeNormal, SuccessfulDeleteDeployReason, "Deleted Deploy: %v", DeployID)
	}
	return nil
}

type FakeDeployControl struct {
	sync.Mutex
	Templates        []appv1.Deployment
	ControllerRefs   []metav1.OwnerReference
	DeleteDeployName []string
	Patches          [][]byte
	Err              error
	CreateLimit      int
	CreateCallCount  int
}

var _ DeployControlInterface = &FakeDeployControl{}

func (f *FakeDeployControl) PatchDeploy(namespace, name string, data []byte) error {
	f.Lock()
	defer f.Unlock()
	f.Patches = append(f.Patches, data)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeDeployControl) CreateDeploys(namespace string, spec *appv1.Deployment, object runtime.Object) error {
	f.Lock()
	defer f.Unlock()
	f.CreateCallCount++
	if f.CreateLimit != 0 && f.CreateCallCount > f.CreateLimit {
		return fmt.Errorf("not creating Deploy, limit %d already reached (create call %d)", f.CreateLimit, f.CreateCallCount)
	}
	f.Templates = append(f.Templates, *spec)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeDeployControl) CreateDeploysWithControllerRef(namespace string, spec *appv1.Deployment, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	f.Lock()
	defer f.Unlock()
	f.CreateCallCount++
	if f.CreateLimit != 0 && f.CreateCallCount > f.CreateLimit {
		return fmt.Errorf("not creating Deploy, limit %d already reached (create call %d)", f.CreateLimit, f.CreateCallCount)
	}
	f.Templates = append(f.Templates, *spec)
	f.ControllerRefs = append(f.ControllerRefs, *controllerRef)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeDeployControl) DeleteDeploy(namespace string, DeployID string, object runtime.Object) error {
	f.Lock()
	defer f.Unlock()
	f.DeleteDeployName = append(f.DeleteDeployName, DeployID)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeDeployControl) Clear() {
	f.Lock()
	defer f.Unlock()
	f.DeleteDeployName = []string{}
	f.Templates = []appv1.Deployment{}
	f.ControllerRefs = []metav1.OwnerReference{}
	f.Patches = [][]byte{}
	f.CreateLimit = 0
	f.CreateCallCount = 0
}
