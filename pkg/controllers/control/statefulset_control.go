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

// Reasons for StatefulSet events
const (
	// FailedCreateStatefulSetReason is added in an event and in a cluster condition
	// when a StatefulSet for a replica set is failed to be created.
	FailedCreateStatefulSetReason = "FailedCreateStatefulSet"
	// SuccessfulCreateStatefulSetReason is added in an event when a StatefulSet for a cluster
	// is successfully created.
	SuccessfulCreateStatefulSetReason = "SuccessfulCreateStatefulSet"
	// FailedDeleteStatefulSetReason is added in an event and in a cluster condition
	// when a StatefulSet for a replica set is failed to be deleted.
	FailedDeleteStatefulSetReason = "FailedDeleteStatefulSet"
	// SuccessfulDeleteStatefulSetReason is added in an event when a StatefulSet for a cluster
	// is successfully deleted.
	SuccessfulDeleteStatefulSetReason = "SuccessfulDeleteStatefulSet"
)

// StatefulSetControlInterface is an interface that knows how to add or delete StatefulSets
// created as an interface to allow testing.
type StatefulSetControlInterface interface {
	// CreateStatefulSets creates new StatefulSets according to the spec.
	CreateStatefulSets(namespace string, statefulSet *appv1.StatefulSet, object runtime.Object) error
	// CreateStatefulSetsWithControllerRef creates new StatefulSets according to the spec, and sets object as the StatefulSet's controller.
	CreateStatefulSetsWithControllerRef(namespace string, statefulSet *appv1.StatefulSet, object runtime.Object, controllerRef *metav1.OwnerReference) error
	// DeleteStatefulSet deletes the StatefulSet identified by StatefulSetID.
	DeleteStatefulSet(namespace string, statefulSetID string, object runtime.Object) error
	// PatchStatefulSet patches the StatefulSet.
	PatchStatefulSet(namespace, name string, data []byte) error
}

// RealStatefulSetControl is the default implementation of StatefulSetControlInterface.
type RealStatefulSetControl struct {
	KubeClient clientset.Interface
	Recorder   record.EventRecorder
}

var _ StatefulSetControlInterface = &RealStatefulSetControl{}

func getStatefulSetsLabelSet(spec *appv1.StatefulSet) labels.Set {
	desiredLabels := make(labels.Set)
	for k, v := range spec.Labels {
		desiredLabels[k] = v
	}
	return desiredLabels
}

func getStatefulSetsFinalizers(spec *appv1.StatefulSet) []string {
	desiredFinalizers := make([]string, len(spec.Finalizers))
	copy(desiredFinalizers, spec.Finalizers)
	return desiredFinalizers
}

func getStatefulSetsAnnotationSet(spec *appv1.StatefulSet) labels.Set {
	desiredAnnotations := make(labels.Set)
	for k, v := range spec.Annotations {
		desiredAnnotations[k] = v
	}
	return desiredAnnotations
}

func (r RealStatefulSetControl) CreateStatefulSets(namespace string, statefulSet *appv1.StatefulSet, object runtime.Object) error {
	return r.createStatefulSets(namespace, statefulSet, object, nil)
}

func (r RealStatefulSetControl) CreateStatefulSetsWithControllerRef(namespace string, statefulSet *appv1.StatefulSet, controllerObject runtime.Object, controllerRef *metav1.OwnerReference) error {
	if err := ValidateControllerRef(controllerRef); err != nil {
		return err
	}
	return r.createStatefulSets(namespace, statefulSet, controllerObject, controllerRef)
}

func (r RealStatefulSetControl) CreateStatefulSetsOnNode(namespace string, statefulSet *appv1.StatefulSet, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	if err := ValidateControllerRef(controllerRef); err != nil {
		return err
	}
	return r.createStatefulSets(namespace, statefulSet, object, controllerRef)
}

func (r RealStatefulSetControl) PatchStatefulSet(namespace, name string, data []byte) error {
	_, err := r.KubeClient.AppsV1().StatefulSets(namespace).Patch(context.TODO(), name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
	return err
}

func GetCloneStatefulSet(statefulSet *appv1.StatefulSet, parentObject runtime.Object, controllerRef *metav1.OwnerReference) (*appv1.StatefulSet, error) {
	desiredLabels := getStatefulSetsLabelSet(statefulSet)
	desiredFinalizers := getStatefulSetsFinalizers(statefulSet)
	desiredAnnotations := getStatefulSetsAnnotationSet(statefulSet)

	cloneSatefulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      desiredLabels,
			Annotations: desiredAnnotations,
			Name:        statefulSet.Name,
			Namespace:   statefulSet.Namespace,
			Finalizers:  desiredFinalizers,
		},
	}
	if controllerRef != nil {
		cloneSatefulSet.OwnerReferences = append(cloneSatefulSet.OwnerReferences, *controllerRef)
	}
	cloneSatefulSet.Spec = *statefulSet.Spec.DeepCopy()
	return cloneSatefulSet, nil
}

func (r RealStatefulSetControl) createStatefulSets(
	namespace string,
	statefulSet *appv1.StatefulSet,
	object runtime.Object,
	controllerRef *metav1.OwnerReference,
) error {
	cloneStatefulSet, err := GetCloneStatefulSet(statefulSet, object, controllerRef)
	if err != nil {
		return err
	}
	if labels.Set(cloneStatefulSet.Labels).AsSelectorPreValidated().Empty() {
		return fmt.Errorf("unable to create StatefulSets, no labels")
	}
	logger := util.LoggerForStatefulSet(cloneStatefulSet, object.GetObjectKind().GroupVersionKind().Kind)
	if newStatefulSet, err := r.KubeClient.AppsV1().StatefulSets(namespace).Create(context.TODO(), cloneStatefulSet, metav1.CreateOptions{}); err != nil {
		r.Recorder.Eventf(object, corev1.EventTypeWarning, FailedCreateStatefulSetReason, "Error creating: %v", err)
		return err
	} else {
		accessor, err := meta.Accessor(object)
		if err != nil {
			logger.Errorf("parentObject does not have ObjectMeta, %v", err)
			return nil
		}
		logger.Infof("Controller %v created StatefulSet %v", accessor.GetName(), newStatefulSet.Name)
		r.Recorder.Eventf(object, corev1.EventTypeNormal, SuccessfulCreateStatefulSetReason, "Created StatefulSet: %v", newStatefulSet.Name)
	}
	return nil
}

func (r RealStatefulSetControl) DeleteStatefulSet(namespace string, StatefulSetID string, object runtime.Object) error {
	accessor, err := meta.Accessor(object)
	if err != nil {
		return fmt.Errorf("object does not have ObjectMeta, %v", err)
	}
	logger := util.LoggerForCluster(accessor)
	StatefulSet, err := r.KubeClient.AppsV1().StatefulSets(namespace).Get(context.TODO(), StatefulSetID, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if StatefulSet.DeletionTimestamp != nil {
		logger.Infof("StatefulSet %s/%s is terminating, skip deleting", StatefulSet.Namespace, StatefulSet.Name)
		return nil
	}
	logger.Infof("Controller %v deleting StatefulSet %v/%v", accessor.GetName(), namespace, StatefulSetID)
	// delete options
	if err := r.KubeClient.AppsV1().StatefulSets(namespace).Delete(context.TODO(), StatefulSetID, metav1.DeleteOptions{}); err != nil {
		r.Recorder.Eventf(object, corev1.EventTypeWarning, FailedDeleteStatefulSetReason, "Error deleting: %v", err)
		return fmt.Errorf("unable to delete StatefulSets: %v", err)
	} else {
		r.Recorder.Eventf(object, corev1.EventTypeNormal, SuccessfulDeleteStatefulSetReason, "Deleted StatefulSet: %v", StatefulSetID)
	}
	return nil
}

type FakeStatefulSetControl struct {
	sync.Mutex
	Templates             []appv1.StatefulSet
	ControllerRefs        []metav1.OwnerReference
	DeleteStatefulSetName []string
	Patches               [][]byte
	Err                   error
	CreateLimit           int
	CreateCallCount       int
}

var _ StatefulSetControlInterface = &FakeStatefulSetControl{}

func (f *FakeStatefulSetControl) PatchStatefulSet(namespace, name string, data []byte) error {
	f.Lock()
	defer f.Unlock()
	f.Patches = append(f.Patches, data)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeStatefulSetControl) CreateStatefulSets(namespace string, spec *appv1.StatefulSet, object runtime.Object) error {
	f.Lock()
	defer f.Unlock()
	f.CreateCallCount++
	if f.CreateLimit != 0 && f.CreateCallCount > f.CreateLimit {
		return fmt.Errorf("not creating StatefulSet, limit %d already reached (create call %d)", f.CreateLimit, f.CreateCallCount)
	}
	f.Templates = append(f.Templates, *spec)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeStatefulSetControl) CreateStatefulSetsWithControllerRef(namespace string, spec *appv1.StatefulSet, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	f.Lock()
	defer f.Unlock()
	f.CreateCallCount++
	if f.CreateLimit != 0 && f.CreateCallCount > f.CreateLimit {
		return fmt.Errorf("not creating StatefulSet, limit %d already reached (create call %d)", f.CreateLimit, f.CreateCallCount)
	}
	f.Templates = append(f.Templates, *spec)
	f.ControllerRefs = append(f.ControllerRefs, *controllerRef)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeStatefulSetControl) DeleteStatefulSet(namespace string, StatefulSetID string, object runtime.Object) error {
	f.Lock()
	defer f.Unlock()
	f.DeleteStatefulSetName = append(f.DeleteStatefulSetName, StatefulSetID)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

func (f *FakeStatefulSetControl) Clear() {
	f.Lock()
	defer f.Unlock()
	f.DeleteStatefulSetName = []string{}
	f.Templates = []appv1.StatefulSet{}
	f.ControllerRefs = []metav1.OwnerReference{}
	f.Patches = [][]byte{}
	f.CreateLimit = 0
	f.CreateCallCount = 0
}
