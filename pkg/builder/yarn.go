package builder

import (
	"context"
	"fmt"
	v1alpha1 "github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/control"
	"github.com/chriskery/hadoop-operator/pkg/util"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var _ Builder = &YarnBuilder{}

type YarnBuilder struct {
	client.Client

	Recorder record.EventRecorder

	// StatefulSetControl is used to add or delete statefulsets.
	StatefulSetControl control.StatefulSetControlInterface

	// ServiceControl is used to add or delete services.
	ServiceControl control.ServiceControlInterface

	// PodControl is used to add or delete services.
	PodControl control.PodControlInterface
}

func (h *YarnBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()
	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)

	h.Client = mgr.GetClient()
	h.Recorder = recorder
	h.StatefulSetControl = control.RealStatefulSetControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.ServiceControl = control.RealServiceControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder}
}

func (h *YarnBuilder) Build(obj interface{}, objStatus interface{}) error {
	cluster := obj.(*v1alpha1.HadoopCluster)
	status := objStatus.(*v1alpha1.HadoopClusterStatus)

	util.InitializeClusterStatuses(status, v1alpha1.ReplicaTypeResourcemanager)
	if err := h.buildResourceManager(cluster, status); err != nil {
		return err
	}

	util.InitializeClusterStatuses(status, v1alpha1.ReplicaTypeNodemanager)
	if err := h.buildNodeManager(cluster, status); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) Clean(obj interface{}) error {
	cluster := obj.(*v1alpha1.HadoopCluster)

	err := h.cleanResourceManager(cluster)
	if err != nil {
		return err
	}

	err = h.cleanNodeManager(cluster)
	if err != nil {
		return err
	}

	return nil
}

func (h *YarnBuilder) buildResourceManager(
	cluster *v1alpha1.HadoopCluster,
	status *v1alpha1.HadoopClusterStatus,
) error {
	podName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeResourcemanager)
	resourceManagerPod := &corev1.Pod{}
	err := h.Get(context.Background(), client.ObjectKey{Name: podName, Namespace: cluster.Namespace}, resourceManagerPod)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildResourceManagerPod(cluster); err != nil {
			return err
		}
	} else {
		util.UpdateClusterReplicaStatuses(status, v1alpha1.ReplicaTypeResourcemanager, resourceManagerPod)
	}
	status.ReplicaStatuses[v1alpha1.ReplicaTypeResourcemanager].Expect = cluster.Spec.Yarn.ResourceManager.Replicas

	serviceName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeResourcemanager)
	err = h.Get(context.Background(), client.ObjectKey{Name: serviceName, Namespace: cluster.Namespace}, &corev1.Service{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildResourceManagerService(cluster, serviceName); err != nil {
			return err
		}
	}

	if isServiceNodePortExpose(cluster.Spec.Yarn.ResourceManager.Expose) {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.Get(context.Background(), client.ObjectKey{Name: serviceNodePortName, Namespace: cluster.Namespace}, &corev1.Service{})
		if err != nil {
			if !errors.IsNotFound(err) {
				return err
			}
			httpPort := cluster.Spec.Yarn.ResourceManager.Expose.HttpNodePort
			if err = h.buildResourceManageNodePortService(cluster, serviceNodePortName, httpPort); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *YarnBuilder) buildResourceManagerService(cluster *v1alpha1.HadoopCluster, name string) error {
	resourceManagerService := getHeadLessNodeServiceSpec(cluster, name, v1alpha1.ReplicaTypeResourcemanager)

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}

func (h *YarnBuilder) buildResourceManagerPod(cluster *v1alpha1.HadoopCluster) error {
	podTemplate, err := getPodSpec(cluster, v1alpha1.ReplicaTypeResourcemanager)
	if err != nil {
		return err
	}

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err = h.PodControl.CreatePodsWithControllerRef(cluster.GetNamespace(), podTemplate, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildNodeManager(
	cluster *v1alpha1.HadoopCluster,
	status *v1alpha1.HadoopClusterStatus,
) error {
	nodeManagerName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNodemanager)
	nodeManagerStatefulSet := &appv1.StatefulSet{}
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: nodeManagerName, Namespace: cluster.Namespace},
		nodeManagerStatefulSet,
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNodeManagerStatefulSet(cluster, nodeManagerName); err != nil {
			return err
		}
	} else {
		if h.isNeedReconcileNodeManagerHPA(cluster, nodeManagerStatefulSet) {
			if err = h.reconcileNodeManagerHPA(cluster, nodeManagerStatefulSet, status); err != nil {
				return err
			}
		}

		if err = h.updateNodeManagerStatus(cluster, status); err != nil {
			return err
		}
	}

	if err = h.buildNodeManagerServices(cluster); err != nil {
		return err
	}
	return nil
}

// isNeedReconcileNodeManagerHPA checks whether need to reconcile node manager HPA.
func (h *YarnBuilder) isNeedReconcileNodeManagerHPA(
	cluster *v1alpha1.HadoopCluster,
	nodeManagerStatefulSet *appv1.StatefulSet,
) bool {
	if cluster.Spec.Yarn.NodeManager.Replicas == nodeManagerStatefulSet.Spec.Replicas {
		return false
	}
	if cluster.Spec.Yarn.NodeManager.Replicas == nil && nodeManagerStatefulSet.Spec.Replicas == nil {
		return false
	}
	if cluster.Spec.Yarn.NodeManager.Replicas == nil && *nodeManagerStatefulSet.Spec.Replicas == 1 {
		return false
	}
	if *cluster.Spec.Yarn.NodeManager.Replicas == 1 && nodeManagerStatefulSet.Spec.Replicas == nil {
		return false
	}
	return *cluster.Spec.Yarn.NodeManager.Replicas != *nodeManagerStatefulSet.Spec.Replicas
}

func (h *YarnBuilder) buildNodeManagerService(
	cluster *v1alpha1.HadoopCluster,
	name string,
) error {
	resourceManagerService := getHeadLessNodeServiceSpec(cluster, name, v1alpha1.ReplicaTypeNodemanager)

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildNodeManagerStatefulSet(
	cluster *v1alpha1.HadoopCluster,
	name string,
) error {
	nodeManagerStatefulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}
	statefulSetSpec, err := h.genNodeManagerStatefulSetSpec(cluster)
	if err != nil {
		return nil
	}
	nodeManagerStatefulSet.Spec = *statefulSetSpec
	nodeManagerStatefulSet.Labels = statefulSetSpec.Selector.MatchLabels

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err = h.StatefulSetControl.CreateStatefulSetsWithControllerRef(cluster.GetNamespace(), nodeManagerStatefulSet, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) genNodeManagerStatefulSetSpec(
	cluster *v1alpha1.HadoopCluster,
) (*appv1.StatefulSetSpec, error) {
	podTemplate, err := getPodSpec(cluster, v1alpha1.ReplicaTypeNodemanager)
	if err != nil {
		return nil, err
	}
	statefulSetSpec := &appv1.StatefulSetSpec{
		Replicas: cluster.Spec.Yarn.NodeManager.Replicas,
		Selector: &metav1.LabelSelector{
			MatchLabels: podTemplate.Labels,
		},
		Template: *podTemplate,
	}

	return statefulSetSpec, nil
}

func (h *YarnBuilder) buildNodeManagerServices(cluster *v1alpha1.HadoopCluster) error {
	replicas := cluster.Spec.Yarn.NodeManager.Replicas
	if replicas == nil {
		return nil
	}

	for i := 0; i < int(*replicas); i++ {
		serviceName := util.GetNodeManagerServiceName(cluster, i)
		err := h.Get(context.Background(), client.ObjectKey{Name: serviceName, Namespace: cluster.Namespace}, &corev1.Service{})
		if err == nil {
			continue
		}
		if !errors.IsNotFound(err) {
			return err
		}

		if err = h.buildNodeManagerService(cluster, serviceName); err != nil {
			return err
		}
	}

	return nil
}

func (h *YarnBuilder) buildResourceManageNodePortService(
	cluster *v1alpha1.HadoopCluster,
	name string,
	nodePort int32,
) error {
	labels := getLabels(cluster, v1alpha1.ReplicaTypeResourcemanager)
	resourceManagerService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:     "http",
					Port:     8088,
					NodePort: nodePort,
				},
			},
		},
	}

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}

func (h *YarnBuilder) updateNodeManagerStatus(cluster *v1alpha1.HadoopCluster, status *v1alpha1.HadoopClusterStatus) error {
	labels := getLabels(cluster, v1alpha1.ReplicaTypeNodemanager)
	// Create selector.
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: labels,
	})
	if err != nil {
		return err
	}

	podList := &corev1.PodList{}
	err = h.List(
		context.Background(),
		podList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	)
	if err != nil {
		return err
	}
	var filter util.ObjectFilterFunction = func(obj metav1.Object) bool {
		//TODO(FIX ME)
		return true
		//return metav1.IsControlledBy(obj, cluster)
	}
	filterPods := util.ConvertPodListWithFilter(podList.Items, filter)

	for _, pod := range filterPods {
		util.UpdateClusterReplicaStatuses(status, v1alpha1.ReplicaTypeNodemanager, pod)
	}
	status.ReplicaStatuses[v1alpha1.ReplicaTypeNodemanager].Expect = cluster.Spec.Yarn.NodeManager.Replicas
	return nil
}

func (h *YarnBuilder) cleanResourceManager(cluster *v1alpha1.HadoopCluster) error {
	podName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeResourcemanager)
	err := h.PodControl.DeletePod(cluster.GetNamespace(), podName, &corev1.Pod{})
	if err != nil {
		return err
	}

	serviceName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeResourcemanager)
	err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceName, &corev1.Service{})
	if err != nil {
		return err
	}

	if isServiceNodePortExpose(cluster.Spec.Yarn.ResourceManager.Expose) {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceNodePortName, &corev1.Service{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *YarnBuilder) cleanNodeManager(cluster *v1alpha1.HadoopCluster) error {
	nodeManagerName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNodemanager)
	err := h.StatefulSetControl.DeleteStatefulSet(cluster.GetNamespace(), nodeManagerName, &appv1.StatefulSet{})
	if err != nil {
		return err
	}

	if cluster.Spec.Yarn.NodeManager.Replicas == nil {
		return nil
	}
	for i := 0; i < int(*cluster.Spec.Yarn.NodeManager.Replicas); i++ {
		serviceName := util.GetNodeManagerServiceName(cluster, i)
		err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceName, &corev1.Service{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *YarnBuilder) reconcileNodeManagerHPA(
	cluster *v1alpha1.HadoopCluster,
	statefulSet *appv1.StatefulSet,
	status *v1alpha1.HadoopClusterStatus,
) error {
	statefulSet.Spec.Replicas = cluster.Spec.Yarn.NodeManager.Replicas
	if statefulSet.Spec.Replicas == nil {
		statefulSet.Spec.Replicas = ptr.To(int32(1))
	}

	err := reconcileStatefulSetHPA(h.StatefulSetControl, statefulSet, *statefulSet.Spec.Replicas)
	if err != nil {
		return err
	}

	msg := fmt.Sprintf("HadoopCluster %s/%s is reconfiguraing nodemanager replicas.", cluster.Namespace, cluster.Name)
	err = util.UpdateClusterConditions(status, v1alpha1.ClusterReconfiguring, util.HadoopclusterReconfiguringReason, msg)
	if err != nil {
		return err
	}
	h.Recorder.Eventf(cluster, corev1.EventTypeNormal, "HadoopClusterReconfiguring", msg)

	return nil
}
