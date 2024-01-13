package builder

import (
	"context"
	"fmt"
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/control"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	utillabels "github.com/chriskery/hadoop-cluster-operator/pkg/util/labels"
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
	cluster := obj.(*hadoopclusterorgv1alpha1.HadoopCluster)
	status := objStatus.(*hadoopclusterorgv1alpha1.HadoopClusterStatus)

	util.InitializeClusterStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager)
	if err := h.buildResourceManager(cluster, status); err != nil {
		return err
	}

	util.InitializeClusterStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeNodemanager)
	if err := h.buildNodeManager(cluster, status); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) Clean(obj interface{}) error {
	cluster := obj.(*hadoopclusterorgv1alpha1.HadoopCluster)

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
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	status *hadoopclusterorgv1alpha1.HadoopClusterStatus,
) error {
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager)

	podName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager)
	resourceManagerPod := &corev1.Pod{}
	err := h.Get(context.Background(), client.ObjectKey{Name: podName, Namespace: cluster.Namespace}, resourceManagerPod)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildResourceManagerPod(cluster, labels); err != nil {
			return err
		}
	} else {
		util.UpdateClusterReplicaStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager, resourceManagerPod)
	}
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager].Expect = cluster.Spec.Yarn.ResourceManager.Replicas

	serviceName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager)
	err = h.Get(context.Background(), client.ObjectKey{Name: serviceName, Namespace: cluster.Namespace}, &corev1.Service{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildResourceManagerService(cluster, labels, serviceName); err != nil {
			return err
		}
	}

	if cluster.Spec.Yarn.ResourceManager.ServiceType == corev1.ServiceTypeNodePort {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.Get(context.Background(), client.ObjectKey{Name: serviceNodePortName, Namespace: cluster.Namespace}, &corev1.Service{})
		if err != nil {
			if !errors.IsNotFound(err) {
				return err
			}
			if err = h.buildResourceManageNodePortService(cluster, labels, serviceNodePortName); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *YarnBuilder) buildResourceManagerService(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string, name string) error {
	resourceManagerService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
		},
	}

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}

func (h *YarnBuilder) buildResourceManagerPod(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) error {
	podTemplate, err := h.genResourceManagerPodSpec(cluster, &cluster.Spec.Yarn.ResourceManager, labels)
	if err != nil {
		return err
	}

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err = h.PodControl.CreatePodsWithControllerRef(cluster.GetNamespace(), podTemplate, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildNodeManager(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	status *hadoopclusterorgv1alpha1.HadoopClusterStatus,
) error {
	nodeManagerName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNodemanager)
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeNodemanager)

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
		if err = h.buildNodeManagerStatefulSet(cluster, nodeManagerName, labels); err != nil {
			return err
		}
	}

	if h.isNeedReconcileNodeManagerHPA(cluster, nodeManagerStatefulSet) {
		if err = h.reconcileNodeManagerHPA(cluster, nodeManagerStatefulSet, status); err != nil {
			return err
		}
	}

	if err = h.updateNodeManagerStatus(cluster, status, labels); err != nil {
		return err
	}

	if err = h.buildNodeManagerServices(cluster, labels); err != nil {
		return err
	}
	return nil
}

// isNeedReconcileNodeManagerHPA checks whether need to reconcile node manager HPA.
func (h *YarnBuilder) isNeedReconcileNodeManagerHPA(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
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
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	name string,
	labels map[string]string,
) error {
	resourceManagerService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
		},
	}

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildNodeManagerStatefulSet(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	name string,
	labels map[string]string,
) error {
	nodeManagerStatefulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
	}
	deploySpec, err := h.genNodeManagerStatefulSetSpec(cluster, labels)
	if err != nil {
		return nil
	}
	nodeManagerStatefulSet.Spec = *deploySpec

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err = h.StatefulSetControl.CreateStatefulSetsWithControllerRef(cluster.GetNamespace(), nodeManagerStatefulSet, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) genNodeManagerStatefulSetSpec(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	labels map[string]string,
) (*appv1.StatefulSetSpec, error) {
	podTemplate, err := h.genNodeManagerPodSpec(cluster, &cluster.Spec.Yarn.NodeManager, labels)
	if err != nil {
		return nil, err
	}
	statefulSetSpec := &appv1.StatefulSetSpec{
		Replicas: cluster.Spec.Yarn.NodeManager.Replicas,
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		Template: *podTemplate,
	}

	return statefulSetSpec, nil
}

func (h *YarnBuilder) genNodeManagerPodSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster, nodeManagerSpec *hadoopclusterorgv1alpha1.YarnNodeManagerSpecTemplate, labels map[string]string) (*corev1.PodTemplateSpec, error) {
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Volumes:       nodeManagerSpec.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(
		podTemplateSpec.Spec.Volumes,
		util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap),
	)

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nodeManagerCmd := []string{"sh", "-c", entrypointCmd}
	containers := []corev1.Container{{
		Name:            string(hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager),
		Image:           nodeManagerSpec.Image,
		Command:         nodeManagerCmd,
		Resources:       nodeManagerSpec.Resources,
		VolumeMounts:    volumeMounts,
		Env:             cluster.Spec.Yarn.NodeManager.Env,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: nodeManagerSpec.ImagePullPolicy,
		SecurityContext: nodeManagerSpec.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, hadoopclusterorgv1alpha1.ReplicaTypeNodemanager)

	if err := setInitContainer(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNodemanager, podTemplateSpec); err != nil {
		return nil, err
	}
	return podTemplateSpec, nil
}

func (h *YarnBuilder) genResourceManagerPodSpec(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	resourceManagerSpec *hadoopclusterorgv1alpha1.YarnResourceManagerSpecTemplate,
	labels map[string]string,
) (*corev1.PodTemplateSpec, error) {
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			Name:   util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager),
		},
		Spec: corev1.PodSpec{
			Volumes:       resourceManagerSpec.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	if podTemplateSpec.Spec.Volumes == nil {
		podTemplateSpec.Spec.Volumes = make([]corev1.Volume, 0)
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap))

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	resourceManagerCmd := []string{"sh", "-c", entrypointCmd}
	containers := []corev1.Container{{
		Name:            string(hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager),
		Image:           resourceManagerSpec.Image,
		Command:         resourceManagerCmd,
		Resources:       resourceManagerSpec.Resources,
		VolumeMounts:    volumeMounts,
		Env:             cluster.Spec.Yarn.ResourceManager.Env,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: resourceManagerSpec.ImagePullPolicy,
		SecurityContext: resourceManagerSpec.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers

	setPodEnv(podTemplateSpec, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager)

	if err := setInitContainer(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager, podTemplateSpec); err != nil {
		return nil, err
	}
	return podTemplateSpec, nil
}

func (h *YarnBuilder) buildNodeManagerServices(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) error {
	replicas := cluster.Spec.HDFS.DataNode.Replicas
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

		if err = h.buildNodeManagerService(cluster, serviceName, labels); err != nil {
			return err
		}
	}

	return nil
}

func (h *YarnBuilder) buildResourceManageNodePortService(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	labels map[string]string,
	name string,
) error {
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
					Name: "http",
					Port: 8088,
				},
			},
		},
	}

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}

func (h *YarnBuilder) updateNodeManagerStatus(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus, labels map[string]string) error {
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
		util.UpdateClusterReplicaStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeNodemanager, pod)
	}
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeNodemanager].Expect = cluster.Spec.Yarn.NodeManager.Replicas
	return nil
}

func (h *YarnBuilder) cleanResourceManager(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	podName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager)
	err := h.PodControl.DeletePod(cluster.GetNamespace(), podName, &corev1.Pod{})
	if err != nil {
		return err
	}

	serviceName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager)
	err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceName, &corev1.Service{})
	if err != nil {
		return err
	}

	if cluster.Spec.Yarn.ResourceManager.ServiceType == corev1.ServiceTypeNodePort {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceNodePortName, &corev1.Service{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *YarnBuilder) cleanNodeManager(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	nodeManagerName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNodemanager)
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
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	statefulSet *appv1.StatefulSet,
	status *hadoopclusterorgv1alpha1.HadoopClusterStatus,
) error {
	statefulSet.Spec.Replicas = cluster.Spec.HDFS.DataNode.Replicas
	if statefulSet.Spec.Replicas == nil {
		statefulSet.Spec.Replicas = ptr.To(int32(1))
	}

	err := reconcileStatefulSetHPA(h.StatefulSetControl, statefulSet, *statefulSet.Spec.Replicas)
	if err != nil {
		return err
	}

	msg := fmt.Sprintf("HadoopCluster %s/%s is reconfiguraing nodemanager replicas.", cluster.Namespace, cluster.Name)
	err = util.UpdateClusterConditions(status, hadoopclusterorgv1alpha1.ClusterReconfiguring, util.HadoopclusterReconfiguringReason, msg)
	if err != nil {
		return err
	}
	h.Recorder.Eventf(cluster, corev1.EventTypeNormal, "HadoopClusterReconfiguring", msg)

	return nil
}
