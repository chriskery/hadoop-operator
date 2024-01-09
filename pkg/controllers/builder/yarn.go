package builder

import (
	"context"
	"fmt"
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/control"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	utillabels "github.com/chriskery/hadoop-cluster-operator/pkg/util/labels"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	replicaTypeResourcemanager = "resourcemanager"
	replicaTypeNodemanager     = "nodemanager"
)

var _ Builder = &YarnBuilder{}

type YarnBuilder struct {
	client.Client

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
	h.StatefulSetControl = control.RealStatefulSetControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.ServiceControl = control.RealServiceControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder}
}

func (h *YarnBuilder) Build(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	if err := h.buildResourceManager(cluster, status); err != nil {
		return err
	}
	if err := h.buildNodeManager(cluster, status); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildResourceManager(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeResourcemanager)

	podName := util.GetResourceManagerName(cluster)
	err := h.Get(context.Background(), client.ObjectKey{Name: podName, Namespace: cluster.Namespace}, &corev1.Pod{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildResourceManagerPod(cluster, labels); err != nil {
			return err
		}
	}

	serviceName := util.GetResourceManagerName(cluster)
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

	ownerRef := util.GenOwnerReference(cluster)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}

func (h *YarnBuilder) buildResourceManagerPod(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) error {
	podTemplate, err := h.genResourceManagerPodSpec(cluster, labels)
	if err != nil {
		return err
	}

	ownerRef := util.GenOwnerReference(cluster)
	if err = h.PodControl.CreatePodsWithControllerRef(cluster.GetNamespace(), podTemplate, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildNodeManager(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	err := h.Get(context.Background(), client.ObjectKey{Name: util.GetNodeManagerStatefulSetName(cluster), Namespace: cluster.Namespace}, &appv1.StatefulSet{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNodeManagerStatefulSet(cluster); err != nil {
			return err
		}
	}

	if err = h.buildNodeManagerServices(cluster); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildNodeManagerService(cluster *hadoopclusterorgv1alpha1.HadoopCluster, name string, labels map[string]string) error {
	resourceManagerService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Type:      cluster.Spec.HDFS.DataNode.ServiceType,
			Selector:  labels,
		},
	}

	ownerRef := util.GenOwnerReference(cluster)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) buildNodeManagerStatefulSet(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeNodemanager)
	nameNodeStatefulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetNodeManagerStatefulSetName(cluster),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
	}
	deploySpec, err := h.genNodeManagerStatefulSetSpec(cluster)
	if err != nil {
		return nil
	}
	nameNodeStatefulSet.Spec = *deploySpec

	ownerRef := util.GenOwnerReference(cluster)
	if err = h.StatefulSetControl.CreateStatefulSetsWithControllerRef(cluster.GetNamespace(), nameNodeStatefulSet, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *YarnBuilder) genResourceManagerDeploySpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster) (*appv1.DeploymentSpec, error) {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeResourcemanager)
	deploymentSpec := &appv1.DeploymentSpec{
		Replicas: cluster.Spec.Yarn.ResourceManager.Replicas,
		Selector: &metav1.LabelSelector{MatchLabels: labels},
	}
	podTemplate, err := h.genResourceManagerPodSpec(cluster, labels)
	if err != nil {
		return nil, err
	}
	deploymentSpec.Template = *podTemplate
	return deploymentSpec, nil
}

func (h *YarnBuilder) genNodeManagerStatefulSetSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster) (*appv1.StatefulSetSpec, error) {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeNodemanager)
	statefulSetSpec := &appv1.StatefulSetSpec{
		Replicas: cluster.Spec.Yarn.NodeManager.Replicas,
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
	}
	podTemplate, err := h.genNodeManagerPodSpec(cluster, labels)
	if err != nil {
		return nil, err
	}
	statefulSetSpec.Template = *podTemplate

	return statefulSetSpec, nil
}

func (h *YarnBuilder) genNodeManagerPodSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) (*corev1.PodTemplateSpec, error) {
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Volumes:       cluster.Spec.Yarn.ResourceManager.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetConfigMapName(cluster))

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nodeManagerCmd := []string{"sh", "-c", fmt.Sprintf("cp %s /tmp/entrypoint && chmod +x /tmp/entrypoint && /tmp/entrypoint", entrypointPath)}
	containers := []corev1.Container{{
		Name:            replicaTypeNodemanager,
		Image:           cluster.Spec.Yarn.ResourceManager.Image,
		Command:         nodeManagerCmd,
		Resources:       cluster.Spec.Yarn.ResourceManager.Resources,
		VolumeMounts:    volumeMounts,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: cluster.Spec.Yarn.ResourceManager.ImagePullPolicy,
		SecurityContext: cluster.Spec.Yarn.ResourceManager.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, replicaTypeNodemanager)
	return podTemplateSpec, nil
}

func (h *YarnBuilder) genResourceManagerPodSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) (*corev1.PodTemplateSpec, error) {
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			Name:   util.GetResourceManagerName(cluster),
		},
		Spec: corev1.PodSpec{
			Volumes:       cluster.Spec.Yarn.ResourceManager.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	if podTemplateSpec.Spec.Volumes == nil {
		podTemplateSpec.Spec.Volumes = make([]corev1.Volume, 0)
	}
	podTemplateSpec.Spec.Volumes = append(
		podTemplateSpec.Spec.Volumes,
		corev1.Volume{
			Name: configKey,
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: util.GetConfigMapName(cluster)}}},
		},
	)

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetConfigMapName(cluster))

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	resourceManagerCmd := []string{"sh", "-c", fmt.Sprintf("cp %s /tmp/entrypoint && chmod +x /tmp/entrypoint && /tmp/entrypoint", entrypointPath)}
	containers := []corev1.Container{{
		Name:            replicaTypeResourcemanager,
		Image:           cluster.Spec.Yarn.ResourceManager.Image,
		Command:         resourceManagerCmd,
		Resources:       cluster.Spec.Yarn.ResourceManager.Resources,
		VolumeMounts:    volumeMounts,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: cluster.Spec.Yarn.ResourceManager.ImagePullPolicy,
		SecurityContext: cluster.Spec.Yarn.ResourceManager.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers

	setPodEnv(podTemplateSpec, replicaTypeResourcemanager)
	return podTemplateSpec, nil
}

func (h *YarnBuilder) buildNodeManagerServices(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	replicas := cluster.Spec.HDFS.DataNode.Replicas
	if replicas == nil {
		return nil
	}

	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeNodemanager)
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

	ownerRef := util.GenOwnerReference(cluster)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), resourceManagerService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}
