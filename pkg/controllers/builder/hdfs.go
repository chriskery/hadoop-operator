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
	"k8s.io/apimachinery/pkg/util/intstr"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	replicaTypeNameNode = "namenode"
	replicaTypeDataNode = "datanode"
)

var _ Builder = &HdfsBuilder{}

type HdfsBuilder struct {
	client.Client

	// StatefulSetControl is used to add or delete statefulsets.
	StatefulSetControl control.StatefulSetControlInterface

	// ServiceControl is used to add or delete services.
	ServiceControl control.ServiceControlInterface

	PodControl control.PodControlInterface
}

func (h *HdfsBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()

	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)
	h.Client = mgr.GetClient()
	h.StatefulSetControl = control.RealStatefulSetControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.ServiceControl = control.RealServiceControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder}
}

func (h *HdfsBuilder) Build(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	if err := h.buildNameNode(cluster, status); err != nil {
		return err
	}
	if err := h.buildDataNode(cluster, status); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildNameNode(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	err := h.Get(context.Background(), client.ObjectKey{Name: util.GetNameNodeName(cluster), Namespace: cluster.Namespace}, &corev1.Pod{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNameNodePod(cluster); err != nil {
			return err
		}
	}

	err = h.Get(context.Background(), client.ObjectKey{Name: util.GetNameNodeName(cluster), Namespace: cluster.Namespace}, &corev1.Service{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		err = h.buildNameNodeService(cluster)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *HdfsBuilder) buildNameNodeService(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeNameNode)
	nameNodeService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetNameNodeName(cluster),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
		},
	}

	ownerRef := util.GenOwnerReference(cluster)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildNameNodePod(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeNameNode)
	podSpec, err := h.genNameNodePodSpec(cluster, labels)
	if err != nil {
		return err
	}

	ownerRef := util.GenOwnerReference(cluster)
	if err = h.PodControl.CreatePodsWithControllerRef(cluster.GetNamespace(), podSpec, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildDataNode(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	err := h.Get(context.Background(), client.ObjectKey{Name: util.GetDataNodeStatefulSetName(cluster), Namespace: cluster.Namespace}, &appv1.StatefulSet{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildDataNodeStatefulSet(cluster); err != nil {
			return err
		}
	}

	if err = h.buildDataNodeServices(cluster); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildDataNodeStatefulSet(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeDataNode)
	dataNodeStatulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetDataNodeStatefulSetName(cluster),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
	}
	statefulSetSpec, err := h.genDataNodeStatefulSetSpec(cluster)
	if err != nil {
		return err
	}
	dataNodeStatulSet.Spec = *statefulSetSpec

	ownerRef := util.GenOwnerReference(cluster)
	if err = h.StatefulSetControl.CreateStatefulSetsWithControllerRef(cluster.GetNamespace(), dataNodeStatulSet, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) genNameNodeDeploySpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster) (*appv1.DeploymentSpec, error) {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeNameNode)

	deploymentSpec := &appv1.DeploymentSpec{
		Replicas: cluster.Spec.HDFS.NameNode.Replicas,
		Selector: &metav1.LabelSelector{MatchLabels: labels},
	}
	podSpec, err := h.genNameNodePodSpec(cluster, labels)
	if err != nil {
		return nil, err
	}

	deploymentSpec.Template = *podSpec
	return deploymentSpec, nil
}

func (h *HdfsBuilder) genNameNodePodSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) (*corev1.PodTemplateSpec, error) {
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name:   util.GetNameNodeName(cluster),
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Volumes:       cluster.Spec.HDFS.NameNode.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetConfigMapName(cluster))

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nameNodeCmd := []string{"sh", "-c", fmt.Sprintf("cp %s /tmp/entrypoint && chmod +x /tmp/entrypoint && /tmp/entrypoint", entrypointPath)}
	containers := []corev1.Container{{
		Name:            replicaTypeNameNode,
		Image:           cluster.Spec.HDFS.NameNode.Image,
		Command:         nameNodeCmd,
		Ports:           []corev1.ContainerPort{{ContainerPort: 9870}},
		Resources:       cluster.Spec.HDFS.NameNode.Resources,
		VolumeMounts:    volumeMounts,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: cluster.Spec.HDFS.NameNode.ImagePullPolicy,
		SecurityContext: cluster.Spec.HDFS.NameNode.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, replicaTypeNameNode)
	return podTemplateSpec, nil
}

func (h *HdfsBuilder) genDataNodeStatefulSetSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster) (*appv1.StatefulSetSpec, error) {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeDataNode)
	statefulSetSpec := &appv1.StatefulSetSpec{
		Replicas: cluster.Spec.HDFS.NameNode.Replicas,
		Selector: &metav1.LabelSelector{MatchLabels: labels},
	}

	podTemplate, err := h.genDataNodePodSpec(cluster)
	if err != nil {
		return nil, err
	}
	statefulSetSpec.Template = *podTemplate
	return statefulSetSpec, nil
}

func (h *HdfsBuilder) genDataNodePodSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster) (*corev1.PodTemplateSpec, error) {
	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeDataNode)

	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Volumes:       cluster.Spec.HDFS.DataNode.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetConfigMapName(cluster))

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nameNodeCmd := []string{"sh", "-c", fmt.Sprintf("cp %s /tmp/entrypoint && chmod +x /tmp/entrypoint && /tmp/entrypoint", entrypointPath)}

	containers := []corev1.Container{{
		Name:            replicaTypeDataNode,
		Image:           cluster.Spec.HDFS.DataNode.Image,
		Command:         nameNodeCmd,
		Ports:           []corev1.ContainerPort{{ContainerPort: 9870}},
		Resources:       cluster.Spec.HDFS.DataNode.Resources,
		VolumeMounts:    volumeMounts,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: cluster.Spec.HDFS.DataNode.ImagePullPolicy,
		SecurityContext: cluster.Spec.HDFS.DataNode.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, replicaTypeDataNode)
	return podTemplateSpec, nil
}

func (h *HdfsBuilder) buildDataNodeServices(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	replicas := cluster.Spec.HDFS.DataNode.Replicas
	if replicas == nil {
		return nil
	}

	labels := utillabels.GenLabels(cluster.GetName(), replicaTypeDataNode)
	for i := 0; i < int(*replicas); i++ {
		serviceName := util.GetDataNodeServiceName(cluster, i)
		if err := h.buildDataNodeService(cluster, serviceName, labels); err != nil {
			return err
		}
	}
	return nil
}

func (h *HdfsBuilder) buildDataNodeService(cluster *hadoopclusterorgv1alpha1.HadoopCluster, name string, labels map[string]string) error {
	err := h.Get(context.Background(), client.ObjectKey{Name: name, Namespace: cluster.Namespace}, &corev1.Service{})
	if err == nil || !errors.IsNotFound(err) {
		return err
	}

	nameNodeService := &corev1.Service{
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
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildDataNodeNodePortService(cluster *hadoopclusterorgv1alpha1.HadoopCluster, name string, labels map[string]string) error {
	err := h.Get(context.Background(), client.ObjectKey{Name: name, Namespace: cluster.Namespace}, &corev1.Service{})
	if err == nil || !errors.IsNotFound(err) {
		return err
	}

	nameNodeService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "datanode",
				Port:       9870,
				TargetPort: intstr.FromInt(9870),
				NodePort:   30000 + 9870,
			}},
		},
	}

	ownerRef := util.GenOwnerReference(cluster)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}
