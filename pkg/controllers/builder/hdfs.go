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
	util.InitializeClusterStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
	if err := h.buildNameNode(cluster, status); err != nil {
		return err
	}

	util.InitializeClusterStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	if err := h.buildDataNode(cluster, status); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) Clean(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	err := h.cleanNameNode(cluster)
	if err != nil {
		return err
	}

	err = h.cleanDataNode(cluster)
	if err != nil {
		return err
	}

	return nil
}

func (h *HdfsBuilder) buildNameNode(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	nameNodePod := &corev1.Pod{}
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode), Namespace: cluster.Namespace},
		nameNodePod,
	)
	var nameNodeActive int32
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNameNodePod(cluster); err != nil {
			return err
		}
	}
	if nameNodePod.Status.Phase == corev1.PodRunning {
		nameNodeActive = 1
	}
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeNameNode].Active = nameNodeActive
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeNameNode].Expect = cluster.Spec.HDFS.NameNode.Replicas

	err = h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode), Namespace: cluster.Namespace},
		&corev1.Service{},
	)
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
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
	nameNodeService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode),
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
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
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
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeDataNode), Namespace: cluster.Namespace},
		&appv1.StatefulSet{},
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildDataNodeStatefulSet(cluster); err != nil {
			return err
		}
	}

	if err = h.updateDataNodeStatus(cluster, status); err != nil {
		return err
	}

	if err = h.buildDataNodeServices(cluster); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) updateDataNodeStatus(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	status *hadoopclusterorgv1alpha1.HadoopClusterStatus,
) error {
	// Create selector.
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeDataNode),
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
	}
	filterPods := util.ConvertPodListWithFilter(podList.Items, filter)

	var active int32
	for _, pod := range filterPods {
		if pod.Status.Phase == corev1.PodRunning {
			active++
		}
	}
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeDataNode].Active = active
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeDataNode].Expect = cluster.Spec.HDFS.DataNode.Replicas
	return nil
}

func (h *HdfsBuilder) buildDataNodeStatefulSet(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	dataNodeStatulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeDataNode),
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
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeNameNode)

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
			Name:   util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode),
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Volumes:       cluster.Spec.HDFS.NameNode.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap))

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nameNodeCmd := []string{"sh", "-c", fmt.Sprintf("cp %s /tmp/entrypoint && chmod +x /tmp/entrypoint && /tmp/entrypoint", entrypointPath)}
	containers := []corev1.Container{{
		Name:            string(hadoopclusterorgv1alpha1.ReplicaTypeNameNode),
		Image:           cluster.Spec.HDFS.NameNode.Image,
		Command:         nameNodeCmd,
		Resources:       cluster.Spec.HDFS.NameNode.Resources,
		VolumeMounts:    volumeMounts,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: cluster.Spec.HDFS.NameNode.ImagePullPolicy,
		SecurityContext: cluster.Spec.HDFS.NameNode.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
	return podTemplateSpec, nil
}

func (h *HdfsBuilder) genDataNodeStatefulSetSpec(cluster *hadoopclusterorgv1alpha1.HadoopCluster) (*appv1.StatefulSetSpec, error) {
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
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
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeDataNode)

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

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(
		podTemplateSpec.Spec.Volumes,
		util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap),
	)

	volumeMounts := cluster.Spec.HDFS.NameNode.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nameNodeCmd := []string{"sh", "-c", fmt.Sprintf("cp %s /tmp/entrypoint && chmod +x /tmp/entrypoint && /tmp/entrypoint", entrypointPath)}

	containers := []corev1.Container{{
		Name:            string(hadoopclusterorgv1alpha1.ReplicaTypeDataNode),
		Image:           cluster.Spec.HDFS.DataNode.Image,
		Command:         nameNodeCmd,
		Resources:       cluster.Spec.HDFS.DataNode.Resources,
		VolumeMounts:    volumeMounts,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: cluster.Spec.HDFS.DataNode.ImagePullPolicy,
		SecurityContext: cluster.Spec.HDFS.DataNode.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	return podTemplateSpec, nil
}

func (h *HdfsBuilder) buildDataNodeServices(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	replicas := cluster.Spec.HDFS.DataNode.Replicas
	if replicas == nil {
		return nil
	}

	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
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

func (h *HdfsBuilder) cleanNameNode(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	nameNodeName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
	err := h.PodControl.DeletePod(cluster.GetNamespace(), nameNodeName, &corev1.Pod{})
	if err != nil {
		return err
	}

	serviceName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
	err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceName, &corev1.Service{})
	if err != nil {
		return err
	}

	return nil
}

func (h *HdfsBuilder) cleanDataNode(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	dataNodeName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	err := h.StatefulSetControl.DeleteStatefulSet(cluster.GetNamespace(), dataNodeName, &appv1.StatefulSet{})
	if err != nil {
		return err
	}

	for i := 0; i < int(*cluster.Spec.HDFS.DataNode.Replicas); i++ {
		serviceName := util.GetDataNodeServiceName(cluster, i)
		err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceName, &corev1.Service{})
		if err != nil {
			return err
		}
	}
	return nil
}
