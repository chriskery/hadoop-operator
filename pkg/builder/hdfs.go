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

var _ Builder = &HdfsBuilder{}

type HdfsBuilder struct {
	client.Client

	Recorder record.EventRecorder

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
	h.Recorder = recorder
	h.StatefulSetControl = control.RealStatefulSetControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.ServiceControl = control.RealServiceControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder}
}

func (h *HdfsBuilder) Build(obj interface{}, objStatus interface{}) error {
	cluster := obj.(*hadoopclusterorgv1alpha1.HadoopCluster)
	status := objStatus.(*hadoopclusterorgv1alpha1.HadoopClusterStatus)

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

func (h *HdfsBuilder) Clean(obj interface{}) error {
	cluster := obj.(*hadoopclusterorgv1alpha1.HadoopCluster)

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
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeNameNode)

	nameNodePod := &corev1.Pod{}
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode), Namespace: cluster.Namespace},
		nameNodePod,
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNameNodePod(cluster, labels); err != nil {
			return err
		}
	} else {
		util.UpdateClusterReplicaStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeNameNode, nameNodePod)
	}
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeNameNode].Expect = cluster.Spec.HDFS.NameNode.Replicas

	serviceName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
	err = h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode), Namespace: cluster.Namespace},
		&corev1.Service{},
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNameNodeService(cluster, labels); err != nil {
			return err
		}
	}

	if cluster.Spec.HDFS.NameNode.ServiceType == corev1.ServiceTypeNodePort {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.Get(context.Background(), client.ObjectKey{Name: serviceNodePortName, Namespace: cluster.Namespace}, &corev1.Service{})
		if err != nil {
			if !errors.IsNotFound(err) {
				return err
			}
			if err = h.buildNameNodeNodePortService(cluster, labels, serviceNodePortName); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *HdfsBuilder) buildNameNodeService(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) error {
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

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildNameNodePod(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) error {
	podSpec, err := h.genNameNodePodSpec(cluster, &cluster.Spec.HDFS.NameNode, labels)
	if err != nil {
		return err
	}

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err = h.PodControl.CreatePodsWithControllerRef(cluster.GetNamespace(), podSpec, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildDataNode(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	status *hadoopclusterorgv1alpha1.HadoopClusterStatus,
) error {
	labels := utillabels.GenLabels(cluster.GetName(), hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	dataNodeStatefulSet := &appv1.StatefulSet{}
	dataNodeName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: dataNodeName, Namespace: cluster.Namespace},
		dataNodeStatefulSet,
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildDataNodeStatefulSet(cluster, dataNodeName, labels); err != nil {
			return err
		}
	}
	if h.isNeedReconcileDataNodeHPA(cluster, dataNodeStatefulSet) {
		if err = h.reconcileDataNodeHPA(cluster, dataNodeStatefulSet, status); err != nil {
			return err
		}
	}

	if err = h.updateDataNodeStatus(cluster, status, labels); err != nil {
		return err
	}

	if err = h.buildDataNodeServices(cluster, labels); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) isNeedReconcileDataNodeHPA(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	dataNodeStatefulSet *appv1.StatefulSet,
) bool {
	if cluster.Spec.HDFS.DataNode.Replicas == nil && dataNodeStatefulSet.Spec.Replicas == nil {
		return false
	}
	if cluster.Spec.HDFS.DataNode.Replicas == nil && *dataNodeStatefulSet.Spec.Replicas == 1 {
		return false
	}
	if *cluster.Spec.HDFS.DataNode.Replicas == 1 && dataNodeStatefulSet.Spec.Replicas == nil {
		return false
	}
	return *cluster.Spec.HDFS.DataNode.Replicas != *dataNodeStatefulSet.Spec.Replicas
}

func (h *HdfsBuilder) updateDataNodeStatus(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	status *hadoopclusterorgv1alpha1.HadoopClusterStatus,
	labels map[string]string,
) error {
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
	}
	filterPods := util.ConvertPodListWithFilter(podList.Items, filter)

	for _, pod := range filterPods {
		util.UpdateClusterReplicaStatuses(status, hadoopclusterorgv1alpha1.ReplicaTypeDataNode, pod)
	}
	status.ReplicaStatuses[hadoopclusterorgv1alpha1.ReplicaTypeDataNode].Expect = cluster.Spec.HDFS.DataNode.Replicas
	return nil
}

func (h *HdfsBuilder) buildDataNodeStatefulSet(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	name string,
	labels map[string]string,
) error {
	dataNodeStatulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
	}
	statefulSetSpec, err := h.genDataNodeStatefulSetSpec(cluster, labels)
	if err != nil {
		return err
	}
	dataNodeStatulSet.Spec = *statefulSetSpec

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err = h.StatefulSetControl.CreateStatefulSetsWithControllerRef(cluster.GetNamespace(), dataNodeStatulSet, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) genNameNodePodSpec(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	nameNodeSpec *hadoopclusterorgv1alpha1.HDFSNameNodeSpecTemplate,
	labels map[string]string,
) (*corev1.PodTemplateSpec, error) {
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name:   util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode),
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Volumes:       nameNodeSpec.Volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap))

	volumeMounts := nameNodeSpec.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	nameNodeCmd := []string{"sh", "-c", entrypointCmd}
	containers := []corev1.Container{{
		Name:            string(hadoopclusterorgv1alpha1.ReplicaTypeNameNode),
		Image:           nameNodeSpec.Image,
		Command:         nameNodeCmd,
		Resources:       nameNodeSpec.Resources,
		VolumeMounts:    volumeMounts,
		Env:             cluster.Spec.HDFS.NameNode.Env,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: nameNodeSpec.ImagePullPolicy,
		SecurityContext: nameNodeSpec.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, hadoopclusterorgv1alpha1.ReplicaTypeNameNode)
	if nameNodeSpec.Format {
		for i := range podTemplateSpec.Spec.Containers {
			podTemplateSpec.Spec.Containers[i].Env = append(podTemplateSpec.Spec.Containers[i].Env, corev1.EnvVar{
				Name:  EnvNameNodeFormat,
				Value: "true",
			})
		}
	}
	return podTemplateSpec, nil
}

func (h *HdfsBuilder) genDataNodeStatefulSetSpec(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	labels map[string]string,
) (*appv1.StatefulSetSpec, error) {
	podTemplate, err := h.genDataNodePodSpec(cluster, &cluster.Spec.HDFS.DataNode, labels)
	if err != nil {
		return nil, err
	}

	statefulSetSpec := &appv1.StatefulSetSpec{
		Replicas: cluster.Spec.HDFS.NameNode.Replicas,
		Selector: &metav1.LabelSelector{MatchLabels: labels},
		Template: *podTemplate,
	}
	return statefulSetSpec, nil
}

func (h *HdfsBuilder) genDataNodePodSpec(
	cluster *hadoopclusterorgv1alpha1.HadoopCluster,
	dataNodeSpec *hadoopclusterorgv1alpha1.HDFSDataNodeSpecTemplate,
	labels map[string]string,
) (*corev1.PodTemplateSpec, error) {
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Volumes:       dataNodeSpec.Volumes,
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

	nameNodeCmd := []string{"sh", "-c", entrypointCmd}

	containers := []corev1.Container{{
		Name:            string(hadoopclusterorgv1alpha1.ReplicaTypeDataNode),
		Image:           dataNodeSpec.Image,
		Command:         nameNodeCmd,
		Resources:       dataNodeSpec.Resources,
		VolumeMounts:    volumeMounts,
		Env:             cluster.Spec.HDFS.DataNode.Env,
		ReadinessProbe:  nil,
		StartupProbe:    nil,
		ImagePullPolicy: dataNodeSpec.ImagePullPolicy,
		SecurityContext: dataNodeSpec.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers
	setPodEnv(podTemplateSpec, hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	if err := setInitContainer(cluster, hadoopclusterorgv1alpha1.ReplicaTypeDataNode, podTemplateSpec); err != nil {
		return nil, err
	}
	return podTemplateSpec, nil
}

func (h *HdfsBuilder) buildDataNodeServices(cluster *hadoopclusterorgv1alpha1.HadoopCluster, labels map[string]string) error {
	replicas := cluster.Spec.HDFS.DataNode.Replicas
	if replicas == nil {
		return nil
	}

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

	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	if err = h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
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

	if cluster.Spec.Yarn.ResourceManager.ServiceType == corev1.ServiceTypeNodePort {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceNodePortName, &corev1.Service{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *HdfsBuilder) cleanDataNode(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	dataNodeName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeDataNode)
	err := h.StatefulSetControl.DeleteStatefulSet(cluster.GetNamespace(), dataNodeName, &appv1.StatefulSet{})
	if err != nil {
		return err
	}

	if cluster.Spec.HDFS.DataNode.Replicas == nil {
		return nil
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

func (h *HdfsBuilder) reconcileDataNodeHPA(
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

	msg := fmt.Sprintf("HadoopCluster %s/%s is reconfiguraing datanodes replicas.", cluster.Namespace, cluster.Name)
	err = util.UpdateClusterConditions(status, hadoopclusterorgv1alpha1.ClusterReconfiguring, util.HadoopclusterReconfiguringReason, msg)
	if err != nil {
		return err
	}
	h.Recorder.Eventf(cluster, corev1.EventTypeNormal, "HadoopClusterReconfiguring", msg)

	return nil
}

func (h *HdfsBuilder) buildNameNodeNodePortService(
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
					Port: 9870,
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
