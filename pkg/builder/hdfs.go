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

func (h *HdfsBuilder) IsBuildCompleted(obj interface{}, objStatus interface{}) bool {
	cluster := obj.(*v1alpha1.HadoopCluster)
	status := objStatus.(*v1alpha1.HadoopClusterStatus)

	nameNodeStatus, ok := status.ReplicaStatuses[v1alpha1.ReplicaTypeNameNode]
	if !ok || !util.ReplicaReady(cluster.Spec.HDFS.NameNode.Replicas, 1, nameNodeStatus.Active) {
		return false
	}
	dataNodeStatus, ok := status.ReplicaStatuses[v1alpha1.ReplicaTypeDataNode]
	if !ok || !util.ReplicaReady(cluster.Spec.HDFS.DataNode.Replicas, 1, dataNodeStatus.Active) {
		return false
	}

	return true
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

func (h *HdfsBuilder) Build(obj interface{}, objStatus interface{}) (bool, error) {
	cluster := obj.(*v1alpha1.HadoopCluster)
	status := objStatus.(*v1alpha1.HadoopClusterStatus)

	util.InitializeClusterStatuses(status, v1alpha1.ReplicaTypeNameNode)
	if err := h.buildNameNode(cluster, status); err != nil {
		return false, err
	}

	util.InitializeClusterStatuses(status, v1alpha1.ReplicaTypeDataNode)
	if err := h.buildDataNode(cluster, status); err != nil {
		return false, err
	}
	return true, nil
}

func (h *HdfsBuilder) Clean(obj interface{}) error {
	cluster := obj.(*v1alpha1.HadoopCluster)

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

func (h *HdfsBuilder) buildNameNode(cluster *v1alpha1.HadoopCluster, status *v1alpha1.HadoopClusterStatus) error {
	nameNodePod := &corev1.Pod{}
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode), Namespace: cluster.Namespace},
		nameNodePod,
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNameNodePod(cluster); err != nil {
			return err
		}
	} else {
		util.UpdateClusterReplicaStatuses(status, v1alpha1.ReplicaTypeNameNode, nameNodePod)
	}
	status.ReplicaStatuses[v1alpha1.ReplicaTypeNameNode].Expect = cluster.Spec.HDFS.NameNode.Replicas

	serviceName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode)
	err = h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode), Namespace: cluster.Namespace},
		&corev1.Service{},
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildNameNodeService(cluster); err != nil {
			return err
		}
	}

	if isServiceNodePortExpose(cluster.Spec.HDFS.NameNode.Expose) {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.Get(context.Background(), client.ObjectKey{Name: serviceNodePortName, Namespace: cluster.Namespace}, &corev1.Service{})
		if err != nil && errors.IsNotFound(err) {
			if err = h.buildNameNodeNodePortService(
				cluster,
				serviceNodePortName,
				cluster.Spec.HDFS.NameNode.Expose.HttpNodePort,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *HdfsBuilder) buildNameNodeService(cluster *v1alpha1.HadoopCluster) error {
	serviceName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode)
	nameNodeService := getHeadLessNodeServiceSpec(cluster, serviceName, v1alpha1.ReplicaTypeNameNode)

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildNameNodePod(cluster *v1alpha1.HadoopCluster) error {
	podSpec, err := getPodSpec(cluster, v1alpha1.ReplicaTypeNameNode)
	if err != nil {
		return err
	}

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err = h.PodControl.CreatePodsWithControllerRef(cluster.GetNamespace(), podSpec, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) buildDataNode(
	cluster *v1alpha1.HadoopCluster,
	status *v1alpha1.HadoopClusterStatus,
) error {
	dataNodeStatefulSet := &appv1.StatefulSet{}
	dataNodeName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeDataNode)
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: dataNodeName, Namespace: cluster.Namespace},
		dataNodeStatefulSet,
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.buildDataNodeStatefulSet(cluster, dataNodeName); err != nil {
			return err
		}
	} else {
		if h.isNeedReconcileDataNodeHPA(cluster, dataNodeStatefulSet) {
			if err = h.reconcileDataNodeHPA(cluster, dataNodeStatefulSet, status); err != nil {
				return err
			}
		}
		if err = h.updateDataNodeStatus(cluster, status, dataNodeStatefulSet.Spec.Selector.MatchLabels); err != nil {
			return err
		}
	}

	if err = h.buildDataNodeServices(cluster); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) isNeedReconcileDataNodeHPA(
	cluster *v1alpha1.HadoopCluster,
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
	cluster *v1alpha1.HadoopCluster,
	status *v1alpha1.HadoopClusterStatus,
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
		util.UpdateClusterReplicaStatuses(status, v1alpha1.ReplicaTypeDataNode, pod)
	}
	status.ReplicaStatuses[v1alpha1.ReplicaTypeDataNode].Expect = cluster.Spec.HDFS.DataNode.Replicas
	return nil
}

func (h *HdfsBuilder) buildDataNodeStatefulSet(
	cluster *v1alpha1.HadoopCluster,
	name string,
) error {
	dataNodeStatulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}
	statefulSetSpec, err := h.genDataNodeStatefulSetSpec(cluster)
	if err != nil {
		return err
	}
	dataNodeStatulSet.Spec = *statefulSetSpec
	dataNodeStatulSet.Labels = statefulSetSpec.Selector.MatchLabels

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err = h.StatefulSetControl.CreateStatefulSetsWithControllerRef(cluster.GetNamespace(), dataNodeStatulSet, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) genDataNodeStatefulSetSpec(cluster *v1alpha1.HadoopCluster) (*appv1.StatefulSetSpec, error) {
	podTemplate, err := getPodSpec(cluster, v1alpha1.ReplicaTypeDataNode)
	if err != nil {
		return nil, err
	}

	statefulSetSpec := &appv1.StatefulSetSpec{
		Replicas: cluster.Spec.HDFS.NameNode.Replicas,
		Selector: &metav1.LabelSelector{MatchLabels: podTemplate.Labels},
		Template: *podTemplate,
	}
	return statefulSetSpec, nil
}

func (h *HdfsBuilder) buildDataNodeServices(cluster *v1alpha1.HadoopCluster) error {
	replicas := cluster.Spec.HDFS.DataNode.Replicas
	if replicas == nil {
		return nil
	}

	for i := 0; i < int(*replicas); i++ {
		serviceName := util.GetDataNodeServiceName(cluster, i)
		if err := h.buildDataNodeService(cluster, serviceName); err != nil {
			return err
		}
	}
	return nil
}

func (h *HdfsBuilder) buildDataNodeService(cluster *v1alpha1.HadoopCluster, name string) error {
	err := h.Get(context.Background(), client.ObjectKey{Name: name, Namespace: cluster.Namespace}, &corev1.Service{})
	if err == nil || !errors.IsNotFound(err) {
		return err
	}

	nameNodeService := getHeadLessNodeServiceSpec(cluster, name, v1alpha1.ReplicaTypeDataNode)

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err = h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HdfsBuilder) cleanNameNode(cluster *v1alpha1.HadoopCluster) error {
	nameNodeName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode)
	err := h.PodControl.DeletePod(cluster.GetNamespace(), nameNodeName, &corev1.Pod{})
	if err != nil {
		return err
	}

	serviceName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode)
	err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceName, &corev1.Service{})
	if err != nil {
		return err
	}

	if isServiceNodePortExpose(cluster.Spec.HDFS.NameNode.Expose) {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceNodePortName, &corev1.Service{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *HdfsBuilder) cleanDataNode(cluster *v1alpha1.HadoopCluster) error {
	dataNodeName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeDataNode)
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
	cluster *v1alpha1.HadoopCluster,
	statefulSet *appv1.StatefulSet,
	status *v1alpha1.HadoopClusterStatus,
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
	err = util.UpdateClusterConditions(status, v1alpha1.ClusterReconfiguring, util.HadoopclusterReconfiguringReason, msg)
	if err != nil {
		return err
	}
	h.Recorder.Eventf(cluster, corev1.EventTypeNormal, "HadoopClusterReconfiguring", msg)

	return nil
}

func (h *HdfsBuilder) buildNameNodeNodePortService(
	cluster *v1alpha1.HadoopCluster,
	name string,
	nodePort int32,
) error {
	labels := getLabels(cluster, v1alpha1.ReplicaTypeNameNode)
	nameNodeService := &corev1.Service{
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
					Port:     9870,
					NodePort: nodePort,
				},
			},
		},
	}

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), nameNodeService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}
