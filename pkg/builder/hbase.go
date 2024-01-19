package builder

import (
	"context"
	"fmt"
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/control"
	"github.com/chriskery/hadoop-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var _ Builder = &HbaseBuilder{}

type HbaseBuilder struct {
	AlwaysBuildCompletedBuilder

	client.Client

	// ServiceControl is used to add or delete services.
	ServiceControl control.ServiceControlInterface

	// PodControl is used to add or delete services.
	PodControl control.PodControlInterface
}

func (h *HbaseBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()
	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)

	h.Client = mgr.GetClient()
	h.ServiceControl = control.RealServiceControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder}
}

func (h *HbaseBuilder) Build(obj interface{}, objStatus interface{}) (bool, error) {
	cluster := obj.(*v1alpha1.HadoopCluster)
	status := objStatus.(*v1alpha1.HadoopClusterStatus)

	if cluster.Spec.Hbase == nil {
		return true, nil
	}

	util.InitializeClusterStatuses(status, v1alpha1.ReplicaTypeHbase)
	if err := h.buildHbasePod(cluster, status); err != nil {
		return false, err
	}

	serviceName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeHbase)
	if err := h.buildHbaseService(cluster, serviceName); err != nil {
		return false, err
	}

	if isServiceNodePortExpose(cluster.Spec.Hbase.Expose) {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err := h.Get(context.Background(), client.ObjectKey{Name: serviceNodePortName, Namespace: cluster.Namespace}, &corev1.Service{})
		if err != nil {
			if !errors.IsNotFound(err) {
				return false, err
			}
			httpPort := cluster.Spec.Hbase.Expose.HttpNodePort
			if err = h.buildResourceManageNodePortService(cluster, serviceNodePortName, httpPort); err != nil {
				return false, err
			}
		}
	}

	return true, nil
}

func (h *HbaseBuilder) buildHbaseService(cluster *v1alpha1.HadoopCluster, serviceName string) error {
	err := h.Get(context.Background(), client.ObjectKey{Name: serviceName, Namespace: cluster.Namespace}, &corev1.Service{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.createHbaseService(cluster, serviceName); err != nil {
			return err
		}
	}
	return nil
}

func (h *HbaseBuilder) buildHbasePod(cluster *v1alpha1.HadoopCluster, status *v1alpha1.HadoopClusterStatus) error {
	podName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeHbase)
	HbasePod := &corev1.Pod{}
	err := h.Get(context.Background(), client.ObjectKey{Name: podName, Namespace: cluster.Namespace}, HbasePod)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err = h.createHbasePod(cluster); err != nil {
			return err
		}
	} else {
		util.UpdateClusterReplicaStatuses(status, v1alpha1.ReplicaTypeHbase, HbasePod)
	}
	status.ReplicaStatuses[v1alpha1.ReplicaTypeHbase].Expect = cluster.Spec.Hbase.Replicas
	return nil
}

func (h *HbaseBuilder) Clean(obj interface{}) error {
	cluster := obj.(*v1alpha1.HadoopCluster)
	return h.cleanHbase(cluster)
}

func (h *HbaseBuilder) createHbaseService(cluster *v1alpha1.HadoopCluster, name string) error {
	HbaseService := getHeadLessNodeServiceSpec(cluster, name, v1alpha1.ReplicaTypeHbase)

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), HbaseService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}

func (h *HbaseBuilder) createHbasePod(cluster *v1alpha1.HadoopCluster) error {
	podTemplate, err := getPodSpec(cluster, v1alpha1.ReplicaTypeHbase)
	if err != nil {
		return err
	}

	podTemplate.Spec.Containers[0].LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/",
				Port: intstr.FromInt32(16010),
			},
		},
		InitialDelaySeconds: 10,
		TimeoutSeconds:      1,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err = h.PodControl.CreatePodsWithControllerRef(cluster.GetNamespace(), podTemplate, cluster, ownerRef); err != nil {
		return err
	}
	return nil
}

func (h *HbaseBuilder) buildResourceManageNodePortService(
	cluster *v1alpha1.HadoopCluster,
	name string,
	nodePort int32,
) error {
	labels := getLabels(cluster, v1alpha1.ReplicaTypeHbase)
	HbaseService := &corev1.Service{
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
					Port:     16010,
					NodePort: nodePort,
				},
			},
		},
	}

	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	if err := h.ServiceControl.CreateServicesWithControllerRef(cluster.GetNamespace(), HbaseService, cluster, ownerRef); err != nil {
		return err
	}

	return nil
}

func (h *HbaseBuilder) cleanHbase(cluster *v1alpha1.HadoopCluster) error {
	if cluster.Spec.Hbase == nil {
		return nil
	}

	podName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeHbase)
	err := h.PodControl.DeletePod(cluster.GetNamespace(), podName, &corev1.Pod{})
	if err != nil {
		return err
	}

	serviceName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeHbase)
	err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceName, &corev1.Service{})
	if err != nil {
		return err
	}

	if isServiceNodePortExpose(cluster.Spec.Hbase.Expose) {
		serviceNodePortName := fmt.Sprintf("%s-nodeport", serviceName)
		err = h.ServiceControl.DeleteService(cluster.GetNamespace(), serviceNodePortName, &corev1.Service{})
		if err != nil {
			return err
		}
	}
	return nil
}
