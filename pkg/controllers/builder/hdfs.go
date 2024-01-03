package builder

import (
	kubeclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/control"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var _ Builder = &HdfsBuilder{}

type HdfsBuilder struct {
	// KubeClientSet is a standard kubernetes clientset.
	KubeClientSet kubeclientset.Interface

	// PodControl is used to add or delete pods.
	PodControl control.PodControlInterface

	// StatefulSetControl is used to add or delete statefulsets.
	StatefulSetControl control.PodControlInterface

	// ServiceControl is used to add or delete services.
	ServiceControl control.ServiceControlInterface

	// ConfigMapControl is used to add or delete services.
	ConfigMapControl control.ConfigMapControlInterface
}

func (h *HdfsBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()

	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)

	h.KubeClientSet = kubeClientSet
	h.PodControl = control.RealPodControl{KubeClient: kubeClientSet, ClusterConfig: cfg, Recorder: recorder}
	h.ServiceControl = control.RealServiceControl{KubeClient: kubeClientSet, Recorder: recorder}
	h.ConfigMapControl = control.RealConfigMapControl{KubeClient: kubeClientSet, Recorder: recorder}
}

const (
	DefaultHadoopHdfsMaster = "hadoop-hdfs-master"
)

func (h *HdfsBuilder) Build(cluster *kubeclusterorgv1alpha1.HadoopCluster, status *kubeclusterorgv1alpha1.HadoopClusterStatus) error {
	return nil
}
