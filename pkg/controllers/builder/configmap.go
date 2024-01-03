package builder

import (
	"bytes"
	kubeclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/control"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	"html/template"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sync"
)

const configMapTemplate = `CORE-SITE.XML_fs.default.name=ConfigMap://{{.NameNodeURI}}
CORE-SITE.XML_fs.defaultFS=ConfigMap://{{.NameNodeURI}}
ConfigMap-SITE.XML_dfs.namenode.rpc-address={{.NameNodeURI}}:8020
ConfigMap-SITE.XML_dfs.replication={{.DataNodeReplicas}}
MAPRED-SITE.XML_mapreduce.framework.name=yarn
MAPRED-SITE.XML_yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.map.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.reduce.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
YARN-SITE.XML_yarn.resourcemanager.hostname={{.ResourceManagerHostname}}
YARN-SITE.XML_yarn.nodemanager.pmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.delete.debug-delay-sec=600
YARN-SITE.XML_yarn.nodemanager.vmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.aux-services=mapreduce_shuffle
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-applications=10000
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-am-resource-percent=0.1
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.queues=default
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.capacity=100
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.user-limit-factor=1
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.maximum-capacity=100
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.state=RUNNING
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_submit_applications=*
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_administer_queue=*
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.node-locality-delay=40
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings=
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings-override.enable=false
`

var (
	onceInitContainer sync.Once
	icGenerator       *configMapGenerator
)

type configMapGenerator struct {
	template string
	image    string
	maxTries int
}

func getConfigMapGenerator() *configMapGenerator {
	onceInitContainer.Do(func() {
		icGenerator = &configMapGenerator{
			template: getConfigMapTemplateOrDefault(""),
		}
	})
	return icGenerator
}

// getConfigMapTemplateOrDefault returns the init container template file if
// it exists, or return initContainerTemplate by default.
func getConfigMapTemplateOrDefault(file string) string {
	b, err := os.ReadFile(file)
	if err == nil {
		return string(b)
	}
	return configMapTemplate
}

type HadoopConfig struct {
	NameNodeURI             string
	DataNodeReplicas        int
	ResourceManagerHostname string
}

func (i *configMapGenerator) GetConfigMapBytes(hadoopConfig HadoopConfig) ([]byte, error) {
	var buf bytes.Buffer
	tpl, err := template.New("configmap").Parse(i.template)
	if err != nil {
		return nil, err
	}
	if err := tpl.Execute(&buf, hadoopConfig); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

var _ Builder = &ConfigMapBuilder{}

type ConfigMapBuilder struct {
	// KubeClientSet is a standard kubernetes clientset.
	KubeClientSet kubeclientset.Interface

	// ConfigMapControl is used to add or delete services.
	ConfigMapControl control.ConfigMapControlInterface
}

func (h *ConfigMapBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()

	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)

	h.KubeClientSet = kubeClientSet
	h.ConfigMapControl = control.RealConfigMapControl{KubeClient: kubeClientSet, Recorder: recorder}
}

func (h *ConfigMapBuilder) Build(cluster *kubeclusterorgv1alpha1.HadoopCluster, status *kubeclusterorgv1alpha1.HadoopClusterStatus) error {
	hadoopConfig := HadoopConfig{
		NameNodeURI:             util.GetNameNodeName(cluster),
		DataNodeReplicas:        int(*cluster.Spec.HDFS.DataNode.Replicas),
		ResourceManagerHostname: util.GetResourceManagerName(cluster),
	}
	configMapBytes, err := getConfigMapGenerator().GetConfigMapBytes(hadoopConfig)
	if err != nil {
		return err
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetConfigMapName(cluster),
			Namespace: cluster.Namespace,
		},
		Data: map[string]string{
			"config": string(configMapBytes),
		},
	}
	ownerRef := util.GenOwnerReference(cluster)
	return h.ConfigMapControl.CreateConfigMapWithControllerRef(cluster.GetNamespace(), configMap, cluster, ownerRef)
}
