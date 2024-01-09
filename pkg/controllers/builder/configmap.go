package builder

import (
	"bytes"
	"context"
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/control"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"text/template"
)

const (
	coreSiteTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>


<configuration>
    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://{{.NameNodeURI}}:9000</value>
    </property>
	
    <property>
      <name>io.file.buffer.size</name>
      <value>4096</value>
    </property>
</configuration>
`

	hdfsSiteTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
    <name>dfs.replication</name>
    <value>{{.DataNodeReplicas}}</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>{{.NameNodeURI}}:9000</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>{{.NameNodeURI}}:9870</value>
  </property>
</configuration>
`
	mapredSiteTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
      <name>mapreduce.framework.name</name>
      <value>yarn</value>
    </property>

    <property>
      <name>mapreduce.jobhistory.address</name>
      <value>0.0.0.0:10020</value>
    </property>
    <property>
      <name>mapreduce.jobhistory.webapp.address</name>
      <value>0.0.0.0:19888</value>
    </property>
</configuration>
`
	yarnSiteTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>


<configuration>
    <property>
      <name>yarn.resourcemanager.recovery.enabled</name>
      <value>true</value>
    </property>
	
    <property>
      <name>yarn.resourcemanager.hostname</name>
      <value>{{.ResourceManagerHostname}}</value>
    </property>

    <property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
    </property>
    <property>
      <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
      <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
	
    <property>
      <name>yarn.log-aggregation-enable</name>
      <value>true</value>
    </property>
    <property>
      <name>yarn.log-aggregation.retain-seconds</name>
      <value>604800</value>
    </property>	

    <property>
       <name>yarn.nodemanager.env-whitelist</name>
       <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>

</configuration>`

	entrypointTemplate = `#/bin/bash
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}"
echo "The value of HADOOP_CONF_DIR is: $HADOOP_CONF_DIR"

HADOOP_OPERATOR_DIR="${HADOOP_OPERATOR_DIR:-/etc/hadoop-operator}"
if [ -d "$HADOOP_OPERATOR_DIR" ]; then
    cp $HADOOP_OPERATOR_DIR/hdfs-site.xml $HADOOP_CONF_DIR
    cp $HADOOP_OPERATOR_DIR/core-site.xml $HADOOP_CONF_DIR
    cp $HADOOP_OPERATOR_DIR/mapred-site.xml $HADOOP_CONF_DIR
    cp $HADOOP_OPERATOR_DIR/yarn-site.xml $HADOOP_CONF_DIR
else
    echo "Directory does not exist: $HADOOP_OPERATOR_DIR"
fi

mkdir -p  /tmp/hadoop-hadoop/dfs/name

case "$HADOOP_ROLE" in
    resourcemanager)
        echo "Environment variable is set to resourcemanager"
        yarn resourcemanager
        ;;
    nodemanager)
        echo "Environment variable is set to nodemanager"
        yarn nodemanager
        ;;
    namenode)
        echo "Environment variable is set to namenode"
        hdfs namenode -format
		hdfs namenode
        ;;
    datanode)
        echo "Environment variable is set to datanode"
        hdfs datanode
        ;;
    *)
        echo "Environment variable is set to an unknown value: $HADOOP_ROLE"
        exit 1
        ;;
esac
`

	coreSiteXmlKey   = "core-site.xml"
	hdfsSiteXmlKey   = "hdfs-site.xml"
	mapredSiteXmlKey = "mapred-site.xml"
	yarnSiteXmlKey   = "yarn-site.xml"
	workersKey       = "workers"

	entrypointKey = "entrypoint"
	configKey     = "config"
)

type configMapGenerator struct {
	template string
}

func getConfigMapGenerator(template string) *configMapGenerator {
	return &configMapGenerator{
		template: template,
	}
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
	if err = tpl.Execute(&buf, hadoopConfig); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

var _ Builder = &ConfigMapBuilder{}

type ConfigMapBuilder struct {
	client.Client

	// ConfigMapControl is used to add or delete services.
	ConfigMapControl control.ConfigMapControlInterface
}

func (h *ConfigMapBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()
	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)

	h.Client = mgr.GetClient()
	h.ConfigMapControl = control.RealConfigMapControl{KubeClient: kubeClientSet, Recorder: recorder}
}

func (h *ConfigMapBuilder) Build(cluster *hadoopclusterorgv1alpha1.HadoopCluster, status *hadoopclusterorgv1alpha1.HadoopClusterStatus) error {
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap), Namespace: cluster.Namespace},
		&corev1.ConfigMap{},
	)
	if err == nil || !errors.IsNotFound(err) {
		return err
	}

	configMap, err := h.buildHadoopConfigMap(cluster)
	if err != nil {
		return err
	}
	ownerRef := util.GenOwnerReference(cluster)
	return h.ConfigMapControl.CreateConfigMapWithControllerRef(cluster.GetNamespace(), configMap, cluster, ownerRef)
}

func (h *ConfigMapBuilder) Clean(cluster *hadoopclusterorgv1alpha1.HadoopCluster) error {
	configMapName := util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap)
	err := h.ConfigMapControl.DeleteConfigMap(cluster.GetNamespace(), configMapName, &corev1.ConfigMap{})
	if err != nil {
		return err
	}

	return nil
}

func (h *ConfigMapBuilder) buildHadoopConfigMap(cluster *hadoopclusterorgv1alpha1.HadoopCluster) (*corev1.ConfigMap, error) {
	hadoopConfig := HadoopConfig{
		NameNodeURI:             util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeNameNode),
		DataNodeReplicas:        int(*cluster.Spec.HDFS.DataNode.Replicas),
		ResourceManagerHostname: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager),
	}
	coreSiteXML, err := getConfigMapGenerator(coreSiteTemplate).GetConfigMapBytes(hadoopConfig)
	if err != nil {
		return nil, err
	}
	hdfsSiteXML, err := getConfigMapGenerator(hdfsSiteTemplate).GetConfigMapBytes(hadoopConfig)
	if err != nil {
		return nil, err
	}
	mapredSiteXML, err := getConfigMapGenerator(mapredSiteTemplate).GetConfigMapBytes(hadoopConfig)
	if err != nil {
		return nil, err
	}
	yarnSiteXML, err := getConfigMapGenerator(yarnSiteTemplate).GetConfigMapBytes(hadoopConfig)
	if err != nil {
		return nil, err
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap),
			Namespace: cluster.Namespace,
		},
		Data: map[string]string{
			yarnSiteXmlKey:   string(yarnSiteXML),
			coreSiteXmlKey:   string(coreSiteXML),
			hdfsSiteXmlKey:   string(hdfsSiteXML),
			mapredSiteXmlKey: string(mapredSiteXML),
			entrypointKey:    entrypointTemplate,
		},
	}
	return configMap, nil
}
