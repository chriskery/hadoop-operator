package builder

import (
	"bytes"
	"context"
	"fmt"
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
	"strconv"
	"text/template"
)

const (
	templatePrefix = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
`
	templateSuffix = `</configuration>`

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
		if [ "$NAME_NODE_FORMAT" = "true" ]; then
	        hdfs namenode -format
		fi
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

	entrypointKey = "entrypoint"
	configKey     = "config"
)

type Property struct {
	Name  string
	Value string
}

type XMLTemplateGetter interface {
	GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) string
	GetKey() string
	Default()
}

var _ XMLTemplateGetter = &coreSiteXMLTemplateGetter{}

type coreSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (c *coreSiteXMLTemplateGetter) Default() {
	c.defaultProperties = []Property{
		{"fs.defaultFS", "hdfs://{{.NameNodeURI}}:9000"},
	}
}

func (c *coreSiteXMLTemplateGetter) GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	return genXmlTemplate(c.defaultProperties)
}

func (c *coreSiteXMLTemplateGetter) GetKey() string {
	return coreSiteXmlKey
}

var _ XMLTemplateGetter = &hdfsSiteXMLTemplateGetter{}

type hdfsSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (h *hdfsSiteXMLTemplateGetter) Default() {
	h.defaultProperties = []Property{
		{"dfs.replication", "{{.DataNodeReplicas}}"},
		{"dfs.namenode.rpc-address", "{{.NameNodeURI}}:9000"},
		{"dfs.namenode.http-address", "{{.NameNodeURI}}:9870"},
	}
}

func (h *hdfsSiteXMLTemplateGetter) GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	hdfsSiteProperties := h.defaultProperties
	if cluster.Spec.HDFS.NameNode.LogAggregationEnable {
		hdfsSiteProperties = append(hdfsSiteProperties,
			Property{"dfs.namenode.log-aggregation.enable", "true"},
			Property{"dfs.namenode.log-aggregation.retain-seconds", strconv.Itoa(int(cluster.Spec.HDFS.NameNode.LogAggregationRetainSeconds))})
	}
	if cluster.Spec.HDFS.NameNode.NameDir != "" {
		hdfsSiteProperties = append(hdfsSiteProperties, Property{"dfs.namenode.name.dir", cluster.Spec.HDFS.NameNode.NameDir})
	}
	if cluster.Spec.HDFS.NameNode.BlockSize > 0 {
		hdfsSiteProperties = append(hdfsSiteProperties, Property{"dfs.blocksize", strconv.Itoa(int(cluster.Spec.HDFS.NameNode.BlockSize))})
	}
	if cluster.Spec.HDFS.DataNode.DataDir != "" {
		hdfsSiteProperties = append(hdfsSiteProperties, Property{"dfs.datanode.data.dir", cluster.Spec.HDFS.NameNode.NameDir})
	}
	return genXmlTemplate(hdfsSiteProperties)
}

func (h *hdfsSiteXMLTemplateGetter) GetKey() string {
	return hdfsSiteXmlKey
}

var _ XMLTemplateGetter = &mapredSiteXMLTemplateGetter{}

type mapredSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (m *mapredSiteXMLTemplateGetter) Default() {
	m.defaultProperties = []Property{
		{"mapreduce.framework.name", "yarn"},
		{"mapreduce.jobhistory.address", "{{.ResourceManagerHostname}}:10020"},
	}
}

func (m *mapredSiteXMLTemplateGetter) GetXMLTemplate(_ *hadoopclusterorgv1alpha1.HadoopCluster) string {
	return genXmlTemplate(m.defaultProperties)
}

func (m *mapredSiteXMLTemplateGetter) GetKey() string {
	return mapredSiteXmlKey
}

var _ XMLTemplateGetter = &yarnSiteXMLTemplateGetter{}

type yarnSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (y *yarnSiteXMLTemplateGetter) Default() {
	y.defaultProperties = []Property{
		{"yarn.resourcemanager.hostname", "{{.ResourceManagerHostname}}"},
		{"yarn.nodemanager.aux-services", "mapreduce_shuffle"},
		{"yarn.resourcemanager.recovery.enabled", "true"},
		{"yarn.nodemanager.aux-services.mapreduce.shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler"},
		{"yarn.nodemanager.env-whitelist", "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME"},
	}
}

// SI Sizes.
const (
	IByte = 1
	KByte = IByte * 1000
	MByte = KByte * 1000
)

func (y *yarnSiteXMLTemplateGetter) GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) string {
	requests := cluster.Spec.Yarn.NodeManager.Resources.Requests
	cpuQuantity := requests.Cpu()
	if cpuQuantity != nil {
		vcores, ok := cpuQuantity.AsInt64()
		if ok && vcores > 0 {
			y.defaultProperties = append(y.defaultProperties, Property{"yarn.nodemanager.resource.cpu-vcores",
				strconv.Itoa(int(vcores))})
		}
	}

	memoryQuantity := requests.Memory()
	if memoryQuantity != nil {
		memory, ok := memoryQuantity.AsInt64()
		if ok && memory > 0 {
			y.defaultProperties = append(y.defaultProperties, Property{"yarn.nodemanager.resource.memory-mb",
				strconv.Itoa(int(memory / MByte))})
		}
	}

	return genXmlTemplate(y.defaultProperties)
}

func (y *yarnSiteXMLTemplateGetter) GetKey() string {
	return yarnSiteXmlKey
}

func generalXmlProperty(property Property) string {
	return "\t<property>\n\t\t<name>" + property.Name + "</name>\n\t\t<value>" + property.Value + "</value>\n\t</property>\n"
}

func generalXmlProperties(properties []Property) string {
	var buf bytes.Buffer
	for _, property := range properties {
		buf.WriteString(generalXmlProperty(property))
	}
	return buf.String()
}

func genXmlTemplate(properties []Property) string {
	return fmt.Sprintf("%s\n%s\n%s", templatePrefix, generalXmlProperties(properties), templateSuffix)
}

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

	XmlGetters []XMLTemplateGetter
}

func (h *ConfigMapBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()
	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)

	h.Client = mgr.GetClient()
	h.ConfigMapControl = control.RealConfigMapControl{KubeClient: kubeClientSet, Recorder: recorder}

	h.XmlGetters = []XMLTemplateGetter{
		&coreSiteXMLTemplateGetter{},
		&hdfsSiteXMLTemplateGetter{},
		&mapredSiteXMLTemplateGetter{},
		&yarnSiteXMLTemplateGetter{},
	}
	for _, xmlGetter := range h.XmlGetters {
		xmlGetter.Default()
	}
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

	configMapData := map[string]string{entrypointKey: entrypointTemplate}
	for _, xmlGetter := range h.XmlGetters {
		xmlTemplate := xmlGetter.GetXMLTemplate(cluster)
		configMapBytes, err := getConfigMapGenerator(xmlTemplate).GetConfigMapBytes(hadoopConfig)
		if err != nil {
			return nil, err
		}
		configMapData[xmlGetter.GetKey()] = string(configMapBytes)
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap),
			Namespace: cluster.Namespace,
		},
		Data: configMapData,
	}
	return configMap, nil
}
