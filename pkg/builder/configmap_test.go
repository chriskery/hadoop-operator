package builder

import (
	"encoding/xml"
	"fmt"
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/util"
	"github.com/chriskery/hadoop-operator/pkg/util/testutil"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestBuildHadoopConfigMap(t *testing.T) {
	mgr, envTest := testutil.NewCtrlManager(t)
	defer envTest.Stop()

	builder := &ConfigMapBuilder{}
	builder.SetupWithManager(mgr, mgr.GetEventRecorderFor("test"))

	cluster := testutil.NewHadoopCluster()
	cluster.Spec.HDFS.DataNode.Replicas = ptr.To(int32(3))
	cluster.Spec.HDFS.NameNode.LogAggregationEnable = true
	cluster.Spec.HDFS.NameNode.NameDir = "test-name-dir"
	cluster.Spec.HDFS.NameNode.BlockSize = 100000
	cluster.Spec.HDFS.DataNode.DataDir = "test-data-dir"
	cluster.Spec.Yarn.NodeManager.Resources = corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("1024Mi")}}
	cluster.Default()

	configMap, err := builder.buildHadoopConfigMap(cluster)
	assert.NoError(t, err)
	assert.NotNil(t, configMap)
	assert.Equal(t, util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeConfigMap), configMap.Name)
	assert.Equal(t, testutil.TestHadoopClusterNamespace, configMap.Namespace)
	assert.Contains(t, configMap.Data, coreSiteXmlKey)
	assert.Contains(t, configMap.Data, hdfsSiteXmlKey)
	assert.Contains(t, configMap.Data, mapredSiteXmlKey)
	assert.Contains(t, configMap.Data, yarnSiteXmlKey)

	coreSiteConfiguration := HadoopConfiguration{}
	err = xml.Unmarshal([]byte(configMap.Data[coreSiteXmlKey]), &coreSiteConfiguration)
	assert.NoError(t, err)
	assert.Greater(t, len(coreSiteConfiguration.Properties), 0)

	hdfsSiteConfiguration := HadoopConfiguration{}
	err = xml.Unmarshal([]byte(configMap.Data[hdfsSiteXmlKey]), &hdfsSiteConfiguration)
	assert.NoError(t, err)
	assert.Greater(t, len(coreSiteConfiguration.Properties), 0)
	assert.Contains(t, hdfsSiteConfiguration.Properties, Property{Name: "dfs.replication", Value: strconv.Itoa(int(*cluster.Spec.HDFS.DataNode.Replicas))})
	assert.Contains(t, hdfsSiteConfiguration.Properties, Property{Name: "dfs.namenode.log-aggregation.enable", Value: strconv.FormatBool(cluster.Spec.HDFS.NameNode.LogAggregationEnable)})
	assert.Contains(t, hdfsSiteConfiguration.Properties, Property{Name: "dfs.namenode.log-aggregation.retain-seconds", Value: strconv.Itoa(hadoopclusterorgv1alpha1.DefaultLogAggregationRetainSeconds)})
	assert.Contains(t, hdfsSiteConfiguration.Properties, Property{Name: "dfs.blocksize", Value: strconv.Itoa(int(cluster.Spec.HDFS.NameNode.BlockSize))})
	assert.Contains(t, hdfsSiteConfiguration.Properties, Property{Name: "dfs.datanode.data.dir", Value: cluster.Spec.HDFS.DataNode.DataDir})
	assert.Contains(t, hdfsSiteConfiguration.Properties, Property{Name: "dfs.namenode.name.dir", Value: cluster.Spec.HDFS.NameNode.NameDir})

	mapredSite := HadoopConfiguration{}
	err = xml.Unmarshal([]byte(configMap.Data[mapredSiteXmlKey]), &mapredSite)
	assert.NoError(t, err)
	assert.Greater(t, len(mapredSite.Properties), 0)

	yarnSite := HadoopConfiguration{}
	err = xml.Unmarshal([]byte(configMap.Data[yarnSiteXmlKey]), &yarnSite)
	assert.NoError(t, err)
	assert.Greater(t, len(yarnSite.Properties), 0)
	assert.Contains(t, yarnSite.Properties, Property{Name: "yarn.nodemanager.resource.cpu-vcores", Value: "1"})
}

func TestSortProperties(t *testing.T) {
	templateGetters := []XMLTemplateGetter{
		&coreSiteXMLTemplateGetter{},
		&hdfsSiteXMLTemplateGetter{},
		&mapredSiteXMLTemplateGetter{},
		&yarnSiteXMLTemplateGetter{},
	}
	for _, getter := range templateGetters {
		getter.Default()
		template := getter.GetXMLTemplate(testutil.NewHadoopCluster())
		sort.Sort(template)

		var templateStr []string
		for i := range template {
			templateStr = append(templateStr, fmt.Sprintf("{\"%s\",\"%s\"}", template[i].Name, template[i].Value))
		}

		fmt.Println(strings.Join(templateStr, ","))
	}
}
