package builder

import (
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/controllers/control"
	"k8s.io/client-go/kubernetes"
	"reflect"
	"testing"
)

func GenConfigMapBuilder() *ConfigMapBuilder {
	var h = &ConfigMapBuilder{}
	return h
}

func TestConfigMapBuilder_Build(t *testing.T) {
	type fields struct {
		KubeClientSet    kubernetes.Interface
		ConfigMapControl control.ConfigMapControlInterface
	}
	type args struct {
		cluster *hadoopclusterorgv1alpha1.HadoopCluster
		status  *hadoopclusterorgv1alpha1.HadoopClusterStatus
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := GenConfigMapBuilder()
			if err := h.Build(tt.args.cluster, tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_configMapGenerator_GetConfigMapBytes(t *testing.T) {
	type fields struct {
		template string
	}
	type args struct {
		hadoopConfig HadoopConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"test-gen",
			fields{template: configMapTemplate},
			args{HadoopConfig{"test-namenode", 2, "test-resource-manager"}},
			[]byte(`CORE-SITE.XML_fs.default.name=ConfigMap://test-namenode
CORE-SITE.XML_fs.defaultFS=ConfigMap://test-namenode
ConfigMap-SITE.XML_dfs.namenode.rpc-address=test-namenode:8020
ConfigMap-SITE.XML_dfs.replication=2
MAPRED-SITE.XML_mapreduce.framework.name=yarn
MAPRED-SITE.XML_yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.map.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.reduce.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
YARN-SITE.XML_yarn.resourcemanager.hostname=test-resource-manager
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
`),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &configMapGenerator{
				template: tt.fields.template,
			}
			got, err := i.GetConfigMapBytes(tt.args.hadoopConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConfigMapBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetConfigMapBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}
