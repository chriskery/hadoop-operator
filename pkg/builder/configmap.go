package builder

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/control"
	"github.com/chriskery/hadoop-operator/pkg/util"
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
`

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

HDFS_PATH="hdfs"
if command -v "hdfs" &> /dev/null
then
    echo "Command hdfs exists."
else
	HDFS_PATH=$HADOOP_HOME/bin/hdfs
    echo "Command 'hdfs'' not found. Setting default value: $HDFS_PATH'"
fi

YARN_PATH="yarn"
if command -v "yarn" &> /dev/null
then
    echo "Command yarn exists."
else
	YARN_PATH=$HADOOP_HOME/bin/yarn
    echo "Command 'yarn'' not found. Setting default value: $YARN_PATH'"
fi

case "$HADOOP_ROLE" in
    resourcemanager)
        echo "Environment variable is set to resourcemanager"
        $YARN_PATH --config $HADOOP_CONF_DIR resourcemanager 
        ;;
    nodemanager)
        echo "Environment variable is set to nodemanager"
        $YARN_PATH --config $HADOOP_CONF_DIR nodemanager 
        ;;
    namenode)
        echo "Environment variable is set to namenode"
        if [ "$NAME_NODE_FORMAT" = "true" ]; then
           $HDFS_PATH namenode -format
        fi
        $HDFS_PATH --daemon start datanode
        $HDFS_PATH namenode
        ;;
    datanode)
        echo "Environment variable is set to datanode"
        $HDFS_PATH datanode
        ;;
    *)
        echo "Environment variable is set to an unknown value: $HADOOP_ROLE"
        exit 0
        ;;
esac
`

	coreSiteXmlKey   = "core-site.xml"
	hdfsSiteXmlKey   = "hdfs-site.xml"
	mapredSiteXmlKey = "mapred-site.xml"
	yarnSiteXmlKey   = "yarn-site.xml"

	entrypointKey = "entrypoint"

	hdfsProtocol               = "hdfs://"
	defaultNameNodeRPCPort     = 9000
	defaultNameNodeHTTPPort    = 9870
	defaultResourceManagerPort = 10020
	defaultDfsReplication      = 2
)

type HadoopConfiguration struct {
	XMLName    xml.Name   `xml:"configuration"`
	Properties []Property `xml:"property"`
}

type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type XMLTemplateGetter interface {
	GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) []Property
	GetKey() string
	Default()
}

var _ XMLTemplateGetter = &coreSiteXMLTemplateGetter{}

type coreSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (c *coreSiteXMLTemplateGetter) Default() {
	nameNodeRpcAddr := fmt.Sprintf("%s%s:%d", hdfsProtocol, NameNodeURIKey, defaultNameNodeRPCPort)
	c.defaultProperties = []Property{
		{"fs.defaultFS", nameNodeRpcAddr},
		{"fs.du.interval", "600000"},
		{"fs.df.interval", "60000"},
		{"fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs"},
		{"fs.trash.interval", "1440"},
		{"fs.permissions.umask-mode", "026"},

		{"hadoop.http.staticuser.user", "root"},
		{"hadoop.proxyuser.kyuubi.hosts", "*"},
		{"hadoop.proxyuser.kyuubi.groups", "*"},
		{"hadoop.proxyuser.presto.hosts", "*"},
		{"hadoop.proxyuser.spark.groups", "*"},
		{"hadoop.proxyuser.hdfs.hosts", "*"},
		{"hadoop.proxyuser.hive.groups", "*"},
		{"hadoop.proxyuser.hive.hosts", "*"},
		{"hadoop.proxyuser.hadoop.groups", "*"},
		{"hadoop.proxyuser.impala.hosts", "*"},
		{"hadoop.proxyuser.hdfs.groups", "*"},
		{"hadoop.proxyuser.spark.host", "*"},
		{"hadoop.proxyuser.hadoop.hosts", "*"},

		{"ipc.client.idlethreshold", "4000"},
		{"ipc.client.connect.max.retries.on.timeouts", "45"},
		{"ipc.client.connection.maxidletime", "10000"},
		{"ipc.client.connect.timeout", "20000"},
		{"ipc.client.kill.max", "10"},
		{"ipc.client.connect.max.retrie", "10"},
		{"ipc.client.connect.retry.interval", "1000"},
		{"ipc.server.log.slow.rpc", "true"},

		{"file.replication", "1"},
		{"io.bytes.per.checksum", "512"},
		{"io.seqfile.compress.blocksize", "1000000"},
		{"io.file.buffer.size", "4096"},
		{"io.mapfile.bloom.size", "1048576"},

		{"dfs.namenode.delegation.token.max-lifetime", "25920000000"},
		{"hadoop.http.authentication.type", "simple"},
		{"hadoop.caller.context.enabled", "false"},
	}
}

func (c *coreSiteXMLTemplateGetter) GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) []Property {
	return c.defaultProperties
}

func (c *coreSiteXMLTemplateGetter) GetKey() string {
	return coreSiteXmlKey
}

var _ XMLTemplateGetter = &hdfsSiteXMLTemplateGetter{}

type hdfsSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (h *hdfsSiteXMLTemplateGetter) Default() {
	nameNodeRpcAddr := fmt.Sprintf("%s:%d", NameNodeURIKey, defaultNameNodeRPCPort)
	nameNodeHttpAddr := fmt.Sprintf("%s:%d", NameNodeURIKey, defaultNameNodeHTTPPort)
	h.defaultProperties = []Property{
		{"dfs.replication", strconv.Itoa(defaultDfsReplication)},
		{"dfs.namenode.rpc-address", nameNodeRpcAddr},
		{"dfs.namenode.http-address", nameNodeHttpAddr},
		{"dfs.webhdfs.enabled", "true"},

		{"dfs.datanode.cache.revocation.timeout.ms", "900000"},
		{"dfs.namenode.resource.check.interval", "5000"},
		{"dfs.namenode.write-lock-reporting-threshold-ms", "5000"},
		{"dfs.namenode.avoid.read.stale.datanode", "false"},
		{"dfs.qjournal.select-input-streams.timeout.ms", "90000"},
		{"nfs.kerberos.principal", ""},
		{"dfs.namenode.audit.log.debug.cmdlist", ""},
		{"dfs.namenode.lease-recheck-interval-ms", "2000"},
		{"dfs.client.block.write.locateFollowingBlock.initial.delay.ms", "400"},
		{"dfs.mover.max-no-move-interval", "60000"},
		{"dfs.namenode.replication.min", "1"},
		{"dfs.datanode.directoryscan.threads", "1"},
		{"dfs.datanode.directoryscan.interval", "21600s"},
		{"dfs.namenode.acls.enabled", "false"},
		{"dfs.journalnode.keytab.file", ""},
		{"dfs.secondary.namenode.keytab.file", ""},
		{"dfs.namenode.resource.du.reserved", "8589934592"},
		{"dfs.namenode.datanode.registration.ip-hostname-check", "false"},
		{"dfs.namenode.path.based.cache.block.map.allocation.percent", "0.25"},
		{"dfs.client.server-defaults.validity.period.ms", "3600000"},
		{"dfs.journalnode.kerberos.principal", ""},
		{"dfs.webhdfs.use.ipc.callq", "true"},
		{"dfs.namenode.edits.noeditlogchannelflush", "false"},
		{"dfs.web.authentication.kerberos.keytab", ""},
		{"dfs.datanode.cache.revocation.polling.ms", "500"},
		{"dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction", "0.75f"},
		{"dfs.namenode.audit.loggers", "default"},
		{"dfs.http.policy", "HTTP_ONLY"},
		{"dfs.namenode.safemode.min.datanodes", "0"},
		{"dfs.datanode.kerberos.principal", ""},
		{"dfs.namenode.metrics.logger.period.seconds", "600"},
		{"dfs.balancer.max-iteration-time", "1200000"},
		{"dfs.webhdfs.socket.read-timeout", "60s"},
		{"dfs.client.use.datanode.hostname", "false"},
		{"dfs.namenode.delegation.token.max-lifetime", "604800000"},
		{"dfs.datanode.drop.cache.behind.writes", "false"},
		{"dfs.namenode.avoid.write.stale.datanode", "false"},
		{"dfs.namenode.kerberos.principal", ""},
		{"dfs.namenode.num.extra.edits.retained", "1000000"},
		{"dfs.namenode.edekcacheloader.initial.delay.ms", "3000"},
		{"nfs.rtmax", "1048576"},
		{"dfs.client.mmap.cache.size", "256"},
		{"dfs.datanode.data.dir.perm", "750"},
		{"dfs.namenode.max-lock-hold-to-release-lease-ms", "25"},
		{"nfs.keytab.file", ""},
		{"dfs.datanode.readahead.bytes", "4194304"},
		{"dfs.namenode.xattrs.enabled", "true"},
		{"dfs.client-write-packet-size", "65536"},
		{"dfs.datanode.bp-ready.timeout", "20s"},
		{"dfs.datanode.transfer.socket.send.buffer.size", "0"},
		{"dfs.namenode.checkpoint.txns", "10000000"},
		{"dfs.client.block.write.retries", "3"},
		{"dfs.namenode.list.openfiles.num.responses", "1000"},
		{"httpfs.buffer.size", "4096"},
		{"dfs.namenode.safemode.threshold-pct", "0.999f"},
		{"dfs.cachereport.intervalMsec", "10000"},
		{"dfs.namenode.servicerpc-bind-host", "0.0.0.0"},
		{"dfs.namenode.list.cache.directives.num.responses", "100"},
		{"dfs.namenode.kerberos.principal.pattern", "*"},
		{"dfs.namenode.replication.max-streams", "100"},
		{"dfs.webhdfs.socket.connect-timeout", "60s"},
		{"dfs.namenode.keytab.file", ""},
		{"nfs.allow.insecure.ports", "true"},
		{"dfs.client.write.exclude.nodes.cache.expiry.interval.millis", "600000"},
		{"dfs.client.mmap.cache.timeout.ms", "3600000"},
		{"dfs.nameservices", "hdfs-cluster"},
		{"dfs.http.address", "0.0.0.0:9870"},
		{"dfs.http.client.retry.policy.enabled", "false"},
		{"dfs.client.failover.connection.retries.on.timeouts", "0"},
		{"dfs.namenode.replication.work.multiplier.per.iteration", "100"},
		{"dfs.datanode.slow.io.warning.threshold.ms", "300"},
		{"dfs.balancer.keytab.file", ""},
		{"dfs.namenode.reject-unresolved-dn-topology-mapping", "false"},
		{"dfs.internal.nameservices", "hdfs-cluster"},
		{"dfs.datanode.drop.cache.behind.reads", "false"},
		{"dfs.image.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec"},
		{"dfs.xframe.value", "SAMEORIGIN"},
		{"dfs.namenode.replication.max-streams-hard-limit", "100"},
		{"dfs.namenode.top.num.users", "10"},
		{"dfs.namenode.resource.checked.volumes", ""},
		{"dfs.datanode.keytab.file", ""},
		{"dfs.namenode.accesstime.precision", "3600000"},
		{"mapreduce.job.acl-view-job", "*"},
		{"dfs.namenode.fs-limits.max-xattrs-per-inode", "32"},
		{"dfs.image.transfer.timeout", "60000"},
		{"dfs.namenode.lifeline.rpc-bind-host", "0.0.0.0"},
		{"nfs.wtmax", "1048576"},
		{"dfs.nameservice.id", "hdfs-cluster"},
		{"dfs.namenode.edit.log.autoroll.check.interval.ms", "300000"},
		{"dfs.namenode.support.allow.format", "true"},
		{"hadoop.hdfs.configuration.version", "1"},
		{"dfs.secondary.namenode.kerberos.internal.spnego.principal", ""},
		{"dfs.stream-buffer-size", "4096"},
		{"dfs.namenode.invalidate.work.pct.per.iteration", "0.32f"},
		{"dfs.qjournal.start-segment.timeout.ms", "90000"},
		{"dfs.client.cache.drop.behind.reads", ""},
		{"dfs.namenode.top.enabled", "true"},
		{"dfs.bytes-per-checksum", "512"},
		{"dfs.namenode.max.objects", "0"},
		{"dfs.cluster.administrators", "*"},
		{"dfs.datanode.max.transfer.threads", "8192"},
		{"dfs.encrypt.data.transfer.algorithm", ""},
		{"dfs.datanode.directoryscan.throttle.limit.ms.per.sec", "1000"},
		{"dfs.image.transfer.chunksize", "65536"},
		{"dfs.namenode.list.encryption.zones.num.responses", "100"},
		{"dfs.namenode.inode.attributes.provider.class", ""},
		{"dfs.client.failover.sleep.base.millis", "500"},
		{"dfs.namenode.decommission.interval", "30s"},
		{"dfs.namenode.path.based.cache.refresh.interval.ms", "30000"},
		{"dfs.permissions.superusergroup", "hadoop"},
		{"dfs.client.socket.send.buffer.size", "0"},
		{"dfs.blockreport.initialDelay", "0s"},
		{"dfs.namenode.inotify.max.events.per.rpc", "1000"},
		{"dfs.namenode.safemode.extension", "30000"},
		{"dfs.client.cache.readahead", ""},
		{"dfs.client.failover.sleep.max.millis", "15000"},
		{"dfs.http.client.failover.sleep.base.millis", "500"},
		{"dfs.xframe.enabled", "true"},
		{"dfs.namenode.delegation.key.update-interval", "86400000"},
		{"dfs.datanode.transfer.socket.recv.buffer.size", "0"},
		{"dfs.namenode.redundancy.interval.seconds", "3s"},
		{"fs.permissions.umask-mode", "026"},
		{"dfs.namenode.fs-limits.max-xattr-size", "16384"},
		{"dfs.trustedchannel.resolver.class", ""},
		{"dfs.http.client.failover.sleep.max.millis", "15000"},
		{"dfs.namenode.blocks.per.postponedblocks.rescan", "10000"},
		{"dfs.data.transfer.saslproperties.resolver.class", ""},
		{"dfs.lock.suppress.warning.interval", "10s"},
		{"dfs.webhdfs.ugi.expire.after.access", "600000"},
		{"dfs.namenode.hosts.provider.classname", "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager"},
		{"dfs.balancer.keytab.enabled", "false"},
		{"dfs.client.block.write.replace-datanode-on-failure.enable", "true"},
		{"dfs.namenode.lifeline.handler.ratio", "0.10"},
		{"dfs.client.use.legacy.blockreader.local", "false"},
		{"dfs.namenode.top.windows.minutes", "1,5,25"},
		{"dfs.webhdfs.rest-csrf.browser-useragents-regex", "^Mozilla.*,^Opera.*"},
		{"nfs.mountd.port", "4242"},
		{"dfs.storage.policy.enabled", "true"},
		{"dfs.namenode.list.cache.pools.num.responses", "100"},
		{"nfs.server.port", "2049"},
		{"dfs.client.cache.drop.behind.writes", ""},
		{"dfs.secondary.namenode.kerberos.principal", ""},
		{"dfs.datanode.balance.max.concurrent.moves", "20"},
		{"dfs.namenode.legacy-oiv-image.dir", ""},
		{"dfs.datanode.block-pinning.enabled", "false"},
		{"dfs.datanode.imbalance.threshold", "10"},
		{"dfs.encrypt.data.transfer.cipher.suites", ""},
		{"dfs.namenode.num.checkpoints.retained", "2"},
		{"dfs.encrypt.data.transfer.cipher.key.bitlength", "128"},
		{"dfs.client.mmap.retry.timeout.ms", "300000"},
		{"dfs.datanode.sync.behind.writes", "false"},
		{"dfs.namenode.fslock.fair", "true"},
		{"dfs.permissions.enabled", "false"},
		{"dfs.blockreport.split.threshold", "1000000"},
		{"dfs.block.scanner.volume.bytes.per.second", "1048576"},
		{"dfs.datanode.balance.bandwidthPerSec", "104857600"},
		{"dfs.namenode.balancer.request.standby", "true"},
		{"dfs.namenode.stale.datanode.interval", "30000"},
		{"hadoop.user.group.metrics.percentiles.intervals", ""},
		{"dfs.namenode.decommission.blocks.per.interval", "500000"},
		{"dfs.namenode.handler.count", "50"},
		{"dfs.image.transfer.bandwidthPerSec", "0"},
		{"dfs.qjournal.write-txns.timeout.ms", "65000"},
		{"dfs.replication.max", "512"},
		{"dfs.namenode.read-lock-reporting-threshold-ms", "5000"},
		{"dfs.datanode.failed.volumes.tolerated", "0"},
		{"dfs.client.block.write.replace-datanode-on-failure.min-replication", "0"},
		{"dfs.client.domain.socket.data.traffic", "false"},
		{"dfs.block.access.token.enable", "true"},
		{"dfs.webhdfs.rest-csrf.methods-to-ignore", "GET,OPTIONS,HEAD,TRACE"},
		{"dfs.namenode.lifeline.handler.count", ""},
		{"dfs.encrypt.data.transfer", "false"},
		{"dfs.namenode.write.stale.datanode.ratio", "0.5f"},
		{"dfs.datanode.available-space-volume-choosing-policy.balanced-space-threshold", "10737418240"},
		{"dfs.data.transfer.protection", "integrity"},
		{"dfs.metrics.percentiles.intervals", ""},
		{"dfs.webhdfs.rest-csrf.custom-header", "X-XSRF-HEADER"},
		{"dfs.datanode.handler.count", "30"},
		{"dfs.client.failover.max.attempts", "15"},
		{"dfs.balancer.max-no-move-interval", "60000"},
		{"dfs.journalnode.kerberos.internal.spnego.principal", ""},
		{"dfs.block.local-path-access.user", ""},
		{"dfs.hosts", ""},
		{"dfs.namenode.block-placement-policy.default.prefer-local-node", "true"},
		{"dfs.namenode.resource.checked.volumes.minimum", "1"},
		{"dfs.http.client.retry.max.attempts", "10"},
		{"dfs.namenode.max.full.block.report.leases", "6"},
		{"dfs.namenode.quota.init-threads", "4"},
		{"dfs.namenode.max.extra.edits.segments.retained", "10000"},
		{"dfs.webhdfs.user.provider.user.pattern", "^[A-Za-z_][A-Za-z0-9._-]*[$]?$"},
		{"dfs.datanode.metrics.logger.period.seconds", "600"},
		{"dfs.client.mmap.enabled", "true"},
		{"dfs.datanode.block.id.layout.upgrade.threads", "12"},
		{"dfs.datanode.use.datanode.hostname", "false"},
		{"dfs.client.context", "default"},
		{"hadoop.fuse.timer.period", "5"},
		{"dfs.balancer.address", "0.0.0.0:0"},
		{"dfs.namenode.lock.detailed-metrics.enabled", "false"},
		{"dfs.namenode.delegation.token.renew-interval", "86400000"},
		{"dfs.namenode.retrycache.heap.percent", "0.03f"},
		{"dfs.web.authentication.kerberos.principal", ""},
		{"dfs.reformat.disabled", "false"},
		{"dfs.blockreport.intervalMsec", "21600000"},
		{"dfs.image.transfer-bootstrap-standby.bandwidthPerSec", "0"},
		{"dfs.balancer.kerberos.principal", ""},
		{"dfs.balancer.block-move.timeout", "600000"},
		{"dfs.namenode.kerberos.internal.spnego.principal", ""},
		{"dfs.namenode.rpc-bind-host", "0.0.0.0"},
		{"dfs.image.compress", "false"},
		{"dfs.namenode.edekcacheloader.interval.ms", "1000"},
		{"dfs.client.failover.connection.retries", "0"},
		{"dfs.namenode.plugins", ""},
		{"dfs.namenode.edit.log.autoroll.multiplier.threshold", "2.0"},
		{"dfs.namenode.top.window.num.buckets", "10"},
		{"dfs.http.client.failover.max.attempts", "15"},
		{"dfs.namenode.checkpoint.check.period", "60s"},
		{"dfs.client.slow.io.warning.threshold.ms", "30000"},
		{"dfs.namenode.http-bind-host", "0.0.0.0"},
		{"dfs.datanode.max.locked.memory", "0"},
		{"dfs.namenode.retrycache.expirytime.millis", "600000"},
		{"dfs.client.block.write.locateFollowingBlock.retries", "8"},
		{"dfs.client.block.write.replace-datanode-on-failure.best-effort", "false"},
		{"dfs.datanode.scan.period.hours", "504"},
		{"dfs.namenode.service.handler.count", "30"},
		{"dfs.webhdfs.rest-csrf.enabled", "false"},
		{"dfs.namenode.enable.retrycache", "true"},
		{"dfs.namenode.edits.asynclogging", "true"},
		{"dfs.datanode.du.reserved", "1073741824"},
		{"dfs.datanode.plugins", ""},
		{"dfs.client.block.write.replace-datanode-on-failure.policy", "DEFAULT"},
		{"dfs.namenode.path.based.cache.retry.interval.ms", "30000"},
		{"dfs.client.local.interfaces", ""},
		{"dfs.namenode.https-bind-host", "0.0.0.0"},
		{"dfs.namenode.startup.delay.block.deletion.sec", "0"},
		{"dfs.namenode.safemode.replication.min", ""},
		{"dfs.namenode.checkpoint.max-retries", "3"},
		{"dfs.datanode.fsdatasetcache.max.threads.per.volume", "4"},
		{"dfs.namenode.decommission.max.concurrent.tracked.nodes", "100"},
		{"dfs.namenode.name.dir.restore", "true"},
		{"dfs.namenode.full.block.report.lease.length.ms", "300000"},
		{"dfs.heartbeat.interval", "3s"},
		{"dfs.namenode.secondary.http-address", "0.0.0.0:50090"},
		{"dfs.datanode.lifeline.interval.seconds", ""},
		{"dfs.support.append", "true"},
		{"dfs.http.client.retry.policy.spec", "10000,6,60000,10"},
		{"dfs.namenode.checkpoint.period", "3600s"},
		{"hadoop.fuse.connection.timeout", "300"},
	}
}

func (h *hdfsSiteXMLTemplateGetter) GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) []Property {
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
		hdfsSiteProperties = append(hdfsSiteProperties, Property{"dfs.datanode.data.dir", cluster.Spec.HDFS.DataNode.DataDir})
	}
	return hdfsSiteProperties
}

func (h *hdfsSiteXMLTemplateGetter) GetKey() string {
	return hdfsSiteXmlKey
}

var _ XMLTemplateGetter = &mapredSiteXMLTemplateGetter{}

type mapredSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (m *mapredSiteXMLTemplateGetter) Default() {
	resourceManagerRPCAddr := fmt.Sprintf("%s:%d", ResourceManagerHostnameKey, defaultResourceManagerPort)
	m.defaultProperties = []Property{
		{"mapreduce.framework.name", "yarn"},
		{"mapreduce.jobhistory.address", resourceManagerRPCAddr},

		{"mapreduce.map.speculative", "true"},
		{"mapreduce.jobhistory.keytab", ""},
		{"mapreduce.jobhistory.recovery.store.class", "org.apache.hadoop.mapreduce.v2.hs.HistoryServerFileSystemStateStoreService"},
		{"mapreduce.shuffle.ssl.enabled", "false"},
		{"mapreduce.client.submit.file.replication", "2"},
		{"mapreduce.job.counters.max", "1000"},
		{"mapreduce.reduce.log.level", "INFO"},
		{"mapreduce.shuffle.port", "13562"},
		{"mapreduce.shuffle.transfer.buffer.size", "131072"},
		{"mapreduce.jobhistory.admin.acl", "*"},
		{"mapreduce.shuffle.transferTo.allowed", "true"},
		{"mapreduce.jobhistory.principal", ""},
		{"mapreduce.map.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}"},
		{"mapreduce.jobhistory.recovery.enable", "false"},
		{"mapreduce.map.log.level", "INFO"},
		{"mapreduce.shuffle.max.connections", "0"},
		{"mapreduce.job.reduces", "7"},
		{"mapreduce.cluster.acls.enabled", "false"},
		{"mapreduce.job.acl-modify-job", " "},
		{"mapreduce.output.fileoutputformat.compress", "false"},
		{"mapreduce.job.reduce.slowstart.completedmaps", "0.05"},
		{"mapreduce.jobhistory.http.policy", "HTTP_ONLY"},
		{"mapreduce.job.queuename", "default"},
		{"mapreduce.application.classpath", "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*,/usr/lib/hadoop-lzo/lib/*"},
		{"mapreduce.job.jvm.numtasks", "20"},
		{"mapreduce.reduce.java.opts", "-XX:ParallelGCThreads=2 -XX:CICompilerCount=2 -javaagent:/opt/apps/TAIHAODOCTOR/taihaodoctor-current/emr-agent/btrace-agent.jar=libs=mr"},
		{"mapreduce.map.output.compress", "true"},
		{"mapreduce.job.userlog.retain.hours", "48"},
		{"mapreduce.job.reducer.preempt.delay.sec", "0"},
		{"mapreduce.job.running.map.limit", "0"},
		{"mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec"},
		{"mapreduce.job.acl-view-job", "*"},
		{"map.sort.class", "org.apache.hadoop.util.QuickSort"},
		{"mapreduce.job.classloader", "false"},
		{"yarn.app.mapreduce.am.job.task.listener.thread-count", "60"},
		{"mapreduce.reduce.speculative", "true"},
		{"yarn.app.mapreduce.am.resource.cpu-vcores", "1"},
		{"mapreduce.output.fileoutputformat.compress.type", "BLOCK"},
		{"mapreduce.task.io.sort.mb", "200"},
		{"yarn.app.mapreduce.am.admin.user.env", ""},
		{"mapreduce.job.maps", "16"},
		{"mapreduce.reduce.cpu.vcores", "1"},
		{"mapreduce.map.sort.spill.percent", "0.8"},
		{"mapreduce.map.memory.mb", "1640"},
		{"mapreduce.task.timeout", "600000"},
		{"mapreduce.am.max-attempts", "2"},
		{"mapreduce.job.log4j-properties-file", ""},
		{"mapreduce.reduce.memory.mb", "3280"},
		{"mapreduce.map.cpu.vcores", "1"},
		{"mapreduce.reduce.shuffle.parallelcopies", "20"},
		{"yarn.app.mapreduce.am.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}"},
		{"mapreduce.job.running.reduce.limit", "0"},
		{"mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec"},
		{"mapreduce.shuffle.max.threads", "0"},
		{"mapreduce.shuffle.manage.os.cache", "false"},
		{"mapreduce.task.io.sort.factor", "48"},
		{"mapreduce.reduce.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}"},
		{"mapreduce.jobhistory.store.class", ""},
		{"yarn.app.mapreduce.client.job.max-retries", "0"},
		{"mapreduce.job.tags", ""},
	}
}

func (m *mapredSiteXMLTemplateGetter) GetXMLTemplate(_ *hadoopclusterorgv1alpha1.HadoopCluster) []Property {
	return m.defaultProperties
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
		{"yarn.resourcemanager.hostname", ResourceManagerHostnameKey},
		{"yarn.nodemanager.aux-services", "mapreduce_shuffle"},

		{"yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user", "hadoop"},
		{"yarn.scheduler.fair.dynamic.max.assign", "true"},
		{"yarn.nodemanager.node-labels.provider.configured-node-partition", ""},
		{"yarn.nodemanager.container-monitor.interval-ms", "3000"},
		{"yarn.nodemanager.linux-container-executor.group", "hadoop"},
		{"yarn.nodemanager.recovery.supervised", "false"},
		{"yarn.client.failover-proxy-provider", "org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider"},
		{"yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/share/hadoop/common/*,$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,$HADOOP_YARN_HOME/share/hadoop/yarn/*,$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*,$HADOOP_HOME/share/hadoop/tools/lib/*,/opt/apps/JINDOSDK/jindosdk-current/lib/*"},
		{"yarn.admin.acl", "*"},
		{"yarn.scheduler.fair.update-interval-ms", "500"},
		{"yarn.resourcemanager.node-removal-untracked.timeout-ms", "1800000"},
		{"yarn.nodemanager.node-labels.provider", "config"},
		{"yarn.nodemanager.address", "${yarn.nodemanager.hostname}:8041"},
		{"yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs", "3600"},
		{"yarn.client.application-client-protocol.poll-interval-ms", "200"},
		{"yarn.scheduler.maximum-allocation-vcores", "32"},
		{"yarn.nodemanager.sleep-delay-before-sigkill.ms", "250"},
		{"yarn.scheduler.fair.preemption.cluster-utilization-threshold", "0.8"},
		{"yarn.nodemanager.process-kill-wait.ms", "2000"},
		{"yarn.timeline-service.enabled", "true"},
		{"yarn.timeline-service.http-cross-origin.enabled", "true"},
		{"yarn.nodemanager.resource.cpu-vcores", "8"},
		{"yarn.nodemanager.container-manager.thread-count", "20"},
		{"yarn.resourcemanager.decommissioning-nodes-watcher.poll-interval-secs", "20"},
		{"yarn.nodemanager.aux-services.spark_shuffle.class", "org.apache.spark.network.yarn.YarnShuffleService"},
		{"yarn.nodemanager.localizer.client.thread-count", "20"},
		{"yarn.web-proxy.keytab", ""},
		{"yarn.resourcemanager.am-scheduling.node-blacklisting-policy", "PORT_RULE"},
		{"yarn.timeline-service.http-authentication.kerberos.principal", ""},
		{"yarn.nodemanager.keytab", ""},
		{"yarn.nodemanager.delete.debug-delay-sec", "60"},
		{"yarn.timeline-service.http-authentication.type", "simple"},
		{"yarn.client.failover-sleep-base-ms", "1"},
		{"yarn.dispatcher.drain-events.timeout", "300000"},
		{"yarn.log-aggregation.retain-seconds", "604800"},
		{"yarn.nodemanager.resource.memory-mb", "9216"},
		{"yarn.nodemanager.disk-health-checker.min-healthy-disks", "0.25"},
		{"yarn.node-labels.enabled", "true"},
		{"yarn.timeline-service.leveldb-timeline-store.max-open-files", "500"},
		{"yarn.resourcemanager.connect.max-wait.ms", "900000"},
		{"yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler"},
		{"yarn.nodemanager.container-metrics.enable", "false"},
		{"yarn.timeline-service.store-class", "org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore"},
		{"yarn.timeline-service.bind-host", "0.0.0.0"},
		{"yarn.resourcemanager.container.liveness-monitor.interval-ms", "600000"},
		{"yarn.resource-types.memory-mb.increment-allocation", "1024"},
		{"yarn.scheduler.fair.locality.threshold.rack", "-1.0"},
		{"yarn.nodemanager.localizer.fetch.thread-count", "4"},
		{"yarn.resourcemanager.recovery.enabled", "false"},
		{"yarn.node-labels.configuration-type", "distributed"},
		{"yarn.client.failover-sleep-max-ms", "1000"},
		{"yarn.timeline-service.ttl-ms", "604800000"},
		{"yarn.resourcemanager.nodemanagers.heartbeat-interval-ms", "1000"},
		{"yarn.fail-fast", "false"},
		{"yarn.resourcemanager.resource-tracker.client.thread-count", "64"},
		{"yarn.scheduler.fair.sizebasedweight", "false"},
		{"yarn.resourcemanager.bind-host", "0.0.0.0"},
		{"yarn.resourcemanager.keytab", ""},
		{"yarn.resourcemanager.nodemanager-connect-retries", "10"},
		{"yarn.nodemanager.delete.thread-count", "4"},
		{"yarn.resource-types.vcores.increment-allocation", "1"},
		{"yarn.nodemanager.principal", ""},
		{"yarn.resourcemanager.proxy-user-privileges.enabled", "false"},
		{"yarn.acl.enable", "false"},
		{"yarn.scheduler.fair.allow-undeclared-pools", "true"},
		{"yarn.nodemanager.resourcemanager.connect.max-wait.ms", "-1"},
		{"hadoop.zk.timeout-ms", "60000"},
		{"hadoop.http.authentication.simple.anonymous.allowed", "true"},
		{"yarn.scheduler.fair.preemption", "false"},
		{"yarn.system-metrics-publisher.enabled", "true"},
		{"yarn.nm.liveness-monitor.expiry-interval-ms", "600000"},
		{"yarn.am.liveness-monitor.expiry-interval-ms", "600000"},
		{"yarn.dispatcher.exit-on-error", "true"},
		{"yarn.timeline-service.leveldb-timeline-store.write-buffer-size", "4194304"},
		{"yarn.nodemanager.linux-container-executor.resources-handler.class", "org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler"},
		{"yarn.timeline-service.leveldb-timeline-store.read-cache-size", "4194304"},
		{"yarn.resourcemanager.enable-node-untracked-without-include-path", "true"},
		{"yarn.resourcemanager.client.thread-count", "50"},
		{"yarn.nodemanager.recovery.enabled", "false"},
		{"yarn.scheduler.fair.locality.threshold.node", "-1.0"},
		{"yarn.resourcemanager.am.max-attempts", "2"},
		{"yarn.resourcemanager.max-completed-applications", "10000"},
		{"yarn.nodemanager.webapp.address", "${yarn.nodemanager.hostname}:8042"},
		{"yarn.resourcemanager.am-scheduling.node-blacklisting-enabled", "false"},
		{"yarn.nodemanager.aux-services.mapreduce_shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler"},
		{"yarn.log-aggregation-enable", "true"},
		{"yarn.timeline-service.principal", ""},
		{"yarn.resourcemanager.store.class", "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore"},
		{"yarn.timeline-service.rolling-period", "half_daily"},
		{"yarn.scheduler.fair.max.assign", "-1"},
		{"yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", "90.0"},
		{"yarn.nodemanager.container-executor.class", "org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor"},
		{"yarn.resourcemanager.scheduler.client.thread-count", "50"},
		{"yarn.nodemanager.resource.memory.enforced", "false"},
		{"yarn.nodemanager.bind-host", "0.0.0.0"},
		{"yarn.scheduler.fair.user-as-default-queue", "false"},
		{"yarn.resourcemanager.connect.retry-interval.ms", "30000"},
		{"yarn.resourcemanager.principal", ""},
		{"yarn.scheduler.minimum-allocation-mb", "32"},
		{"yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb", "0"},
		{"yarn.scheduler.fair.assignmultiple", "false"},
		{"yarn.timeline-service.keytab", ""},
		{"yarn.scheduler.maximum-allocation-mb", "9216"},
		{"yarn.nodemanager.vmem-check-enabled", "false"},
		{"yarn.nodemanager.resource.memory.enabled", "false"},
		{"yarn.nodemanager.vmem-pmem-ratio", "2.1"},
		{"yarn.authorization-provider", ""},
		{"yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users", "true"},
		{"yarn.web-proxy.principal", ""},
		{"yarn.resourcemanager.amlauncher.thread-count", "50"},
	}
}

// SI Sizes.
const (
	IByte = 1
	KByte = IByte * 1000
	MByte = KByte * 1000
)

func (y *yarnSiteXMLTemplateGetter) GetXMLTemplate(cluster *hadoopclusterorgv1alpha1.HadoopCluster) []Property {
	yarnProperties := y.defaultProperties
	requests := cluster.Spec.Yarn.NodeManager.Resources.Requests
	cpuQuantity := requests.Cpu()
	if cpuQuantity != nil {
		vcores, ok := cpuQuantity.AsInt64()
		if ok && vcores > 0 {
			yarnProperties = append(yarnProperties, Property{"yarn.nodemanager.resource.cpu-vcores",
				strconv.Itoa(int(vcores))})
		}
	}

	memoryQuantity := requests.Memory()
	if memoryQuantity != nil {
		memory, ok := memoryQuantity.AsInt64()
		if ok && memory > 0 {
			yarnProperties = append(yarnProperties, Property{"yarn.nodemanager.resource.memory-mb",
				strconv.Itoa(int(memory / MByte))})
		}
	}

	return yarnProperties
}

func (y *yarnSiteXMLTemplateGetter) GetKey() string {
	return yarnSiteXmlKey
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
	ResourceManagerHostname string
}

const (
	NameNodeURIKey             = "{{.NameNodeURI}}"
	ResourceManagerHostnameKey = "{{.ResourceManagerHostname}}"
)

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

func (h *ConfigMapBuilder) Build(obj interface{}, _ interface{}) error {
	cluster := obj.(*hadoopclusterorgv1alpha1.HadoopCluster)
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
	ownerRef := util.GenOwnerReference(cluster, hadoopclusterorgv1alpha1.GroupVersion.WithKind(hadoopclusterorgv1alpha1.HadoopClusterKind).Kind)
	return h.ConfigMapControl.CreateConfigMapWithControllerRef(cluster.GetNamespace(), configMap, cluster, ownerRef)
}

func (h *ConfigMapBuilder) Clean(obj interface{}) error {
	cluster := obj.(*hadoopclusterorgv1alpha1.HadoopCluster)

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
		ResourceManagerHostname: util.GetReplicaName(cluster, hadoopclusterorgv1alpha1.ReplicaTypeResourcemanager),
	}

	configMapData := map[string]string{entrypointKey: entrypointTemplate}
	for _, xmlGetter := range h.XmlGetters {
		xmlTemplate := xmlGetter.GetXMLTemplate(cluster)
		hadoopConfiguration := HadoopConfiguration{Properties: xmlTemplate}
		marshal, err := xml.MarshalIndent(hadoopConfiguration, "", "  ")
		if err != nil {
			return nil, err
		}
		templateStr := templatePrefix + string(marshal)
		configMapBytes, err := getConfigMapGenerator(templateStr).GetConfigMapBytes(hadoopConfig)
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
