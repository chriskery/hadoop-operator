package builder

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"text/template"

	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/control"
	"github.com/chriskery/hadoop-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

/*
 Hbase configuration for distribute, currently not use

 sed -i 's/zookeeper:2181/localhost:2181/' "$HBASE_HOME/conf/hbase-site.xml"
    echo "Starting Zookeeper..."
    "hbase" zookeeper &>"/tmp/zookeeper.log" &

    echo "Starting HBase Master..."
    "hbase-daemon.sh" start master

    echo "Starting HBase RegionServer..."
    # HBase versions < 1.0 fail to start RegionServer without SSH being installed
    if [ "$(echo /hbase-* | sed 's,/hbase-,,' | cut -c 1)" = 0 ]; then
        "local-regionservers.sh" start 1
    else
        "hbase-daemon.sh" start regionserver
    fi

    # kill any pre-existing rest instances before starting new ones
    pgrep -f proc_rest && pkill -9 -f proc_rest
    echo "Starting HBase Stargate Rest API server..."
    "hbase-daemon.sh" start rest

    # kill any pre-existing thrift instances before starting new ones
    pgrep -f proc_thrift && pkill -9 -f proc_thrift
    echo "Starting HBase Thrift API server..."
    "hbase-daemon.sh" start thrift
*/

const (
	templatePrefix = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
`

	entrypointTemplate = `#/bin/bash
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}"
echo "The value of HADOOP_CONF_DIR is: $HADOOP_CONF_DIR"

HADOOP_OPERATOR_DIR="${HADOOP_OPERATOR_DIR:-/etc/hadoop-operator}"

mkdir -p /tmp/hadoop-hadoop/dfs/name

HDFS_PATH="hdfs"
if ! command -v "hdfs" &> /dev/null; then
    HDFS_PATH=$HADOOP_HOME/bin/hdfs
    echo "Command 'hdfs'' not found. Setting default value: $HDFS_PATH'"
fi

YARN_PATH="yarn"
if ! command -v "yarn" &> /dev/null; then
    YARN_PATH=$HADOOP_HOME/bin/yarn
    echo "Command 'yarn'' not found. Setting default value: $YARN_PATH'"
fi

copyHadoopConfig() {
    echo "Copying configuration files..."
    cp $HADOOP_OPERATOR_DIR/hdfs-site.xml $HADOOP_CONF_DIR
    cp $HADOOP_OPERATOR_DIR/core-site.xml $HADOOP_CONF_DIR
    cp $HADOOP_OPERATOR_DIR/mapred-site.xml $HADOOP_CONF_DIR
    cp $HADOOP_OPERATOR_DIR/yarn-site.xml $HADOOP_CONF_DIR
}

HBASE_CONF_DIR="${HBASE_CONF_DIR:-/hbase/conf}"

copyHbaseConfig() {
    echo "Copying configuration files..."
    cp $HADOOP_OPERATOR_DIR/hbase-site.xml $HBASE_CONF_DIR
}

startHbase() {
    HBASE_HOME="$(dirname $(which hbase))/.."
    sed -i 's/zookeeper:2181/localhost:2181/' "$HBASE_HOME/conf/hbase-site.xml"
    echo "Starting Zookeeper..."
    "hbase" zookeeper &>"/tmp/zookeeper.log" &

    echo "Starting HBase Master..."
    "hbase-daemon.sh" start master

    echo "Starting HBase RegionServer..."
    # HBase versions < 1.0 fail to start RegionServer without SSH being installed
    if [ "$(echo /hbase-* | sed 's,/hbase-,,' | cut -c 1)" = 0 ]; then
        "local-regionservers.sh" start 1
    else
        "hbase-daemon.sh" start regionserver
    fi

    # kill any pre-existing rest instances before starting new ones
    pgrep -f proc_rest && pkill -9 -f proc_rest
    echo "Starting HBase Stargate Rest API server..."
    "hbase-daemon.sh" start rest

    # kill any pre-existing thrift instances before starting new ones
    pgrep -f proc_thrift && pkill -9 -f proc_thrift
    echo "Starting HBase Thrift API server..."
    "hbase-daemon.sh" start thrift

    while true; do tail -f $HBASE_LOG_DIR/hbase--master-$(hostname).log; done
}

startService() {
    case "$HADOOP_ROLE" in
        resourcemanager)
            echo "Environment variable is set to resourcemanager"
            copyHadoopConfig
            $YARN_PATH --config $HADOOP_CONF_DIR resourcemanager 
            ;;
        nodemanager)
            echo "Environment variable is set to nodemanager"
            copyHadoopConfig
            $YARN_PATH --config $HADOOP_CONF_DIR nodemanager 
            ;;
        namenode)
            echo "Environment variable is set to namenode"
            copyHadoopConfig
            if [ "$NAME_NODE_FORMAT" = "true" ]; then
                $HDFS_PATH namenode -format
            fi
            $HDFS_PATH --daemon start datanode
            $HDFS_PATH namenode
            ;;
        datanode)
            echo "Environment variable is set to datanode"
            copyHadoopConfig
            $HDFS_PATH datanode
            ;;
        hbase)
            echo "Environment variable is set to hbase"
            copyHbaseConfig
            startHbase
            ;;
        *)
            echo "Environment variable HADOOP_ROLE is not set to a recognized value: $HADOOP_ROLE"
            exit 0
            ;;
    esac
}

# Check if HADOOP_OPERATOR_DIR exists before proceeding with any role-specific operations
if [ -d "$HADOOP_OPERATOR_DIR" ]; then
    startService
else
    echo "Directory does not exist: $HADOOP_OPERATOR_DIR"
    exit 1
fi
`

	coreSiteXmlKey   = "core-site.xml"
	hdfsSiteXmlKey   = "hdfs-site.xml"
	mapredSiteXmlKey = "mapred-site.xml"
	yarnSiteXmlKey   = "yarn-site.xml"
	hbaseSiteXmlKey  = "hbase-site.xml"

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

// Properties Implement sort.Interface for ConfigItems.
type Properties []Property

func (c Properties) Len() int { return len(c) }

func (c Properties) Less(i, j int) bool {
	return c[i].Name < c[j].Name
}

func (c Properties) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type XMLTemplateGetter interface {
	GetXMLTemplate(cluster *v1alpha1.HadoopCluster) Properties
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
		{"dfs.namenode.delegation.token.max-lifetime", "25920000000"},
		{"file.replication", "1"},
		{"fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs"},
		{"fs.defaultFS", nameNodeRpcAddr},
		{"fs.df.interval", "60000"},
		{"fs.du.interval", "600000"},
		{"fs.permissions.umask-mode", "026"},
		{"fs.trash.interval", "1440"},
		{"hadoop.caller.context.enabled", "false"},
		{"hadoop.http.authentication.type", "simple"},
		{"hadoop.http.staticuser.user", "root"},
		{"hadoop.proxyuser.hadoop.groups", "*"},
		{"hadoop.proxyuser.hadoop.hosts", "*"},
		{"hadoop.proxyuser.hdfs.groups", "*"},
		{"hadoop.proxyuser.hdfs.hosts", "*"},
		{"hadoop.proxyuser.hive.groups", "*"},
		{"hadoop.proxyuser.hive.hosts", "*"},
		{"hadoop.proxyuser.impala.hosts", "*"},
		{"hadoop.proxyuser.kyuubi.groups", "*"},
		{"hadoop.proxyuser.kyuubi.hosts", "*"},
		{"hadoop.proxyuser.presto.hosts", "*"},
		{"hadoop.proxyuser.spark.groups", "*"},
		{"hadoop.proxyuser.spark.host", "*"},
		{"io.bytes.per.checksum", "512"},
		{"io.file.buffer.size", "4096"},
		{"io.mapfile.bloom.size", "1048576"},
		{"io.seqfile.compress.blocksize", "1000000"},
		{"ipc.client.connect.max.retrie", "10"},
		{"ipc.client.connect.max.retries.on.timeouts", "45"},
		{"ipc.client.connect.retry.interval", "1000"},
		{"ipc.client.connect.timeout", "20000"},
		{"ipc.client.connection.maxidletime", "10000"},
		{"ipc.client.idlethreshold", "4000"},
		{"ipc.client.kill.max", "10"},
		{"ipc.server.log.slow.rpc", "true"},
	}
}

func (c *coreSiteXMLTemplateGetter) GetXMLTemplate(cluster *v1alpha1.HadoopCluster) Properties {
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
		{"dfs.balancer.address", "0.0.0.0:0"},
		{"dfs.balancer.block-move.timeout", "600000"},
		{"dfs.balancer.kerberos.principal", ""},
		{"dfs.balancer.keytab.enabled", "false"},
		{"dfs.balancer.keytab.file", ""},
		{"dfs.balancer.max-iteration-time", "1200000"},
		{"dfs.balancer.max-no-move-interval", "60000"},
		{"dfs.block.access.token.enable", "true"},
		{"dfs.block.local-path-access.user", ""},
		{"dfs.block.scanner.volume.bytes.per.second", "1048576"},
		{"dfs.blockreport.initialDelay", "0s"},
		{"dfs.blockreport.intervalMsec", "21600000"},
		{"dfs.blockreport.split.threshold", "1000000"},
		{"dfs.bytes-per-checksum", "512"},
		{"dfs.cachereport.intervalMsec", "10000"},
		{"dfs.client-write-packet-size", "65536"},
		{"dfs.client.block.write.locateFollowingBlock.initial.delay.ms", "400"},
		{"dfs.client.block.write.locateFollowingBlock.retries", "8"},
		{"dfs.client.block.write.replace-datanode-on-failure.best-effort", "false"},
		{"dfs.client.block.write.replace-datanode-on-failure.enable", "true"},
		{"dfs.client.block.write.replace-datanode-on-failure.min-replication", "0"},
		{"dfs.client.block.write.replace-datanode-on-failure.policy", "DEFAULT"},
		{"dfs.client.block.write.retries", "3"},
		{"dfs.client.cache.drop.behind.reads", ""},
		{"dfs.client.cache.drop.behind.writes", ""},
		{"dfs.client.cache.readahead", ""},
		{"dfs.client.context", "default"},
		{"dfs.client.domain.socket.data.traffic", "false"},
		{"dfs.client.failover.connection.retries", "0"},
		{"dfs.client.failover.connection.retries.on.timeouts", "0"},
		{"dfs.client.failover.max.attempts", "15"},
		{"dfs.client.failover.sleep.base.millis", "500"},
		{"dfs.client.failover.sleep.max.millis", "15000"},
		{"dfs.client.local.interfaces", ""},
		{"dfs.client.mmap.cache.size", "256"},
		{"dfs.client.mmap.cache.timeout.ms", "3600000"},
		{"dfs.client.mmap.enabled", "true"},
		{"dfs.client.mmap.retry.timeout.ms", "300000"},
		{"dfs.client.server-defaults.validity.period.ms", "3600000"},
		{"dfs.client.slow.io.warning.threshold.ms", "30000"},
		{"dfs.client.socket.send.buffer.size", "0"},
		{"dfs.client.use.datanode.hostname", "false"},
		{"dfs.client.use.legacy.blockreader.local", "false"},
		{"dfs.client.write.exclude.nodes.cache.expiry.interval.millis", "600000"},
		{"dfs.cluster.administrators", "*"},
		{"dfs.data.transfer.protection", "integrity"},
		{"dfs.data.transfer.saslproperties.resolver.class", ""},
		{"dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction", "0.75f"},
		{"dfs.datanode.available-space-volume-choosing-policy.balanced-space-threshold", "10737418240"},
		{"dfs.datanode.balance.bandwidthPerSec", "104857600"},
		{"dfs.datanode.balance.max.concurrent.moves", "20"},
		{"dfs.datanode.block-pinning.enabled", "false"},
		{"dfs.datanode.block.id.layout.upgrade.threads", "12"},
		{"dfs.datanode.bp-ready.timeout", "20s"},
		{"dfs.datanode.cache.revocation.polling.ms", "500"},
		{"dfs.datanode.cache.revocation.timeout.ms", "900000"},
		{"dfs.datanode.data.dir.perm", "750"},
		{"dfs.datanode.directoryscan.interval", "21600s"},
		{"dfs.datanode.directoryscan.threads", "1"},
		{"dfs.datanode.directoryscan.throttle.limit.ms.per.sec", "1000"},
		{"dfs.datanode.drop.cache.behind.reads", "false"},
		{"dfs.datanode.drop.cache.behind.writes", "false"},
		{"dfs.datanode.du.reserved", "1073741824"},
		{"dfs.datanode.failed.volumes.tolerated", "0"},
		{"dfs.datanode.fsdatasetcache.max.threads.per.volume", "4"},
		{"dfs.datanode.handler.count", "30"},
		{"dfs.datanode.imbalance.threshold", "10"},
		{"dfs.datanode.kerberos.principal", ""},
		{"dfs.datanode.keytab.file", ""},
		{"dfs.datanode.lifeline.interval.seconds", ""},
		{"dfs.datanode.max.locked.memory", "0"},
		{"dfs.datanode.max.transfer.threads", "8192"},
		{"dfs.datanode.metrics.logger.period.seconds", "600"},
		{"dfs.datanode.plugins", ""},
		{"dfs.datanode.readahead.bytes", "4194304"},
		{"dfs.datanode.scan.period.hours", "504"},
		{"dfs.datanode.slow.io.warning.threshold.ms", "300"},
		{"dfs.datanode.sync.behind.writes", "false"},
		{"dfs.datanode.transfer.socket.recv.buffer.size", "0"},
		{"dfs.datanode.transfer.socket.send.buffer.size", "0"},
		{"dfs.datanode.use.datanode.hostname", "false"},
		{"dfs.encrypt.data.transfer", "false"},
		{"dfs.encrypt.data.transfer.algorithm", ""},
		{"dfs.encrypt.data.transfer.cipher.key.bitlength", "128"},
		{"dfs.encrypt.data.transfer.cipher.suites", ""},
		{"dfs.heartbeat.interval", "3s"},
		{"dfs.hosts", ""},
		{"dfs.http.address", "0.0.0.0:9870"},
		{"dfs.http.client.failover.max.attempts", "15"},
		{"dfs.http.client.failover.sleep.base.millis", "500"},
		{"dfs.http.client.failover.sleep.max.millis", "15000"},
		{"dfs.http.client.retry.max.attempts", "10"},
		{"dfs.http.client.retry.policy.enabled", "false"},
		{"dfs.http.client.retry.policy.spec", "10000,6,60000,10"},
		{"dfs.http.policy", "HTTP_ONLY"},
		{"dfs.image.compress", "false"},
		{"dfs.image.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec"},
		{"dfs.image.transfer-bootstrap-standby.bandwidthPerSec", "0"},
		{"dfs.image.transfer.bandwidthPerSec", "0"},
		{"dfs.image.transfer.chunksize", "65536"},
		{"dfs.image.transfer.timeout", "60000"},
		{"dfs.internal.nameservices", "hdfs-cluster"},
		{"dfs.journalnode.kerberos.internal.spnego.principal", ""},
		{"dfs.journalnode.kerberos.principal", ""},
		{"dfs.journalnode.keytab.file", ""},
		{"dfs.lock.suppress.warning.interval", "10s"},
		{"dfs.metrics.percentiles.intervals", ""},
		{"dfs.mover.max-no-move-interval", "60000"},
		{"dfs.namenode.accesstime.precision", "3600000"},
		{"dfs.namenode.acls.enabled", "false"},
		{"dfs.namenode.audit.log.debug.cmdlist", ""},
		{"dfs.namenode.audit.loggers", "default"},
		{"dfs.namenode.avoid.read.stale.datanode", "false"},
		{"dfs.namenode.avoid.write.stale.datanode", "false"},
		{"dfs.namenode.balancer.request.standby", "true"},
		{"dfs.namenode.block-placement-policy.default.prefer-local-node", "true"},
		{"dfs.namenode.blocks.per.postponedblocks.rescan", "10000"},
		{"dfs.namenode.checkpoint.check.period", "60s"},
		{"dfs.namenode.checkpoint.max-retries", "3"},
		{"dfs.namenode.checkpoint.period", "3600s"},
		{"dfs.namenode.checkpoint.txns", "10000000"},
		{"dfs.namenode.datanode.registration.ip-hostname-check", "false"},
		{"dfs.namenode.decommission.blocks.per.interval", "500000"},
		{"dfs.namenode.decommission.interval", "30s"},
		{"dfs.namenode.decommission.max.concurrent.tracked.nodes", "100"},
		{"dfs.namenode.delegation.key.update-interval", "86400000"},
		{"dfs.namenode.delegation.token.max-lifetime", "604800000"},
		{"dfs.namenode.delegation.token.renew-interval", "86400000"},
		{"dfs.namenode.edekcacheloader.initial.delay.ms", "3000"},
		{"dfs.namenode.edekcacheloader.interval.ms", "1000"},
		{"dfs.namenode.edit.log.autoroll.check.interval.ms", "300000"},
		{"dfs.namenode.edit.log.autoroll.multiplier.threshold", "2.0"},
		{"dfs.namenode.edits.asynclogging", "true"},
		{"dfs.namenode.edits.noeditlogchannelflush", "false"},
		{"dfs.namenode.enable.retrycache", "true"},
		{"dfs.namenode.fs-limits.max-xattr-size", "16384"},
		{"dfs.namenode.fs-limits.max-xattrs-per-inode", "32"},
		{"dfs.namenode.fslock.fair", "true"},
		{"dfs.namenode.full.block.report.lease.length.ms", "300000"},
		{"dfs.namenode.handler.count", "50"},
		{"dfs.namenode.hosts.provider.classname", "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager"},
		{"dfs.namenode.http-address", nameNodeHttpAddr},
		{"dfs.namenode.http-bind-host", "0.0.0.0"},
		{"dfs.namenode.https-bind-host", "0.0.0.0"},
		{"dfs.namenode.inode.attributes.provider.class", ""},
		{"dfs.namenode.inotify.max.events.per.rpc", "1000"},
		{"dfs.namenode.invalidate.work.pct.per.iteration", "0.32f"},
		{"dfs.namenode.kerberos.internal.spnego.principal", ""},
		{"dfs.namenode.kerberos.principal", ""},
		{"dfs.namenode.kerberos.principal.pattern", "*"},
		{"dfs.namenode.keytab.file", ""},
		{"dfs.namenode.lease-recheck-interval-ms", "2000"},
		{"dfs.namenode.legacy-oiv-image.dir", ""},
		{"dfs.namenode.lifeline.handler.count", ""},
		{"dfs.namenode.lifeline.handler.ratio", "0.10"},
		{"dfs.namenode.lifeline.rpc-bind-host", "0.0.0.0"},
		{"dfs.namenode.list.cache.directives.num.responses", "100"},
		{"dfs.namenode.list.cache.pools.num.responses", "100"},
		{"dfs.namenode.list.encryption.zones.num.responses", "100"},
		{"dfs.namenode.list.openfiles.num.responses", "1000"},
		{"dfs.namenode.lock.detailed-metrics.enabled", "false"},
		{"dfs.namenode.max-lock-hold-to-release-lease-ms", "25"},
		{"dfs.namenode.max.extra.edits.segments.retained", "10000"},
		{"dfs.namenode.max.full.block.report.leases", "6"},
		{"dfs.namenode.max.objects", "0"},
		{"dfs.namenode.metrics.logger.period.seconds", "600"},
		{"dfs.namenode.name.dir.restore", "true"},
		{"dfs.namenode.num.checkpoints.retained", "2"},
		{"dfs.namenode.num.extra.edits.retained", "1000000"},
		{"dfs.namenode.path.based.cache.block.map.allocation.percent", "0.25"},
		{"dfs.namenode.path.based.cache.refresh.interval.ms", "30000"},
		{"dfs.namenode.path.based.cache.retry.interval.ms", "30000"},
		{"dfs.namenode.plugins", ""},
		{"dfs.namenode.quota.init-threads", "4"},
		{"dfs.namenode.read-lock-reporting-threshold-ms", "5000"},
		{"dfs.namenode.redundancy.interval.seconds", "3s"},
		{"dfs.namenode.reject-unresolved-dn-topology-mapping", "false"},
		{"dfs.namenode.replication.max-streams", "100"},
		{"dfs.namenode.replication.max-streams-hard-limit", "100"},
		{"dfs.namenode.replication.min", "1"},
		{"dfs.namenode.replication.work.multiplier.per.iteration", "100"},
		{"dfs.namenode.resource.check.interval", "5000"},
		{"dfs.namenode.resource.checked.volumes", ""},
		{"dfs.namenode.resource.checked.volumes.minimum", "1"},
		{"dfs.namenode.resource.du.reserved", "8589934592"},
		{"dfs.namenode.retrycache.expirytime.millis", "600000"},
		{"dfs.namenode.retrycache.heap.percent", "0.03f"},
		{"dfs.namenode.rpc-address", nameNodeRpcAddr},
		{"dfs.namenode.rpc-bind-host", "0.0.0.0"},
		{"dfs.namenode.safemode.extension", "30000"},
		{"dfs.namenode.safemode.min.datanodes", "0"},
		{"dfs.namenode.safemode.replication.min", ""},
		{"dfs.namenode.safemode.threshold-pct", "0.999f"},
		{"dfs.namenode.secondary.http-address", "0.0.0.0:50090"},
		{"dfs.namenode.service.handler.count", "30"},
		{"dfs.namenode.servicerpc-bind-host", "0.0.0.0"},
		{"dfs.namenode.stale.datanode.interval", "30000"},
		{"dfs.namenode.startup.delay.block.deletion.sec", "0"},
		{"dfs.namenode.support.allow.format", "true"},
		{"dfs.namenode.top.enabled", "true"},
		{"dfs.namenode.top.num.users", "10"},
		{"dfs.namenode.top.window.num.buckets", "10"},
		{"dfs.namenode.top.windows.minutes", "1,5,25"},
		{"dfs.namenode.write-lock-reporting-threshold-ms", "5000"},
		{"dfs.namenode.write.stale.datanode.ratio", "0.5f"},
		{"dfs.namenode.xattrs.enabled", "true"},
		{"dfs.nameservice.id", "hdfs-cluster"},
		{"dfs.nameservices", "hdfs-cluster"},
		{"dfs.permissions.enabled", "false"},
		{"dfs.permissions.superusergroup", "hadoop"},
		{"dfs.qjournal.select-input-streams.timeout.ms", "90000"},
		{"dfs.qjournal.start-segment.timeout.ms", "90000"},
		{"dfs.qjournal.write-txns.timeout.ms", "65000"},
		{"dfs.reformat.disabled", "false"},
		{"dfs.replication", strconv.Itoa(defaultDfsReplication)},
		{"dfs.replication.max", "512"},
		{"dfs.secondary.namenode.kerberos.internal.spnego.principal", ""},
		{"dfs.secondary.namenode.kerberos.principal", ""},
		{"dfs.secondary.namenode.keytab.file", ""},
		{"dfs.storage.policy.enabled", "true"},
		{"dfs.stream-buffer-size", "4096"},
		{"dfs.support.append", "true"},
		{"dfs.trustedchannel.resolver.class", ""},
		{"dfs.web.authentication.kerberos.keytab", ""},
		{"dfs.web.authentication.kerberos.principal", ""},
		{"dfs.webhdfs.enabled", "true"},
		{"dfs.webhdfs.rest-csrf.browser-useragents-regex", "^Mozilla.*,^Opera.*"},
		{"dfs.webhdfs.rest-csrf.custom-header", "X-XSRF-HEADER"},
		{"dfs.webhdfs.rest-csrf.enabled", "false"},
		{"dfs.webhdfs.rest-csrf.methods-to-ignore", "GET,OPTIONS,HEAD,TRACE"},
		{"dfs.webhdfs.socket.connect-timeout", "60s"},
		{"dfs.webhdfs.socket.read-timeout", "60s"},
		{"dfs.webhdfs.ugi.expire.after.access", "600000"},
		{"dfs.webhdfs.use.ipc.callq", "true"},
		{"dfs.webhdfs.user.provider.user.pattern", "^[A-Za-z_][A-Za-z0-9._-]*[$]?$"},
		{"dfs.xframe.enabled", "true"},
		{"dfs.xframe.value", "SAMEORIGIN"},
		{"fs.permissions.umask-mode", "026"},
		{"hadoop.fuse.connection.timeout", "300"},
		{"hadoop.fuse.timer.period", "5"},
		{"hadoop.hdfs.configuration.version", "1"},
		{"hadoop.user.group.metrics.percentiles.intervals", ""},
		{"httpfs.buffer.size", "4096"},
		{"mapreduce.job.acl-view-job", "*"},
		{"nfs.allow.insecure.ports", "true"},
		{"nfs.kerberos.principal", ""},
		{"nfs.keytab.file", ""},
		{"nfs.mountd.port", "4242"},
		{"nfs.rtmax", "1048576"},
		{"nfs.server.port", "2049"},
		{"nfs.wtmax", "1048576"},
	}
}

func (h *hdfsSiteXMLTemplateGetter) GetXMLTemplate(cluster *v1alpha1.HadoopCluster) Properties {
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
		{"map.sort.class", "org.apache.hadoop.util.QuickSort"},
		{"mapreduce.am.max-attempts", "2"},
		{"mapreduce.client.submit.file.replication", "2"},
		{"mapreduce.cluster.acls.enabled", "false"},
		{"mapreduce.framework.name", "yarn"},
		{"mapreduce.job.acl-modify-job", " "},
		{"mapreduce.job.acl-view-job", "*"},
		{"mapreduce.job.classloader", "false"},
		{"mapreduce.job.counters.max", "1000"},
		{"mapreduce.job.jvm.numtasks", "20"},
		{"mapreduce.job.log4j-properties-file", ""},
		{"mapreduce.job.maps", "16"},
		{"mapreduce.job.queuename", "default"},
		{"mapreduce.job.reduce.slowstart.completedmaps", "0.05"},
		{"mapreduce.job.reducer.preempt.delay.sec", "0"},
		{"mapreduce.job.reduces", "7"},
		{"mapreduce.job.running.map.limit", "0"},
		{"mapreduce.job.running.reduce.limit", "0"},
		{"mapreduce.job.tags", ""},
		{"mapreduce.job.userlog.retain.hours", "48"},
		{"mapreduce.jobhistory.address", resourceManagerRPCAddr},
		{"mapreduce.jobhistory.admin.acl", "*"},
		{"mapreduce.jobhistory.http.policy", "HTTP_ONLY"},
		{"mapreduce.jobhistory.keytab", ""},
		{"mapreduce.jobhistory.principal", ""},
		{"mapreduce.jobhistory.recovery.enable", "false"},
		{"mapreduce.jobhistory.recovery.store.class", "org.apache.hadoop.mapreduce.v2.hs.HistoryServerFileSystemStateStoreService"},
		{"mapreduce.jobhistory.store.class", ""},
		{"mapreduce.map.cpu.vcores", "1"},
		{"mapreduce.map.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}"},
		{"mapreduce.map.log.level", "INFO"},
		//{"mapreduce.map.memory.mb", "1640"},
		{"mapreduce.map.output.compress", "true"},
		{"mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec"},
		{"mapreduce.map.sort.spill.percent", "0.8"},
		{"mapreduce.map.speculative", "true"},
		{"mapreduce.output.fileoutputformat.compress", "false"},
		{"mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec"},
		{"mapreduce.output.fileoutputformat.compress.type", "BLOCK"},
		{"mapreduce.reduce.cpu.vcores", "1"},
		{"mapreduce.reduce.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}"},
		{"mapreduce.reduce.log.level", "INFO"},
		//{"mapreduce.reduce.memory.mb", "3280"},
		{"mapreduce.reduce.shuffle.parallelcopies", "20"},
		{"mapreduce.reduce.speculative", "true"},
		{"mapreduce.shuffle.manage.os.cache", "false"},
		{"mapreduce.shuffle.max.connections", "0"},
		{"mapreduce.shuffle.max.threads", "0"},
		{"mapreduce.shuffle.port", "13562"},
		{"mapreduce.shuffle.ssl.enabled", "false"},
		{"mapreduce.shuffle.transfer.buffer.size", "131072"},
		{"mapreduce.shuffle.transferTo.allowed", "true"},
		{"mapreduce.task.io.sort.factor", "48"},
		{"mapreduce.task.io.sort.mb", "200"},
		{"mapreduce.task.timeout", "600000"},
		{"yarn.app.mapreduce.am.admin.user.env", ""},
		{"yarn.app.mapreduce.am.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}"},
		{"yarn.app.mapreduce.am.job.task.listener.thread-count", "60"},
		{"yarn.app.mapreduce.am.resource.cpu-vcores", "1"},
		{"yarn.app.mapreduce.client.job.max-retries", "0"},
	}
}

func (m *mapredSiteXMLTemplateGetter) GetXMLTemplate(cluster *v1alpha1.HadoopCluster) Properties {
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
		{"hadoop.http.authentication.simple.anonymous.allowed", "true"},

		{"yarn.acl.enable", "false"},
		{"yarn.admin.acl", "*"},
		{"yarn.am.liveness-monitor.expiry-interval-ms", "600000"},
		{"yarn.authorization-provider", ""},
		{"yarn.client.application-client-protocol.poll-interval-ms", "200"},
		{"yarn.client.failover-proxy-provider", "org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider"},
		{"yarn.client.failover-sleep-base-ms", "1"},
		{"yarn.client.failover-sleep-max-ms", "1000"},
		{"yarn.dispatcher.drain-events.timeout", "300000"},
		{"yarn.dispatcher.exit-on-error", "true"},
		{"yarn.fail-fast", "false"},
		{"yarn.log-aggregation-enable", "true"},
		{"yarn.log-aggregation.retain-seconds", "604800"},
		{"yarn.nm.liveness-monitor.expiry-interval-ms", "600000"},
		{"yarn.node-labels.configuration-type", "distributed"},
		{"yarn.node-labels.enabled", "true"},
		{"yarn.nodemanager.address", "${yarn.nodemanager.hostname}:8041"},
		{"yarn.nodemanager.aux-services", "mapreduce_shuffle"},
		{"yarn.nodemanager.aux-services.mapreduce_shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler"},
		{"yarn.nodemanager.aux-services.spark_shuffle.class", "org.apache.spark.network.yarn.YarnShuffleService"},
		{"yarn.nodemanager.bind-host", "0.0.0.0"},
		{"yarn.nodemanager.container-executor.class", "org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor"},
		{"yarn.nodemanager.container-manager.thread-count", "20"},
		{"yarn.nodemanager.container-metrics.enable", "false"},
		{"yarn.nodemanager.container-monitor.interval-ms", "3000"},
		{"yarn.nodemanager.delete.debug-delay-sec", "60"},
		{"yarn.nodemanager.delete.thread-count", "4"},
		{"yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", "90.0"},
		{"yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb", "0"},
		{"yarn.nodemanager.disk-health-checker.min-healthy-disks", "0.25"},
		{"yarn.nodemanager.keytab", ""},
		{"yarn.nodemanager.linux-container-executor.group", "hadoop"},
		{"yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users", "true"},
		{"yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user", "hadoop"},
		{"yarn.nodemanager.linux-container-executor.resources-handler.class", "org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler"},
		{"yarn.nodemanager.localizer.client.thread-count", "20"},
		{"yarn.nodemanager.localizer.fetch.thread-count", "4"},
		{"yarn.nodemanager.node-labels.provider", "config"},
		{"yarn.nodemanager.node-labels.provider.configured-node-partition", ""},
		{"yarn.nodemanager.principal", ""},
		{"yarn.nodemanager.process-kill-wait.ms", "2000"},
		{"yarn.nodemanager.recovery.enabled", "false"},
		{"yarn.nodemanager.recovery.supervised", "false"},
		{"yarn.nodemanager.resource.memory.enabled", "false"},
		{"yarn.nodemanager.resource.memory.enforced", "false"},
		{"yarn.nodemanager.resourcemanager.connect.max-wait.ms", "-1"},
		{"yarn.nodemanager.sleep-delay-before-sigkill.ms", "250"},
		{"yarn.nodemanager.vmem-check-enabled", "false"},
		{"yarn.nodemanager.vmem-pmem-ratio", "2.1"},
		{"yarn.nodemanager.webapp.address", "${yarn.nodemanager.hostname}:8042"},
		{"yarn.resource-types.memory-mb.increment-allocation", "1024"},
		{"yarn.resource-types.vcores.increment-allocation", "1"},
		{"yarn.resourcemanager.am-scheduling.node-blacklisting-enabled", "false"},
		{"yarn.resourcemanager.am-scheduling.node-blacklisting-policy", "PORT_RULE"},
		{"yarn.resourcemanager.am.max-attempts", "2"},
		{"yarn.resourcemanager.amlauncher.thread-count", "50"},
		{"yarn.resourcemanager.bind-host", "0.0.0.0"},
		{"yarn.resourcemanager.client.thread-count", "50"},
		{"yarn.resourcemanager.connect.max-wait.ms", "900000"},
		{"yarn.resourcemanager.connect.retry-interval.ms", "30000"},
		{"yarn.resourcemanager.container.liveness-monitor.interval-ms", "600000"},
		{"yarn.resourcemanager.decommissioning-nodes-watcher.poll-interval-secs", "20"},
		{"yarn.resourcemanager.enable-node-untracked-without-include-path", "true"},
		{"yarn.resourcemanager.hostname", "{{.ResourceManagerHostname}}"},
		{"yarn.resourcemanager.keytab", ""},
		{"yarn.resourcemanager.max-completed-applications", "10000"},
		{"yarn.resourcemanager.node-removal-untracked.timeout-ms", "1800000"},
		{"yarn.resourcemanager.nodemanager-connect-retries", "10"},
		{"yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs", "3600"},
		{"yarn.resourcemanager.nodemanagers.heartbeat-interval-ms", "1000"},
		{"yarn.resourcemanager.principal", ""},
		{"yarn.resourcemanager.proxy-user-privileges.enabled", "false"},
		{"yarn.resourcemanager.recovery.enabled", "false"},
		{"yarn.resourcemanager.resource-tracker.client.thread-count", "64"},
		{"yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler"},
		{"yarn.resourcemanager.scheduler.client.thread-count", "50"},
		{"yarn.resourcemanager.store.class", "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore"},
		{"yarn.scheduler.fair.allow-undeclared-pools", "true"},
		{"yarn.scheduler.fair.assignmultiple", "false"},
		{"yarn.scheduler.fair.dynamic.max.assign", "true"},
		{"yarn.scheduler.fair.locality.threshold.node", "-1.0"},
		{"yarn.scheduler.fair.locality.threshold.rack", "-1.0"},
		{"yarn.scheduler.fair.max.assign", "-1"},
		{"yarn.scheduler.fair.preemption", "false"},
		{"yarn.scheduler.fair.preemption.cluster-utilization-threshold", "0.8"},
		{"yarn.scheduler.fair.sizebasedweight", "false"},
		{"yarn.scheduler.fair.update-interval-ms", "500"},
		{"yarn.scheduler.fair.user-as-default-queue", "false"},
		//{"yarn.scheduler.maximum-allocation-mb", "9216"},
		//{"yarn.scheduler.maximum-allocation-vcores", "32"},
		//{"yarn.scheduler.minimum-allocation-mb", "32"},
		{"yarn.system-metrics-publisher.enabled", "true"},
		{"yarn.timeline-service.bind-host", "0.0.0.0"},
		{"yarn.timeline-service.enabled", "false"},
		{"yarn.timeline-service.http-authentication.kerberos.principal", ""},
		{"yarn.timeline-service.http-authentication.type", "simple"},
		{"yarn.timeline-service.http-cross-origin.enabled", "true"},
		{"yarn.timeline-service.keytab", ""},
		{"yarn.timeline-service.leveldb-timeline-store.max-open-files", "500"},
		{"yarn.timeline-service.leveldb-timeline-store.read-cache-size", "4194304"},
		{"yarn.timeline-service.leveldb-timeline-store.write-buffer-size", "4194304"},
		{"yarn.timeline-service.principal", ""},
		{"yarn.timeline-service.rolling-period", "half_daily"},
		{"yarn.timeline-service.store-class", "org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore"},
		{"yarn.timeline-service.ttl-ms", "604800000"},
		{"yarn.web-proxy.keytab", ""},
		{"yarn.web-proxy.principal", ""},
	}
}

var _ XMLTemplateGetter = &hbaseSiteXMLTemplateGetter{}

type hbaseSiteXMLTemplateGetter struct {
	defaultProperties []Property
}

func (m *hbaseSiteXMLTemplateGetter) Default() {
	nameNodeRpcAddr := fmt.Sprintf("%s%s:%d", hdfsProtocol, NameNodeURIKey, defaultNameNodeRPCPort)
	m.defaultProperties = []Property{
		{"hbase.rootdir", fmt.Sprintf("%s/hbase", nameNodeRpcAddr)},
		{"hbase.cluster.distributed", "true"},
		{"hbase.unsafe.stream.capability.enforce", "false"},
	}
}

func (m *hbaseSiteXMLTemplateGetter) GetXMLTemplate(cluster *v1alpha1.HadoopCluster) Properties {
	return m.defaultProperties
}

func (m *hbaseSiteXMLTemplateGetter) GetKey() string {
	return hbaseSiteXmlKey
}

// SI Sizes.
const (
	IByte = 1
	KByte = IByte * 1000
	MByte = KByte * 1000
)

func (y *yarnSiteXMLTemplateGetter) GetXMLTemplate(cluster *v1alpha1.HadoopCluster) Properties {
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

func (h *ConfigMapBuilder) IsBuildCompleted(obj interface{}, _ interface{}) bool {
	cluster := obj.(*v1alpha1.HadoopCluster)
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, v1alpha1.ReplicaTypeConfigMap), Namespace: cluster.Namespace},
		&corev1.ConfigMap{},
	)
	return err == nil
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
		&hbaseSiteXMLTemplateGetter{},
	}
	for _, xmlGetter := range h.XmlGetters {
		xmlGetter.Default()
	}
}

func (h *ConfigMapBuilder) Build(obj interface{}, _ interface{}) (bool, error) {
	cluster := obj.(*v1alpha1.HadoopCluster)
	err := h.Get(
		context.Background(),
		client.ObjectKey{Name: util.GetReplicaName(cluster, v1alpha1.ReplicaTypeConfigMap), Namespace: cluster.Namespace},
		&corev1.ConfigMap{},
	)
	if err == nil || !errors.IsNotFound(err) {
		return err == nil, err
	}

	configMap, err := h.buildHadoopConfigMap(cluster)
	if err != nil {
		return false, err
	}
	ownerRef := util.GenOwnerReference(cluster, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopClusterKind).Kind)
	return true, h.ConfigMapControl.CreateConfigMapWithControllerRef(cluster.GetNamespace(), configMap, cluster, ownerRef)
}

func (h *ConfigMapBuilder) Clean(obj interface{}) error {
	cluster := obj.(*v1alpha1.HadoopCluster)

	configMapName := util.GetReplicaName(cluster, v1alpha1.ReplicaTypeConfigMap)
	err := h.ConfigMapControl.DeleteConfigMap(cluster.GetNamespace(), configMapName, &corev1.ConfigMap{})
	if err != nil {
		return err
	}

	return nil
}

func (h *ConfigMapBuilder) buildHadoopConfigMap(cluster *v1alpha1.HadoopCluster) (*corev1.ConfigMap, error) {
	hadoopConfig := HadoopConfig{
		NameNodeURI:             util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode),
		ResourceManagerHostname: util.GetReplicaName(cluster, v1alpha1.ReplicaTypeResourcemanager),
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
			Name:      util.GetReplicaName(cluster, v1alpha1.ReplicaTypeConfigMap),
			Namespace: cluster.Namespace,
		},
		Data: configMapData,
	}
	return configMap, nil
}
