package v1alpha1

import (
	"k8s.io/utils/ptr"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHadoopClusterDefault(t *testing.T) {
	hadoopCluster := &HadoopCluster{}
	hadoopCluster.Default()

	assert.Equal(t, DefaultImage, hadoopCluster.Spec.HDFS.DataNode.Image)
	assert.Equal(t, DefaultImage, hadoopCluster.Spec.HDFS.NameNode.Image)
	assert.Equal(t, DefaultImage, hadoopCluster.Spec.Yarn.NodeManager.Image)
	assert.Equal(t, DefaultImage, hadoopCluster.Spec.Yarn.ResourceManager.Image)
}

func TestHadoopClusterValidateCreate(t *testing.T) {
	hadoopCluster := &HadoopCluster{}
	hadoopCluster.Default()

	hadoopCluster.Spec.Yarn.ResourceManager.Replicas = ptr.To(int32(2))
	hadoopCluster.Spec.HDFS.NameNode.Replicas = ptr.To(int32(2))

	warnings, err := hadoopCluster.ValidateCreate()
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(warnings))

	hadoopCluster.Spec.HDFS.NameNode.Replicas = ptr.To(int32(1))
	hadoopCluster.Spec.Yarn.ResourceManager.Replicas = ptr.To(int32(1))

	warnings, err = hadoopCluster.ValidateCreate()
	assert.Nil(t, err)
	assert.Equal(t, len(warnings), 0)
}

func TestHadoopClusterValidateUpdate(t *testing.T) {
	hadoopCluster := &HadoopCluster{}
	warnings, err := hadoopCluster.ValidateUpdate(nil)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(warnings))
}

func TestHadoopClusterValidateDelete(t *testing.T) {
	hadoopCluster := &HadoopCluster{}
	warnings, err := hadoopCluster.ValidateDelete()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(warnings))
}
