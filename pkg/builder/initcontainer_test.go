package builder

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/config"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestInitContainer(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	defer ginkgo.GinkgoRecover()

	config.Config.HadoopInitContainerImage = config.HadoopInitContainerImageDefault
	config.Config.HadoopInitContainerTemplateFile = config.HadoopInitContainerTemplateFileDefault

	testCases := []struct {
		podSpec     *corev1.PodTemplateSpec
		rtype       v1alpha1.ReplicaType
		expected    int
		exepctedErr error
	}{
		{
			podSpec:     &corev1.PodTemplateSpec{},
			rtype:       v1alpha1.ReplicaTypeNameNode,
			expected:    0,
			exepctedErr: nil,
		},
		{
			podSpec:     &corev1.PodTemplateSpec{},
			rtype:       v1alpha1.ReplicaTypeResourcemanager,
			expected:    1,
			exepctedErr: nil,
		},
		{
			podSpec:     &corev1.PodTemplateSpec{},
			rtype:       v1alpha1.ReplicaTypeDataNode,
			expected:    1,
			exepctedErr: nil,
		},
		{
			podSpec:     &corev1.PodTemplateSpec{},
			rtype:       v1alpha1.ReplicaTypeNodemanager,
			expected:    1,
			exepctedErr: nil,
		},
	}

	hadoopCluster := testutil.NewHadoopCluster()
	for _, t := range testCases {
		err := setInitContainer(hadoopCluster, t.rtype, t.podSpec)
		if t.exepctedErr == nil {
			gomega.Expect(err).To(gomega.BeNil())
		} else {
			gomega.Expect(err).To(gomega.Equal(t.exepctedErr))
		}
		gomega.Expect(len(t.podSpec.Spec.InitContainers)).To(gomega.Equal(t.expected))
	}
}
