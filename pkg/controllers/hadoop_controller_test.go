package controllers_test

import (
	"context"
	"fmt"
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"
	"time"

	. "github.com/onsi/gomega"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
)

func waitForClusterCreation(ctx context.Context, hadoopCluster *hadoopclusterorgv1alpha1.HadoopCluster, client runtimeClient.Client) {
	EventuallyWithOffset(1, func() string {
		hadoopClusterCreated := hadoopclusterorgv1alpha1.HadoopCluster{}
		if err := client.Get(
			ctx,
			types.NamespacedName{Name: hadoopCluster.Name, Namespace: hadoopCluster.Namespace},
			&hadoopClusterCreated,
		); err != nil {
			return fmt.Sprintf("%v+", err)
		}
		logrus.Infof("%v", hadoopClusterCreated)
		if len(hadoopCluster.Status.Conditions) == 0 {
			return "not ready"
		}

		return "ready"

	}, testutil.ClusterCreationTimeout, 1*time.Second).Should(Equal("ready"))

}
