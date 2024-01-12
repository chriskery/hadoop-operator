package testutil

import (
	hadoopclusterorgv1alpha1 "github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"testing"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(hadoopclusterorgv1alpha1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func NewCtrlManager(t *testing.T) (manager.Manager, *envtest.Environment) {
	envTest := &envtest.Environment{}
	cfg, err := envTest.Start()
	if err != nil {
		t.Fatalf("failed to start envtest: %v", err)
	}
	defer envTest.Stop()

	// 使用 cfg 创建一个假的 manager.Manager 对象
	mgr, err := manager.New(cfg, manager.Options{
		NewClient: func(config *rest.Config, options client.Options) (client.Client, error) {
			// 返回一个假的客户端
			return fake.NewClientBuilder().Build(), nil
		},
	})

	if err != nil {
		t.Fatalf("failed to start envtest: %v", err)
	}
	return mgr, envTest
}
