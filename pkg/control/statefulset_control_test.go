package control

import (
	"encoding/json"
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util/testutil"
	appv1 "k8s.io/api/apps/v1"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	utiltesting "k8s.io/client-go/util/testing"
)

func TestStatefulSets(t *testing.T) {
	ns := metav1.NamespaceDefault
	body := runtime.EncodeOrDie(
		clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion),
		&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "empty_pod"}})
	fakeHandler := utiltesting.FakeHandler{
		StatusCode:   200,
		ResponseBody: body,
	}
	testServer := httptest.NewServer(&fakeHandler)
	defer testServer.Close()
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: testServer.URL, ContentConfig: restclient.ContentConfig{GroupVersion: &v1.SchemeGroupVersion}})

	statefulSetControl := RealStatefulSetControl{
		KubeClient: clientset,
		Recorder:   &record.FakeRecorder{},
	}

	testCluster := testutil.NewHadoopCluster()

	testName := "statefulset-name"
	labels := map[string]string{v1alpha1.ClusterNameLabel: testCluster.Name}
	statefulSetTemplate := testutil.NewBaseStatefulSet(testName, testCluster, labels)
	statefulSetTemplate.SetOwnerReferences([]metav1.OwnerReference{})

	// Make sure createReplica sends a POST to the apiserver with a pod from the controllers pod template
	err := statefulSetControl.CreateStatefulSets(ns, statefulSetTemplate, testCluster)
	assert.NoError(t, err, "unexpected error: %v", err)

	expectedStatefulSet := appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:   testName,
			Labels: labels,
		},
		Spec: statefulSetTemplate.Spec,
	}
	fakeHandler.ValidateRequest(t,
		"/apis/apps/v1/namespaces/default/statefulsets", "POST", nil)
	var actualStatefulSet = &appv1.StatefulSet{}
	err = json.Unmarshal([]byte(fakeHandler.RequestBody), actualStatefulSet)
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, apiequality.Semantic.DeepDerivative(&expectedStatefulSet, actualStatefulSet),
		"Body: %s", fakeHandler.RequestBody)
}
