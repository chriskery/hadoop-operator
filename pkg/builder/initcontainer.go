package builder

import (
	"bytes"
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/config"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	"html/template"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"os"
	"sync"
)

var (
	initContainerTemplate = `
- name: init-hadoop
  image: {{.InitContainerImage}}
  imagePullPolicy: IfNotPresent
  resources:
    limits:
      cpu: 100m
      memory: 20Mi
    requests:
      cpu: 50m
      memory: 10Mi
  command: ['sh', '-c', 'until nslookup {{.MasterAddr}}; do echo waiting for {{.MasterAddr}}; sleep 2; done;']`
	onceInitContainer sync.Once
	icGenerator       *initContainerGenerator
)

type initContainerGenerator struct {
	template string
	image    string
}

func getInitContainerGenerator() *initContainerGenerator {
	onceInitContainer.Do(func() {
		icGenerator = &initContainerGenerator{
			template: getInitContainerTemplateOrDefault(config.Config.HadoopInitContainerTemplateFile),
			image:    config.Config.HadoopInitContainerImage,
		}
	})
	return icGenerator
}

func (i *initContainerGenerator) GetInitContainer(masterAddr string) ([]corev1.Container, error) {
	var buf bytes.Buffer
	tpl, err := template.New("container").Parse(i.template)
	if err != nil {
		return nil, err
	}
	if err = tpl.Execute(&buf, struct {
		MasterAddr         string
		InitContainerImage string
	}{
		MasterAddr:         masterAddr,
		InitContainerImage: i.image,
	}); err != nil {
		return nil, err
	}

	var result []corev1.Container
	err = yaml.Unmarshal(buf.Bytes(), &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// getInitContainerTemplateOrDefault returns the init container template file if
// it exists, or return initContainerTemplate by default.
func getInitContainerTemplateOrDefault(file string) string {
	b, err := os.ReadFile(file)
	if err == nil {
		return string(b)
	}
	return initContainerTemplate
}

func setInitContainer(cluster *v1alpha1.HadoopCluster, replicaType v1alpha1.ReplicaType, podTemplate *corev1.PodTemplateSpec) error {
	g := getInitContainerGenerator()

	masterAddr := ""
	switch replicaType {
	case v1alpha1.ReplicaTypeDataNode, v1alpha1.ReplicaTypeResourcemanager:
		masterAddr = util.GetReplicaName(cluster, v1alpha1.ReplicaTypeNameNode)
	case v1alpha1.ReplicaTypeNodemanager:
		masterAddr = util.GetReplicaName(cluster, v1alpha1.ReplicaTypeResourcemanager)
	default:
		return nil
	}

	initContainers, err := g.GetInitContainer(masterAddr)
	if err != nil {
		return err
	}
	podTemplate.Spec.InitContainers = append(podTemplate.Spec.InitContainers,
		initContainers...)

	return nil
}
