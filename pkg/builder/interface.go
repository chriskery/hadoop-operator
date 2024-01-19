package builder

import (
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type Builder interface {
	SetupWithManager(mgr manager.Manager, recorder record.EventRecorder)
	Build(obj interface{}, objStatus interface{}) (bool, error)
	Clean(obj interface{}) error
	IsBuildCompleted(obj interface{}, objStatus interface{}) bool
}

type AlwaysBuildCompletedBuilder struct {
}

func (builder *AlwaysBuildCompletedBuilder) IsBuildCompleted(obj interface{}, objStatus interface{}) bool {
	return true
}

func ResourceBuilders(mgr manager.Manager, recorder record.EventRecorder) []Builder {
	var builders = []Builder{
		&ConfigMapBuilder{},
		&HdfsBuilder{},
		&YarnBuilder{},
		&HbaseBuilder{},
	}
	for _, builder := range builders {
		builder.SetupWithManager(mgr, recorder)
	}
	return builders
}
