package controllers

import (
	"fmt"
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/controllers/hadoopapplication"
	"github.com/chriskery/hadoop-operator/pkg/controllers/hadoopcluster"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const ErrTemplateSchemeNotSupported = "scheme %s is not supported yet"

type ReconcilerSetupFunc func(manager manager.Manager) error

var SupportedSchemeReconciler = map[string]ReconcilerSetupFunc{
	v1alpha1.HadoopClusterKind: func(mgr manager.Manager) error {
		if err := hadoopcluster.NewReconciler(mgr).SetupWithManager(mgr); err != nil {
			return err
		}

		if err := (&v1alpha1.HadoopCluster{}).SetupWebhookWithManager(mgr); err != nil {
			return err
		}
		return nil
	},
	v1alpha1.HadoopApplicationPlural: func(mgr manager.Manager) error {
		if err := hadoopapplication.NewReconciler(mgr).SetupWithManager(mgr); err != nil {
			return err
		}

		if err := (&v1alpha1.HadoopApplication{}).SetupWebhookWithManager(mgr); err != nil {
			return err
		}
		return nil
	},
}

type EnabledSchemes []string

func (es *EnabledSchemes) String() string {
	return strings.Join(*es, ",")
}

func (es *EnabledSchemes) Set(kind string) error {
	kind = strings.ToLower(kind)
	for supportedKind := range SupportedSchemeReconciler {
		if strings.ToLower(supportedKind) == kind {
			*es = append(*es, supportedKind)
			return nil
		}
	}
	return fmt.Errorf(ErrTemplateSchemeNotSupported, kind)
}

func (es *EnabledSchemes) FillAll() {
	for supportedKind := range SupportedSchemeReconciler {
		*es = append(*es, supportedKind)
	}
}

func (es *EnabledSchemes) Empty() bool {
	return len(*es) == 0
}
