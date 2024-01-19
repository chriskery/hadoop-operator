/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	DefaultImage                       = "apache/hadoop:3"
	DefaultHbaseImage                  = "harisekhon/hbase"
	DefaultLogAggregationRetainSeconds = 604800
)

// log is for logging in this package.
var hadoopclusterlog = logf.Log.WithName("hadoopcluster-resource")

func (r *HadoopCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-kubecluster-org-v1alpha1-hadoopcluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=kubecluster.org,resources=hadoopclusters,verbs=create;update,versions=v1alpha1,name=mhadoopcluster.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &HadoopCluster{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *HadoopCluster) Default() {
	hadoopclusterlog.Info("default", "name", r.Name)
	setDefaultsHDFS(&r.Spec.HDFS)
	setDefaultsYarn(&r.Spec.Yarn)

	if r.Spec.Hbase != nil && r.Spec.Hbase.Image == "" {
		r.Spec.Hbase.Image = DefaultHbaseImage
	}
}

func setDefaultsYarn(r *YarnSpec) {
	setDefaultsYarnNodeManager(&r.NodeManager)
	setDefaultsYarnResourceManager(&r.ResourceManager)
}

func setDefaultsYarnResourceManager(yarnSpecTemplate *YarnResourceManagerSpecTemplate) {
	if yarnSpecTemplate.Replicas == nil {
		yarnSpecTemplate.Replicas = ptr.To(int32(1))
	}
	if yarnSpecTemplate.Image == "" {
		yarnSpecTemplate.Image = DefaultImage
	}
	if yarnSpecTemplate.ImagePullPolicy == "" {
		yarnSpecTemplate.ImagePullPolicy = "IfNotPresent"
	}
}

func setDefaultsYarnNodeManager(yarnSpec *YarnNodeManagerSpecTemplate) {
	if yarnSpec.Replicas == nil {
		yarnSpec.Replicas = ptr.To(int32(1))
	}
	if yarnSpec.Image == "" {
		yarnSpec.Image = DefaultImage
	}
	if yarnSpec.ImagePullPolicy == "" {
		yarnSpec.ImagePullPolicy = "IfNotPresent"
	}
}

func setDefaultsHDFS(r *HDFSSpec) {
	setDefaultsYarnDataNode(&r.DataNode)
	setDefaultsYarnNameNode(&r.NameNode)
}

func setDefaultsYarnNameNode(hdfsSpec *HDFSNameNodeSpecTemplate) {
	if hdfsSpec.Replicas == nil {
		hdfsSpec.Replicas = ptr.To(int32(1))
	}
	if hdfsSpec.Image == "" {
		hdfsSpec.Image = DefaultImage
	}
	if hdfsSpec.ImagePullPolicy == "" {
		hdfsSpec.ImagePullPolicy = "IfNotPresent"
	}
	if hdfsSpec.LogAggregationEnable && hdfsSpec.LogAggregationRetainSeconds <= 0 {
		hdfsSpec.LogAggregationRetainSeconds = DefaultLogAggregationRetainSeconds
	}
}

func setDefaultsYarnDataNode(hdfsSpec *HDFSDataNodeSpecTemplate) {
	if hdfsSpec.Replicas == nil {
		hdfsSpec.Replicas = ptr.To(int32(1))
	}
	if hdfsSpec.Image == "" {
		hdfsSpec.Image = DefaultImage
	}
	if hdfsSpec.ImagePullPolicy == "" {
		hdfsSpec.ImagePullPolicy = "IfNotPresent"
	}
}

//+kubebuilder:webhook:path=/validate-kubecluster-org-v1alpha1-hadoopcluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=kubecluster.org,resources=hadoopclusters,verbs=create;update,versions=v1alpha1,name=vhadoopcluster.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HadoopCluster{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopCluster) ValidateCreate() (admission.Warnings, error) {
	hadoopclusterlog.Info("validate create", "name", r.Name)
	if r.Spec.HDFS.NameNode.Replicas != nil && *r.Spec.HDFS.NameNode.Replicas > 1 {
		return admission.Warnings{}, fmt.Errorf("NameNode replicas must set to 1, current %d", *r.Spec.HDFS.NameNode.Replicas)
	}

	if r.Spec.Yarn.ResourceManager.Replicas != nil && *r.Spec.Yarn.ResourceManager.Replicas > 1 {
		return admission.Warnings{}, fmt.Errorf("ResourceManager replicas must set to 1, current %d", *r.Spec.Yarn.ResourceManager.Replicas)
	}

	warnings := admission.Warnings{}
	if r.Spec.Yarn.ResourceManager.Image != DefaultImage {
		warnings = append(warnings, fmt.Sprintf("%s's image should install hadoop dependency, run may fail", ReplicaTypeResourcemanager))
	}
	if r.Spec.Yarn.NodeManager.Image != DefaultImage {
		warnings = append(warnings, fmt.Sprintf("%s's image should install hadoop dependency, run may fail", ReplicaTypeNodemanager))
	}
	if r.Spec.HDFS.NameNode.Image != DefaultImage {
		warnings = append(warnings, fmt.Sprintf("%s's image should install hadoop dependency, run may fail", ReplicaTypeNameNode))
	}
	if r.Spec.HDFS.DataNode.Image != DefaultImage {
		warnings = append(warnings, fmt.Sprintf("%s's image should install hadoop dependency, run may fail", ReplicaTypeDataNode))
	}
	return warnings, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopCluster) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	hadoopclusterlog.Info("validate update", "name", r.Name)

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopCluster) ValidateDelete() (warnings admission.Warnings, err error) {
	hadoopclusterlog.Info("validate delete", "name", r.Name)

	return nil, nil
}
