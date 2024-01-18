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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var hadoopapplicationlog = logf.Log.WithName("hadoopapplication-resource")

func (r *HadoopApplication) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-kubecluster-org-v1alpha1-hadoopapplication,mutating=true,failurePolicy=fail,sideEffects=None,groups=kubecluster.org,resources=hadoopapplications,verbs=create;update,versions=v1alpha1,name=mhadoopapplication.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &HadoopApplication{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *HadoopApplication) Default() {
	hadoopapplicationlog.Info("default", "name", r.Name)
	r.Spec.DataLoaderSpec.RestartPolicy = corev1.RestartPolicyNever

	if r.Spec.DataLoaderSpec != nil {
		for index, container := range r.Spec.DataLoaderSpec.Containers {
			if len(container.Image) > 0 {
				continue
			}
			container.Image = DefaultImage
			r.Spec.DataLoaderSpec.Containers[index] = container
		}
	}

	if r.Spec.ExecutorSpec.Replicas == nil {
		r.Spec.ExecutorSpec.Replicas = ptr.To(int32(1))
	}

	if r.Spec.ExecutorSpec.Image == "" {
		r.Spec.ExecutorSpec.Image = DefaultImage
	}
}

//+kubebuilder:webhook:path=/validate-kubecluster-org-v1alpha1-hadoopapplication,mutating=false,failurePolicy=fail,sideEffects=None,groups=kubecluster.org,resources=hadoopapplications,verbs=create;update,versions=v1alpha1,name=vhadoopapplication.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HadoopApplication{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopApplication) ValidateCreate() (admission.Warnings, error) {
	hadoopapplicationlog.Info("validate create", "name", r.Name)

	warnings := admission.Warnings{}

	return warnings, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopApplication) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	hadoopapplicationlog.Info("validate update", "name", r.Name)

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopApplication) ValidateDelete() (warnings admission.Warnings, err error) {
	hadoopapplicationlog.Info("validate delete", "name", r.Name)

	return nil, nil
}
