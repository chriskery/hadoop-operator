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
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var hadoopjoblog = logf.Log.WithName("hadoopjob-resource")

func (r *HadoopJob) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-kubecluster-org-v1alpha1-hadoopjob,mutating=true,failurePolicy=fail,sideEffects=None,groups=kubecluster.org,resources=hadoopjobs,verbs=create;update,versions=v1alpha1,name=mhadoopjob.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &HadoopJob{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *HadoopJob) Default() {
	hadoopjoblog.Info("default", "name", r.Name)

}

//+kubebuilder:webhook:path=/validate-kubecluster-org-v1alpha1-hadoopjob,mutating=false,failurePolicy=fail,sideEffects=None,groups=kubecluster.org,resources=hadoopjobs,verbs=create;update,versions=v1alpha1,name=vhadoopjob.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HadoopJob{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopJob) ValidateCreate() (admission.Warnings, error) {
	hadoopjoblog.Info("validate create", "name", r.Name)

	warnings := admission.Warnings{}

	return warnings, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopJob) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	hadoopjoblog.Info("validate update", "name", r.Name)

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *HadoopJob) ValidateDelete() (warnings admission.Warnings, err error) {
	hadoopjoblog.Info("validate delete", "name", r.Name)

	return nil, nil
}
