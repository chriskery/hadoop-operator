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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// HadoopApplicationKind is the kind name.
	HadoopApplicationKind = "HadoopApplication"
	// HadoopApplicationPlural is the TensorflowPlural for HadoopApplication.
	HadoopApplicationPlural = "HadoopApplications"
	// HadoopApplicationSingular is the singular for HadoopApplication.
	HadoopApplicationSingular = "HadoopApplication"

	// ApplicationNameLabel represents the label key for the cluster name, the value is the cluster name.
	ApplicationNameLabel = "kubeclusetr.org/application-name"
)

// HdoopApplicationType describes the type of a Spark application.
type HdoopApplicationType string

// Different types of Spark applications.
const (
	JavaApplicationType   HdoopApplicationType = "Java"
	ScalaApplicationType  HdoopApplicationType = "Scala"
	PythonApplicationType HdoopApplicationType = "Python"
	RApplicationType      HdoopApplicationType = "R"
)

type DataLoaderSpec corev1.PodSpec

// HadoopApplicationSpec defines the desired state of HadoopApplication
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
type HadoopApplicationSpec struct {
	// MainFile is the path to a bundled JAR, Python, or R file of the application.
	MainApplicationFile string `json:"mainApplicationFile"`

	// Arguments is a list of arguments to be passed to the application.
	// +optional
	Arguments []string `json:"arguments,omitempty"`

	ExecutorSpec HadoopNodeSpec `json:"executorSpec,omitempty"`

	NameNodeDirFormat bool `json:"nameNodeDirFormat,omitempty"`

	DataLoaderSpec *DataLoaderSpec `json:"dataLoaderSpec,omitempty"`

	// List of environment variables to set in the container.
	// Cannot be updated.
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	Env []corev1.EnvVar `json:"env,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=env"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true
// ApplicationCondition describes current state of a cluster
type ApplicationCondition struct {
	// Type of application condition.
	Type ApplicationConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
	// The last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

type ApplicationConditionType string

const (
	// ApplicationCreated means the application has been accepted by the system,
	// but one or more of the pods/services has not been started.
	// This includes time before pods being scheduled and launched.
	ApplicationCreated ApplicationConditionType = "Created"

	// ApplicationDataLoading means all sub-resources (e.g. services/pods) of this application
	// have been successfully submitted.
	ApplicationDataLoading ApplicationConditionType = "DataLoading"

	// ApplicationSubmitted means all sub-resources (e.g. services/pods) of this application
	// have been successfully submitted.
	ApplicationSubmitted ApplicationConditionType = "Submitted"

	// ApplicationRunning means all sub-resources (e.g. services/pods) of this application
	// have been successfully scheduled and launched.
	// The training is running without error.
	ApplicationRunning ApplicationConditionType = "Running"

	// ApplicationSucceeded means all sub-resources (e.g. services/pods) of this application
	// reached phase have terminated in success.
	// The training is complete without error.
	ApplicationSucceeded ApplicationConditionType = "Succeeded"

	// ApplicationFailed means one or more sub-resources (e.g. services/pods) of this application
	// reached phase failed with no restarting.
	// The training has failed its execution.
	ApplicationFailed ApplicationConditionType = "Failed"
)

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true
// HadoopApplicationStatus defines the observed state of HadoopApplication
type HadoopApplicationStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// Conditions is an array of current observed application conditions.
	Conditions []ApplicationCondition `json:"conditions"`

	// Represents time when the application was acknowledged by the application controller.
	// It is not guaranteed to be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Represents time when the application was completed. It is not guaranteed to
	// be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=hadoopapplications
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name="Age",type=date
// +kubebuilder:printcolumn:JSONPath=`.status.conditions[-1:].type`,name="State",type=string
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,path=hadoopapplications,shortName={"hda","hdas"}
// HadoopApplication is the Schema for the hadoopapplications API
type HadoopApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HadoopApplicationSpec   `json:"spec,omitempty"`
	Status HadoopApplicationStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimzxachinery/pkg/runtime.Object
// +resource:path=hadoopapplications

// HadoopApplicationList contains a list of HadoopApplication
type HadoopApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HadoopApplication `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HadoopApplication{}, &HadoopApplicationList{})
}
