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
	// HadoopJobKind is the kind name.
	HadoopJobKind = "HadoopJob"
	// HadoopJobPlural is the TensorflowPlural for HadoopJob.
	HadoopJobPlural = "HadoopJobs"
	// HadoopJobSingular is the singular for HadoopJob.
	HadoopJobSingular = "HadoopJob"

	// JobNameLabel represents the label key for the cluster name, the value is the cluster name.
	JobNameLabel = "kubeclusetr.org/job-name"
)

// SparkApplicationType describes the type of a Spark application.
type SparkApplicationType string

// Different types of Spark applications.
const (
	JavaApplicationType   SparkApplicationType = "Java"
	ScalaApplicationType  SparkApplicationType = "Scala"
	PythonApplicationType SparkApplicationType = "Python"
	RApplicationType      SparkApplicationType = "R"
)

// HadoopJobSpec defines the desired state of HadoopJob
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
type HadoopJobSpec struct {
	// MainFile is the path to a bundled JAR, Python, or R file of the application.
	MainApplicationFile string `json:"mainApplicationFile"`

	// Arguments is a list of arguments to be passed to the application.
	// +optional
	Arguments []string `json:"arguments,omitempty"`

	ExecutorSpec HadoopNodeSpec `json:"executorSpec,omitempty"`

	// List of environment variables to set in the container.
	// Cannot be updated.
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	Env []corev1.EnvVar `json:"env,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=env"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true
// JobCondition describes current state of a cluster
type JobCondition struct {
	// Type of job condition.
	Type JobConditionType `json:"type"`
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

type JobConditionType string

const (
	// JobCreated means the job has been accepted by the system,
	// but one or more of the pods/services has not been started.
	// This includes time before pods being scheduled and launched.
	JobCreated JobConditionType = "Created"

	// JobSubmitted means all sub-resources (e.g. services/pods) of this job
	// have been successfully submitted.
	JobSubmitted JobConditionType = "Submitted"

	// JobRunning means all sub-resources (e.g. services/pods) of this job
	// have been successfully scheduled and launched.
	// The training is running without error.
	JobRunning JobConditionType = "Running"

	// JobSucceeded means all sub-resources (e.g. services/pods) of this job
	// reached phase have terminated in success.
	// The training is complete without error.
	JobSucceeded JobConditionType = "Succeeded"

	// JobFailed means one or more sub-resources (e.g. services/pods) of this job
	// reached phase failed with no restarting.
	// The training has failed its execution.
	JobFailed JobConditionType = "Failed"
)

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true
// HadoopJobStatus defines the observed state of HadoopJob
type HadoopJobStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// Conditions is an array of current observed job conditions.
	Conditions []JobCondition `json:"conditions"`

	// Represents time when the job was acknowledged by the job controller.
	// It is not guaranteed to be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Represents time when the job was completed. It is not guaranteed to
	// be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=hadoopjobs
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name="Age",type=date
// +kubebuilder:printcolumn:JSONPath=`.status.conditions[-1:].type`,name="State",type=string
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,path=hadoopjobs,shortName={"hdj","hdjs"}
// HadoopJob is the Schema for the hadoopjobs API
type HadoopJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HadoopJobSpec   `json:"spec,omitempty"`
	Status HadoopJobStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimzxachinery/pkg/runtime.Object
// +resource:path=hadoopjobs

// HadoopJobList contains a list of HadoopJob
type HadoopJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HadoopJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HadoopJob{}, &HadoopJobList{})
}
