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
	// HadoopClusterKind is the kind name.
	HadoopClusterKind = "HadoopCluster"
	// HadoopClusterPlural is the TensorflowPlural for HadoopCluster.
	HadoopClusterPlural = "HadoopClusters"
	// HadoopClusterSingular is the singular for HadoopCluster.
	HadoopClusterSingular = "HadoopCluster"

	// ControllerNameLabel represents the label key for the operator name, e.g. tf-operator, mpi-operator, etc.
	ControllerNameLabel = "kubeclusetr.org/controller-name"

	// ClusterNameLabel represents the label key for the cluster name, the value is the cluster name.
	ClusterNameLabel = "kubeclusetr.org/clusetr-name"

	ReplicaTypeLabel = "kubeclusetr.org/relica-type"

	DeletionLabel = "kubeclusetr.org/deletion"
)

type HadoopNodeSpec struct {
	// Number of desired pods. This is a pointer to distinguish between explicit
	// zero and not specified. Defaults to 1.
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
	// Container image name.
	// More info: https://kubernetes.io/docs/concepts/containers/images
	// This field is optional to allow higher level config management to default or override
	// container images in workload controllers like Deployments and StatefulSets.
	// +optional
	Image string `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	// Pod volumes to mount into the container's filesystem.
	// Cannot be updated.
	// +optional
	// +patchMergeKey=mountPath
	// +patchStrategy=merge
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath" protobuf:"bytes,9,rep,name=volumeMounts"`
	// Compute Resources required by this container.
	// Cannot be updated.
	// More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,8,opt,name=resources"`
	// Image pull policy.
	// One of Always, Never, IfNotPresent.
	// Defaults to Always if :latest tag is specified, or IfNotPresent otherwise.
	// Cannot be updated.
	// More info: https://kubernetes.io/docs/concepts/containers/images#updating-images
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,14,opt,name=imagePullPolicy,casttype=PullPolicy"`
	// SecurityContext defines the security options the container should be run with.
	// If set, the fields of SecurityContext override the equivalent fields of PodSecurityContext.
	// More info: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty" protobuf:"bytes,15,opt,name=securityContext"`
	// Host networking requested for this pod. Use the host's network namespace.
	// If this option is set, the ports that will be used must be specified.
	// Default to false.
	// +k8s:conversion-gen=false
	// +optional
	HostNetwork      bool                          `json:"hostNetwork,omitempty" protobuf:"varint,11,opt,name=hostNetwork"`
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,15,rep,name=imagePullSecrets"`
	// List of volumes that can be mounted by containers belonging to the pod.
	// More info: https://kubernetes.io/docs/concepts/storage/volumes
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	Volumes []corev1.Volume `json:"volumes,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,1,rep,name=volumes"`
}

type HDFSSpec struct {
	NameNode HDFSNameNodeSpecTemplate `json:"nameNode,omitempty"`
	DataNode HDFSDataNodeSpecTemplate `json:"dataNode,omitempty"`
}

type HDFSNameNodeSpecTemplate struct {
	HadoopNodeSpec

	ServiceType corev1.ServiceType `json:"serviceType,omitempty" protobuf:"bytes,16,opt,name=serviceType,casttype=ServiceType"`
}

type HDFSDataNodeSpecTemplate struct {
	HadoopNodeSpec
}

type YarnSpec struct {
	NodeManager     YarnNodeManagerSpecTemplate     `json:"nodeManager,omitempty"`
	ResourceManager YarnResourceManagerSpecTemplate `json:"resourceManager,omitempty"`
}

type YarnNodeManagerSpecTemplate struct {
	HadoopNodeSpec

	ServiceType corev1.ServiceType `json:"serviceType,omitempty" protobuf:"bytes,16,opt,name=serviceType,casttype=ServiceType"`
}

type YarnResourceManagerSpecTemplate struct {
	HadoopNodeSpec

	ServiceType corev1.ServiceType `json:"serviceType,omitempty" protobuf:"bytes,16,opt,name=serviceType,casttype=ServiceType"`
}

// HadoopClusterSpec defines the desired state of HadoopCluster
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
type HadoopClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	HDFS HDFSSpec `json:"hdfs,omitempty"`
	Yarn YarnSpec `json:"yarn,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true
// ClusterCondition describes current state of a cluster
type ClusterCondition struct {
	// Type of job condition.
	Type ClusterConditionType `json:"type"`
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

type ClusterConditionType string

const (
	// ClusterCreated means the job has been accepted by the system,
	// but one or more of the pods/services has not been started.
	// This includes time before pods being scheduled and launched.
	ClusterCreated ClusterConditionType = "Created"

	// ClusterRunning means all sub-resources (e.g. services/pods) of this job
	// have been successfully scheduled and launched.
	// The training is running without error.
	ClusterRunning ClusterConditionType = "Running"

	// ClusterRestarting means one or more sub-resources (e.g. services/pods) of this job
	// reached phase failed but maybe restarted according to it's restart policy
	// which specified by user in v1.PodTemplateSpec.
	// The training is freezing/pending.
	ClusterRestarting ClusterConditionType = "Restarting"
)

// +k8s:openapi-gen=true
// ReplicaStatus represents the current observed state of the replica.
type ReplicaStatus struct {
	// The number of actively running pods.
	Active int32 `json:"active,omitempty"`
	// The number of actively running pods.
	Expect *int32 `json:"expect,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true
// HadoopClusterStatus defines the observed state of HadoopCluster
type HadoopClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// Conditions is an array of current observed job conditions.
	Conditions []ClusterCondition `json:"conditions"`

	// ReplicaStatuses is map of ReplicaType and ReplicaStatus,
	// specifies the status of each replica.
	ReplicaStatuses map[ReplicaType]*ReplicaStatus `json:"replicaStatuses"`

	// Represents time when the job was acknowledged by the job controller.
	// It is not guaranteed to be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	StartTime *metav1.Time `json:"startTime,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=hadoopclusters
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name="Age",type=date
// +kubebuilder:printcolumn:JSONPath=`.status.conditions[-1:].type`,name="State",type=string
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,path=hadoopclusters,shortName={"hdc","hdcs"}
// HadoopCluster is the Schema for the hadoopclusters API
type HadoopCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HadoopClusterSpec   `json:"spec,omitempty"`
	Status HadoopClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimzxachinery/pkg/runtime.Object
// +resource:path=hadoopclusters

// HadoopClusterList contains a list of HadoopCluster
type HadoopClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HadoopCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HadoopCluster{}, &HadoopClusterList{})
}
