//go:build !ignore_autogenerated

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

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ClusterCondition) DeepCopyInto(out *ClusterCondition) {
	*out = *in
	in.LastUpdateTime.DeepCopyInto(&out.LastUpdateTime)
	in.LastTransitionTime.DeepCopyInto(&out.LastTransitionTime)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ClusterCondition.
func (in *ClusterCondition) DeepCopy() *ClusterCondition {
	if in == nil {
		return nil
	}
	out := new(ClusterCondition)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ExposeSpec) DeepCopyInto(out *ExposeSpec) {
	*out = *in
	in.Ingress.DeepCopyInto(&out.Ingress)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ExposeSpec.
func (in *ExposeSpec) DeepCopy() *ExposeSpec {
	if in == nil {
		return nil
	}
	out := new(ExposeSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HDFSDataNodeSpecTemplate) DeepCopyInto(out *HDFSDataNodeSpecTemplate) {
	*out = *in
	in.HadoopNodeSpec.DeepCopyInto(&out.HadoopNodeSpec)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HDFSDataNodeSpecTemplate.
func (in *HDFSDataNodeSpecTemplate) DeepCopy() *HDFSDataNodeSpecTemplate {
	if in == nil {
		return nil
	}
	out := new(HDFSDataNodeSpecTemplate)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HDFSNameNodeSpecTemplate) DeepCopyInto(out *HDFSNameNodeSpecTemplate) {
	*out = *in
	in.HadoopNodeSpec.DeepCopyInto(&out.HadoopNodeSpec)
	in.Expose.DeepCopyInto(&out.Expose)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HDFSNameNodeSpecTemplate.
func (in *HDFSNameNodeSpecTemplate) DeepCopy() *HDFSNameNodeSpecTemplate {
	if in == nil {
		return nil
	}
	out := new(HDFSNameNodeSpecTemplate)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HDFSSpec) DeepCopyInto(out *HDFSSpec) {
	*out = *in
	in.NameNode.DeepCopyInto(&out.NameNode)
	in.DataNode.DeepCopyInto(&out.DataNode)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HDFSSpec.
func (in *HDFSSpec) DeepCopy() *HDFSSpec {
	if in == nil {
		return nil
	}
	out := new(HDFSSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopCluster) DeepCopyInto(out *HadoopCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopCluster.
func (in *HadoopCluster) DeepCopy() *HadoopCluster {
	if in == nil {
		return nil
	}
	out := new(HadoopCluster)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *HadoopCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopClusterList) DeepCopyInto(out *HadoopClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]HadoopCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopClusterList.
func (in *HadoopClusterList) DeepCopy() *HadoopClusterList {
	if in == nil {
		return nil
	}
	out := new(HadoopClusterList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *HadoopClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopClusterSpec) DeepCopyInto(out *HadoopClusterSpec) {
	*out = *in
	in.HDFS.DeepCopyInto(&out.HDFS)
	in.Yarn.DeepCopyInto(&out.Yarn)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopClusterSpec.
func (in *HadoopClusterSpec) DeepCopy() *HadoopClusterSpec {
	if in == nil {
		return nil
	}
	out := new(HadoopClusterSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopClusterStatus) DeepCopyInto(out *HadoopClusterStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]ClusterCondition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.ReplicaStatuses != nil {
		in, out := &in.ReplicaStatuses, &out.ReplicaStatuses
		*out = make(map[ReplicaType]*ReplicaStatus, len(*in))
		for key, val := range *in {
			var outVal *ReplicaStatus
			if val == nil {
				(*out)[key] = nil
			} else {
				inVal := (*in)[key]
				in, out := &inVal, &outVal
				*out = new(ReplicaStatus)
				(*in).DeepCopyInto(*out)
			}
			(*out)[key] = outVal
		}
	}
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopClusterStatus.
func (in *HadoopClusterStatus) DeepCopy() *HadoopClusterStatus {
	if in == nil {
		return nil
	}
	out := new(HadoopClusterStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopJob) DeepCopyInto(out *HadoopJob) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopJob.
func (in *HadoopJob) DeepCopy() *HadoopJob {
	if in == nil {
		return nil
	}
	out := new(HadoopJob)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *HadoopJob) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopJobList) DeepCopyInto(out *HadoopJobList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]HadoopJob, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopJobList.
func (in *HadoopJobList) DeepCopy() *HadoopJobList {
	if in == nil {
		return nil
	}
	out := new(HadoopJobList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *HadoopJobList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopJobSpec) DeepCopyInto(out *HadoopJobSpec) {
	*out = *in
	if in.Arguments != nil {
		in, out := &in.Arguments, &out.Arguments
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	in.ExecutorSpec.DeepCopyInto(&out.ExecutorSpec)
	if in.Env != nil {
		in, out := &in.Env, &out.Env
		*out = make([]v1.EnvVar, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopJobSpec.
func (in *HadoopJobSpec) DeepCopy() *HadoopJobSpec {
	if in == nil {
		return nil
	}
	out := new(HadoopJobSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopJobStatus) DeepCopyInto(out *HadoopJobStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]JobCondition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopJobStatus.
func (in *HadoopJobStatus) DeepCopy() *HadoopJobStatus {
	if in == nil {
		return nil
	}
	out := new(HadoopJobStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopNodeSpec) DeepCopyInto(out *HadoopNodeSpec) {
	*out = *in
	in.HadoopPodSpec.DeepCopyInto(&out.HadoopPodSpec)
	if in.Lifecycle != nil {
		in, out := &in.Lifecycle, &out.Lifecycle
		*out = new(v1.Lifecycle)
		(*in).DeepCopyInto(*out)
	}
	if in.DeleteOnTermination != nil {
		in, out := &in.DeleteOnTermination, &out.DeleteOnTermination
		*out = new(bool)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopNodeSpec.
func (in *HadoopNodeSpec) DeepCopy() *HadoopNodeSpec {
	if in == nil {
		return nil
	}
	out := new(HadoopNodeSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *HadoopPodSpec) DeepCopyInto(out *HadoopPodSpec) {
	*out = *in
	if in.Env != nil {
		in, out := &in.Env, &out.Env
		*out = make([]v1.EnvVar, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.Replicas != nil {
		in, out := &in.Replicas, &out.Replicas
		*out = new(int32)
		**out = **in
	}
	if in.VolumeMounts != nil {
		in, out := &in.VolumeMounts, &out.VolumeMounts
		*out = make([]v1.VolumeMount, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	in.Resources.DeepCopyInto(&out.Resources)
	if in.SecurityContext != nil {
		in, out := &in.SecurityContext, &out.SecurityContext
		*out = new(v1.SecurityContext)
		(*in).DeepCopyInto(*out)
	}
	if in.ImagePullSecrets != nil {
		in, out := &in.ImagePullSecrets, &out.ImagePullSecrets
		*out = make([]v1.LocalObjectReference, len(*in))
		copy(*out, *in)
	}
	if in.Volumes != nil {
		in, out := &in.Volumes, &out.Volumes
		*out = make([]v1.Volume, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new HadoopPodSpec.
func (in *HadoopPodSpec) DeepCopy() *HadoopPodSpec {
	if in == nil {
		return nil
	}
	out := new(HadoopPodSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobCondition) DeepCopyInto(out *JobCondition) {
	*out = *in
	in.LastUpdateTime.DeepCopyInto(&out.LastUpdateTime)
	in.LastTransitionTime.DeepCopyInto(&out.LastTransitionTime)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new JobCondition.
func (in *JobCondition) DeepCopy() *JobCondition {
	if in == nil {
		return nil
	}
	out := new(JobCondition)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReplicaStatus) DeepCopyInto(out *ReplicaStatus) {
	*out = *in
	if in.Expect != nil {
		in, out := &in.Expect, &out.Expect
		*out = new(int32)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReplicaStatus.
func (in *ReplicaStatus) DeepCopy() *ReplicaStatus {
	if in == nil {
		return nil
	}
	out := new(ReplicaStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *YarnNodeManagerSpecTemplate) DeepCopyInto(out *YarnNodeManagerSpecTemplate) {
	*out = *in
	in.HadoopNodeSpec.DeepCopyInto(&out.HadoopNodeSpec)
	in.Expose.DeepCopyInto(&out.Expose)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new YarnNodeManagerSpecTemplate.
func (in *YarnNodeManagerSpecTemplate) DeepCopy() *YarnNodeManagerSpecTemplate {
	if in == nil {
		return nil
	}
	out := new(YarnNodeManagerSpecTemplate)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *YarnResourceManagerSpecTemplate) DeepCopyInto(out *YarnResourceManagerSpecTemplate) {
	*out = *in
	in.HadoopNodeSpec.DeepCopyInto(&out.HadoopNodeSpec)
	in.Expose.DeepCopyInto(&out.Expose)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new YarnResourceManagerSpecTemplate.
func (in *YarnResourceManagerSpecTemplate) DeepCopy() *YarnResourceManagerSpecTemplate {
	if in == nil {
		return nil
	}
	out := new(YarnResourceManagerSpecTemplate)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *YarnSpec) DeepCopyInto(out *YarnSpec) {
	*out = *in
	in.NodeManager.DeepCopyInto(&out.NodeManager)
	in.ResourceManager.DeepCopyInto(&out.ResourceManager)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new YarnSpec.
func (in *YarnSpec) DeepCopy() *YarnSpec {
	if in == nil {
		return nil
	}
	out := new(YarnSpec)
	in.DeepCopyInto(out)
	return out
}
