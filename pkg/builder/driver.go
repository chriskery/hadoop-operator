package builder

import (
	"context"
	"fmt"
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-cluster-operator/pkg/control"
	"github.com/chriskery/hadoop-cluster-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strings"
)

var _ Builder = &DriverBuilder{}

type DriverBuilder struct {
	client.Client

	PodControl control.PodControlInterface
}

func (driverBuilder *DriverBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()

	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)
	driverBuilder.Client = mgr.GetClient()
	driverBuilder.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder}
}

// Build creates driver pod for hadoop job
func (driverBuilder *DriverBuilder) Build(obj interface{}, objStatus interface{}) error {
	job := obj.(*v1alpha1.HadoopJob)
	driverPod := &corev1.Pod{}
	driverPodName := util.GetReplicaName(job, v1alpha1.ReplicaTypeDriver)
	err := driverBuilder.Get(context.Background(), types.NamespacedName{Namespace: job.GetNamespace(), Name: driverPodName}, driverPod)
	if err == nil || !errors.IsNotFound(err) {
		return err
	}

	podTemplateSpec := driverBuilder.genDriverPodSpec(job, driverPodName)
	ownerRef := util.GenOwnerReference(job, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopJobKind).Kind)
	if err = driverBuilder.PodControl.CreatePodsWithControllerRef(job.GetNamespace(), podTemplateSpec, job, ownerRef); err != nil {
		return err
	}

	jobStatus := objStatus.(*v1alpha1.HadoopJobStatus)
	msg := fmt.Sprintf("Driver job %s submitted", driverPodName)
	err = util.UpdateJobConditions(jobStatus, v1alpha1.JobSubmitted, util.HadoopJobSubmittedReason, msg)
	if err != nil {
		return err
	}

	return nil
}

// genDriverPodSpec generates driver pod spec for hadoop job
func (driverBuilder *DriverBuilder) genDriverPodSpec(job *v1alpha1.HadoopJob, driverPodName string) *corev1.PodTemplateSpec {
	labels := job.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[v1alpha1.ReplicaTypeLabel] = string(v1alpha1.ReplicaTypeDriver)
	labels[v1alpha1.JobNameLabel] = job.GetName()

	driverPodSpec := job.Spec.ExecutorSpec
	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			Name:   driverPodName,
		},
		Spec: corev1.PodSpec{
			Volumes:       driverPodSpec.Volumes,
			RestartPolicy: corev1.RestartPolicyNever,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
		},
	}

	if podTemplateSpec.Spec.Volumes == nil {
		podTemplateSpec.Spec.Volumes = make([]corev1.Volume, 0)
	}

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetReplicaName(job, v1alpha1.ReplicaTypeConfigMap))

	volumeMounts := driverPodSpec.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	submitCMD := driverBuilder.getSubmitCMD(job)

	driverCmd := []string{
		"sh",
		"-c",
		strings.Join([]string{entrypointCmd, submitCMD}, " && "),
	}
	containers := []corev1.Container{{
		Name:            string(v1alpha1.ReplicaTypeDriver),
		Image:           driverPodSpec.Image,
		Command:         driverCmd,
		Resources:       driverPodSpec.Resources,
		VolumeMounts:    volumeMounts,
		Env:             job.Spec.Env,
		ImagePullPolicy: driverPodSpec.ImagePullPolicy,
		SecurityContext: driverPodSpec.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers

	setPodEnv(&v1alpha1.HadoopCluster{ObjectMeta: *job.ObjectMeta.DeepCopy()}, podTemplateSpec.Spec.Containers, v1alpha1.ReplicaTypeDriver)
}
	job := obj.(*v1alpha1.HadoopJob)

	driverPodName := util.GetReplicaName(job, v1alpha1.ReplicaTypeDriver)
	err := driverBuilder.PodControl.DeletePod(job.GetNamespace(), driverPodName, &corev1.Pod{})
	if err != nil {
		return err
	}
	return nil
}

func (driverBuilder *DriverBuilder) getSubmitCMD(job *v1alpha1.HadoopJob) string {
	args := []string{"jar", job.Spec.MainApplicationFile}
	args = append(args, job.Spec.Arguments...)

	return fmt.Sprintf("hadoop %s", strings.Join(args, " "))
}
