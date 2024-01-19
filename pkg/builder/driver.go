package builder

import (
	"context"
	"fmt"
	"github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"github.com/chriskery/hadoop-operator/pkg/control"
	"github.com/chriskery/hadoop-operator/pkg/util"
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
	AlwaysBuildCompletedBuilder

	client.Client

	Recorder record.EventRecorder

	PodControl control.PodControlInterface
}

func (driverBuilder *DriverBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()

	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)
	driverBuilder.Client = mgr.GetClient()
	driverBuilder.Recorder = recorder
	driverBuilder.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder, ClusterConfig: cfg}
}

// Build creates driver pod for hadoop application
func (driverBuilder *DriverBuilder) Build(obj interface{}, objStatus interface{}) (bool, error) {
	application := obj.(*v1alpha1.HadoopApplication)
	driverPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      util.GetReplicaName(application, v1alpha1.ReplicaTypeDriver),
		Namespace: application.GetNamespace(),
	}}
	err := driverBuilder.Get(context.Background(), types.NamespacedName{Namespace: driverPod.GetNamespace(), Name: driverPod.GetName()}, driverPod)
	if err == nil || !errors.IsNotFound(err) {
		return false, err
	}

	if !IsHDFSReady(&v1alpha1.HadoopCluster{ObjectMeta: metav1.ObjectMeta{
		Name:      application.GetName(),
		Namespace: application.GetNamespace(),
	}}, driverBuilder.PodControl) {
		msg := fmt.Sprintf("HDFS is not ready, waiting for driver application running %s", driverPod.GetName())
		driverBuilder.Recorder.Eventf(application, corev1.EventTypeWarning, "HDFSNotReady", msg)
		return false, fmt.Errorf(msg)
	}

	podTemplateSpec := driverBuilder.genDriverPodSpec(application, driverPod.GetName())
	ownerRef := util.GenOwnerReference(application, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopApplicationKind).Kind)
	if err = driverBuilder.PodControl.CreatePodsWithControllerRef(application.GetNamespace(), podTemplateSpec, application, ownerRef); err != nil {
		return false, err
	}

	applicationStatus := objStatus.(*v1alpha1.HadoopApplicationStatus)
	msg := fmt.Sprintf("Driver application %s submitted", driverPod.GetName())
	err = util.UpdateApplicationConditions(applicationStatus, v1alpha1.ApplicationSubmitted, util.HadoopApplicationSubmittedReason, msg)
	if err != nil {
		return false, err
	}

	return true, nil
}

// genDriverPodSpec generates driver pod spec for hadoop application
func (driverBuilder *DriverBuilder) genDriverPodSpec(application *v1alpha1.HadoopApplication, driverPodName string) *corev1.PodTemplateSpec {
	labels := getLabels(application, v1alpha1.ReplicaTypeDriver)
	util.MergeMap(labels, application.GetLabels())

	driverPodSpec := application.Spec.ExecutorSpec
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

	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(podTemplateSpec.Spec.Volumes, util.GetReplicaName(application, v1alpha1.ReplicaTypeConfigMap))

	volumeMounts := driverPodSpec.VolumeMounts
	volumeMounts = appendHadoopConfigMapVolumeMount(volumeMounts)

	submitCMD := driverBuilder.getSubmitCMD(application)
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
		Env:             application.Spec.Env,
		ImagePullPolicy: driverPodSpec.ImagePullPolicy,
		SecurityContext: driverPodSpec.SecurityContext,
	}}

	podTemplateSpec.Spec.Containers = containers

	setPodEnv(
		&v1alpha1.HadoopCluster{ObjectMeta: *application.ObjectMeta.DeepCopy()},
		podTemplateSpec.Spec.Containers,
		v1alpha1.ReplicaTypeDriver,
	)
	return podTemplateSpec
}

// Clean deletes driver pod
func (driverBuilder *DriverBuilder) Clean(obj interface{}) error {
	application := obj.(*v1alpha1.HadoopApplication)

	driverPodName := util.GetReplicaName(application, v1alpha1.ReplicaTypeDriver)
	err := driverBuilder.PodControl.DeletePod(application.GetNamespace(), driverPodName, &corev1.Pod{})
	if err != nil {
		return err
	}
	return nil
}

func (driverBuilder *DriverBuilder) getSubmitCMD(application *v1alpha1.HadoopApplication) string {
	args := []string{"jar", application.Spec.MainApplicationFile}
	args = append(args, application.Spec.Arguments...)

	return fmt.Sprintf("hadoop %s", strings.Join(args, " "))
}
