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

var _ Builder = &DataloaderBuilder{}

type DataloaderBuilder struct {
	client.Client

	PodControl control.PodControlInterface
}

func (dataloaderBuilder *DataloaderBuilder) SetupWithManager(mgr manager.Manager, recorder record.EventRecorder) {
	cfg := mgr.GetConfig()

	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)
	dataloaderBuilder.Client = mgr.GetClient()
	dataloaderBuilder.PodControl = control.RealPodControl{KubeClient: kubeClientSet, Recorder: recorder}
}

// Build creates dataloader pod for hadoop application
func (dataloaderBuilder *DataloaderBuilder) Build(obj interface{}, objStatus interface{}) error {
	application := obj.(*v1alpha1.HadoopApplication)
	if application.Spec.DataLoaderSpec == nil {
		return fmt.Errorf("DataloaderSpec is nil")
	}

	dataloaderPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      util.GetReplicaName(application, v1alpha1.ReplicaTypeDataloader),
		Namespace: application.GetNamespace(),
	}}
	err := dataloaderBuilder.Get(context.Background(), types.NamespacedName{Namespace: dataloaderPod.GetNamespace(), Name: dataloaderPod.GetName()}, dataloaderPod)
	if err == nil || !errors.IsNotFound(err) {
		return err
	}

	podTemplateSpec := dataloaderBuilder.genDataloaderPodSpec(application, dataloaderPod.GetName())
	dataloaderPod.Labels = podTemplateSpec.Labels

	ownerRef := util.GenOwnerReference(application, v1alpha1.GroupVersion.WithKind(v1alpha1.HadoopApplicationKind).Kind)
	if err = dataloaderBuilder.PodControl.CreatePodsWithControllerRef(application.GetNamespace(), podTemplateSpec, application, ownerRef); err != nil {
		return err
	}

	applicationStatus := objStatus.(*v1alpha1.HadoopApplicationStatus)
	msg := fmt.Sprintf("Hadoop application %s dataloading", dataloaderPod.GetName())
	err = util.UpdateApplicationConditions(applicationStatus, v1alpha1.ApplicationDataLoading, util.HadoopApplicationSubmittedReason, msg)
	if err != nil {
		return err
	}

	return nil
}

// Clean deletes dataloader pod
func (dataloaderBuilder *DataloaderBuilder) Clean(obj interface{}) error {
	application := obj.(*v1alpha1.HadoopApplication)

	dataloaderPodName := util.GetReplicaName(application, v1alpha1.ReplicaTypeDataloader)
	err := dataloaderBuilder.PodControl.DeletePod(application.GetNamespace(), dataloaderPodName, &corev1.Pod{})
	if err != nil {
		return err
	}
	return nil
}

func (dataloaderBuilder *DataloaderBuilder) genDataloaderPodSpec(
	application *v1alpha1.HadoopApplication,
	dataloaderName string,
) *corev1.PodTemplateSpec {
	labels := getLabels(application, v1alpha1.ReplicaTypeDataloader)
	util.MergeMap(labels, application.GetLabels())

	podTemplateSpec := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			Name:   dataloaderName,
		},
		Spec: corev1.PodSpec(*application.Spec.DataLoaderSpec),
	}

	podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicyNever

	if podTemplateSpec.Spec.Volumes == nil {
		podTemplateSpec.Spec.Volumes = make([]corev1.Volume, 0)
	}
	podTemplateSpec.Spec.Volumes = appendHadoopConfigMapVolume(
		podTemplateSpec.Spec.Volumes,
		util.GetReplicaName(application, v1alpha1.ReplicaTypeConfigMap),
	)

	for index, container := range podTemplateSpec.Spec.Containers {
		container.VolumeMounts = appendHadoopConfigMapVolumeMount(container.VolumeMounts)

		dataloaderCMD := strings.Join(container.Command, " ")
		dataloaderCMD += strings.Join(container.Args, " ")
		driverCmd := []string{
			"sh",
			"-c",
			strings.Join([]string{entrypointCmd, "sleep 20", dataloaderCMD}, " && "),
		}
		container.Command = driverCmd
		container.Args = []string{}

		podTemplateSpec.Spec.Containers[index] = container
	}

	setPodEnv(
		&v1alpha1.HadoopCluster{ObjectMeta: *application.ObjectMeta.DeepCopy()},
		podTemplateSpec.Spec.Containers,
		v1alpha1.ReplicaTypeDataloader,
	)
	setInitContainer(
		&v1alpha1.HadoopCluster{ObjectMeta: *application.ObjectMeta.DeepCopy()},
		v1alpha1.ReplicaTypeDataloader,
		podTemplateSpec,
	)
	return podTemplateSpec
}
