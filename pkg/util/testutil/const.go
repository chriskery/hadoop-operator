package testutil

import (
	corev1 "k8s.io/api/core/v1"
	"time"
)

const (
	TestHadoopClusterName      = "test-hadoop-cluster"
	TestHadoopClusterNamespace = corev1.NamespaceDefault

	ClusterCreationTimeout = 10 * time.Second
	ClusterDeletionTimeout = 5 * time.Second
)
