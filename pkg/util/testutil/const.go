package testutil

import "time"

const (
	TestHadoopClusterName = "test-hadoop-cluster"

	ClusterCreationTimeout = 10 * time.Second
	ClusterDeletionTimeout = 5 * time.Second
)
