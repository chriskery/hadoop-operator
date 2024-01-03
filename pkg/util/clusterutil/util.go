package clusterutil

func IsRetryableExitCode(exitCode int32) bool {
	return exitCode >= 128
}
