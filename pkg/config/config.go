package config

// Config is the global configuration for the training operator.
var Config struct {
	HadoopInitContainerTemplateFile string
	HadoopInitContainerImage        string
}

const (
	// HadoopInitContainerImageDefault is the default image for the Hadoop
	// init container.
	HadoopInitContainerImageDefault = "alpine:3.10"
	// HadoopInitContainerTemplateFileDefault is the default template file for
	// the Hadoop init container.
	HadoopInitContainerTemplateFileDefault = "/etc/config/initContainer.yaml"
)
