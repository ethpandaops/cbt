package transformation

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

var (
	// ErrExecRequired is returned when exec is not specified
	ErrExecRequired = errors.New("exec is required")
)

// TransformationTypeExec identifies exec transformation models
const TransformationTypeExec = "exec"

// Exec represents a transformation exec model with YAML config
type Exec struct {
	Config `yaml:",inline"`
	Exec   string `yaml:"exec"`
}

// ExecParser parses exec transformation models
type ExecParser struct{}

// NewTransformationExec creates a new transformation exec model from content
func NewTransformationExec(content []byte) (*Exec, error) {
	var config *Exec
	if err := yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// Validate checks if the transformation exec model is valid
func (c *Exec) Validate() error {
	if c.Exec == "" {
		return ErrExecRequired
	}

	return nil
}

// GetType returns the transformation model type
func (c *Exec) GetType() string {
	return TransformationTypeExec
}

// GetConfig returns the transformation model configuration
func (c *Exec) GetConfig() Config {
	return c.Config
}

// GetValue returns the exec command
func (c *Exec) GetValue() string {
	return c.Exec
}
