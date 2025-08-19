package transformation

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

const TransformationTypeExec = "exec"

type TransformationExec struct {
	TransformationConfig `yaml:",inline"`
	Exec                 string `yaml:"exec"`
}

type TransformationExecParser struct{}

func NewTransformationExec(content []byte) (*TransformationExec, error) {
	var config *TransformationExec
	if err := yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

func (c *TransformationExec) Validate() error {
	if c.Exec == "" {
		return fmt.Errorf("exec is required")
	}

	return nil
}

func (c *TransformationExec) GetType() string {
	return TransformationTypeExec
}

func (c *TransformationExec) GetConfig() TransformationConfig {
	return c.TransformationConfig
}

func (c *TransformationExec) GetValue() string {
	return c.Exec
}
