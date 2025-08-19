package transformation

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"
)

const TransformationTypeSQL = "sql"

type TransformationSQL struct {
	TransformationConfig `yaml:",inline"`
	Content              string `yaml:"-"`
}

type TransformationSQLParser struct{}

func NewTransformationSQL(content []byte) (*TransformationSQL, error) {
	parts := bytes.SplitN(content, []byte("\n---\n"), 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid frontmatter")
	}

	var config *TransformationSQL
	// Parse YAML frontmatter (skip "---\n" prefix)
	if err := yaml.Unmarshal(parts[0][4:], &config); err != nil {
		return nil, fmt.Errorf("failed to parse frontmatter: %w", err)
	}

	config.Content = string(parts[1])

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

func (c *TransformationSQL) Validate() error {
	if c.Content == "" {
		return fmt.Errorf("sql content is required")
	}

	return nil
}

func (c *TransformationSQL) GetType() string {
	return TransformationTypeSQL
}

func (c *TransformationSQL) GetConfig() TransformationConfig {
	return c.TransformationConfig
}

func (c *TransformationSQL) GetValue() string {
	return c.Content
}
