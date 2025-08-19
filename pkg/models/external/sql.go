package external

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"
)

const ExternalTypeSQL = "sql"

type ExternalSQL struct {
	ExternalConfig `yaml:",inline"`
	Content        string `yaml:"-"`
}

type ExternalSQLParser struct{}

func NewExternalSQL(content []byte) (*ExternalSQL, error) {
	parts := bytes.SplitN(content, []byte("\n---\n"), 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid frontmatter")
	}

	var config *ExternalSQL
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

func (c *ExternalSQL) Validate() error {
	if c.Content == "" {
		return fmt.Errorf("sql content is required")
	}

	return nil
}

func (c *ExternalSQL) GetType() string {
	return ExternalTypeSQL
}

func (c *ExternalSQL) GetConfig() ExternalConfig {
	return c.ExternalConfig
}

func (c *ExternalSQL) GetValue() string {
	return c.Content
}
