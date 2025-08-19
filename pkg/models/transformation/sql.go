// Package transformation provides transformation model configuration and validation
package transformation

import (
	"bytes"
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

var (
	// ErrInvalidFrontmatter is returned when frontmatter is invalid
	ErrInvalidFrontmatter = errors.New("invalid frontmatter")
	// ErrSQLContentRequired is returned when SQL content is not specified
	ErrSQLContentRequired = errors.New("sql content is required")
)

// TransformationTypeSQL identifies SQL transformation models
const TransformationTypeSQL = "sql"

// SQL represents a transformation SQL model with YAML frontmatter
type SQL struct {
	Config  `yaml:",inline"`
	Content string `yaml:"-"`
}

// SQLParser parses SQL transformation models
type SQLParser struct{}

// NewTransformationSQL creates a new transformation SQL model from content
func NewTransformationSQL(content []byte) (*SQL, error) {
	parts := bytes.SplitN(content, []byte("\n---\n"), 2)
	if len(parts) != 2 {
		return nil, ErrInvalidFrontmatter
	}

	var config *SQL
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

// Validate checks if the transformation SQL model is valid
func (c *SQL) Validate() error {
	if c.Content == "" {
		return ErrSQLContentRequired
	}

	return nil
}

// GetType returns the transformation model type
func (c *SQL) GetType() string {
	return TransformationTypeSQL
}

// GetConfig returns the transformation model configuration
func (c *SQL) GetConfig() Config {
	return c.Config
}

// GetValue returns the SQL content
func (c *SQL) GetValue() string {
	return c.Content
}
