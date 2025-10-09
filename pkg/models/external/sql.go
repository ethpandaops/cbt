package external

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

// ExternalTypeSQL identifies SQL external models
const ExternalTypeSQL = "sql"

// SQL represents an external SQL model with YAML frontmatter
type SQL struct {
	Config  `yaml:",inline"`
	Content string `yaml:"-"`
}

// SQLParser parses SQL external models
type SQLParser struct{}

// NewExternalSQL creates a new external SQL model from content
func NewExternalSQL(content []byte) (*SQL, error) {
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

	return config, nil
}

// Validate checks if the external SQL model is valid
func (c *SQL) Validate() error {
	if c.Content == "" {
		return ErrSQLContentRequired
	}

	return c.Config.Validate()
}

// GetType returns the external model type
func (c *SQL) GetType() string {
	return ExternalTypeSQL
}

// GetConfig returns the external model configuration
func (c *SQL) GetConfig() Config {
	return c.Config
}

// GetConfigMutable returns a mutable pointer to the external model configuration
func (c *SQL) GetConfigMutable() *Config {
	return &c.Config
}

// GetValue returns the SQL content
func (c *SQL) GetValue() string {
	return c.Content
}

// SetDefaultDatabase applies the default database if not already set
func (c *SQL) SetDefaultDatabase(defaultDB string) {
	c.SetDefaults(defaultDB)
}

// GetIntervalType returns the interval type for this external model
func (c *SQL) GetIntervalType() string {
	if c.Interval != nil {
		return c.Interval.Type
	}
	return ""
}
