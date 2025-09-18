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
	BaseConfig Config  `yaml:",inline"` // Base configuration
	Handler    Handler // Type-specific handler
	Content    string  `yaml:"-"` // SQL content
}

// NewTransformationSQL creates a new transformation SQL model from content
func NewTransformationSQL(content []byte) (*SQL, error) {
	parts := bytes.SplitN(content, []byte("\n---\n"), 2)
	if len(parts) != 2 {
		return nil, ErrInvalidFrontmatter
	}

	// First, parse just the base config to determine the type
	var baseConfig Config
	yamlContent := parts[0][4:] // Skip "---\n"
	if err := yaml.Unmarshal(yamlContent, &baseConfig); err != nil {
		return nil, fmt.Errorf("failed to parse base config: %w", err)
	}

	// Create the appropriate handler based on the type using the factory
	adminTable := AdminTable{
		Database: "admin", // This will be set properly by the service
		Table:    "",      // This will be set properly by the service
	}

	handler, err := CreateHandler(baseConfig.Type, yamlContent, adminTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create handler for type %s: %w", baseConfig.Type, err)
	}

	return &SQL{
		BaseConfig: baseConfig,
		Handler:    handler,
		Content:    string(parts[1]),
	}, nil
}

// Validate checks if the transformation SQL model is valid
func (c *SQL) Validate() error {
	if c.Content == "" {
		return ErrSQLContentRequired
	}

	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}

	// Validate type-specific configuration
	if c.Handler != nil {
		return c.Handler.Validate()
	}

	return nil
}

// GetType returns the transformation model type
func (c *SQL) GetType() string {
	return TransformationTypeSQL
}

// GetConfig returns the base transformation model configuration
func (c *SQL) GetConfig() *Config {
	return &c.BaseConfig
}

// GetValue returns the SQL content
func (c *SQL) GetValue() string {
	return c.Content
}

// SetDefaultDatabase applies the default database if not already set
func (c *SQL) SetDefaultDatabase(defaultDB string) {
	c.BaseConfig.SetDefaults(defaultDB)
}

// GetHandler returns the type-specific handler
func (c *SQL) GetHandler() Handler {
	return c.Handler
}

// GetID returns the unique identifier
func (c *SQL) GetID() string {
	return c.BaseConfig.GetID()
}
