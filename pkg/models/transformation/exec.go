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
	Config  `yaml:",inline"`
	Exec    string  `yaml:"exec"`
	Handler Handler `yaml:"-"` // Type-specific handler
}

// NewTransformationExec creates a new transformation exec model from content
func NewTransformationExec(content []byte) (*Exec, error) {
	var config *Exec
	if err := yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	// Create the appropriate handler based on the type using the factory
	adminTable := AdminTable{
		Database: "admin", // This will be set properly by the service
		Table:    "",      // This will be set properly by the service
	}

	handler, err := CreateHandler(config.Type, content, adminTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create handler for type %s: %w", config.Type, err)
	}

	config.Handler = handler

	return config, nil
}

// Validate checks if the transformation exec model is valid
func (c *Exec) Validate() error {
	if c.Exec == "" {
		return ErrExecRequired
	}

	if err := c.Config.Validate(); err != nil {
		return err
	}

	// Validate type-specific configuration
	if c.Handler != nil {
		return c.Handler.Validate()
	}

	return nil
}

// GetType returns the transformation model type
func (c *Exec) GetType() string {
	return TransformationTypeExec
}

// GetConfig returns the transformation model configuration
func (c *Exec) GetConfig() *Config {
	return &c.Config
}

// GetValue returns the exec command
func (c *Exec) GetValue() string {
	return c.Exec
}

// SetDefaultDatabase applies the default database if not already set
func (c *Exec) SetDefaultDatabase(defaultDB string) {
	c.SetDefaults(defaultDB)
}

// GetID returns the unique identifier
func (c *Exec) GetID() string {
	return c.Config.GetID()
}

// GetHandler returns the type-specific handler
func (c *Exec) GetHandler() Handler {
	return c.Handler
}
