package transformation

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

// Error variables for handler operations
var (
	ErrTypeRequired       = errors.New("transformation type is required")
	ErrInvalidType        = errors.New("invalid transformation type")
	ErrHandlerNotFound    = errors.New("handler not registered")
	ErrAdminTableNotFound = errors.New("admin table not configured")
)

// BaseConfig is the minimal configuration to determine the transformation type
type BaseConfig struct {
	Type Type `yaml:"type"`
}

// ParseType parses only the type field from YAML to determine the transformation type
func ParseType(data []byte) (Type, error) {
	var base BaseConfig
	if err := yaml.Unmarshal(data, &base); err != nil {
		return "", fmt.Errorf("failed to parse transformation type: %w", err)
	}

	if base.Type == "" {
		return "", ErrTypeRequired
	}

	if base.Type != TypeIncremental && base.Type != TypeScheduled {
		return "", fmt.Errorf("%w: %s (must be 'incremental' or 'scheduled')", ErrInvalidType, base.Type)
	}

	return base.Type, nil
}

// NewHandler creates the appropriate handler based on the transformation type
func NewHandler(transformationType Type, data []byte, adminConfig map[string]AdminTable) (Handler, error) {
	adminTable, ok := adminConfig[string(transformationType)]
	if !ok {
		return nil, fmt.Errorf("%w for transformation type: %s", ErrAdminTableNotFound, transformationType)
	}

	return CreateHandler(transformationType, data, adminTable)
}
