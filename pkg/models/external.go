package models

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ethpandaops/cbt/pkg/models/external"
)

var (
	// ErrInvalidExternalType is returned when an invalid external type is specified
	ErrInvalidExternalType = errors.New("invalid external type")
)

// ExternalType represents the type of an external model
type ExternalType string

const (
	// ExternalTypeSQL represents SQL external model type
	ExternalTypeSQL ExternalType = external.ExternalTypeSQL
)

// External defines the interface for external models
type External interface {
	GetType() string
	GetID() string
	GetConfig() external.Config
	GetValue() string
}

// NewExternal creates a new external model from file content
func NewExternal(content []byte, filePath string) (External, error) {
	ext := filepath.Ext(filePath)

	if ext == ExtSQL {
		model, parseErr := external.NewExternalSQL(content)
		if parseErr != nil {
			return nil, parseErr
		}

		return model, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrInvalidExternalType, filePath)
}
