package models

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

var (
	// ErrInvalidTransformationType is returned when an invalid transformation type is specified
	ErrInvalidTransformationType = errors.New("invalid transformation type")
)

// TransformationType defines the type of transformation model
type TransformationType string

const (
	// TransformationTypeSQL represents SQL transformation models
	TransformationTypeSQL TransformationType = transformation.TransformationTypeSQL
	// TransformationTypeExec represents exec transformation models
	TransformationTypeExec TransformationType = transformation.TransformationTypeExec

	// ExtSQL is the SQL file extension
	ExtSQL = ".sql"
	// ExtYAML is the YAML file extension
	ExtYAML = ".yaml"
	// ExtYML is the alternative YAML file extension
	ExtYML = ".yml"
)

// Transformation defines the interface for transformation models
type Transformation interface {
	GetType() string
	GetID() string
	GetConfig() *transformation.Config
	GetValue() string
}

// NewTransformation creates a new transformation model from file content
func NewTransformation(content []byte, filePath string) (Transformation, error) {
	ext := filepath.Ext(filePath)

	switch ext {
	case ExtSQL:
		model, parseErr := transformation.NewTransformationSQL(content)
		if parseErr != nil {
			return nil, parseErr
		}

		return model, nil
	case ExtYAML, ExtYML:
		model, parseErr := transformation.NewTransformationExec(content)
		if parseErr != nil {
			return nil, parseErr
		}

		return model, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrInvalidTransformationType, filePath)
}
