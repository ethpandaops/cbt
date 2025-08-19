package models

import (
	"fmt"
	"path/filepath"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

type TransformationType string

const (
	TransformationTypeSQL  TransformationType = transformation.TransformationTypeSQL
	TransformationTypeExec TransformationType = transformation.TransformationTypeExec
)

type Transformation interface {
	GetType() string
	GetID() string
	GetConfig() transformation.TransformationConfig
	GetValue() string
}

func NewTransformation(content []byte, filePath string) (Transformation, error) {
	ext := filepath.Ext(filePath)

	switch ext {
	case ".sql":
		transformation, parseErr := transformation.NewTransformationSQL(content)
		if parseErr != nil {
			return nil, parseErr
		}

		return transformation, nil
	case ".yaml", ".yml":
		transformation, parseErr := transformation.NewTransformationExec(content)
		if parseErr != nil {
			return nil, parseErr
		}

		return transformation, nil
	}

	return nil, fmt.Errorf("invalid transformation type: %s", filePath)
}
