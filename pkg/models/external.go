package models

import (
	"fmt"
	"path/filepath"

	"github.com/ethpandaops/cbt/pkg/models/external"
)

type ExternalType string

const (
	ExternalTypeSQL ExternalType = external.ExternalTypeSQL
)

type External interface {
	GetType() string
	GetID() string
	GetConfig() external.ExternalConfig
	GetValue() string
}

func NewExternal(content []byte, filePath string) (External, error) {
	ext := filepath.Ext(filePath)

	switch ext {
	case ".sql":
		external, parseErr := external.NewExternalSQL(content)
		if parseErr != nil {
			return nil, parseErr
		}

		return external, nil
	}

	return nil, fmt.Errorf("invalid external type: %s", filePath)
}
