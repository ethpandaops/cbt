// Package models handles model discovery, parsing, and management
package models

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ModelFile represents a discovered model file
type ModelFile struct {
	FilePath   string
	IsExternal bool
}

// ModelDiscovery handles discovering model files from the filesystem
type ModelDiscovery struct {
	modelsDir string
}

// NewModelDiscovery creates a new model discovery instance
func NewModelDiscovery(modelsDir string) *ModelDiscovery {
	return &ModelDiscovery{modelsDir: modelsDir}
}

// DiscoverAll discovers all model files in the models directory
func (d *ModelDiscovery) DiscoverAll() ([]ModelFile, error) {
	var models []ModelFile

	externalPath := filepath.Join(d.modelsDir, "external")
	transformationPath := filepath.Join(d.modelsDir, "transformations")

	// Discover external models
	externalModels, err := d.discoverInPath(externalPath, true)
	if err != nil {
		return nil, fmt.Errorf("failed to discover external models: %w", err)
	}
	models = append(models, externalModels...)

	// Discover transformation models
	transformationModels, err := d.discoverInPath(transformationPath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to discover transformation models: %w", err)
	}
	models = append(models, transformationModels...)

	return models, nil
}

func (d *ModelDiscovery) discoverInPath(basePath string, isExternal bool) ([]ModelFile, error) {
	var models []ModelFile

	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // Skip if directory doesn't exist
			}
			return err
		}

		if info.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".sql" || ext == ".yaml" || ext == ".yml" {
			models = append(models, ModelFile{
				FilePath:   path,
				IsExternal: isExternal,
			})
		}

		return nil
	})

	return models, err
}
