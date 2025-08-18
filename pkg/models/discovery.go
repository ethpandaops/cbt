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
	pathConfig *PathConfig
}

// NewModelDiscovery creates a new model discovery instance
func NewModelDiscovery(pathConfig *PathConfig) *ModelDiscovery {
	// Ensure defaults are set
	if pathConfig == nil {
		pathConfig = &PathConfig{}
	}
	pathConfig.SetDefaults()
	return &ModelDiscovery{pathConfig: pathConfig}
}

// DiscoverAll discovers all model files in the configured paths
func (d *ModelDiscovery) DiscoverAll() ([]ModelFile, error) {
	var models []ModelFile

	// Discover external models from all configured paths
	for _, path := range d.pathConfig.GetExternalPaths() {
		externalModels, err := d.discoverInPath(path, true)
		if err != nil {
			return nil, fmt.Errorf("failed to discover external models in %s: %w", path, err)
		}
		models = append(models, externalModels...)
	}

	// Discover transformation models from all configured paths
	for _, path := range d.pathConfig.GetTransformationPaths() {
		transformationModels, err := d.discoverInPath(path, false)
		if err != nil {
			return nil, fmt.Errorf("failed to discover transformation models in %s: %w", path, err)
		}
		models = append(models, transformationModels...)
	}

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
