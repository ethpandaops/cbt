package models

import (
	"os"
	"path/filepath"
	"strings"
)

// ModelFile represents a discovered model file with its content
type ModelFile struct {
	FilePath  string
	Extension string
	Content   []byte
}

// DiscoverPaths walks directories to find model files (.sql, .yaml, .yml)
func DiscoverPaths(directories []string) ([]*ModelFile, error) {
	var models []*ModelFile

	for _, path := range directories {
		err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
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
			if ext == ExtSQL || ext == ExtYAML || ext == ExtYML {
				content, err := os.ReadFile(path) // #nosec G304 - path is validated model file
				if err != nil {
					return err
				}

				models = append(models, &ModelFile{
					FilePath:  path,
					Extension: ext,
					Content:   content,
				})
			}

			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	return models, nil
}
