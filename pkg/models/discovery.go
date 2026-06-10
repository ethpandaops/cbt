package models

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

// readFileFromRoot reads a file using root-scoped APIs to prevent symlink TOCTOU traversal.
func readFileFromRoot(root *os.Root, relPath string) ([]byte, error) {
	f, err := root.Open(relPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	return io.ReadAll(f)
}

// ModelFile represents a discovered model file with its content
type ModelFile struct {
	FilePath  string
	Extension string
	Content   []byte
}

// DiscoverPaths finds model files (.sql, .yaml, .yml) at the given paths.
// Each path may be a directory, which is walked recursively, or an individual
// model file.
func DiscoverPaths(directories []string) ([]*ModelFile, error) {
	var models []*ModelFile

	for _, dir := range directories {
		discovered, err := discoverInDirectory(dir)
		if err != nil {
			return nil, err
		}

		models = append(models, discovered...)
	}

	return models, nil
}

// discoverFile loads a single model file, root-scoped to its parent directory
// so the file itself cannot be a symlink escaping it.
func discoverFile(path string) ([]*ModelFile, error) {
	ext := strings.ToLower(filepath.Ext(path))
	if ext != ExtSQL && ext != ExtYAML && ext != ExtYML {
		return nil, nil
	}

	parent := filepath.Dir(path)

	root, err := os.OpenRoot(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}
	defer func() { _ = root.Close() }()

	content, err := readFileFromRoot(root, filepath.Base(path))
	if err != nil {
		return nil, err
	}

	return []*ModelFile{{
		FilePath:  path,
		Extension: ext,
		Content:   content,
	}}, nil
}

func discoverInDirectory(dir string) ([]*ModelFile, error) {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	if !info.IsDir() {
		return discoverFile(dir)
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}
	defer func() { _ = root.Close() }()

	var models []*ModelFile

	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}

			return err
		}

		if d.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		if ext == ExtSQL || ext == ExtYAML || ext == ExtYML {
			relPath, relErr := filepath.Rel(dir, path)
			if relErr != nil {
				return relErr
			}

			content, readErr := readFileFromRoot(root, relPath)
			if readErr != nil {
				return readErr
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

	return models, nil
}
