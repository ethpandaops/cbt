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

// openRootSkippingMissing opens a root-scoped handle at path. A missing path is
// treated as a soft skip (skip=true, nil error) so callers can ignore paths that
// disappear between discovery and open; any other error is returned.
func openRootSkippingMissing(path string) (root *os.Root, skip bool, err error) {
	root, err = os.OpenRoot(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}

		return nil, false, err
	}

	return root, false, nil
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

	root, skip, err := openRootSkippingMissing(parent)
	if err != nil {
		return nil, err
	}
	if skip {
		return nil, nil
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

	// os.Stat above already confirmed dir exists and is a directory, so a
	// missing-path result here would only arise from a concurrent removal race;
	// surface any open error rather than silently skipping.
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()

	var models []*ModelFile

	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		file, fErr := collectWalkedModel(root, dir, path, d, walkErr)
		if fErr != nil {
			return fErr
		}
		if file != nil {
			models = append(models, file)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return models, nil
}

// collectWalkedModel turns a single WalkDir entry into a *ModelFile. It returns
// (nil, nil) for entries that should be skipped (directories, non-model
// extensions, or paths that vanished mid-walk) and an error for unexpected
// failures.
func collectWalkedModel(
	root *os.Root,
	dir, path string,
	d os.DirEntry,
	walkErr error,
) (*ModelFile, error) {
	if walkErr != nil {
		if os.IsNotExist(walkErr) {
			return nil, nil
		}

		return nil, walkErr
	}

	if d.IsDir() {
		return nil, nil
	}

	ext := strings.ToLower(filepath.Ext(path))
	if ext != ExtSQL && ext != ExtYAML && ext != ExtYML {
		return nil, nil
	}

	relPath, relErr := filepath.Rel(dir, path)
	if relErr != nil {
		return nil, relErr
	}

	content, readErr := readFileFromRoot(root, relPath)
	if readErr != nil {
		return nil, readErr
	}

	return &ModelFile{
		FilePath:  path,
		Extension: ext,
		Content:   content,
	}, nil
}
