package models

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverPathsDirectory(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.sql"), []byte("select 1"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "nested", "b.yaml"), []byte("key: value"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("nope"), 0o600))

	models, err := DiscoverPaths([]string{dir})
	require.NoError(t, err)
	require.Len(t, models, 2)
}

// Individual model file paths must be supported alongside directories: callers
// such as the xatu-cbt test harness configure models as explicit file paths,
// which worked prior to the os.Root rework (filepath.Walk visits a file path
// directly, while os.OpenRoot on a file fails with ENOTDIR).
func TestDiscoverPathsIndividualFile(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "model.sql")
	require.NoError(t, os.WriteFile(path, []byte("select 1"), 0o600))

	models, err := DiscoverPaths([]string{path})
	require.NoError(t, err)
	require.Len(t, models, 1)
	assert.Equal(t, path, models[0].FilePath)
	assert.Equal(t, ExtSQL, models[0].Extension)
	assert.Equal(t, []byte("select 1"), models[0].Content)
}

func TestDiscoverPathsMixedFileAndDirectory(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.sql"), []byte("select 1"), 0o600))

	fileDir := t.TempDir()
	path := filepath.Join(fileDir, "b.yml")
	require.NoError(t, os.WriteFile(path, []byte("key: value"), 0o600))

	models, err := DiscoverPaths([]string{dir, path})
	require.NoError(t, err)
	require.Len(t, models, 2)
}

func TestDiscoverPathsFileWithUnsupportedExtension(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "notes.txt")
	require.NoError(t, os.WriteFile(path, []byte("nope"), 0o600))

	models, err := DiscoverPaths([]string{path})
	require.NoError(t, err)
	assert.Empty(t, models)
}

func TestDiscoverPathsMissingPath(t *testing.T) {
	models, err := DiscoverPaths([]string{filepath.Join(t.TempDir(), "does-not-exist")})
	require.NoError(t, err)
	assert.Empty(t, models)
}

func TestDiscoverPathsFileSymlinkEscapingParentIsRejected(t *testing.T) {
	outside := t.TempDir()
	target := filepath.Join(outside, "secret.sql")
	require.NoError(t, os.WriteFile(target, []byte("select 'secret'"), 0o600))

	dir := t.TempDir()
	link := filepath.Join(dir, "model.sql")
	require.NoError(t, os.Symlink(target, link))

	_, err := DiscoverPaths([]string{link})
	require.Error(t, err)
}
