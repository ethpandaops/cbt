package models

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverFileParentMissing(t *testing.T) {
	// Parent directory does not exist -> os.OpenRoot returns IsNotExist -> (nil, nil).
	path := filepath.Join(t.TempDir(), "missing-dir", "model.sql")
	got, err := discoverFile(path)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestDiscoverFileParentNotDirectory(t *testing.T) {
	// Parent is a regular file -> os.OpenRoot returns a non-IsNotExist error.
	regular := filepath.Join(t.TempDir(), "afile")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))

	path := filepath.Join(regular, "model.sql")
	_, err := discoverFile(path)
	require.Error(t, err)
	assert.False(t, os.IsNotExist(err))
}

func TestDiscoverInDirectoryStatNotDirectory(t *testing.T) {
	// Stat on a path under a regular file yields ENOTDIR (not IsNotExist).
	regular := filepath.Join(t.TempDir(), "afile")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))

	_, err := discoverInDirectory(filepath.Join(regular, "sub"))
	require.Error(t, err)
	assert.False(t, os.IsNotExist(err))
}

func TestDiscoverInDirectorySymlinkEscapeReadError(t *testing.T) {
	// A directory containing a .sql symlink that escapes the root makes the
	// root-scoped read fail during the walk, returning the error.
	outside := t.TempDir()
	target := filepath.Join(outside, "secret.sql")
	require.NoError(t, os.WriteFile(target, []byte("select 'secret'"), 0o600))

	dir := t.TempDir()
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "escape.sql")))

	_, err := discoverInDirectory(dir)
	require.Error(t, err)
}

func TestDiscoverInDirectoryOpenRootError(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("permission-based OpenRoot error not reproducible for root or on windows")
	}

	// A directory with no permissions: os.Stat succeeds (reports a dir) but
	// os.OpenRoot fails with a non-IsNotExist permission error.
	dir := t.TempDir()
	locked := filepath.Join(dir, "locked")
	require.NoError(t, os.Mkdir(locked, 0o000))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })

	_, err := discoverInDirectory(locked)
	require.Error(t, err)
	assert.False(t, os.IsNotExist(err))
}

func TestCollectWalkedModelWalkErrNotExist(t *testing.T) {
	// A vanished entry (IsNotExist walk error) is skipped rather than failing.
	file, err := collectWalkedModel(nil, "/dir", "/dir/gone.sql", nil, os.ErrNotExist)
	require.NoError(t, err)
	assert.Nil(t, file)
}

// fileDirEntry is a minimal os.DirEntry standing in for a regular file.
type fileDirEntry struct{ name string }

func (e fileDirEntry) Name() string             { return e.name }
func (fileDirEntry) IsDir() bool                { return false }
func (fileDirEntry) Type() os.FileMode          { return 0 }
func (fileDirEntry) Info() (os.FileInfo, error) { return nil, nil }

func TestCollectWalkedModelRelError(t *testing.T) {
	// filepath.Rel fails when dir is absolute but path is relative, exercising
	// the otherwise unreachable Rel error branch. A non-dir .sql entry gets past
	// the directory and extension checks.
	_, err := collectWalkedModel(nil, "/abs/dir", "relative/path.sql", fileDirEntry{name: "path.sql"}, nil)
	require.Error(t, err)
}

func TestDiscoverInDirectoryWalkError(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("permission-based walk error not reproducible for root or on windows")
	}

	// An unreadable subdirectory makes WalkDir invoke the callback with a
	// permission error (not IsNotExist), which propagates out.
	dir := t.TempDir()
	sub := filepath.Join(dir, "locked")
	require.NoError(t, os.Mkdir(sub, 0o000))
	t.Cleanup(func() { _ = os.Chmod(sub, 0o755) })

	_, err := discoverInDirectory(dir)
	require.Error(t, err)
}
