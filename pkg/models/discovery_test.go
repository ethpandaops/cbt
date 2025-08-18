package models

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelDiscovery_DiscoverAll(t *testing.T) {
	// Create temp directory structure
	tmpDir := t.TempDir()

	// Create model files
	externalDir := filepath.Join(tmpDir, "external")
	transformDir := filepath.Join(tmpDir, "transformations")
	require.NoError(t, os.MkdirAll(externalDir, 0o755))
	require.NoError(t, os.MkdirAll(transformDir, 0o755))

	// Create test files
	files := map[string]bool{
		filepath.Join(externalDir, "model1.sql"):      true,
		filepath.Join(externalDir, "model2.yaml"):     true,
		filepath.Join(transformDir, "transform1.yml"): false,
		filepath.Join(transformDir, "transform2.sql"): false,
		filepath.Join(transformDir, "ignored.txt"):    false, // Should be ignored
	}

	for path := range files {
		require.NoError(t, os.WriteFile(path, []byte("test"), 0o644))
	}

	// Test discovery
	discovery := NewModelDiscovery(tmpDir)
	models, err := discovery.DiscoverAll()

	assert.NoError(t, err)
	assert.Len(t, models, 4) // Only .sql, .yaml, .yml files

	// Verify external flags
	externalCount := 0
	transformCount := 0
	for _, model := range models {
		if model.IsExternal {
			externalCount++
		} else {
			transformCount++
		}
	}
	assert.Equal(t, 2, externalCount)
	assert.Equal(t, 2, transformCount)
}

func TestModelDiscovery_MissingDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Don't create subdirectories
	discovery := NewModelDiscovery(tmpDir)
	models, err := discovery.DiscoverAll()

	// Should not error on missing directories
	assert.NoError(t, err)
	assert.Empty(t, models)
}
