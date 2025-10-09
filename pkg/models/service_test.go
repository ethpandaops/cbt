package models

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceWithDefaultDatabases(t *testing.T) {
	tests := []struct {
		name                     string
		config                   *Config
		expectedExternalDB       string
		expectedTransformationDB string
	}{
		{
			name: "both model types have different default databases",
			config: &Config{
				External: ExternalConfig{
					Paths:           []string{},
					DefaultDatabase: "raw_data",
				},
				Transformation: TransformationConfig{
					Paths:           []string{},
					DefaultDatabase: "analytics",
				},
			},
			expectedExternalDB:       "raw_data",
			expectedTransformationDB: "analytics",
		},
		{
			name: "both model types have same default database",
			config: &Config{
				External: ExternalConfig{
					Paths:           []string{},
					DefaultDatabase: "shared_db",
				},
				Transformation: TransformationConfig{
					Paths:           []string{},
					DefaultDatabase: "shared_db",
				},
			},
			expectedExternalDB:       "shared_db",
			expectedTransformationDB: "shared_db",
		},
		{
			name: "only external has default database",
			config: &Config{
				External: ExternalConfig{
					Paths:           []string{},
					DefaultDatabase: "external_db",
				},
				Transformation: TransformationConfig{
					Paths:           []string{},
					DefaultDatabase: "",
				},
			},
			expectedExternalDB:       "external_db",
			expectedTransformationDB: "",
		},
		{
			name: "only transformation has default database",
			config: &Config{
				External: ExternalConfig{
					Paths:           []string{},
					DefaultDatabase: "",
				},
				Transformation: TransformationConfig{
					Paths:           []string{},
					DefaultDatabase: "trans_db",
				},
			},
			expectedExternalDB:       "",
			expectedTransformationDB: "trans_db",
		},
		{
			name: "neither has default database",
			config: &Config{
				External: ExternalConfig{
					Paths: []string{},
				},
				Transformation: TransformationConfig{
					Paths: []string{},
				},
			},
			expectedExternalDB:       "",
			expectedTransformationDB: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validate config sets defaults properly
			err := tt.config.Validate()
			assert.NoError(t, err)

			// Check that default databases are correctly set
			assert.Equal(t, tt.expectedExternalDB, tt.config.External.DefaultDatabase)
			assert.Equal(t, tt.expectedTransformationDB, tt.config.Transformation.DefaultDatabase)

			// Verify that paths are set to defaults when empty
			assert.Equal(t, []string{"models/external"}, tt.config.External.Paths)
			assert.Equal(t, []string{"models/transformations"}, tt.config.Transformation.Paths)
		})
	}
}

func TestServiceCreation(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	// Create a config with default databases
	cfg := &Config{
		External: ExternalConfig{
			Paths:           []string{"testdata/external"},
			DefaultDatabase: "test_external_db",
		},
		Transformation: TransformationConfig{
			Paths:           []string{"testdata/transformations"},
			DefaultDatabase: "test_trans_db",
		},
	}

	clickhouseCfg := &clickhouse.Config{
		URL:         "http://localhost:8123",
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
	}

	// Create service (without actual Redis connection for unit test)
	svc, err := NewService(log, cfg, (*redis.Client)(nil), clickhouseCfg)
	require.NoError(t, err)
	require.NotNil(t, svc)

	// Verify the service was created successfully
	// The actual default database application is tested in the individual model tests
	assert.Equal(t, "test_external_db", cfg.External.DefaultDatabase)
	assert.Equal(t, "test_trans_db", cfg.Transformation.DefaultDatabase)
}

func TestServiceDependencyPlaceholders(t *testing.T) {
	// Create temporary directories for test models
	transformationDir := t.TempDir()
	externalDir := t.TempDir()

	// Create external models that will be referenced
	externalContent1 := `---
table: beacon_blocks
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT * FROM test`

	externalContent2 := `---
database: custom_db
table: custom_table
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT * FROM test`

	// Write external models
	err := os.WriteFile(filepath.Join(externalDir, "beacon_blocks.sql"), []byte(externalContent1), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(externalDir, "custom.sql"), []byte(externalContent2), 0o644)
	require.NoError(t, err)

	// Create transformation models with placeholder dependencies
	transformationContent := `type: incremental
table: test_transform
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - "{{external}}.beacon_blocks"
  - "{{transformation}}.other_transform"
  - custom_db.custom_table
exec: "echo test"`

	err = os.WriteFile(filepath.Join(transformationDir, "test.yml"), []byte(transformationContent), 0o644)
	require.NoError(t, err)

	// Create another transformation that uses default database
	transformationContent2 := `type: incremental
table: other_transform
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - "{{external}}.beacon_blocks"
exec: "echo test"`

	err = os.WriteFile(filepath.Join(transformationDir, "other.yml"), []byte(transformationContent2), 0o644)
	require.NoError(t, err)

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	clickhouseCfg := &clickhouse.Config{
		URL:         "http://localhost:8123",
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
	}

	config := &Config{
		External: ExternalConfig{
			Paths:           []string{externalDir},
			DefaultDatabase: "ethereum",
		},
		Transformation: TransformationConfig{
			Paths:           []string{transformationDir},
			DefaultDatabase: "analytics",
		},
	}

	svc, err := NewService(log, config, nil, clickhouseCfg)
	require.NoError(t, err)

	err = svc.Start()
	require.NoError(t, err)

	// Get the service implementation to access transformationModels
	s := svc.(*service)

	// Verify dependencies were substituted correctly
	for _, model := range s.transformationModels {
		verifyModelDependencies(t, model)
	}
}

// verifyModelDependencies is a helper to check model dependencies
func verifyModelDependencies(t *testing.T, model Transformation) {
	cfg := model.GetConfig()
	handler := model.GetHandler()
	if handler == nil {
		return
	}

	depProvider, ok := handler.(interface{ GetFlattenedDependencies() []string })
	if !ok {
		return
	}

	flatDeps := depProvider.GetFlattenedDependencies()

	// Should not contain any placeholders
	for _, dep := range flatDeps {
		assert.NotContains(t, dep, "{{external}}")
		assert.NotContains(t, dep, "{{transformation}}")
	}

	// Check specific substitutions
	if cfg.Table == "test_transform" {
		assert.Contains(t, flatDeps, "ethereum.beacon_blocks")
		assert.Contains(t, flatDeps, "analytics.other_transform")
		assert.Contains(t, flatDeps, "custom_db.custom_table")
	}
	if cfg.Table == "other_transform" {
		assert.Contains(t, flatDeps, "ethereum.beacon_blocks")
	}
}

func TestServiceValidatesDatabase(t *testing.T) {
	// Create temporary directories for test models
	transformationDir := t.TempDir()
	externalDir := t.TempDir()

	// Create an external model to serve as a dependency
	externalContent := `---
database: dep_db
table: dep_table
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT * FROM test`

	externalFile := filepath.Join(externalDir, "dependency.sql")
	err := os.WriteFile(externalFile, []byte(externalContent), 0o644)
	require.NoError(t, err)

	// Create a transformation model without database field
	transformationContent := `type: incremental
table: test_table
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - dep_db.dep_table
exec: "echo test"`

	transformationFile := filepath.Join(transformationDir, "test.yml")
	err = os.WriteFile(transformationFile, []byte(transformationContent), 0o644)
	require.NoError(t, err)

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	clickhouseCfg := &clickhouse.Config{
		URL:         "http://localhost:8123",
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
	}

	tests := []struct {
		name        string
		config      *Config
		shouldError bool
		errorMsg    string
	}{
		{
			name: "fails when no default database is provided",
			config: &Config{
				External: ExternalConfig{
					Paths:           []string{externalDir},
					DefaultDatabase: "dep_db",
				},
				Transformation: TransformationConfig{
					Paths:           []string{transformationDir},
					DefaultDatabase: "", // No default database
				},
			},
			shouldError: true,
			errorMsg:    "database is required",
		},
		{
			name: "succeeds when default database is provided",
			config: &Config{
				External: ExternalConfig{
					Paths:           []string{externalDir},
					DefaultDatabase: "dep_db",
				},
				Transformation: TransformationConfig{
					Paths:           []string{transformationDir},
					DefaultDatabase: "test_db", // Has default database
				},
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := NewService(log, tt.config, nil, clickhouseCfg)
			require.NoError(t, err)

			err = svc.Start()
			if tt.shouldError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
