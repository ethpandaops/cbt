package models

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// writeFile is a small helper for the temp-dir fixture builders below.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
}

func newTestLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	return log
}

// mustOverride decodes a YAML override fragment into an *Override for test config.
func mustOverride(t *testing.T, src string) *Override {
	t.Helper()
	var o Override
	require.NoError(t, yaml.Unmarshal([]byte(src), &o))
	return &o
}

// TestServiceApplyOverridesFullPipeline drives the entire override resolution and
// application chain through a real Start(): real model parsing, real handlers,
// type mapping, default-DB resolution, application, disabling and unmatched warnings.
func TestServiceApplyOverridesFullPipeline(t *testing.T) {
	externalDir := t.TempDir()
	transformationDir := t.TempDir()

	externalDefaultDB := `---
table: beacon_blocks
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT 1`
	writeFile(t, externalDir, "beacon_blocks.sql", externalDefaultDB)

	externalToDisable := `---
table: disabled_ext
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT 1`
	writeFile(t, externalDir, "disabled_ext.sql", externalToDisable)

	// Transformation in the default DB, referenced by table-only override.
	transMain := `type: incremental
table: test_transform
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - ethereum.beacon_blocks
exec: "echo test"`
	writeFile(t, transformationDir, "test.yml", transMain)

	// Transformation to be disabled via table-only override.
	transDisabled := `type: incremental
table: disabled_one
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - ethereum.beacon_blocks
exec: "echo test"`
	writeFile(t, transformationDir, "disabled.yml", transDisabled)

	cfg := &Config{
		External: ExternalConfig{
			Paths:           []string{externalDir},
			DefaultDatabase: "ethereum",
		},
		Transformation: TransformationConfig{
			Paths:           []string{transformationDir},
			DefaultDatabase: "analytics",
		},
		Overrides: map[string]*Override{
			// Full-ID external override -> resolveOverrideByType + findOverride full-ID + ApplyToExternal.
			"ethereum.beacon_blocks": mustOverride(t, "config:\n  lag: 500\n"),
			// Table-only key resolved via external default DB -> resolveOverrideWithDefaults external branch.
			"disabled_ext": mustOverride(t, "enabled: false\n"),
			// Table-only key resolved via transformation default DB + applied.
			"test_transform": mustOverride(t, "config:\n  interval:\n    max: 999\n"),
			// Table-only key -> disables a transformation.
			"disabled_one": mustOverride(t, "enabled: false\n"),
			// No matching model -> warnUnmatchedOverrides.
			"nonexistent.model": mustOverride(t, "enabled: false\n"),
		},
	}

	chCfg := &clickhouse.Config{URL: "http://localhost:8123", Cluster: "c", LocalSuffix: "_local"}
	svc, err := NewService(newTestLogger(), cfg, chCfg)
	require.NoError(t, err)
	require.NoError(t, svc.Start())

	s := svc.(*service)

	// Disabled models were filtered out.
	assert.Len(t, s.externalModels, 1)
	assert.Equal(t, "ethereum.beacon_blocks", s.externalModels[0].GetID())
	assert.Len(t, s.transformationModels, 1)
	assert.Equal(t, "analytics.test_transform", s.transformationModels[0].GetID())

	// External lag override applied.
	assert.Equal(t, uint64(500), s.externalModels[0].GetConfig().Lag)

	// GetDAG returns a populated reader.
	dag := svc.GetDAG()
	require.NotNil(t, dag)
	node, err := dag.GetExternalNode("ethereum.beacon_blocks")
	require.NoError(t, err)
	assert.Equal(t, "ethereum.beacon_blocks", node.GetID())

	// Render + env getters delegate to the template engine.
	trans := s.transformationModels[0]
	rendered, err := svc.RenderTransformation(trans, 0, 100, time.Now())
	require.NoError(t, err)
	assert.NotEmpty(t, rendered)

	env, err := svc.GetTransformationEnvironmentVariables(trans, 0, 100, time.Now())
	require.NoError(t, err)
	require.NotNil(t, env)

	extRendered, err := svc.RenderExternal(s.externalModels[0], nil)
	require.NoError(t, err)
	assert.NotEmpty(t, extRendered)

	// Stop is a no-op but must be covered.
	require.NoError(t, svc.Stop())
}

// TestServiceApplyOverridesNoMatchUnresolvable exercises resolveOverrideWithDefaults
// returning ErrOverrideModelNotFound (table-only key matching no model in either default DB).
func TestServiceApplyOverridesUnresolvable(t *testing.T) {
	externalDir := t.TempDir()
	transformationDir := t.TempDir()

	writeFile(t, externalDir, "ext.sql", `---
table: my_ext
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT 1`)

	cfg := &Config{
		External: ExternalConfig{
			Paths:           []string{externalDir},
			DefaultDatabase: "ethereum",
		},
		Transformation: TransformationConfig{
			Paths:           []string{transformationDir},
			DefaultDatabase: "analytics",
		},
		Overrides: map[string]*Override{
			// Table-only key that matches neither default DB -> unresolvable + unmatched.
			"totally_unknown": mustOverride(t, "config:\n  lag: 1\n"),
		},
	}

	chCfg := &clickhouse.Config{URL: "http://localhost:8123"}
	svc, err := NewService(newTestLogger(), cfg, chCfg)
	require.NoError(t, err)
	require.NoError(t, svc.Start())
}

// TestServiceApplyOverridesResolveError forces ResolveConfig to fail for a matched
// model, covering the warn branch in resolveOverrideByType.
func TestServiceApplyOverridesResolveError(t *testing.T) {
	externalDir := t.TempDir()
	transformationDir := t.TempDir()

	writeFile(t, externalDir, "ext.sql", `---
database: raw
table: my_ext
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT 1`)

	cfg := &Config{
		External: ExternalConfig{Paths: []string{externalDir}},
		Transformation: TransformationConfig{
			Paths:           []string{transformationDir},
			DefaultDatabase: "analytics",
		},
		Overrides: map[string]*Override{
			// Config is a scalar that cannot decode into ExternalOverride -> ResolveConfig errors.
			"raw.my_ext": mustOverride(t, "config: not-a-map\n"),
		},
	}

	chCfg := &clickhouse.Config{URL: "http://localhost:8123"}
	svc, err := NewService(newTestLogger(), cfg, chCfg)
	require.NoError(t, err)
	// Start should still succeed; the override simply fails to apply with a warning.
	require.NoError(t, svc.Start())
}

func TestServiceStartParseError(t *testing.T) {
	// An external file with invalid frontmatter makes NewExternal fail inside parseModels.
	externalDir := t.TempDir()
	writeFile(t, externalDir, "bad.sql", "no frontmatter here")

	cfg := &Config{
		External:       ExternalConfig{Paths: []string{externalDir}},
		Transformation: TransformationConfig{Paths: []string{t.TempDir()}},
	}

	svc, err := NewService(newTestLogger(), cfg, &clickhouse.Config{})
	require.NoError(t, err)
	require.Error(t, svc.Start())
}

func TestServiceStartTransformationParseError(t *testing.T) {
	// A transformation file with an unsupported extension surfaced as a model path
	// can't fail discovery, so use invalid YAML content instead.
	transformationDir := t.TempDir()
	writeFile(t, transformationDir, "bad.yml", "type: incremental\ninterval: not-an-object\n")

	cfg := &Config{
		External:       ExternalConfig{Paths: []string{t.TempDir()}},
		Transformation: TransformationConfig{Paths: []string{transformationDir}, DefaultDatabase: "db"},
	}

	svc, err := NewService(newTestLogger(), cfg, &clickhouse.Config{})
	require.NoError(t, err)
	require.Error(t, svc.Start())
}

func TestServiceStartBuildDAGError(t *testing.T) {
	// Transformation depends on a model that does not exist -> BuildGraph fails in Start.
	transformationDir := t.TempDir()
	writeFile(t, transformationDir, "t.yml", `type: incremental
table: t
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - missing.dependency
exec: "echo test"`)

	cfg := &Config{
		External:       ExternalConfig{Paths: []string{t.TempDir()}},
		Transformation: TransformationConfig{Paths: []string{transformationDir}, DefaultDatabase: "db"},
	}

	svc, err := NewService(newTestLogger(), cfg, &clickhouse.Config{})
	require.NoError(t, err)
	require.ErrorIs(t, svc.Start(), ErrNonExistentDependency)
}

func TestServiceParseModelsExternalDiscoverError(t *testing.T) {
	// A symlink that escapes its parent directory makes discovery (and thus
	// parseModels for external paths) return an error.
	cfg := &Config{
		External:       ExternalConfig{Paths: []string{escapingSymlinkPath(t)}},
		Transformation: TransformationConfig{Paths: []string{t.TempDir()}},
	}

	svc, err := NewService(newTestLogger(), cfg, &clickhouse.Config{})
	require.NoError(t, err)
	err = svc.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to discover models")
}

func TestServiceParseModelsTransformationDiscoverError(t *testing.T) {
	cfg := &Config{
		External:       ExternalConfig{Paths: []string{t.TempDir()}},
		Transformation: TransformationConfig{Paths: []string{escapingSymlinkPath(t)}},
	}

	svc, err := NewService(newTestLogger(), cfg, &clickhouse.Config{})
	require.NoError(t, err)
	err = svc.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to discover models")
}

// escapingSymlinkPath creates a .sql symlink that points outside its parent
// directory, which discovery rejects with an error.
func escapingSymlinkPath(t *testing.T) string {
	t.Helper()
	outside := t.TempDir()
	target := filepath.Join(outside, "secret.sql")
	require.NoError(t, os.WriteFile(target, []byte("---\ntable: x\n---\nSELECT 1"), 0o600))

	dir := t.TempDir()
	link := filepath.Join(dir, "model.sql")
	require.NoError(t, os.Symlink(target, link))
	return link
}

func TestServiceSubstitutePlaceholdersNilHandler(t *testing.T) {
	svc := &service{log: newTestLogger().WithField("service", "models"), config: &Config{}}
	// Model with a nil handler returns early without panicking.
	require.NotPanics(t, func() {
		svc.substitutePlaceholders(&mockTransformation{handler: nil})
	})
}

func TestServiceShouldSkipModelWrongTypes(t *testing.T) {
	svc := &service{log: newTestLogger().WithField("service", "models"), config: &Config{}}
	applied := make(map[string]bool)

	// External case but value is not an External.
	assert.False(t, svc.shouldSkipModel("not-external", TypeExternal, applied))
	// Transformation case but value is not a Transformation.
	assert.False(t, svc.shouldSkipModel("not-transformation", TypeTransformation, applied))
}

func TestServiceExternalValidationError(t *testing.T) {
	// External model with no database and no default DB fails validation after defaults.
	externalDir := t.TempDir()
	writeFile(t, externalDir, "ext.sql", `---
table: my_ext
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT 1`)

	cfg := &Config{
		External:       ExternalConfig{Paths: []string{externalDir}}, // no default DB
		Transformation: TransformationConfig{Paths: []string{t.TempDir()}},
	}

	svc, err := NewService(newTestLogger(), cfg, &clickhouse.Config{})
	require.NoError(t, err)
	err = svc.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
}
