package liveconfig

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errReader = errors.New("reader failure")

func testLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	return log
}

// fakeOverrideReader is a configurable OverrideReader for tests.
type fakeOverrideReader struct {
	version    int64
	versionErr error

	overrides    []admin.ConfigOverride
	overridesErr error
}

func (f *fakeOverrideReader) GetConfigOverrideVersion(_ context.Context) (int64, error) {
	return f.version, f.versionErr
}

func (f *fakeOverrideReader) GetAllConfigOverrides(_ context.Context) ([]admin.ConfigOverride, error) {
	return f.overrides, f.overridesErr
}

// snapshotHandler embeds the shared FakeHandler and additionally implements the
// optional configSnapshotRestorer / overrideApplier / baseConfigSerializer
// interfaces the Applier probes for. snapshot is an opaque token used to verify
// restore round-trips.
type snapshotHandler struct {
	*testutil.FakeHandler

	snapshot     any
	restoredWith any
	appliedWith  *models.TransformationOverride

	serializeJSON json.RawMessage
	serializeErr  error

	// disableSerializer makes ToBaseConfigJSON unavailable so the snapshot is
	// not a baseConfigSerializer.
	disableSerializer bool
}

func newSnapshotHandler() *snapshotHandler {
	return &snapshotHandler{
		FakeHandler:   &testutil.FakeHandler{},
		snapshot:      "base-snapshot",
		serializeJSON: json.RawMessage(`{"base":true}`),
	}
}

func (h *snapshotHandler) SnapshotConfig() any { return snapshotToken{owner: h} }

func (h *snapshotHandler) RestoreConfig(snap any) { h.restoredWith = snap }

func (h *snapshotHandler) ApplyOverrides(ov *models.TransformationOverride) { h.appliedWith = ov }

// snapshotToken is the value returned by SnapshotConfig. It optionally
// implements baseConfigSerializer depending on the owning handler.
type snapshotToken struct {
	owner *snapshotHandler
}

func (s snapshotToken) ToBaseConfigJSON() (json.RawMessage, error) {
	if s.owner.disableSerializer {
		// Not reachable: when disableSerializer is set we return a plain
		// snapshot via plainSnapshotHandler instead.
		return nil, nil
	}

	return s.owner.serializeJSON, s.owner.serializeErr
}

// plainHandler implements transformation.Handler and configSnapshotRestorer but
// its snapshot does NOT implement baseConfigSerializer.
type plainHandler struct {
	*testutil.FakeHandler

	restoredWith any
}

func newPlainHandler() *plainHandler {
	return &plainHandler{FakeHandler: &testutil.FakeHandler{}}
}

func (h *plainHandler) SnapshotConfig() any    { return plainSnapshot{} }
func (h *plainHandler) RestoreConfig(snap any) { h.restoredWith = snap }

type plainSnapshot struct{}

// bareHandler implements transformation.Handler but neither snapshot nor apply
// interfaces, so it is skipped during snapshotting and override application.
type bareHandler struct {
	*testutil.FakeHandler
}

func newBareHandler() *bareHandler {
	return &bareHandler{FakeHandler: &testutil.FakeHandler{}}
}

// newTransformationNode builds a FakeTransformation with the given id/handler.
func newTransformationNode(id string, handler transformation.Handler) *testutil.FakeTransformation {
	return &testutil.FakeTransformation{ID: id, Handler: handler}
}

// newExternalNode builds a models.Node wrapping a FakeExternal.
func newExternalNode(id string, lag uint64, cache *external.CacheConfig) models.Node {
	ext := &testutil.FakeExternal{
		ID: id,
		Config: external.Config{
			Lag:   lag,
			Cache: cache,
		},
	}

	return models.Node{NodeType: models.NodeTypeExternal, Model: ext}
}

func TestNewApplierSnapshots(t *testing.T) {
	t.Parallel()

	snapH := newSnapshotHandler()
	bareH := newBareHandler()

	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{
			newTransformationNode("db.snap", snapH),
			newTransformationNode("db.bare", bareH),
			newTransformationNode("db.nilhandler", nil),
		},
		Externals: []models.Node{
			newExternalNode("db.ext_cache", 5, &external.CacheConfig{
				IncrementalScanInterval: time.Minute,
				FullScanInterval:        5 * time.Minute,
			}),
			newExternalNode("db.ext_nocache", 7, nil),
			// A node whose Model is not models.External is skipped.
			{NodeType: models.NodeTypeExternal, Model: "not-an-external"},
		},
	}

	a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

	// snap and bare handlers are captured; nil-handler node is skipped.
	require.Contains(t, a.transformationBases, "db.snap")
	require.NotContains(t, a.transformationBases, "db.bare")
	require.NotContains(t, a.transformationBases, "db.nilhandler")

	// External bases captured for the two valid external models only.
	require.Contains(t, a.externalBases, "db.ext_cache")
	require.Contains(t, a.externalBases, "db.ext_nocache")
	require.Len(t, a.externalBases, 2)

	assert.Equal(t, uint64(5), a.externalBases["db.ext_cache"].Lag)
	assert.Equal(t, time.Minute, a.externalBases["db.ext_cache"].IncrementalScanInterval)
	assert.Equal(t, uint64(7), a.externalBases["db.ext_nocache"].Lag)
	assert.Zero(t, a.externalBases["db.ext_nocache"].IncrementalScanInterval)
}

func TestCheckAndApplyVersionError(t *testing.T) {
	t.Parallel()

	a := NewApplier(&testutil.FakeDAGReader{}, &fakeOverrideReader{versionErr: errReader}, testLogger())

	applied, err := a.CheckAndApply(context.Background())
	require.ErrorIs(t, err, errReader)
	assert.False(t, applied)
}

func TestCheckAndApplyUnchanged(t *testing.T) {
	t.Parallel()

	// lastVersion starts at 0; a version of 0 means "no change".
	a := NewApplier(&testutil.FakeDAGReader{}, &fakeOverrideReader{version: 0}, testLogger())

	applied, err := a.CheckAndApply(context.Background())
	require.NoError(t, err)
	assert.False(t, applied)
}

func TestCheckAndApplyFetchError(t *testing.T) {
	t.Parallel()

	a := NewApplier(&testutil.FakeDAGReader{}, &fakeOverrideReader{
		version:      2,
		overridesErr: errReader,
	}, testLogger())

	applied, err := a.CheckAndApply(context.Background())
	require.ErrorIs(t, err, errReader)
	assert.False(t, applied)
}

func TestCheckAndApplySuccess(t *testing.T) {
	t.Parallel()

	snapH := newSnapshotHandler()
	disabled := false
	enabledOv := boolPtr(false)

	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{newTransformationNode("db.snap", snapH)},
		TransformationByID: map[string]models.Transformation{
			"db.snap": newTransformationNode("db.snap", snapH),
		},
	}

	reader := &fakeOverrideReader{
		version: 3,
		overrides: []admin.ConfigOverride{
			{
				ModelID:  "db.snap",
				Type:     string(models.TypeTransformation),
				Enabled:  enabledOv,
				Override: mustJSON(t, models.TransformationOverride{Tags: []string{"x"}}),
			},
		},
	}

	a := NewApplier(dag, reader, testLogger())

	applied, err := a.CheckAndApply(context.Background())
	require.NoError(t, err)
	assert.True(t, applied)
	assert.True(t, a.IsModelDisabled("db.snap"))
	assert.Equal(t, disabled, a.IsModelDisabled("other"))
	require.NotNil(t, snapH.appliedWith)
	assert.Equal(t, []string{"x"}, snapH.appliedWith.Tags)

	// Second call with the same version is a no-op.
	applied, err = a.CheckAndApply(context.Background())
	require.NoError(t, err)
	assert.False(t, applied)
}

func TestApplySingleOverride(t *testing.T) {
	t.Parallel()

	t.Run("unknown type", func(t *testing.T) {
		t.Parallel()

		a := NewApplier(&testutil.FakeDAGReader{}, &fakeOverrideReader{}, testLogger())

		// Unknown type just logs a warning; nothing panics.
		a.applySingleOverride(&admin.ConfigOverride{ModelID: "db.x", Type: "mystery"})
	})

	t.Run("routes external", func(t *testing.T) {
		t.Parallel()

		ext := &testutil.FakeExternal{ID: "db.ext", Config: external.Config{Lag: 5}}
		dag := &testutil.FakeDAGReader{
			Externals:    []models.Node{{NodeType: models.NodeTypeExternal, Model: ext}},
			ExternalByID: map[string]models.External{"db.ext": ext},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		ext.Config.Lag = 100
		a.applySingleOverride(&admin.ConfigOverride{ModelID: "db.ext", Type: string(models.TypeExternal)})

		// Routed to applyExternalOverride, which restored the base Lag.
		assert.Equal(t, uint64(5), ext.Config.Lag)
	})
}

func TestApplyTransformationOverride(t *testing.T) {
	t.Parallel()

	t.Run("not found in dag", func(t *testing.T) {
		t.Parallel()

		dag := &testutil.FakeDAGReader{GetTransformationErr: errReader}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())
		a.applyTransformationOverride(&admin.ConfigOverride{ModelID: "missing", Type: string(models.TypeTransformation)})
	})

	t.Run("nil handler", func(t *testing.T) {
		t.Parallel()

		node := newTransformationNode("db.x", nil)
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{"db.x": node},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())
		a.applyTransformationOverride(&admin.ConfigOverride{ModelID: "db.x", Type: string(models.TypeTransformation)})
	})

	t.Run("restore then apply override", func(t *testing.T) {
		t.Parallel()

		snapH := newSnapshotHandler()
		node := newTransformationNode("db.x", snapH)
		dag := &testutil.FakeDAGReader{
			Transformations:    []models.Transformation{node},
			TransformationByID: map[string]models.Transformation{"db.x": node},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		a.applyTransformationOverride(&admin.ConfigOverride{
			ModelID:  "db.x",
			Type:     string(models.TypeTransformation),
			Override: mustJSON(t, models.TransformationOverride{Tags: []string{"t"}}),
		})

		require.NotNil(t, snapH.restoredWith)
		require.NotNil(t, snapH.appliedWith)
		assert.Equal(t, []string{"t"}, snapH.appliedWith.Tags)
	})

	t.Run("invalid override json", func(t *testing.T) {
		t.Parallel()

		snapH := newSnapshotHandler()
		node := newTransformationNode("db.x", snapH)
		dag := &testutil.FakeDAGReader{
			Transformations:    []models.Transformation{node},
			TransformationByID: map[string]models.Transformation{"db.x": node},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		a.applyTransformationOverride(&admin.ConfigOverride{
			ModelID:  "db.x",
			Type:     string(models.TypeTransformation),
			Override: json.RawMessage(`{not json`),
		})

		// Restore still happened, but no override applied due to parse failure.
		require.NotNil(t, snapH.restoredWith)
		assert.Nil(t, snapH.appliedWith)
	})

	t.Run("handler without snapshot or apply interfaces", func(t *testing.T) {
		t.Parallel()

		bareH := newBareHandler()
		node := newTransformationNode("db.x", bareH)
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{"db.x": node},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		a.applyTransformationOverride(&admin.ConfigOverride{
			ModelID:  "db.x",
			Type:     string(models.TypeTransformation),
			Override: mustJSON(t, models.TransformationOverride{Tags: []string{"t"}}),
		})
	})

	t.Run("restorer without base snapshot", func(t *testing.T) {
		t.Parallel()

		// Handler implements restorer, but the model was not snapshotted at
		// construction (not in the DAG's transformation list), so the
		// snapshot lookup misses and RestoreConfig is skipped.
		snapH := newSnapshotHandler()
		node := newTransformationNode("db.late", snapH)
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{"db.late": node},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		a.applyTransformationOverride(&admin.ConfigOverride{
			ModelID: "db.late",
			Type:    string(models.TypeTransformation),
		})

		assert.Nil(t, snapH.restoredWith)
	})
}

func TestApplyExternalOverride(t *testing.T) {
	t.Parallel()

	t.Run("not found in dag", func(t *testing.T) {
		t.Parallel()

		dag := &testutil.FakeDAGReader{GetExternalErr: errReader}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())
		a.applyExternalOverride(&admin.ConfigOverride{ModelID: "missing", Type: string(models.TypeExternal)})
	})

	t.Run("restore base and apply override with cache", func(t *testing.T) {
		t.Parallel()

		ext := &testutil.FakeExternal{
			ID: "db.ext",
			Config: external.Config{
				Lag: 10,
				Cache: &external.CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        5 * time.Minute,
				},
			},
		}
		dag := &testutil.FakeDAGReader{
			Externals:    []models.Node{{NodeType: models.NodeTypeExternal, Model: ext}},
			ExternalByID: map[string]models.External{"db.ext": ext},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		// Mutate config away from base to prove restore happens.
		ext.Config.Lag = 999

		newLag := uint64(42)
		a.applyExternalOverride(&admin.ConfigOverride{
			ModelID:  "db.ext",
			Type:     string(models.TypeExternal),
			Override: mustJSON(t, models.ExternalOverride{Lag: &newLag}),
		})

		assert.Equal(t, uint64(42), ext.Config.Lag)
	})

	t.Run("restore base without cache", func(t *testing.T) {
		t.Parallel()

		ext := &testutil.FakeExternal{
			ID:     "db.ext",
			Config: external.Config{Lag: 3},
		}
		dag := &testutil.FakeDAGReader{
			Externals:    []models.Node{{NodeType: models.NodeTypeExternal, Model: ext}},
			ExternalByID: map[string]models.External{"db.ext": ext},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		ext.Config.Lag = 100
		a.applyExternalOverride(&admin.ConfigOverride{ModelID: "db.ext", Type: string(models.TypeExternal)})

		assert.Equal(t, uint64(3), ext.Config.Lag)
	})

	t.Run("invalid override json", func(t *testing.T) {
		t.Parallel()

		ext := &testutil.FakeExternal{ID: "db.ext", Config: external.Config{Lag: 3}}
		dag := &testutil.FakeDAGReader{
			Externals:    []models.Node{{NodeType: models.NodeTypeExternal, Model: ext}},
			ExternalByID: map[string]models.External{"db.ext": ext},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		a.applyExternalOverride(&admin.ConfigOverride{
			ModelID:  "db.ext",
			Type:     string(models.TypeExternal),
			Override: json.RawMessage(`{bad`),
		})

		// Base restored, override not applied.
		assert.Equal(t, uint64(3), ext.Config.Lag)
	})
}

func TestRevertNonOverridden(t *testing.T) {
	t.Parallel()

	snapH := newSnapshotHandler()
	bareH := newBareHandler()
	keptH := newSnapshotHandler()

	ext := &testutil.FakeExternal{
		ID: "db.ext",
		Config: external.Config{
			Lag: 8,
			Cache: &external.CacheConfig{
				IncrementalScanInterval: time.Minute,
				FullScanInterval:        2 * time.Minute,
			},
		},
	}
	extNoCache := &testutil.FakeExternal{ID: "db.ext2", Config: external.Config{Lag: 4}}
	keptExt := &testutil.FakeExternal{ID: "db.ext_kept", Config: external.Config{Lag: 1}}

	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{
			newTransformationNode("db.snap", snapH),
			newTransformationNode("db.bare", bareH),
			newTransformationNode("db.kept", keptH),
		},
		TransformationByID: map[string]models.Transformation{
			"db.snap": newTransformationNode("db.snap", snapH),
			"db.bare": newTransformationNode("db.bare", bareH),
			"db.kept": newTransformationNode("db.kept", keptH),
		},
		Externals: []models.Node{
			{NodeType: models.NodeTypeExternal, Model: ext},
			{NodeType: models.NodeTypeExternal, Model: extNoCache},
			{NodeType: models.NodeTypeExternal, Model: keptExt},
		},
		ExternalByID: map[string]models.External{
			"db.ext":      ext,
			"db.ext2":     extNoCache,
			"db.ext_kept": keptExt,
		},
	}

	a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

	// Mutate everything, then revert all except the "kept" ids.
	ext.Config.Lag = 999
	extNoCache.Config.Lag = 999
	keptExt.Config.Lag = 999

	a.revertNonOverridden(map[string]bool{"db.kept": true, "db.ext_kept": true})

	require.NotNil(t, snapH.restoredWith)
	assert.Equal(t, uint64(8), ext.Config.Lag)
	assert.Equal(t, time.Minute, ext.Config.Cache.IncrementalScanInterval)
	assert.Equal(t, uint64(4), extNoCache.Config.Lag)

	// Kept models are not reverted.
	assert.Nil(t, keptH.restoredWith)
	assert.Equal(t, uint64(999), keptExt.Config.Lag)
}

func TestRevertNonOverriddenLookupMisses(t *testing.T) {
	t.Parallel()

	// transformationBases / externalBases reference ids whose lookups fail or
	// whose handlers are nil, exercising the continue branches.
	snapH := newSnapshotHandler()

	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{newTransformationNode("db.snap", snapH)},
		Externals: []models.Node{
			newExternalNode("db.ext", 2, nil),
		},
	}

	a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

	// Now make the DAG lookups fail for the snapshotted ids.
	a.dag = &testutil.FakeDAGReader{
		GetTransformationErr: errReader,
		GetExternalErr:       errReader,
	}

	a.revertNonOverridden(map[string]bool{})
	assert.Nil(t, snapH.restoredWith)
}

func TestRevertNonOverriddenNilHandler(t *testing.T) {
	t.Parallel()

	snapH := newSnapshotHandler()
	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{newTransformationNode("db.snap", snapH)},
	}
	a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

	// Swap the DAG so the looked-up node has a nil handler.
	a.dag = &testutil.FakeDAGReader{
		TransformationByID: map[string]models.Transformation{
			"db.snap": newTransformationNode("db.snap", nil),
		},
	}

	a.revertNonOverridden(map[string]bool{})
	assert.Nil(t, snapH.restoredWith)
}

func TestRevertNonOverriddenBareHandler(t *testing.T) {
	t.Parallel()

	// Snapshotted via a snapshot handler, but on revert the DAG returns a bare
	// handler that does not implement restorer, exercising the type-assert-miss.
	snapH := newSnapshotHandler()
	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{newTransformationNode("db.snap", snapH)},
	}
	a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

	a.dag = &testutil.FakeDAGReader{
		TransformationByID: map[string]models.Transformation{
			"db.snap": newTransformationNode("db.snap", newBareHandler()),
		},
	}

	a.revertNonOverridden(map[string]bool{})
}

func TestGetBaseConfig(t *testing.T) {
	t.Parallel()

	t.Run("transformation serializable", func(t *testing.T) {
		t.Parallel()

		snapH := newSnapshotHandler()
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{newTransformationNode("db.snap", snapH)},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		raw, err := a.GetBaseConfig("db.snap")
		require.NoError(t, err)
		assert.JSONEq(t, `{"base":true}`, string(raw))
	})

	t.Run("transformation serialize error", func(t *testing.T) {
		t.Parallel()

		snapH := newSnapshotHandler()
		snapH.serializeErr = errReader
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{newTransformationNode("db.snap", snapH)},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		_, err := a.GetBaseConfig("db.snap")
		require.ErrorIs(t, err, errReader)
	})

	t.Run("transformation not serializable", func(t *testing.T) {
		t.Parallel()

		plainH := newPlainHandler()
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{newTransformationNode("db.plain", plainH)},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		_, err := a.GetBaseConfig("db.plain")
		require.ErrorIs(t, err, ErrSnapshotNotSerializable)
	})

	t.Run("external base", func(t *testing.T) {
		t.Parallel()

		dag := &testutil.FakeDAGReader{
			Externals: []models.Node{
				newExternalNode("db.ext", 9, &external.CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        3 * time.Minute,
				}),
			},
		}
		a := NewApplier(dag, &fakeOverrideReader{}, testLogger())

		raw, err := a.GetBaseConfig("db.ext")
		require.NoError(t, err)

		var out map[string]any
		require.NoError(t, json.Unmarshal(raw, &out))
		assert.InDelta(t, float64(9), out["lag"], 0)

		cache, ok := out["cache"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "1m0s", cache["incremental_scan_interval"])
		assert.Equal(t, "3m0s", cache["full_scan_interval"])
	})

	t.Run("unknown model", func(t *testing.T) {
		t.Parallel()

		a := NewApplier(&testutil.FakeDAGReader{}, &fakeOverrideReader{}, testLogger())

		raw, err := a.GetBaseConfig("db.nope")
		require.NoError(t, err)
		assert.Nil(t, raw)
	})
}

func TestIsModelDisabled(t *testing.T) {
	t.Parallel()

	a := NewApplier(&testutil.FakeDAGReader{}, &fakeOverrideReader{}, testLogger())
	assert.False(t, a.IsModelDisabled("db.x"))

	a.disabledModels["db.x"] = true
	assert.True(t, a.IsModelDisabled("db.x"))
}

// --- helpers ---

func boolPtr(b bool) *bool { return &b }

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()

	data, err := json.Marshal(v)
	require.NoError(t, err)

	return data
}
