package handlers

import (
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildExternalModelFull(t *testing.T) {
	t.Run("populates interval, cache and lag", func(t *testing.T) {
		cfg := external.Config{
			Database: "ethereum",
			Table:    "blocks",
			Lag:      5,
			Cache: &external.CacheConfig{
				IncrementalScanInterval: 30,
				FullScanInterval:        60,
			},
		}
		ext := &fakeExternalWithInterval{
			FakeExternal: testutil.FakeExternal{ID: "ethereum.blocks", Config: cfg},
			intervalType: "block",
		}

		model := buildExternalModel("ethereum.blocks", ext, &testutil.FakeDAGReader{}, configOverrideStatus{
			hasOverride: true,
			isDisabled:  true,
		})

		require.NotNil(t, model.Interval)
		require.NotNil(t, model.Interval.Type)
		assert.Equal(t, "block", *model.Interval.Type)
		require.NotNil(t, model.Cache)
		require.NotNil(t, model.Cache.IncrementalScanInterval)
		require.NotNil(t, model.Cache.FullScanInterval)
		require.NotNil(t, model.Lag)
		assert.Equal(t, 5, *model.Lag)
		require.NotNil(t, model.HasOverride)
		assert.True(t, *model.HasOverride)
		require.NotNil(t, model.IsDisabled)
		assert.True(t, *model.IsDisabled)
	})

	t.Run("empty interval type is not populated", func(t *testing.T) {
		ext := &fakeExternalWithInterval{
			FakeExternal: testutil.FakeExternal{
				ID:     "ethereum.blocks",
				Config: external.Config{Database: "ethereum", Table: "blocks"},
			},
			intervalType: "",
		}

		model := buildExternalModel("ethereum.blocks", ext, &testutil.FakeDAGReader{}, configOverrideStatus{})

		assert.Nil(t, model.Interval)
		assert.Nil(t, model.Cache)
		assert.Nil(t, model.Lag)
	})
}

func TestBuildTransformationModelScheduled(t *testing.T) {
	handler := &scheduledTestHandler{
		schedule: "0 * * * *",
		tags:     []string{"daily", "report"},
	}
	trans := &testutil.FakeTransformation{
		ID:      "analytics.daily",
		Value:   "SELECT 1",
		Type:    "sql",
		Handler: handler,
		Config: transformation.Config{
			Database: "analytics",
			Table:    "daily",
			Type:     transformation.TypeScheduled,
		},
	}

	model := buildTransformationModel("analytics.daily", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	assert.Equal(t, generated.TransformationModelTypeScheduled, model.Type)
	assert.Equal(t, generated.TransformationModelContentTypeSql, model.ContentType)
	require.NotNil(t, model.Schedule)
	assert.Equal(t, "0 * * * *", *model.Schedule)
	require.NotNil(t, model.Tags)
	assert.Equal(t, []string{"daily", "report"}, *model.Tags)
}

func TestBuildTransformationModelScheduledNoHandler(t *testing.T) {
	// Handler does not implement scheduleProvider; scheduled fields stay nil.
	trans := &testutil.FakeTransformation{
		ID:      "analytics.daily",
		Value:   "echo hi",
		Type:    "exec",
		Handler: &flatDepTestHandler{},
		Config: transformation.Config{
			Database: "analytics",
			Table:    "daily",
			Type:     transformation.TypeScheduled,
		},
	}

	model := buildTransformationModel("analytics.daily", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	assert.Equal(t, generated.TransformationModelTypeScheduled, model.Type)
	assert.Equal(t, generated.TransformationModelContentTypeExec, model.ContentType)
	assert.Nil(t, model.Schedule)
	assert.Nil(t, model.Tags)
}

func TestBuildTransformationModelScheduledEmptyTags(t *testing.T) {
	handler := &scheduledTestHandler{schedule: "@daily", tags: nil}
	trans := &testutil.FakeTransformation{
		ID:      "analytics.daily",
		Type:    "unknown",
		Handler: handler,
		Config: transformation.Config{
			Database: "analytics",
			Table:    "daily",
			Type:     transformation.TypeScheduled,
		},
	}

	model := buildTransformationModel("analytics.daily", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	// Unknown content type defaults to SQL.
	assert.Equal(t, generated.TransformationModelContentTypeSql, model.ContentType)
	require.NotNil(t, model.Schedule)
	assert.Nil(t, model.Tags)
}

func TestBuildTransformationModelIncrementalFull(t *testing.T) {
	handler := &incrementalTestHandler{
		minInterval:      10,
		maxInterval:      100,
		forwardfill:      "@every 1m",
		backfill:         "@every 5m",
		tags:             []string{"core"},
		intervalType:     "slot",
		limits:           &transformation.Limits{Min: 1, Max: 1000},
		fillDirection:    "forward",
		allowGapSkipping: true,
		fillBuffer:       7,
	}
	trans := &testutil.FakeTransformation{
		ID:      "analytics.block_stats",
		Value:   "SELECT 1",
		Type:    "sql",
		Handler: handler,
		Config: transformation.Config{
			Database: "analytics",
			Table:    "block_stats",
			Type:     transformation.TypeIncremental,
		},
	}

	model := buildTransformationModel("analytics.block_stats", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	assert.Equal(t, generated.TransformationModelTypeIncremental, model.Type)

	require.NotNil(t, model.Interval)
	require.NotNil(t, model.Interval.Min)
	assert.Equal(t, 10, *model.Interval.Min)
	require.NotNil(t, model.Interval.Max)
	assert.Equal(t, 100, *model.Interval.Max)
	require.NotNil(t, model.Interval.Type)
	assert.Equal(t, "slot", *model.Interval.Type)

	require.NotNil(t, model.Schedules)
	require.NotNil(t, model.Schedules.Forwardfill)
	assert.Equal(t, "@every 1m", *model.Schedules.Forwardfill)
	require.NotNil(t, model.Schedules.Backfill)
	assert.Equal(t, "@every 5m", *model.Schedules.Backfill)

	require.NotNil(t, model.Limits)
	require.NotNil(t, model.Limits.Min)
	assert.Equal(t, 1, *model.Limits.Min)
	require.NotNil(t, model.Limits.Max)
	assert.Equal(t, 1000, *model.Limits.Max)

	require.NotNil(t, model.Fill)
	require.NotNil(t, model.Fill.Direction)
	assert.Equal(t, generated.TransformationModelFillDirection("forward"), *model.Fill.Direction)
	require.NotNil(t, model.Fill.AllowGapSkipping)
	assert.True(t, *model.Fill.AllowGapSkipping)
	require.NotNil(t, model.Fill.Buffer)
	assert.Equal(t, 7, *model.Fill.Buffer)

	require.NotNil(t, model.Tags)
	assert.Equal(t, []string{"core"}, *model.Tags)
}

func TestBuildTransformationModelIncrementalMinimal(t *testing.T) {
	// Handler implements incrementalProvider but most optional sub-interfaces
	// return empty/zero values so the optional fields stay nil.
	handler := &incrementalTestHandler{
		minInterval:  0,
		maxInterval:  0,
		forwardfill:  "",
		backfill:     "",
		intervalType: "",
		limits:       nil,
	}
	trans := &testutil.FakeTransformation{
		ID:      "analytics.block_stats",
		Type:    "sql",
		Handler: handler,
		Config: transformation.Config{
			Database: "analytics",
			Table:    "block_stats",
			Type:     transformation.TypeIncremental,
		},
	}

	model := buildTransformationModel("analytics.block_stats", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	require.NotNil(t, model.Interval)
	assert.Nil(t, model.Interval.Min)
	assert.Nil(t, model.Interval.Max)
	assert.Nil(t, model.Interval.Type)
	assert.Nil(t, model.Schedules)
	assert.Nil(t, model.Limits)
	// Fill is still set because allowGapSkipping is always populated.
	require.NotNil(t, model.Fill)
	assert.Nil(t, model.Fill.Direction)
	assert.Nil(t, model.Fill.Buffer)
	require.NotNil(t, model.Fill.AllowGapSkipping)
	assert.False(t, *model.Fill.AllowGapSkipping)
	assert.Nil(t, model.Tags)
}

func TestBuildTransformationModelIncrementalLimitsZero(t *testing.T) {
	handler := &incrementalTestHandler{
		limits: &transformation.Limits{Min: 0, Max: 0},
	}
	trans := &testutil.FakeTransformation{
		ID:      "analytics.block_stats",
		Type:    "sql",
		Handler: handler,
		Config: transformation.Config{
			Database: "analytics",
			Table:    "block_stats",
			Type:     transformation.TypeIncremental,
		},
	}

	model := buildTransformationModel("analytics.block_stats", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	assert.Nil(t, model.Limits)
}

func TestBuildTransformationModelIncrementalNonIncrementalHandler(t *testing.T) {
	// Handler does NOT implement incrementalProvider, so populateIncrementalFields
	// returns early.
	trans := &testutil.FakeTransformation{
		ID:      "analytics.block_stats",
		Type:    "sql",
		Handler: &scheduledTestHandler{},
		Config: transformation.Config{
			Database: "analytics",
			Table:    "block_stats",
			Type:     transformation.TypeIncremental,
		},
	}

	model := buildTransformationModel("analytics.block_stats", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	assert.Nil(t, model.Interval)
	assert.Nil(t, model.Schedules)
	assert.Nil(t, model.Limits)
	assert.Nil(t, model.Fill)
}

func TestBuildTransformationModelStructuredDeps(t *testing.T) {
	dag := &dagWithStructuredDeps{
		deps: []transformation.Dependency{
			{IsGroup: false, SingleDep: "ethereum.blocks"},
			{IsGroup: true, GroupDeps: []string{"a.b", "c.d"}},
		},
	}
	trans := &testutil.FakeTransformation{
		ID:      "analytics.block_stats",
		Type:    "sql",
		Handler: &incrementalTestHandler{},
		Config: transformation.Config{
			Database: "analytics",
			Table:    "block_stats",
			Type:     transformation.TypeIncremental,
		},
	}

	model := buildTransformationModel("analytics.block_stats", trans, dag, configOverrideStatus{})

	require.NotNil(t, model.DependsOn)
	assert.Len(t, *model.DependsOn, 2)
}

func TestBuildTransformationModelIncrementalNoLimitsOrFill(t *testing.T) {
	// Handler implements incrementalProvider but not LimitsHandler/FillHandler,
	// so populateLimits and populateFill take their early-return branches.
	trans := &testutil.FakeTransformation{
		ID:      "analytics.block_stats",
		Type:    "sql",
		Handler: &incrementalNoExtrasHandler{},
		Config: transformation.Config{
			Database: "analytics",
			Table:    "block_stats",
			Type:     transformation.TypeIncremental,
		},
	}

	model := buildTransformationModel("analytics.block_stats", trans, &testutil.FakeDAGReader{}, configOverrideStatus{})

	assert.Nil(t, model.Limits)
	assert.Nil(t, model.Fill)
	require.NotNil(t, model.Interval)
}

func TestIntPtr(t *testing.T) {
	assert.Nil(t, intPtr(0))
	require.NotNil(t, intPtr(5))
	assert.Equal(t, 5, *intPtr(5))
}

func TestStringPtr(t *testing.T) {
	assert.Nil(t, stringPtr(""))
	require.NotNil(t, stringPtr("x"))
	assert.Equal(t, "x", *stringPtr("x"))
}

func TestToInt(t *testing.T) {
	assert.Equal(t, 0, toInt(0))
	assert.Equal(t, 42, toInt(42))
}

// fakeExternalWithInterval extends FakeExternal to also implement
// intervalTypeProvider via GetIntervalType.
type fakeExternalWithInterval struct {
	testutil.FakeExternal
	intervalType string
}

func (f *fakeExternalWithInterval) GetIntervalType() string { return f.intervalType }

// dagWithStructuredDeps embeds FakeDAGReader and overrides GetStructuredDependencies
// so buildTransformationModel produces a populated DependsOn list.
type dagWithStructuredDeps struct {
	testutil.FakeDAGReader
	deps []transformation.Dependency
}

func (d *dagWithStructuredDeps) GetStructuredDependencies(_ string) []transformation.Dependency {
	return d.deps
}
