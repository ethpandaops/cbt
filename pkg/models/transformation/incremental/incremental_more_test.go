package incremental

import (
	"encoding/json"
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptrUint64(v uint64) *uint64 { return &v }
func ptrString(v string) *string { return &v }
func ptrBool(v bool) *bool       { return &v }

func TestHandler_Config(t *testing.T) {
	cfg := &Config{Database: "db", Table: "tbl"}
	handler := &Handler{config: cfg}

	got, ok := handler.Config().(*Config)
	require.True(t, ok)
	assert.Same(t, cfg, got)
}

func TestHandler_ValidateErrorBranches(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		errType error
		errMsg  string
	}{
		{
			name: "missing interval",
			config: &Config{
				Database:     "db",
				Table:        "tbl",
				Schedules:    &SchedulesConfig{ForwardFill: "@every 10s"},
				Dependencies: []transformation.Dependency{{SingleDep: "src.t"}},
			},
			errType: ErrIntervalRequired,
		},
		{
			name: "invalid interval (validate fails)",
			config: &Config{
				Database:     "db",
				Table:        "tbl",
				Interval:     &IntervalConfig{Min: 100, Max: 0, Type: "slot"},
				Schedules:    &SchedulesConfig{ForwardFill: "@every 10s"},
				Dependencies: []transformation.Dependency{{SingleDep: "src.t"}},
			},
			errMsg: "interval validation failed",
		},
		{
			name: "schedules present but empty",
			config: &Config{
				Database:     "db",
				Table:        "tbl",
				Interval:     &IntervalConfig{Min: 0, Max: 100, Type: "slot"},
				Schedules:    &SchedulesConfig{},
				Dependencies: []transformation.Dependency{{SingleDep: "src.t"}},
			},
			errType: ErrNoSchedulesConfig,
		},
		{
			name: "invalid schedule format",
			config: &Config{
				Database:     "db",
				Table:        "tbl",
				Interval:     &IntervalConfig{Min: 0, Max: 100, Type: "slot"},
				Schedules:    &SchedulesConfig{ForwardFill: "not a cron"},
				Dependencies: []transformation.Dependency{{SingleDep: "src.t"}},
			},
			errMsg: "schedules validation failed",
		},
		{
			name: "invalid fill direction",
			config: &Config{
				Database:     "db",
				Table:        "tbl",
				Interval:     &IntervalConfig{Min: 0, Max: 100, Type: "slot"},
				Schedules:    &SchedulesConfig{ForwardFill: "@every 10s"},
				Dependencies: []transformation.Dependency{{SingleDep: "src.t"}},
				Fill:         &FillConfig{Direction: "sideways"},
			},
			errMsg: "fill validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{config: tt.config}
			err := handler.Validate()
			require.Error(t, err)
			if tt.errType != nil {
				require.ErrorIs(t, err, tt.errType)
			}
			if tt.errMsg != "" {
				assert.Contains(t, err.Error(), tt.errMsg)
			}
		})
	}
}

func TestHandler_ValidateWithValidFill(t *testing.T) {
	handler := &Handler{config: &Config{
		Database:     "db",
		Table:        "tbl",
		Interval:     &IntervalConfig{Min: 0, Max: 100, Type: "slot"},
		Schedules:    &SchedulesConfig{ForwardFill: "@every 10s"},
		Dependencies: []transformation.Dependency{{SingleDep: "src.t"}},
		Fill:         &FillConfig{Direction: "tail", Buffer: 5},
	}}

	require.NoError(t, handler.Validate())
}

func TestHandler_IntervalGetters(t *testing.T) {
	t.Run("with interval", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Interval: &IntervalConfig{Min: 10, Max: 200, Type: "epoch"},
		}}

		assert.Equal(t, uint64(200), handler.GetMaxInterval())
		assert.Equal(t, uint64(10), handler.GetMinInterval())
		assert.Equal(t, "epoch", handler.GetIntervalType())
		assert.False(t, handler.AllowsPartialIntervals())

		minI, maxI := handler.GetInterval()
		assert.Equal(t, uint64(10), minI)
		assert.Equal(t, uint64(200), maxI)
	})

	t.Run("nil interval", func(t *testing.T) {
		handler := &Handler{config: &Config{}}

		assert.Equal(t, uint64(0), handler.GetMaxInterval())
		assert.Equal(t, uint64(0), handler.GetMinInterval())
		assert.Empty(t, handler.GetIntervalType())
		assert.False(t, handler.AllowsPartialIntervals())
	})

	t.Run("partial intervals allowed when min is zero", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Interval: &IntervalConfig{Min: 0, Max: 100, Type: "slot"},
		}}

		assert.True(t, handler.AllowsPartialIntervals())
	})
}

func TestHandler_ScheduleGetters(t *testing.T) {
	t.Run("with schedules", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Schedules: &SchedulesConfig{ForwardFill: "@every 10s", Backfill: "@every 30s"},
		}}

		assert.True(t, handler.IsForwardFillEnabled())
		assert.True(t, handler.IsBackfillEnabled())

		ff, bf := handler.GetSchedules()
		assert.Equal(t, "@every 10s", ff)
		assert.Equal(t, "@every 30s", bf)
	})

	t.Run("nil schedules", func(t *testing.T) {
		handler := &Handler{config: &Config{}}

		assert.False(t, handler.IsForwardFillEnabled())
		assert.False(t, handler.IsBackfillEnabled())

		ff, bf := handler.GetSchedules()
		assert.Empty(t, ff)
		assert.Empty(t, bf)
	})

	t.Run("empty schedules disabled", func(t *testing.T) {
		handler := &Handler{config: &Config{Schedules: &SchedulesConfig{}}}

		assert.False(t, handler.IsForwardFillEnabled())
		assert.False(t, handler.IsBackfillEnabled())
	})
}

func TestHandler_FillGetters(t *testing.T) {
	t.Run("nil fill uses defaults", func(t *testing.T) {
		handler := &Handler{config: &Config{}}

		assert.Equal(t, "head", handler.GetFillDirection())
		assert.True(t, handler.AllowGapSkipping())
		assert.Equal(t, uint64(0), handler.GetFillBuffer())
	})

	t.Run("fill with explicit direction", func(t *testing.T) {
		handler := &Handler{config: &Config{Fill: &FillConfig{Direction: "tail"}}}

		assert.Equal(t, "tail", handler.GetFillDirection())
	})

	t.Run("fill with empty direction falls back to default", func(t *testing.T) {
		handler := &Handler{config: &Config{Fill: &FillConfig{}}}

		assert.Equal(t, "head", handler.GetFillDirection())
	})

	t.Run("allow gap skipping explicitly false", func(t *testing.T) {
		handler := &Handler{config: &Config{Fill: &FillConfig{AllowGapSkipping: ptrBool(false)}}}

		assert.False(t, handler.AllowGapSkipping())
	})

	t.Run("fill buffer set", func(t *testing.T) {
		handler := &Handler{config: &Config{Fill: &FillConfig{Buffer: 42}}}

		assert.Equal(t, uint64(42), handler.GetFillBuffer())
	})
}

func TestHandler_GetLimits(t *testing.T) {
	t.Run("nil limits", func(t *testing.T) {
		handler := &Handler{config: &Config{}}
		assert.Nil(t, handler.GetLimits())
	})

	t.Run("with limits", func(t *testing.T) {
		handler := &Handler{config: &Config{Limits: &LimitsConfig{Min: 5, Max: 50}}}

		limits := handler.GetLimits()
		require.NotNil(t, limits)
		assert.Equal(t, uint64(5), limits.Min)
		assert.Equal(t, uint64(50), limits.Max)
	})
}

func TestHandler_GetFlatDependencies(t *testing.T) {
	handler := newHandlerWithConfig(&Config{
		Dependencies: []transformation.Dependency{
			{SingleDep: "db.a"},
			{IsGroup: true, GroupDeps: []string{"db.b", "db.c"}},
		},
	})

	assert.Equal(t, []string{"db.a", "db.b", "db.c"}, handler.GetFlatDependencies())
}

func TestHandler_NewHandlerSetOriginal(t *testing.T) {
	// Exercises the setOriginal closure passed to NewDependencyBase via newHandler.
	handler := newHandlerWithConfig(&Config{
		Dependencies: []transformation.Dependency{{SingleDep: "{{external}}.src"}},
	})

	handler.SubstituteDependencyPlaceholders("ext_db", "trans_db")

	require.Len(t, handler.config.OriginalDependencies, 1)
	assert.Equal(t, "{{external}}.src", handler.config.OriginalDependencies[0].SingleDep)
	assert.Equal(t, "ext_db.src", handler.config.Dependencies[0].SingleDep)
}

func TestHandler_NewHandlerClosures(t *testing.T) {
	// Exercises every accessor closure wired by newHandler via NewDependencyBase:
	// identity, tags, dependencies, original and setOriginal.
	handler := newHandlerWithConfig(&Config{
		Database:     "test_db",
		Table:        "test_table",
		Tags:         []string{"tag1", "tag2"},
		Dependencies: []transformation.Dependency{{SingleDep: "{{external}}.src"}},
	})

	assert.Equal(t, "test_db.test_table", handler.GetID())
	assert.Equal(t, []string{"tag1", "tag2"}, handler.GetTags())
	assert.Equal(t, []transformation.Dependency{{SingleDep: "{{external}}.src"}}, handler.GetDependencies())
	assert.Nil(t, handler.GetOriginalDependencies())

	handler.SubstituteDependencyPlaceholders("ext_db", "trans_db")

	require.Len(t, handler.GetOriginalDependencies(), 1)
	assert.Equal(t, "{{external}}.src", handler.GetOriginalDependencies()[0].SingleDep)
	assert.Equal(t, "ext_db.src", handler.GetDependencies()[0].SingleDep)
}

func TestHandler_ApplyOverrides(t *testing.T) {
	t.Run("nil override is a no-op", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Interval:  &IntervalConfig{Min: 1, Max: 2, Type: "slot"},
			Schedules: &SchedulesConfig{ForwardFill: "@every 1s"},
		}}

		handler.ApplyOverrides(nil)

		assert.Equal(t, uint64(1), handler.config.Interval.Min)
		assert.Equal(t, "@every 1s", handler.config.Schedules.ForwardFill)
	})

	t.Run("applies interval, schedules, limits, tags", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Interval:  &IntervalConfig{Min: 1, Max: 2, Type: "slot"},
			Schedules: &SchedulesConfig{ForwardFill: "@every 1s", Backfill: "@every 2s"},
			Tags:      []string{"base"},
		}}

		handler.ApplyOverrides(&transformation.Override{
			Interval:  &transformation.IntervalOverride{Min: ptrUint64(10), Max: ptrUint64(20)},
			Schedules: &transformation.SchedulesOverride{ForwardFill: ptrString("@every 5s")},
			Limits:    &transformation.LimitsOverride{Min: ptrUint64(100), Max: ptrUint64(200)},
			Tags:      []string{"extra"},
		})

		assert.Equal(t, uint64(10), handler.config.Interval.Min)
		assert.Equal(t, uint64(20), handler.config.Interval.Max)
		assert.Equal(t, "@every 5s", handler.config.Schedules.ForwardFill)
		assert.Equal(t, "@every 2s", handler.config.Schedules.Backfill)
		require.NotNil(t, handler.config.Limits)
		assert.Equal(t, uint64(100), handler.config.Limits.Min)
		assert.Equal(t, uint64(200), handler.config.Limits.Max)
		assert.Equal(t, []string{"base", "extra"}, handler.config.Tags)
	})

	t.Run("limits override on existing limits config", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Limits: &LimitsConfig{Min: 1, Max: 2},
		}}

		handler.ApplyOverrides(&transformation.Override{
			Limits: &transformation.LimitsOverride{Min: ptrUint64(7), Max: ptrUint64(8)},
		})

		require.NotNil(t, handler.config.Limits)
		assert.Equal(t, uint64(7), handler.config.Limits.Min)
		assert.Equal(t, uint64(8), handler.config.Limits.Max)
	})

	t.Run("override with nil interval and schedules config", func(t *testing.T) {
		handler := &Handler{config: &Config{Tags: []string{"a"}}}

		handler.ApplyOverrides(&transformation.Override{Tags: []string{"b"}})

		assert.Nil(t, handler.config.Interval)
		assert.Nil(t, handler.config.Schedules)
		assert.Equal(t, []string{"a", "b"}, handler.config.Tags)
	})
}

func TestSnapshot_ToBaseConfigJSON(t *testing.T) {
	tests := []struct {
		name      string
		snapshot  *Snapshot
		wantKey   string
		wantNoKey string
	}{
		{
			name: "with limits",
			snapshot: &Snapshot{
				IntervalMin: 1, IntervalMax: 2,
				ForwardFill: "@every 1s", Backfill: "@every 2s",
				HasLimits: true, LimitsMin: 10, LimitsMax: 20,
				Tags: []string{"t1"},
			},
			wantKey: "limits",
		},
		{
			name: "without limits",
			snapshot: &Snapshot{
				IntervalMin: 1, IntervalMax: 2,
				ForwardFill: "@every 1s",
				Tags:        []string{"t1"},
			},
			wantNoKey: "limits",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.snapshot.ToBaseConfigJSON()
			require.NoError(t, err)

			var obj map[string]any
			require.NoError(t, json.Unmarshal(raw, &obj))

			assert.Contains(t, obj, "interval")
			assert.Contains(t, obj, "schedules")
			assert.Contains(t, obj, "tags")

			if tt.wantKey != "" {
				assert.Contains(t, obj, tt.wantKey)
			}
			if tt.wantNoKey != "" {
				assert.NotContains(t, obj, tt.wantNoKey)
			}
		})
	}
}

func TestHandler_SnapshotAndRestoreConfig(t *testing.T) {
	t.Run("snapshot captures all overridable fields", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Interval:  &IntervalConfig{Min: 1, Max: 2, Type: "slot"},
			Schedules: &SchedulesConfig{ForwardFill: "@every 1s", Backfill: "@every 2s"},
			Limits:    &LimitsConfig{Min: 10, Max: 20},
			Tags:      []string{"t1", "t2"},
		}}

		snap, ok := handler.SnapshotConfig().(*Snapshot)
		require.True(t, ok)
		assert.Equal(t, uint64(1), snap.IntervalMin)
		assert.Equal(t, uint64(2), snap.IntervalMax)
		assert.Equal(t, "@every 1s", snap.ForwardFill)
		assert.Equal(t, "@every 2s", snap.Backfill)
		assert.True(t, snap.HasLimits)
		assert.Equal(t, uint64(10), snap.LimitsMin)
		assert.Equal(t, uint64(20), snap.LimitsMax)
		assert.Equal(t, []string{"t1", "t2"}, snap.Tags)
	})

	t.Run("snapshot with nil sub-configs", func(t *testing.T) {
		handler := &Handler{config: &Config{Tags: []string{"only"}}}

		snap, ok := handler.SnapshotConfig().(*Snapshot)
		require.True(t, ok)
		assert.Equal(t, uint64(0), snap.IntervalMin)
		assert.Empty(t, snap.ForwardFill)
		assert.False(t, snap.HasLimits)
		assert.Equal(t, []string{"only"}, snap.Tags)
	})

	t.Run("restore from snapshot with limits", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Interval:  &IntervalConfig{Min: 99, Max: 99, Type: "slot"},
			Schedules: &SchedulesConfig{ForwardFill: "old", Backfill: "old"},
			Limits:    &LimitsConfig{Min: 0, Max: 0},
			Tags:      []string{"old"},
		}}

		handler.RestoreConfig(&Snapshot{
			IntervalMin: 1, IntervalMax: 2,
			ForwardFill: "@every 1s", Backfill: "@every 2s",
			HasLimits: true, LimitsMin: 10, LimitsMax: 20,
			Tags: []string{"new"},
		})

		assert.Equal(t, uint64(1), handler.config.Interval.Min)
		assert.Equal(t, uint64(2), handler.config.Interval.Max)
		assert.Equal(t, "@every 1s", handler.config.Schedules.ForwardFill)
		assert.Equal(t, "@every 2s", handler.config.Schedules.Backfill)
		require.NotNil(t, handler.config.Limits)
		assert.Equal(t, uint64(10), handler.config.Limits.Min)
		assert.Equal(t, uint64(20), handler.config.Limits.Max)
		assert.Equal(t, []string{"new"}, handler.config.Tags)
	})

	t.Run("restore creates limits config when nil", func(t *testing.T) {
		handler := &Handler{config: &Config{}}

		handler.RestoreConfig(&Snapshot{
			HasLimits: true, LimitsMin: 3, LimitsMax: 4,
		})

		require.NotNil(t, handler.config.Limits)
		assert.Equal(t, uint64(3), handler.config.Limits.Min)
		assert.Equal(t, uint64(4), handler.config.Limits.Max)
	})

	t.Run("restore clears limits when snapshot has none", func(t *testing.T) {
		handler := &Handler{config: &Config{
			Limits: &LimitsConfig{Min: 1, Max: 2},
		}}

		handler.RestoreConfig(&Snapshot{HasLimits: false})

		assert.Nil(t, handler.config.Limits)
	})

	t.Run("restore with wrong type is a no-op", func(t *testing.T) {
		handler := &Handler{config: &Config{Tags: []string{"unchanged"}}}

		handler.RestoreConfig("not a snapshot")

		assert.Equal(t, []string{"unchanged"}, handler.config.Tags)
	})
}

func TestRegister(t *testing.T) {
	Register()

	handler, err := transformation.CreateHandler(
		transformation.TypeIncremental,
		[]byte("type: incremental\ndatabase: db\ntable: tbl\ninterval:\n  max: 100\n  type: slot\nschedules:\n  forwardfill: \"@every 10s\"\ndependencies:\n  - src.t\n"),
		transformation.AdminTable{Database: "admin", Table: "cbt"},
	)
	require.NoError(t, err)
	require.NotNil(t, handler)
	assert.Equal(t, transformation.TypeIncremental, handler.Type())
}
