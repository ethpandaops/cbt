package scheduled

import (
	"encoding/json"
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptrString(v string) *string { return &v }

func TestNewHandler_DecodeError(t *testing.T) {
	// Strict decode rejects malformed/unknown content, exercising the error path.
	_, err := NewHandler([]byte("type: scheduled\nunknown_field: oops\n"), transformation.AdminTable{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse scheduled config")
}

func TestHandler_Config(t *testing.T) {
	cfg := &Config{Database: "db", Table: "tbl", Schedule: "@every 1m"}
	handler := &Handler{config: cfg}

	got, ok := handler.Config().(*Config)
	require.True(t, ok)
	assert.Same(t, cfg, got)
}

func TestNewHandler_Closures(t *testing.T) {
	// Exercises the identity and tags closures wired into NewDependencyBase.
	handler, err := NewHandler([]byte(
		"type: scheduled\ndatabase: test_db\ntable: test_table\nschedule: \"@every 1m\"\ntags:\n  - tag1\n  - tag2\n",
	), transformation.AdminTable{Database: "admin", Table: "cbt"})
	require.NoError(t, err)
	require.NotNil(t, handler)

	assert.Equal(t, "test_db.test_table", handler.GetID())
	assert.Equal(t, []string{"tag1", "tag2"}, handler.GetTags())
}

func TestHandler_ApplyOverrides(t *testing.T) {
	t.Run("nil override is a no-op", func(t *testing.T) {
		handler := &Handler{config: &Config{Schedule: "@every 1m", Tags: []string{"base"}}}

		handler.ApplyOverrides(nil)

		assert.Equal(t, "@every 1m", handler.config.Schedule)
		assert.Equal(t, []string{"base"}, handler.config.Tags)
	})

	t.Run("applies schedule and tags", func(t *testing.T) {
		handler := &Handler{config: &Config{Schedule: "@every 1m", Tags: []string{"base"}}}

		handler.ApplyOverrides(&transformation.Override{
			Schedule: ptrString("@hourly"),
			Tags:     []string{"extra"},
		})

		assert.Equal(t, "@hourly", handler.config.Schedule)
		assert.Equal(t, []string{"base", "extra"}, handler.config.Tags)
	})

	t.Run("nil schedule override keeps existing", func(t *testing.T) {
		handler := &Handler{config: &Config{Schedule: "@every 1m"}}

		handler.ApplyOverrides(&transformation.Override{Tags: []string{"x"}})

		assert.Equal(t, "@every 1m", handler.config.Schedule)
		assert.Equal(t, []string{"x"}, handler.config.Tags)
	})
}

func TestSnapshot_ToBaseConfigJSON(t *testing.T) {
	snapshot := &Snapshot{Schedule: "@every 1m", Tags: []string{"t1", "t2"}}

	raw, err := snapshot.ToBaseConfigJSON()
	require.NoError(t, err)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(raw, &obj))

	assert.Equal(t, "@every 1m", obj["schedule"])
	assert.Contains(t, obj, "tags")
}

func TestHandler_SnapshotAndRestoreConfig(t *testing.T) {
	t.Run("snapshot captures schedule and tags", func(t *testing.T) {
		handler := &Handler{config: &Config{Schedule: "@every 1m", Tags: []string{"t1", "t2"}}}

		snap, ok := handler.SnapshotConfig().(*Snapshot)
		require.True(t, ok)
		assert.Equal(t, "@every 1m", snap.Schedule)
		assert.Equal(t, []string{"t1", "t2"}, snap.Tags)
	})

	t.Run("restore from snapshot", func(t *testing.T) {
		handler := &Handler{config: &Config{Schedule: "old", Tags: []string{"old"}}}

		handler.RestoreConfig(&Snapshot{Schedule: "@hourly", Tags: []string{"new"}})

		assert.Equal(t, "@hourly", handler.config.Schedule)
		assert.Equal(t, []string{"new"}, handler.config.Tags)
	})

	t.Run("restore with wrong type is a no-op", func(t *testing.T) {
		handler := &Handler{config: &Config{Schedule: "unchanged", Tags: []string{"unchanged"}}}

		handler.RestoreConfig(42)

		assert.Equal(t, "unchanged", handler.config.Schedule)
		assert.Equal(t, []string{"unchanged"}, handler.config.Tags)
	})
}

func TestRegister(t *testing.T) {
	Register()

	handler, err := transformation.CreateHandler(
		transformation.TypeScheduled,
		[]byte("type: scheduled\ndatabase: db\ntable: tbl\nschedule: \"@every 1m\"\n"),
		transformation.AdminTable{Database: "admin", Table: "cbt"},
	)
	require.NoError(t, err)
	require.NotNil(t, handler)
	assert.Equal(t, transformation.TypeScheduled, handler.Type())
}
