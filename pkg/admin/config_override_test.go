package admin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheManager_ConfigOverride_RoundTrip exercises set/get/delete and the
// version counter against an in-memory Redis.
func TestCacheManager_ConfigOverride_RoundTrip(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	enabled := true
	override := &ConfigOverride{
		ModelID:  "db.table",
		Type:     "transformation",
		Enabled:  &enabled,
		Override: json.RawMessage(`{"foo":"bar"}`),
	}

	// Version starts at zero (no key present).
	v0, err := cm.GetConfigOverrideVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), v0)

	// Get on a missing key returns nil, nil.
	missing, err := cm.GetConfigOverride(ctx, "db.table")
	require.NoError(t, err)
	assert.Nil(t, missing)

	// Set stores the override and bumps the version.
	require.NoError(t, cm.SetConfigOverride(ctx, override))

	v1, err := cm.GetConfigOverrideVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), v1)

	got, err := cm.GetConfigOverride(ctx, "db.table")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "db.table", got.ModelID)
	assert.Equal(t, "transformation", got.Type)
	require.NotNil(t, got.Enabled)
	assert.True(t, *got.Enabled)

	// Delete removes it and bumps the version again.
	require.NoError(t, cm.DeleteConfigOverride(ctx, "db.table"))

	v2, err := cm.GetConfigOverrideVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), v2)

	gone, err := cm.GetConfigOverride(ctx, "db.table")
	require.NoError(t, err)
	assert.Nil(t, gone)
}

// TestCacheManager_GetAllConfigOverrides covers SCAN iteration, the version-key
// skip, and unmarshalling of multiple overrides.
func TestCacheManager_GetAllConfigOverrides(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	// Empty -> empty slice.
	all, err := cm.GetAllConfigOverrides(ctx)
	require.NoError(t, err)
	assert.Empty(t, all)

	for _, id := range []string{"db.a", "db.b", "db.c"} {
		require.NoError(t, cm.SetConfigOverride(ctx, &ConfigOverride{
			ModelID:  id,
			Type:     "external",
			Override: json.RawMessage(`{}`),
		}))
	}

	all, err = cm.GetAllConfigOverrides(ctx)
	require.NoError(t, err)
	require.Len(t, all, 3)

	ids := make([]string, 0, len(all))
	for _, o := range all {
		ids = append(ids, o.ModelID)
	}

	assert.ElementsMatch(t, []string{"db.a", "db.b", "db.c"}, ids)
}

// TestCacheManager_GetAllConfigOverrides_UnmarshalError covers the unmarshal error
// branch when a stored value is not valid JSON.
func TestCacheManager_GetAllConfigOverrides_UnmarshalError(t *testing.T) {
	mr, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	require.NoError(t, mr.Set(configOverrideKeyPrefix+"db.bad", "not-json"))

	_, err := cm.GetAllConfigOverrides(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal config override")
}

// TestCacheManager_GetConfigOverride_UnmarshalError covers the single-get unmarshal
// error branch.
func TestCacheManager_GetConfigOverride_UnmarshalError(t *testing.T) {
	mr, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	require.NoError(t, mr.Set(configOverrideKeyPrefix+"db.bad", "not-json"))

	_, err := cm.GetConfigOverride(ctx, "db.bad")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal config override")
}

// TestCacheManager_DeleteAllConfigOverrides covers both the no-keys early return
// and the multi-key delete path.
func TestCacheManager_DeleteAllConfigOverrides(t *testing.T) {
	t.Run("no keys present", func(t *testing.T) {
		_, client := setupTestRedis(t)
		cm := NewCacheManager(client)
		ctx := context.Background()

		require.NoError(t, cm.DeleteAllConfigOverrides(ctx))

		// Version untouched because there was nothing to delete.
		v, err := cm.GetConfigOverrideVersion(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), v)
	})

	t.Run("deletes all present keys", func(t *testing.T) {
		_, client := setupTestRedis(t)
		cm := NewCacheManager(client)
		ctx := context.Background()

		for _, id := range []string{"db.a", "db.b"} {
			require.NoError(t, cm.SetConfigOverride(ctx, &ConfigOverride{
				ModelID:  id,
				Override: json.RawMessage(`{}`),
			}))
		}

		require.NoError(t, cm.DeleteAllConfigOverrides(ctx))

		all, err := cm.GetAllConfigOverrides(ctx)
		require.NoError(t, err)
		assert.Empty(t, all)
	})
}

// TestCacheManager_ConfigOverride_RedisErrors covers the error branches that fire
// when the underlying Redis connection is closed.
func TestCacheManager_ConfigOverride_RedisErrors(t *testing.T) {
	tests := []struct {
		name string
		call func(ctx context.Context, cm *CacheManager) error
		msg  string
	}{
		{
			name: "GetConfigOverride",
			call: func(ctx context.Context, cm *CacheManager) error {
				_, err := cm.GetConfigOverride(ctx, "db.table")
				return err
			},
			msg: "failed to get config override",
		},
		{
			name: "GetAllConfigOverrides",
			call: func(ctx context.Context, cm *CacheManager) error {
				_, err := cm.GetAllConfigOverrides(ctx)
				return err
			},
			msg: "failed to scan config overrides",
		},
		{
			name: "SetConfigOverride",
			call: func(ctx context.Context, cm *CacheManager) error {
				return cm.SetConfigOverride(ctx, &ConfigOverride{
					ModelID:  "db.table",
					Override: json.RawMessage(`{}`),
				})
			},
			msg: "failed to set config override",
		},
		{
			name: "DeleteConfigOverride",
			call: func(ctx context.Context, cm *CacheManager) error {
				return cm.DeleteConfigOverride(ctx, "db.table")
			},
			msg: "failed to delete config override",
		},
		{
			name: "DeleteAllConfigOverrides",
			call: func(ctx context.Context, cm *CacheManager) error {
				return cm.DeleteAllConfigOverrides(ctx)
			},
			msg: "failed to scan config overrides for deletion",
		},
		{
			name: "GetConfigOverrideVersion",
			call: func(ctx context.Context, cm *CacheManager) error {
				_, err := cm.GetConfigOverrideVersion(ctx)
				return err
			},
			msg: "failed to get config override version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := NewCacheManager(closedRedisClient(t))

			err := tt.call(context.Background(), cm)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.msg)
		})
	}
}

// TestCacheManager_GetAllConfigOverrides_GetErrorDuringScan covers the per-key Get
// failure inside the SCAN loop (a non-Nil error aborts the walk).
func TestCacheManager_GetAllConfigOverrides_GetErrorDuringScan(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	require.NoError(t, cm.SetConfigOverride(ctx, &ConfigOverride{
		ModelID:  "db.a",
		Override: json.RawMessage(`{}`),
	}))

	// Fault the GET so it returns a non-Nil error after SCAN found the key.
	client.AddHook(faultHook{cmdName: "get", err: errInjected})

	_, err := cm.GetAllConfigOverrides(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get config override")
}

// TestCacheManager_GetAllConfigOverrides_NilDuringScan covers the redis.Nil branch
// inside the SCAN loop: a key vanishes between SCAN and GET and is skipped.
func TestCacheManager_GetAllConfigOverrides_NilDuringScan(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	require.NoError(t, cm.SetConfigOverride(ctx, &ConfigOverride{
		ModelID:  "db.a",
		Override: json.RawMessage(`{}`),
	}))

	// Fault the GET with redis.Nil so the key is treated as already-deleted.
	client.AddHook(faultHook{cmdName: "get", err: redis.Nil})

	all, err := cm.GetAllConfigOverrides(ctx)
	require.NoError(t, err)
	assert.Empty(t, all)
}

// TestCacheManager_SetConfigOverride_MarshalError covers the json.Marshal failure
// branch via an invalid raw-message override.
func TestCacheManager_SetConfigOverride_MarshalError(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)

	err := cm.SetConfigOverride(context.Background(), &ConfigOverride{
		ModelID:  "db.a",
		Override: json.RawMessage("not-valid-json"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal config override")
}

// TestCacheManager_DeleteAllConfigOverrides_ExecError covers the pipe.Exec failure
// branch when deleting the discovered keys.
func TestCacheManager_DeleteAllConfigOverrides_ExecError(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	require.NoError(t, cm.SetConfigOverride(ctx, &ConfigOverride{
		ModelID:  "db.a",
		Override: json.RawMessage(`{}`),
	}))

	// Fault the pipelined DEL so Exec returns an error after the scan found keys.
	client.AddHook(faultHook{cmdName: "del", err: errInjected})

	err := cm.DeleteAllConfigOverrides(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete all config overrides")
}

// closedRedisClient returns a redis client whose server has been shut down, so all
// operations fail with a connection error.
func closedRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	mr, client := setupTestRedis(t)
	mr.Close()

	return client
}
