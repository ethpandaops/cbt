package admin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newServiceWithRedis builds an admin service whose cache manager is backed by the
// given redis client.
func newServiceWithRedis(client *redis.Client) Service {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	config := TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}

	return NewService(log, &mockClickhouseClient{}, "", "", config, client)
}

// TestService_OverridesWithoutCacheManager verifies every cache-backed wrapper
// returns the unavailable error (or nil) when no Redis client was configured.
func TestService_OverridesWithoutCacheManager(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	config := TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	svc := NewService(log, &mockClickhouseClient{}, "", "", config, nil)

	ctx := context.Background()

	// GetExternalBounds returns (nil, nil) when cache manager is absent.
	bounds, err := svc.GetExternalBounds(ctx, "db.table")
	require.NoError(t, err)
	assert.Nil(t, bounds)

	// The remaining wrappers return ErrCacheManagerUnavailable.
	errCalls := []struct {
		name string
		call func() error
	}{
		{"SetExternalBounds", func() error {
			return svc.SetExternalBounds(ctx, &BoundsCache{ModelID: "db.table"})
		}},
		{"DeleteExternalBounds", func() error {
			return svc.DeleteExternalBounds(ctx, "db.table")
		}},
		{"AcquireBoundsLock", func() error {
			_, err := svc.AcquireBoundsLock(ctx, "db.table")
			return err
		}},
		{"GetConfigOverride", func() error {
			_, err := svc.GetConfigOverride(ctx, "db.table")
			return err
		}},
		{"GetAllConfigOverrides", func() error {
			_, err := svc.GetAllConfigOverrides(ctx)
			return err
		}},
		{"SetConfigOverride", func() error {
			return svc.SetConfigOverride(ctx, &ConfigOverride{ModelID: "db.table"})
		}},
		{"DeleteConfigOverride", func() error {
			return svc.DeleteConfigOverride(ctx, "db.table")
		}},
		{"DeleteAllConfigOverrides", func() error {
			return svc.DeleteAllConfigOverrides(ctx)
		}},
		{"GetConfigOverrideVersion", func() error {
			_, err := svc.GetConfigOverrideVersion(ctx)
			return err
		}},
	}

	for _, c := range errCalls {
		t.Run(c.name, func(t *testing.T) {
			err := c.call()
			require.ErrorIs(t, err, ErrCacheManagerUnavailable)
		})
	}
}

// TestService_OverridesWithCacheManager exercises every cache-backed wrapper
// through to the underlying cache manager backed by miniredis.
func TestService_OverridesWithCacheManager(t *testing.T) {
	_, client := setupTestRedis(t)
	svc := newServiceWithRedis(client)
	ctx := context.Background()

	// Bounds round-trip.
	cache := &BoundsCache{ModelID: "db.table", Min: 1, Max: 100}
	require.NoError(t, svc.SetExternalBounds(ctx, cache))

	got, err := svc.GetExternalBounds(ctx, "db.table")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, uint64(1), got.Min)
	assert.Equal(t, uint64(100), got.Max)

	require.NoError(t, svc.DeleteExternalBounds(ctx, "db.table"))

	gone, err := svc.GetExternalBounds(ctx, "db.table")
	require.NoError(t, err)
	assert.Nil(t, gone)

	// Bounds lock acquire/release.
	lock, err := svc.AcquireBoundsLock(ctx, "db.table")
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.NoError(t, lock.Unlock(ctx))

	// Config override round-trip via the service wrappers.
	override := &ConfigOverride{
		ModelID:  "db.table",
		Type:     "transformation",
		Override: json.RawMessage(`{}`),
	}
	require.NoError(t, svc.SetConfigOverride(ctx, override))

	v, err := svc.GetConfigOverrideVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), v)

	gotOverride, err := svc.GetConfigOverride(ctx, "db.table")
	require.NoError(t, err)
	require.NotNil(t, gotOverride)
	assert.Equal(t, "db.table", gotOverride.ModelID)

	allOverrides, err := svc.GetAllConfigOverrides(ctx)
	require.NoError(t, err)
	require.Len(t, allOverrides, 1)

	require.NoError(t, svc.DeleteConfigOverride(ctx, "db.table"))

	require.NoError(t, svc.SetConfigOverride(ctx, override))
	require.NoError(t, svc.DeleteAllConfigOverrides(ctx))

	remaining, err := svc.GetAllConfigOverrides(ctx)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}
