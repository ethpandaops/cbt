package admin

import (
	"context"
)

// GetExternalBounds retrieves cached external model bounds
func (a *service) GetExternalBounds(ctx context.Context, modelID string) (*BoundsCache, error) {
	if a.cacheManager == nil {
		return nil, nil
	}

	return a.cacheManager.GetBounds(ctx, modelID)
}

// SetExternalBounds stores external model bounds in cache
func (a *service) SetExternalBounds(ctx context.Context, cache *BoundsCache) error {
	if a.cacheManager == nil {
		return ErrCacheManagerUnavailable
	}

	return a.cacheManager.SetBounds(ctx, cache)
}

// DeleteExternalBounds removes cached external model bounds
func (a *service) DeleteExternalBounds(ctx context.Context, modelID string) error {
	if a.cacheManager == nil {
		return ErrCacheManagerUnavailable
	}

	return a.cacheManager.DeleteBounds(ctx, modelID)
}

// AcquireBoundsLock acquires a distributed lock for bounds updates on a specific model
func (a *service) AcquireBoundsLock(ctx context.Context, modelID string) (BoundsLock, error) {
	if a.cacheManager == nil {
		return nil, ErrCacheManagerUnavailable
	}

	return a.cacheManager.AcquireLock(ctx, modelID)
}

// GetConfigOverride retrieves a config override for a specific model.
func (a *service) GetConfigOverride(ctx context.Context, modelID string) (*ConfigOverride, error) {
	if a.cacheManager == nil {
		return nil, ErrCacheManagerUnavailable
	}

	return a.cacheManager.GetConfigOverride(ctx, modelID)
}

// GetAllConfigOverrides retrieves all config overrides.
func (a *service) GetAllConfigOverrides(ctx context.Context) ([]ConfigOverride, error) {
	if a.cacheManager == nil {
		return nil, ErrCacheManagerUnavailable
	}

	return a.cacheManager.GetAllConfigOverrides(ctx)
}

// SetConfigOverride stores a config override.
func (a *service) SetConfigOverride(ctx context.Context, override *ConfigOverride) error {
	if a.cacheManager == nil {
		return ErrCacheManagerUnavailable
	}

	return a.cacheManager.SetConfigOverride(ctx, override)
}

// DeleteConfigOverride removes a config override for a specific model.
func (a *service) DeleteConfigOverride(ctx context.Context, modelID string) error {
	if a.cacheManager == nil {
		return ErrCacheManagerUnavailable
	}

	return a.cacheManager.DeleteConfigOverride(ctx, modelID)
}

// DeleteAllConfigOverrides removes all config overrides.
func (a *service) DeleteAllConfigOverrides(ctx context.Context) error {
	if a.cacheManager == nil {
		return ErrCacheManagerUnavailable
	}

	return a.cacheManager.DeleteAllConfigOverrides(ctx)
}

// GetConfigOverrideVersion returns the current config override version counter.
func (a *service) GetConfigOverrideVersion(ctx context.Context) (int64, error) {
	if a.cacheManager == nil {
		return 0, ErrCacheManagerUnavailable
	}

	return a.cacheManager.GetConfigOverrideVersion(ctx)
}
