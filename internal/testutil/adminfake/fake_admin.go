// Package adminfake provides a configurable fake admin.Service for tests.
//
// It lives in its own package (rather than internal/testutil) because it imports
// pkg/admin; keeping it separate avoids an import cycle for pkg/admin's own in-package
// tests, which depend on the base testutil helpers (miniredis, containers).
package adminfake

import (
	"context"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
)

// FakeAdminService is a configurable in-memory fake implementing admin.Service for unit tests.
//
// The zero value behaves like the most common hand-rolled copy: every method returns
// zero values and no error. Override individual methods via the *Fn function fields, or
// rely on the built-in stateful conveniences:
//   - LastPositions / FirstPositions / Gaps / ExternalBounds maps back the position,
//     gap and bounds getters.
//   - RecordCompletion records the latest position into LastPositions.
//   - SetExternalBounds stores into ExternalBounds keyed by cache.ModelID.
//   - ConfigOverrides / ConfigOverrideByID back the config-override getters.
//
// All map access is guarded by an internal mutex so the fake is race-safe.
type FakeAdminService struct {
	mu sync.RWMutex

	// Stateful conveniences. Nil maps are treated as empty (lazily allocated on write).
	LastPositions  map[string]uint64
	FirstPositions map[string]uint64
	Gaps           map[string][]admin.GapInfo
	ExternalBounds map[string]*admin.BoundsCache

	// ExternalBoundsDefault is returned by GetExternalBounds for any modelID not present in
	// the ExternalBounds map. It mirrors the single-value bounds mocks used by the executor
	// tests where the same cache is returned regardless of the requested model.
	ExternalBoundsDefault *admin.BoundsCache

	// ConfigOverrides backs GetAllConfigOverrides; ConfigOverrideByID backs GetConfigOverride.
	ConfigOverrides    []admin.ConfigOverride
	ConfigOverrideByID map[string]*admin.ConfigOverride

	// Admin table identifiers returned by the GetXxxAdmin* getters. Empty strings fall
	// back to sensible defaults to match the most common hand-rolled copies.
	IncrementalDatabase string
	IncrementalTable    string
	ScheduledDatabase   string
	ScheduledTable      string

	// Optional callback invoked by SetExternalBounds before storing, useful for
	// capturing the saved cache in assertions.
	OnSetExternalBounds func(*admin.BoundsCache)

	// Function-field overrides. When set they take precedence over the default behavior.
	GetNextUnprocessedPositionFn    func(ctx context.Context, modelID string) (uint64, error)
	GetLastProcessedPositionFn      func(ctx context.Context, modelID string) (uint64, error)
	GetFirstPositionFn              func(ctx context.Context, modelID string) (uint64, error)
	RecordCompletionFn              func(ctx context.Context, modelID string, position, interval uint64) error
	RecordScheduledCompletionFn     func(ctx context.Context, modelID string, startDateTime time.Time) error
	GetLastScheduledExecutionFn     func(ctx context.Context, modelID string) (*time.Time, error)
	GetAllLastScheduledExecutionsFn func(ctx context.Context, modelIDs []string) (map[string]*time.Time, error)
	GetCoverageFn                   func(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error)
	GetProcessedRangesFn            func(ctx context.Context, modelID string) ([]admin.ProcessedRange, error)
	GetAllProcessedRangesFn         func(ctx context.Context, modelIDs []string) (map[string][]admin.ProcessedRange, error)
	FindGapsFn                      func(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]admin.GapInfo, error)
	ConsolidateHistoricalDataFn     func(ctx context.Context, modelID string) (uint64, error)
	DeletePeriodFn                  func(ctx context.Context, modelID string, startPos, endPos uint64) (uint64, error)
	GetExternalBoundsFn             func(ctx context.Context, modelID string) (*admin.BoundsCache, error)
	SetExternalBoundsFn             func(ctx context.Context, cache *admin.BoundsCache) error
	DeleteExternalBoundsFn          func(ctx context.Context, modelID string) error
	AcquireBoundsLockFn             func(ctx context.Context, modelID string) (admin.BoundsLock, error)
	GetConfigOverrideFn             func(ctx context.Context, modelID string) (*admin.ConfigOverride, error)
	GetAllConfigOverridesFn         func(ctx context.Context) ([]admin.ConfigOverride, error)
	SetConfigOverrideFn             func(ctx context.Context, override *admin.ConfigOverride) error
	DeleteConfigOverrideFn          func(ctx context.Context, modelID string) error
	DeleteAllConfigOverridesFn      func(ctx context.Context) error
	GetConfigOverrideVersionFn      func(ctx context.Context) (int64, error)
}

// GetNextUnprocessedPosition returns the recorded position for modelID, or 0.
func (f *FakeAdminService) GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error) {
	if f.GetNextUnprocessedPositionFn != nil {
		return f.GetNextUnprocessedPositionFn(ctx, modelID)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.LastPositions[modelID], nil
}

// GetLastProcessedPosition returns the recorded position for modelID, or 0.
func (f *FakeAdminService) GetLastProcessedPosition(ctx context.Context, modelID string) (uint64, error) {
	if f.GetLastProcessedPositionFn != nil {
		return f.GetLastProcessedPositionFn(ctx, modelID)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.LastPositions[modelID], nil
}

// GetFirstPosition returns the recorded first position for modelID, or 0.
func (f *FakeAdminService) GetFirstPosition(ctx context.Context, modelID string) (uint64, error) {
	if f.GetFirstPositionFn != nil {
		return f.GetFirstPositionFn(ctx, modelID)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.FirstPositions[modelID], nil
}

// RecordCompletion records position into LastPositions for modelID.
func (f *FakeAdminService) RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error {
	if f.RecordCompletionFn != nil {
		return f.RecordCompletionFn(ctx, modelID, position, interval)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.LastPositions == nil {
		f.LastPositions = make(map[string]uint64)
	}

	f.LastPositions[modelID] = position

	return nil
}

// RecordScheduledCompletion records a completed scheduled transformation (no-op by default).
func (f *FakeAdminService) RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error {
	if f.RecordScheduledCompletionFn != nil {
		return f.RecordScheduledCompletionFn(ctx, modelID, startDateTime)
	}

	return nil
}

// GetLastScheduledExecution returns nil by default.
func (f *FakeAdminService) GetLastScheduledExecution(ctx context.Context, modelID string) (*time.Time, error) {
	if f.GetLastScheduledExecutionFn != nil {
		return f.GetLastScheduledExecutionFn(ctx, modelID)
	}

	return nil, nil
}

// GetAllLastScheduledExecutions returns an empty map by default.
func (f *FakeAdminService) GetAllLastScheduledExecutions(ctx context.Context, modelIDs []string) (map[string]*time.Time, error) {
	if f.GetAllLastScheduledExecutionsFn != nil {
		return f.GetAllLastScheduledExecutionsFn(ctx, modelIDs)
	}

	return make(map[string]*time.Time), nil
}

// GetCoverage returns false by default.
func (f *FakeAdminService) GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error) {
	if f.GetCoverageFn != nil {
		return f.GetCoverageFn(ctx, modelID, startPos, endPos)
	}

	return false, nil
}

// GetProcessedRanges returns an empty slice by default.
func (f *FakeAdminService) GetProcessedRanges(ctx context.Context, modelID string) ([]admin.ProcessedRange, error) {
	if f.GetProcessedRangesFn != nil {
		return f.GetProcessedRangesFn(ctx, modelID)
	}

	return []admin.ProcessedRange{}, nil
}

// GetAllProcessedRanges returns an empty map by default.
func (f *FakeAdminService) GetAllProcessedRanges(ctx context.Context, modelIDs []string) (map[string][]admin.ProcessedRange, error) {
	if f.GetAllProcessedRangesFn != nil {
		return f.GetAllProcessedRangesFn(ctx, modelIDs)
	}

	return make(map[string][]admin.ProcessedRange), nil
}

// FindGaps returns the recorded gaps for modelID, or an empty slice.
func (f *FakeAdminService) FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]admin.GapInfo, error) {
	if f.FindGapsFn != nil {
		return f.FindGapsFn(ctx, modelID, minPos, maxPos, interval)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	if gaps, ok := f.Gaps[modelID]; ok {
		return gaps, nil
	}

	return []admin.GapInfo{}, nil
}

// ConsolidateHistoricalData returns 0 by default.
func (f *FakeAdminService) ConsolidateHistoricalData(ctx context.Context, modelID string) (uint64, error) {
	if f.ConsolidateHistoricalDataFn != nil {
		return f.ConsolidateHistoricalDataFn(ctx, modelID)
	}

	return 0, nil
}

// DeletePeriod returns 0 by default.
func (f *FakeAdminService) DeletePeriod(ctx context.Context, modelID string, startPos, endPos uint64) (uint64, error) {
	if f.DeletePeriodFn != nil {
		return f.DeletePeriodFn(ctx, modelID, startPos, endPos)
	}

	return 0, nil
}

// GetExternalBounds returns the stored bounds for modelID, or nil.
func (f *FakeAdminService) GetExternalBounds(ctx context.Context, modelID string) (*admin.BoundsCache, error) {
	if f.GetExternalBoundsFn != nil {
		return f.GetExternalBoundsFn(ctx, modelID)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	if bounds, ok := f.ExternalBounds[modelID]; ok {
		return bounds, nil
	}

	return f.ExternalBoundsDefault, nil
}

// SetExternalBounds stores cache keyed by cache.ModelID.
func (f *FakeAdminService) SetExternalBounds(ctx context.Context, cache *admin.BoundsCache) error {
	if f.OnSetExternalBounds != nil {
		f.OnSetExternalBounds(cache)
	}

	if f.SetExternalBoundsFn != nil {
		return f.SetExternalBoundsFn(ctx, cache)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ExternalBounds == nil {
		f.ExternalBounds = make(map[string]*admin.BoundsCache)
	}

	f.ExternalBounds[cache.ModelID] = cache

	return nil
}

// DeleteExternalBounds removes the stored bounds for modelID.
func (f *FakeAdminService) DeleteExternalBounds(ctx context.Context, modelID string) error {
	if f.DeleteExternalBoundsFn != nil {
		return f.DeleteExternalBoundsFn(ctx, modelID)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.ExternalBounds, modelID)

	return nil
}

// AcquireBoundsLock returns a no-op FakeBoundsLock by default.
func (f *FakeAdminService) AcquireBoundsLock(ctx context.Context, modelID string) (admin.BoundsLock, error) {
	if f.AcquireBoundsLockFn != nil {
		return f.AcquireBoundsLockFn(ctx, modelID)
	}

	return &FakeBoundsLock{}, nil
}

// GetIncrementalAdminDatabase returns IncrementalDatabase, defaulting to "admin".
func (f *FakeAdminService) GetIncrementalAdminDatabase() string {
	if f.IncrementalDatabase != "" {
		return f.IncrementalDatabase
	}

	return "admin"
}

// GetIncrementalAdminTable returns IncrementalTable, defaulting to "cbt_incremental".
func (f *FakeAdminService) GetIncrementalAdminTable() string {
	if f.IncrementalTable != "" {
		return f.IncrementalTable
	}

	return "cbt_incremental"
}

// GetScheduledAdminDatabase returns ScheduledDatabase, defaulting to "admin".
func (f *FakeAdminService) GetScheduledAdminDatabase() string {
	if f.ScheduledDatabase != "" {
		return f.ScheduledDatabase
	}

	return "admin"
}

// GetScheduledAdminTable returns ScheduledTable, defaulting to "cbt_scheduled".
func (f *FakeAdminService) GetScheduledAdminTable() string {
	if f.ScheduledTable != "" {
		return f.ScheduledTable
	}

	return "cbt_scheduled"
}

// GetConfigOverride returns the override stored under modelID, or nil.
func (f *FakeAdminService) GetConfigOverride(ctx context.Context, modelID string) (*admin.ConfigOverride, error) {
	if f.GetConfigOverrideFn != nil {
		return f.GetConfigOverrideFn(ctx, modelID)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ConfigOverrideByID == nil {
		return nil, nil
	}

	return f.ConfigOverrideByID[modelID], nil
}

// GetAllConfigOverrides returns ConfigOverrides.
func (f *FakeAdminService) GetAllConfigOverrides(ctx context.Context) ([]admin.ConfigOverride, error) {
	if f.GetAllConfigOverridesFn != nil {
		return f.GetAllConfigOverridesFn(ctx)
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.ConfigOverrides, nil
}

// SetConfigOverride is a no-op by default.
func (f *FakeAdminService) SetConfigOverride(ctx context.Context, override *admin.ConfigOverride) error {
	if f.SetConfigOverrideFn != nil {
		return f.SetConfigOverrideFn(ctx, override)
	}

	return nil
}

// DeleteConfigOverride is a no-op by default.
func (f *FakeAdminService) DeleteConfigOverride(ctx context.Context, modelID string) error {
	if f.DeleteConfigOverrideFn != nil {
		return f.DeleteConfigOverrideFn(ctx, modelID)
	}

	return nil
}

// DeleteAllConfigOverrides is a no-op by default.
func (f *FakeAdminService) DeleteAllConfigOverrides(ctx context.Context) error {
	if f.DeleteAllConfigOverridesFn != nil {
		return f.DeleteAllConfigOverridesFn(ctx)
	}

	return nil
}

// GetConfigOverrideVersion returns 0 by default.
func (f *FakeAdminService) GetConfigOverrideVersion(ctx context.Context) (int64, error) {
	if f.GetConfigOverrideVersionFn != nil {
		return f.GetConfigOverrideVersionFn(ctx)
	}

	return 0, nil
}

// FakeBoundsLock is a no-op implementation of admin.BoundsLock for tests.
type FakeBoundsLock struct{}

// Unlock is a no-op.
func (l *FakeBoundsLock) Unlock(_ context.Context) error { return nil }

var (
	_ admin.Service    = (*FakeAdminService)(nil)
	_ admin.BoundsLock = (*FakeBoundsLock)(nil)
)
