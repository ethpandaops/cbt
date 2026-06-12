// Package coordinatorfake provides a configurable fake coordinator.Service for tests.
//
// It lives in its own package (rather than internal/testutil) because it imports
// pkg/coordinator; keeping it separate avoids an import cycle for pkg/coordinator's own
// in-package tests, which depend on the base testutil fakes.
package coordinatorfake

import (
	"context"
	"sync"

	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
)

// FakeCoordinator is a configurable fake implementing coordinator.Service for tests.
//
// The zero value is a no-op coordinator that counts Process and RunConsolidation calls.
// StartErr / StopErr are returned from Start / Stop. ProcessCalls and ConsolidationCalls
// are guarded by an internal mutex so they are safe to read from assertions after
// concurrent processing.
type FakeCoordinator struct {
	mu sync.Mutex

	ProcessCalls       int
	ConsolidationCalls int

	StartErr error
	StopErr  error

	// ProcessFn / ProcessExternalScanFn / RunConsolidationFn override the default behavior
	// when set; the call counters are still incremented for Process and RunConsolidation.
	ProcessFn             func(transformation models.Transformation, direction coordinator.Direction)
	ProcessExternalScanFn func(modelID, scanType string)
	RunConsolidationFn    func(ctx context.Context)

	TriggerBoundsRefreshFn func(ctx context.Context, modelID string) error
	TriggerScheduledRunFn  func(ctx context.Context, modelID string) error
}

// Start returns StartErr.
func (f *FakeCoordinator) Start(_ context.Context) error { return f.StartErr }

// Stop returns StopErr.
func (f *FakeCoordinator) Stop() error { return f.StopErr }

// Process increments ProcessCalls and invokes ProcessFn when set.
func (f *FakeCoordinator) Process(transformation models.Transformation, direction coordinator.Direction) {
	f.mu.Lock()
	f.ProcessCalls++
	f.mu.Unlock()

	if f.ProcessFn != nil {
		f.ProcessFn(transformation, direction)
	}
}

// ProcessExternalScan invokes ProcessExternalScanFn when set, otherwise is a no-op.
func (f *FakeCoordinator) ProcessExternalScan(modelID, scanType string) {
	if f.ProcessExternalScanFn != nil {
		f.ProcessExternalScanFn(modelID, scanType)
	}
}

// RunConsolidation increments ConsolidationCalls and invokes RunConsolidationFn when set.
func (f *FakeCoordinator) RunConsolidation(ctx context.Context) {
	f.mu.Lock()
	f.ConsolidationCalls++
	f.mu.Unlock()

	if f.RunConsolidationFn != nil {
		f.RunConsolidationFn(ctx)
	}
}

// TriggerBoundsRefresh returns nil by default.
func (f *FakeCoordinator) TriggerBoundsRefresh(ctx context.Context, modelID string) error {
	if f.TriggerBoundsRefreshFn != nil {
		return f.TriggerBoundsRefreshFn(ctx, modelID)
	}

	return nil
}

// TriggerScheduledRun returns nil by default.
func (f *FakeCoordinator) TriggerScheduledRun(ctx context.Context, modelID string) error {
	if f.TriggerScheduledRunFn != nil {
		return f.TriggerScheduledRunFn(ctx, modelID)
	}

	return nil
}

// ProcessCallCount returns the number of Process calls in a race-safe way.
func (f *FakeCoordinator) ProcessCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.ProcessCalls
}

// ConsolidationCallCount returns the number of RunConsolidation calls in a race-safe way.
func (f *FakeCoordinator) ConsolidationCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.ConsolidationCalls
}

var _ coordinator.Service = (*FakeCoordinator)(nil)
