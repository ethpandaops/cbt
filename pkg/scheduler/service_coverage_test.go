package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/liveconfig"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extNode(id string, cache *external.CacheConfig) models.Node {
	return models.Node{
		NodeType: models.NodeTypeExternal,
		Model: &testutil.FakeExternal{
			ID:     id,
			Config: external.Config{Cache: cache},
		},
	}
}

func TestTriggerInitialExternalScans(t *testing.T) {
	cache := &external.CacheConfig{
		IncrementalScanInterval: time.Minute,
		FullScanInterval:        5 * time.Minute,
	}

	completeBounds := &admin.BoundsCache{ModelID: "ext.complete", InitialScanComplete: true}
	incompleteBounds := &admin.BoundsCache{ModelID: "ext.incomplete", InitialScanComplete: false}

	tests := []struct {
		name        string
		externals   []models.Node
		admin       *adminfake.FakeAdminService
		nilAdmin    bool
		wantScanned []string
	}{
		{
			name:        "no external nodes",
			externals:   nil,
			admin:       &adminfake.FakeAdminService{},
			wantScanned: nil,
		},
		{
			name:        "non-external node is skipped",
			externals:   []models.Node{{NodeType: models.NodeTypeExternal, Model: "not-external"}},
			admin:       &adminfake.FakeAdminService{},
			wantScanned: nil,
		},
		{
			name:        "external without cache config is skipped",
			externals:   []models.Node{extNode("ext.nocache", nil)},
			admin:       &adminfake.FakeAdminService{},
			wantScanned: nil,
		},
		{
			name:      "bounds nil triggers scan",
			externals: []models.Node{extNode("ext.nil", cache)},
			admin: &adminfake.FakeAdminService{
				ExternalBoundsDefault: nil,
			},
			wantScanned: []string{"ext.nil"},
		},
		{
			name:      "bounds incomplete triggers scan",
			externals: []models.Node{extNode("ext.incomplete", cache)},
			admin: &adminfake.FakeAdminService{
				ExternalBounds: map[string]*admin.BoundsCache{"ext.incomplete": incompleteBounds},
			},
			wantScanned: []string{"ext.incomplete"},
		},
		{
			name:      "bounds complete skips scan",
			externals: []models.Node{extNode("ext.complete", cache)},
			admin: &adminfake.FakeAdminService{
				ExternalBounds: map[string]*admin.BoundsCache{"ext.complete": completeBounds},
			},
			wantScanned: nil,
		},
		{
			name:      "bounds error still triggers scan when bounds nil",
			externals: []models.Node{extNode("ext.err", cache)},
			admin: &adminfake.FakeAdminService{
				GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
					return nil, errTrackerBoom
				},
			},
			wantScanned: []string{"ext.err"},
		},
		{
			name:        "nil admin service skips bounds check",
			externals:   []models.Node{extNode("ext.noadmin", cache)},
			nilAdmin:    true,
			wantScanned: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu      sync.Mutex
				scanned []string
			)
			coord := &coordinatorfake.FakeCoordinator{
				ProcessExternalScanFn: func(modelID, _ string) {
					mu.Lock()
					scanned = append(scanned, modelID)
					mu.Unlock()
				},
			}

			dag := &testutil.FakeDAGReader{Externals: tt.externals}
			svc := newTestService(dag, coord)
			if !tt.nilAdmin {
				svc.admin = tt.admin
			}

			svc.triggerInitialExternalScans(context.Background())

			mu.Lock()
			defer mu.Unlock()
			assert.ElementsMatch(t, tt.wantScanned, scanned)
		})
	}
}

func TestSetLiveOverrides(t *testing.T) {
	svc := newTestService(&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{})

	applier := liveconfig.NewApplier(&testutil.FakeDAGReader{}, &adminfake.FakeAdminService{}, logrus.New())
	svc.SetLiveOverrides(applier)

	assert.Same(t, applier, svc.liveOverrides)
}

// TestPollLiveOverridesAppliesChange covers pollLiveOverrides: the ticker fires,
// CheckAndApply reports a change, and (because a ticker is running) the task list
// is rebuilt via UpdateTasks. A short poll interval keeps this fast.
func TestPollLiveOverridesAppliesChange(t *testing.T) {
	var version atomic.Int64
	version.Store(1)

	adminSvc := &adminfake.FakeAdminService{
		GetConfigOverrideVersionFn: func(_ context.Context) (int64, error) {
			return version.Load(), nil
		},
		GetAllConfigOverridesFn: func(_ context.Context) ([]admin.ConfigOverride, error) {
			return nil, nil
		},
	}

	dag := &testutil.FakeDAGReader{}
	svc := newTestService(dag, &coordinatorfake.FakeCoordinator{})
	svc.liveOverridePollInterval = 5 * time.Millisecond
	svc.liveOverrides = liveconfig.NewApplier(dag, adminSvc, logrus.New())

	// Install a fake ticker so the "changed" branch calls UpdateTasks.
	fakeTicker := &countingTicker{}
	svc.ticker = fakeTicker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		svc.pollLiveOverrides(ctx)
		close(done)
	}()

	// First tick: version 1 != lastVersion 0 -> changed -> UpdateTasks called.
	require.Eventually(t, func() bool {
		return fakeTicker.updateCount.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "expected UpdateTasks to be called after override change")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLiveOverrides did not exit after context cancel")
	}
}

// TestPollLiveOverridesError covers the CheckAndApply error branch (warn +
// continue) and the s.done exit path.
func TestPollLiveOverridesError(t *testing.T) {
	adminSvc := &adminfake.FakeAdminService{
		GetConfigOverrideVersionFn: func(_ context.Context) (int64, error) {
			return 0, errTrackerBoom
		},
	}

	dag := &testutil.FakeDAGReader{}
	svc := newTestService(dag, &coordinatorfake.FakeCoordinator{})
	svc.liveOverridePollInterval = 5 * time.Millisecond
	svc.liveOverrides = liveconfig.NewApplier(dag, adminSvc, logrus.New())

	var checks atomic.Int64
	adminSvc.GetConfigOverrideVersionFn = func(_ context.Context) (int64, error) {
		checks.Add(1)
		return 0, errTrackerBoom
	}

	done := make(chan struct{})
	go func() {
		svc.pollLiveOverrides(context.Background())
		close(done)
	}()

	require.Eventually(t, func() bool {
		return checks.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "expected at least one failing override check")

	// Exit via s.done.
	close(svc.done)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLiveOverrides did not exit after done close")
	}
}

// countingTicker is a tickerService fake that counts UpdateTasks calls.
type countingTicker struct {
	updateCount atomic.Int64
}

func (c *countingTicker) Start(_ context.Context) error { return nil }
func (c *countingTicker) Stop() error                   { return nil }
func (c *countingTicker) UpdateTasks(_ []scheduledTask) { c.updateCount.Add(1) }

var _ tickerService = (*countingTicker)(nil)

// TestServiceStartStopWithLiveOverrides exercises the full Start/Stop lifecycle
// including the live-override polling goroutine and a clean shutdown.
func TestServiceStartStopWithLiveOverrides(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	cfg := &Config{Concurrency: 1, Consolidation: "@every 10m", ShutdownTimeout: 2}
	redisOpt := &redis.Options{Addr: mr.Addr()}

	dag := &testutil.FakeDAGReader{}
	svc, err := NewService(log, cfg, redisOpt, dag, &coordinatorfake.FakeCoordinator{}, &adminfake.FakeAdminService{})
	require.NoError(t, err)

	s, ok := svc.(*service)
	require.True(t, ok)
	s.liveOverridePollInterval = 5 * time.Millisecond
	s.SetLiveOverrides(liveconfig.NewApplier(dag, &adminfake.FakeAdminService{}, log))

	require.NoError(t, svc.Start(context.Background()))
	require.NoError(t, svc.Stop())
}
