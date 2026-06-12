package coordinator

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// forwardHandler builds a composite handler for forward-fill scenarios.
func forwardHandler(
	ctrl *gomock.Controller,
	maxInterval, minInterval uint64,
	limits *transformation.Limits,
	forwardFill, allowGapSkipping, allowPartial, trackPosition bool,
) *compositeHandler {
	h := newCompositeHandler(ctrl)
	h.MockHandler.EXPECT().Type().Return(transformation.TypeIncremental).AnyTimes()
	h.MockHandler.EXPECT().Config().Return(&transformation.Config{Type: transformation.TypeIncremental}).AnyTimes()
	h.MockHandler.EXPECT().Validate().Return(nil).AnyTimes()
	h.MockHandler.EXPECT().ShouldTrackPosition().Return(trackPosition).AnyTimes()
	h.MockHandler.EXPECT().GetTemplateVariables(gomock.Any(), gomock.Any()).Return(map[string]any{}).AnyTimes()
	h.MockHandler.EXPECT().GetAdminTable().Return(transformation.AdminTable{}).AnyTimes()
	h.MockHandler.EXPECT().RecordCompletion(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	h.MockIntervalHandler.EXPECT().GetMaxInterval().Return(maxInterval).AnyTimes()
	h.MockIntervalHandler.EXPECT().GetMinInterval().Return(minInterval).AnyTimes()
	h.MockIntervalHandler.EXPECT().AllowsPartialIntervals().Return(allowPartial).AnyTimes()
	h.MockIntervalHandler.EXPECT().AllowGapSkipping().Return(allowGapSkipping).AnyTimes()
	h.MockScheduleHandler.EXPECT().IsForwardFillEnabled().Return(forwardFill).AnyTimes()
	h.MockScheduleHandler.EXPECT().IsBackfillEnabled().Return(false).AnyTimes()
	h.MockScheduleHandler.EXPECT().GetLimits().Return(limits).AnyTimes()

	return h
}

// TestProcessForwardRouting covers the high-level routing in processForward.
func TestProcessForwardRouting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("forward fill disabled returns early", func(t *testing.T) {
		h := forwardHandler(ctrl, 100, 0, nil, false, true, false, true)
		svc := &service{log: logrus.NewEntry(logrus.New())}
		assert.NotPanics(t, func() {
			svc.processForward(&testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})

	t.Run("position error returns early", func(t *testing.T) {
		h := forwardHandler(ctrl, 100, 0, nil, true, true, false, true)
		validator := validation.NewMockValidator()
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin:     newAdminNextErrService(),
		}
		assert.NotPanics(t, func() {
			svc.processForward(&testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})

	t.Run("position beyond limit returns early", func(t *testing.T) {
		h := forwardHandler(ctrl, 100, 0, &transformation.Limits{Max: 50}, true, true, false, true)
		validator := validation.NewMockValidator()
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin:     newAdminPosService(100),
		}
		assert.NotPanics(t, func() {
			svc.processForward(&testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})

	t.Run("gap-aware path enqueues via queue manager", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, true, false, true)
		harness := newCoordHarness(t)
		harness.admin.LastPositions = map[string]uint64{"db.model": 100}
		harness.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		harness.svc.processForward(&testutil.FakeTransformation{ID: "db.model", Handler: h})

		pending, err := harness.svc.queueManager.IsTaskPendingOrRunning(forwardPending("db.model"))
		require.NoError(t, err)
		assert.True(t, pending)
	})

	t.Run("normal path (no gap skipping) enqueues", func(t *testing.T) {
		// allowGapSkipping=false routes through calculateProcessingInterval path.
		h := forwardHandler(ctrl, 50, 0, nil, true, false, false, true)
		harness := newCoordHarness(t)
		harness.admin.LastPositions = map[string]uint64{"db.model2": 100}
		harness.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		harness.svc.processForward(&testutil.FakeTransformation{ID: "db.model2", Handler: h})

		pending, err := harness.svc.queueManager.IsTaskPendingOrRunning(forwardPending("db.model2"))
		require.NoError(t, err)
		assert.True(t, pending)
	})

	t.Run("scheduled transformation (no position tracking) uses normal path", func(t *testing.T) {
		// trackPosition=false -> normal path even with gap skipping allowed.
		h := forwardHandler(ctrl, 50, 0, nil, true, true, false, false)
		harness := newCoordHarness(t)
		harness.admin.LastPositions = map[string]uint64{"db.model3": 100}
		harness.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		harness.svc.processForward(&testutil.FakeTransformation{ID: "db.model3", Handler: h})

		pending, err := harness.svc.queueManager.IsTaskPendingOrRunning(forwardPending("db.model3"))
		require.NoError(t, err)
		assert.True(t, pending)
	})

	t.Run("normal path with partial interval stop returns early", func(t *testing.T) {
		// allowPartial true with dependency limit below min interval -> shouldReturn.
		h := forwardHandler(ctrl, 50, 40, nil, true, false, true, true)
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 110, nil // availableInterval=10 < minInterval(40) -> stop
		}
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin:     newAdminPosService(100),
		}
		assert.NotPanics(t, func() {
			svc.processForward(&testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})
}

// TestShouldAllowGapSkipping covers the default (non-interval handler) and the
// interval handler branches.
func TestShouldAllowGapSkipping(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &service{log: logrus.NewEntry(logrus.New())}

	t.Run("plain handler defaults to true", func(t *testing.T) {
		assert.True(t, svc.shouldAllowGapSkipping(newPlainHandler(ctrl)))
	})

	t.Run("interval handler returns configured value", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, false, false, true)
		assert.False(t, svc.shouldAllowGapSkipping(h))
	})
}

// TestGetProcessingPosition covers the error path, the initial-position path,
// the start-position error path and the existing-position path.
func TestGetProcessingPosition(t *testing.T) {
	t.Run("next position error", func(t *testing.T) {
		svc := &service{log: logrus.NewEntry(logrus.New()), admin: newAdminNextErrService()}
		_, err := svc.getProcessingPosition(context.Background(), &testutil.FakeTransformation{ID: "m"})
		assert.ErrorIs(t, err, errBoom)
	})

	t.Run("initial position computed when zero", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.GetStartPositionFunc = func(_ context.Context, _ string) (uint64, error) {
			return 777, nil
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), admin: newAdminPosService(0), validator: validator}
		pos, err := svc.getProcessingPosition(context.Background(), &testutil.FakeTransformation{ID: "m"})
		require.NoError(t, err)
		assert.Equal(t, uint64(777), pos)
	})

	t.Run("initial position error", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.GetStartPositionFunc = func(_ context.Context, _ string) (uint64, error) {
			return 0, errBoom
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), admin: newAdminPosService(0), validator: validator}
		_, err := svc.getProcessingPosition(context.Background(), &testutil.FakeTransformation{ID: "m"})
		assert.ErrorIs(t, err, errBoom)
	})

	t.Run("existing position returned", func(t *testing.T) {
		svc := &service{log: logrus.NewEntry(logrus.New()), admin: newAdminPosService(500)}
		pos, err := svc.getProcessingPosition(context.Background(), &testutil.FakeTransformation{ID: "m"})
		require.NoError(t, err)
		assert.Equal(t, uint64(500), pos)
	})
}

// TestGetMaxLimit covers the limits-set and no-limits branches.
func TestGetMaxLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &service{log: logrus.NewEntry(logrus.New())}

	t.Run("limits configured", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, &transformation.Limits{Max: 999}, true, false, false, true)
		assert.Equal(t, uint64(999), svc.getMaxLimit(h))
	})

	t.Run("no limits", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, false, false, true)
		assert.Equal(t, uint64(0), svc.getMaxLimit(h))
	})

	t.Run("plain handler returns zero", func(t *testing.T) {
		assert.Equal(t, uint64(0), svc.getMaxLimit(newPlainHandler(ctrl)))
	})
}

// TestIsPositionBeyondLimit covers both branches.
func TestIsPositionBeyondLimit(t *testing.T) {
	svc := &service{log: logrus.NewEntry(logrus.New())}
	assert.True(t, svc.isPositionBeyondLimit("m", 100, 50))
	assert.False(t, svc.isPositionBeyondLimit("m", 10, 50))
	assert.False(t, svc.isPositionBeyondLimit("m", 100, 0))
}

// TestCalculateProcessingInterval covers the interval-adjustment, partial-interval
// stop, partial-interval-adjust and plain-handler branches.
func TestCalculateProcessingInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("plain handler zero interval no partial", func(t *testing.T) {
		svc := &service{log: logrus.NewEntry(logrus.New())}
		interval, stop := svc.calculateProcessingInterval(context.Background(),
			&testutil.FakeTransformation{ID: "m"}, newPlainHandler(ctrl), 100, 0)
		assert.False(t, stop)
		assert.Equal(t, uint64(0), interval)
	})

	t.Run("interval clamped to max limit", func(t *testing.T) {
		h := forwardHandler(ctrl, 100, 0, nil, true, false, false, true)
		svc := &service{log: logrus.NewEntry(logrus.New())}
		interval, stop := svc.calculateProcessingInterval(context.Background(),
			&testutil.FakeTransformation{ID: "m", Handler: h}, h, 100, 150)
		assert.False(t, stop)
		assert.Equal(t, uint64(50), interval) // 150 - 100
	})

	t.Run("partial interval signals stop", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 40, nil, true, false, true, true)
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 110, nil // available 10 < min 40 -> stop
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		_, stop := svc.calculateProcessingInterval(context.Background(),
			&testutil.FakeTransformation{ID: "m", Handler: h}, h, 100, 0)
		assert.True(t, stop)
	})

	t.Run("partial interval adjusts down", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 5, nil, true, false, true, true)
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 130, nil // available 30 >= min 5, < interval 50 -> use 30
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		interval, stop := svc.calculateProcessingInterval(context.Background(),
			&testutil.FakeTransformation{ID: "m", Handler: h}, h, 100, 0)
		assert.False(t, stop)
		assert.Equal(t, uint64(30), interval)
	})
}

// TestProcessForwardWithGapSkippingReal exercises the real implementation's
// enqueue, gap-skip recursion, beyond-limit and no-more-positions branches.
func TestProcessForwardWithGapSkippingReal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("calculate interval stop returns early", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 40, nil, true, true, true, true)
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 110, nil // available 10 < min 40 -> stop
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		assert.NotPanics(t, func() {
			svc.processForwardWithGapSkipping(context.Background(),
				&testutil.FakeTransformation{ID: "m", Handler: h}, 100, 0)
		})
	})

	t.Run("validation error returns early", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, true, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{}, errBoom
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		assert.NotPanics(t, func() {
			svc.processForwardWithGapSkipping(context.Background(),
				&testutil.FakeTransformation{ID: "m", Handler: h}, 100, 0)
		})
	})

	t.Run("can process enqueues", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, true, false, true)
		harness := newCoordHarness(t)
		harness.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		harness.svc.processForwardWithGapSkipping(context.Background(),
			&testutil.FakeTransformation{ID: "db.gap", Handler: h}, 100, 0)

		pending, err := harness.svc.queueManager.IsTaskPendingOrRunning(forwardPending("db.gap"))
		require.NoError(t, err)
		assert.True(t, pending)
	})

	t.Run("gap detected recurses then enqueues", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, true, false, true)
		harness := newCoordHarness(t)
		harness.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, pos, _ uint64) (validation.Result, error) {
			if pos == 100 {
				return validation.Result{CanProcess: false, NextValidPos: 200}, nil
			}

			return validation.Result{CanProcess: true}, nil
		}
		harness.svc.processForwardWithGapSkipping(context.Background(),
			&testutil.FakeTransformation{ID: "db.gap2", Handler: h}, 100, 0)

		// Task is enqueued at the skipped-to position (200), not the original (100).
		pending, err := harness.svc.queueManager.IsTaskPendingOrRunning(forwardPending("db.gap2"))
		require.NoError(t, err)
		assert.True(t, pending)
	})

	t.Run("gap beyond limit returns", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, true, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: false, NextValidPos: 500}, nil
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		assert.NotPanics(t, func() {
			svc.processForwardWithGapSkipping(context.Background(),
				&testutil.FakeTransformation{ID: "m", Handler: h}, 100, 200)
		})
	})

	t.Run("no more valid positions", func(t *testing.T) {
		h := forwardHandler(ctrl, 50, 0, nil, true, true, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: false, NextValidPos: 0}, nil
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		assert.NotPanics(t, func() {
			svc.processForwardWithGapSkipping(context.Background(),
				&testutil.FakeTransformation{ID: "m", Handler: h}, 100, 0)
		})
	})
}

// forwardPending builds an incremental payload matching a forward enqueue.
func forwardPending(modelID string) tasks.IncrementalPayload {
	return tasks.IncrementalPayload{ModelID: modelID, Direction: string(DirectionForward)}
}

// adminPos returns a fake admin service whose next position is fixed.
func newAdminPosService(pos uint64) *adminfake.FakeAdminService {
	return &adminfake.FakeAdminService{
		GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
			return pos, nil
		},
	}
}

// newAdminNextErrService returns a fake admin service that errors on next position.
func newAdminNextErrService() *adminfake.FakeAdminService {
	return &adminfake.FakeAdminService{
		GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
			return 0, errBoom
		},
	}
}
