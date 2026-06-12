package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

var errBoom = errors.New("boom")

// backfillHandler builds a composite handler configured for backfill scenarios.
func backfillHandler(
	ctrl *gomock.Controller,
	maxInterval, minInterval uint64,
	limits *transformation.Limits,
	forwardFill, backfill bool,
) *compositeHandler {
	h := newCompositeHandler(ctrl)
	h.setupDefaultExpectations()
	h.MockIntervalHandler.EXPECT().GetMaxInterval().Return(maxInterval).AnyTimes()
	h.MockIntervalHandler.EXPECT().GetMinInterval().Return(minInterval).AnyTimes()
	h.MockIntervalHandler.EXPECT().AllowsPartialIntervals().Return(false).AnyTimes()
	h.MockIntervalHandler.EXPECT().AllowGapSkipping().Return(true).AnyTimes()
	h.MockScheduleHandler.EXPECT().IsForwardFillEnabled().Return(forwardFill).AnyTimes()
	h.MockScheduleHandler.EXPECT().IsBackfillEnabled().Return(backfill).AnyTimes()
	h.MockScheduleHandler.EXPECT().GetLimits().Return(limits).AnyTimes()

	return h
}

// TestProcessBack covers the entry-point guards in processBack.
func TestProcessBack(t *testing.T) {
	t.Run("nil handler returns early", func(t *testing.T) {
		svc := &service{log: logrus.NewEntry(logrus.New())}
		assert.NotPanics(t, func() {
			svc.processBack(&testutil.FakeTransformation{ID: "m"})
		})
	})

	t.Run("handler without schedule support returns early", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Plain handler does not implement ScheduleHandler.
		handler := newPlainHandler(ctrl)
		handler.EXPECT().ShouldTrackPosition().Return(true).AnyTimes()

		svc := &service{log: logrus.NewEntry(logrus.New())}
		svc.processBack(&testutil.FakeTransformation{ID: "m", Handler: handler})
	})

	t.Run("backfill disabled returns early", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		handler := backfillHandler(ctrl, 100, 0, nil, true, false)

		svc := &service{log: logrus.NewEntry(logrus.New())}
		svc.processBack(&testutil.FakeTransformation{ID: "m", Handler: handler})
	})

	t.Run("backfill enabled proceeds to opportunities", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		handler := backfillHandler(ctrl, 100, 0, nil, true, true)

		// admin returns 0 next position -> getBackfillBounds returns hasData true,
		// lastEndPos 0, then handleNoDataCase with forward fill enabled -> stop.
		svc := &service{
			log:   logrus.NewEntry(logrus.New()),
			admin: &adminfake.FakeAdminService{LastPositions: map[string]uint64{}},
		}
		svc.processBack(&testutil.FakeTransformation{ID: "m", Handler: handler})
	})
}

// TestGetMaxInterval covers both branches of getMaxInterval.
func TestGetMaxIntervalHelper(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &service{log: logrus.NewEntry(logrus.New())}

	t.Run("with interval handler", func(t *testing.T) {
		h := backfillHandler(ctrl, 42, 0, nil, true, true)
		assert.Equal(t, uint64(42), svc.getMaxInterval(h))
	})

	t.Run("plain handler returns zero", func(t *testing.T) {
		h := newPlainHandler(ctrl)
		assert.Equal(t, uint64(0), svc.getMaxInterval(h))
	})
}

// TestHandleNoDataCase covers the three branches.
func TestHandleNoDataCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &service{log: logrus.NewEntry(logrus.New())}

	t.Run("has data returns true", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, true, true)
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.True(t, svc.handleNoDataCase(trans, h, 500))
	})

	t.Run("no data with forward fill enabled returns false", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, true, true)
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.False(t, svc.handleNoDataCase(trans, h, 0))
	})

	t.Run("no data with forward fill disabled returns true", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.True(t, svc.handleNoDataCase(trans, h, 0))
	})

	t.Run("plain handler no data returns true", func(t *testing.T) {
		h := newPlainHandler(ctrl)
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.True(t, svc.handleNoDataCase(trans, h, 0))
	})
}

// TestLogExistingData covers both the positive and zero branches.
func TestLogExistingData(_ *testing.T) {
	svc := &service{log: logrus.NewEntry(logrus.New())}
	svc.logExistingData("m", 100, 200, 50) // lastEndPos > 0 branch
	svc.logExistingData("m", 0, 0, 50)     // lastEndPos == 0 branch
}

// TestGetBackfillBounds covers the error and success paths.
func TestGetBackfillBounds(t *testing.T) {
	t.Run("error getting next unprocessed position", func(t *testing.T) {
		svc := &service{
			log: logrus.NewEntry(logrus.New()),
			admin: &adminfake.FakeAdminService{
				GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
					return 0, errBoom
				},
			},
		}
		_, _, hasData := svc.getBackfillBounds(context.Background(), "m")
		assert.False(t, hasData)
	})

	t.Run("success returns positions", func(t *testing.T) {
		svc := &service{
			log: logrus.NewEntry(logrus.New()),
			admin: &adminfake.FakeAdminService{
				LastPositions: map[string]uint64{"m": 300},
			},
		}
		lastPos, lastEndPos, hasData := svc.getBackfillBounds(context.Background(), "m")
		assert.True(t, hasData)
		assert.Equal(t, uint64(300), lastPos)
		assert.Equal(t, uint64(300), lastEndPos)
	})
}

// TestCalculateBackfillScanRangeError covers the GetValidRange error branch and
// the applyMinimumLimit no-limit branch.
func TestCalculateBackfillScanRangeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validator := validation.NewMockValidator()
	validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
		return 0, 0, errBoom
	}

	h := backfillHandler(ctrl, 100, 0, nil, true, true)
	svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
	_, err := svc.calculateBackfillScanRange(context.Background(), &testutil.FakeTransformation{ID: "m", Handler: h}, 1000)
	assert.ErrorIs(t, err, errBoom)
}

// TestApplyMinimumLimit covers the branch where calculated position exceeds limit
// and the no-limits-configured branch.
func TestApplyMinimumLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &service{log: logrus.NewEntry(logrus.New())}

	t.Run("limit higher than initial uses limit", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, &transformation.Limits{Min: 500}, true, true)
		assert.Equal(t, uint64(500), svc.applyMinimumLimit("m", h, 100))
	})

	t.Run("calculated higher than limit uses calculated", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, &transformation.Limits{Min: 50}, true, true)
		assert.Equal(t, uint64(100), svc.applyMinimumLimit("m", h, 100))
	})

	t.Run("no limits configured uses calculated", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, true, true)
		assert.Equal(t, uint64(100), svc.applyMinimumLimit("m", h, 100))
	})

	t.Run("plain handler uses calculated", func(t *testing.T) {
		h := newPlainHandler(ctrl)
		assert.Equal(t, uint64(100), svc.applyMinimumLimit("m", h, 100))
	})
}

// TestFindGaps covers the scan-range error path, find-gaps error path, no-gaps
// path, and the gaps-found path.
func TestFindGaps(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	h := backfillHandler(ctrl, 100, 0, nil, true, true)
	trans := &testutil.FakeTransformation{ID: "m", Handler: h}

	t.Run("scan range error returns nil", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 0, errBoom
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		assert.Nil(t, svc.findGaps(context.Background(), trans, 1000, 100))
	})

	t.Run("find gaps error returns nil", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 2000, nil
		}
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin: &adminfake.FakeAdminService{
				FindGapsFn: func(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
					return nil, errBoom
				},
			},
		}
		assert.Nil(t, svc.findGaps(context.Background(), trans, 1000, 100))
	})

	t.Run("no gaps returns empty slice", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 2000, nil
		}
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin:     &adminfake.FakeAdminService{Gaps: map[string][]admin.GapInfo{}},
		}
		gaps := svc.findGaps(context.Background(), trans, 1000, 100)
		assert.NotNil(t, gaps)
		assert.Empty(t, gaps)
	})

	t.Run("gaps found returns them and logs summary", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 2000, nil
		}
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin: &adminfake.FakeAdminService{
				Gaps: map[string][]admin.GapInfo{
					"m": {{StartPos: 100, EndPos: 200}},
				},
			},
		}
		gaps := svc.findGaps(context.Background(), trans, 1000, 100)
		assert.Len(t, gaps, 1)
	})
}

// TestShouldContinueProcessing covers both the under-limit and at-limit branches.
func TestShouldContinueProcessing(t *testing.T) {
	svc := &service{log: logrus.NewEntry(logrus.New())}

	t.Run("under limit continues", func(t *testing.T) {
		stats := &gapProcessingStats{checked: 5}
		assert.True(t, svc.shouldContinueProcessing("m", stats, 5, 20, 10))
	})

	t.Run("at limit stops", func(t *testing.T) {
		stats := &gapProcessingStats{checked: 10}
		assert.False(t, svc.shouldContinueProcessing("m", stats, 10, 20, 10))
	})
}

// TestHandleUnfillableGap exercises the stats and logging path.
func TestHandleUnfillableGap(t *testing.T) {
	svc := &service{log: logrus.NewEntry(logrus.New())}
	stats := &gapProcessingStats{}
	svc.handleUnfillableGap("m", admin.GapInfo{StartPos: 100, EndPos: 200}, 0, stats)
	assert.Equal(t, 1, stats.skipped)
}

// TestLogProcessingSummary covers the only-when-no-enqueued branch.
func TestLogProcessingSummary(_ *testing.T) {
	svc := &service{log: logrus.NewEntry(logrus.New())}
	svc.logProcessingSummary("m", &gapProcessingStats{checked: 5, enqueued: 0}, 10) // logs
	svc.logProcessingSummary("m", &gapProcessingStats{checked: 5, enqueued: 2}, 10) // no log
	svc.logProcessingSummary("m", &gapProcessingStats{checked: 0, enqueued: 0}, 10) // no log
}

// TestIsGapFillable covers the nil handler, validation error, cannot-process and
// fillable branches.
func TestIsGapFillable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("nil handler not fillable", func(t *testing.T) {
		svc := &service{log: logrus.NewEntry(logrus.New())}
		assert.False(t, svc.isGapFillable(context.Background(), &testutil.FakeTransformation{ID: "m"}, admin.GapInfo{}, 0))
	})

	t.Run("validation error not fillable", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{}, errBoom
		}
		h := backfillHandler(ctrl, 100, 10, nil, true, true)
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.False(t, svc.isGapFillable(context.Background(), trans, admin.GapInfo{StartPos: 100, EndPos: 200}, 0))
	})

	t.Run("cannot process logs and returns false", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{
				CanProcess:   false,
				NextValidPos: 500,
				Errors:       []error{errBoom},
			}, nil
		}
		h := backfillHandler(ctrl, 100, 10, nil, true, true)
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.False(t, svc.isGapFillable(context.Background(), trans, admin.GapInfo{StartPos: 100, EndPos: 200}, 0))
	})

	t.Run("can process returns true", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		h := backfillHandler(ctrl, 100, 10, nil, true, true)
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.True(t, svc.isGapFillable(context.Background(), trans, admin.GapInfo{StartPos: 100, EndPos: 200}, 0))
	})

	t.Run("cannot process without next pos or errors", func(t *testing.T) {
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: false}, nil
		}
		h := backfillHandler(ctrl, 100, 10, nil, true, true)
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		assert.False(t, svc.isGapFillable(context.Background(), trans, admin.GapInfo{StartPos: 100, EndPos: 200}, 0))
	})
}

// TestLogIntervalAdjustment exercises every switch branch.
func TestLogIntervalAdjustment(_ *testing.T) {
	svc := &service{log: logrus.NewEntry(logrus.New())}
	// minInterval==0 && gapSize>maxInterval -> capped branch
	svc.logIntervalAdjustment("m", 0, 200, 0, 100, 100)
	// gapSize<minInterval -> min branch
	svc.logIntervalAdjustment("m", 0, 5, 10, 100, 10)
	// gapSize<maxInterval && gapSize!=intervalToUse -> adjusted branch
	svc.logIntervalAdjustment("m", 0, 50, 10, 100, 60)
	// default no-op branch
	svc.logIntervalAdjustment("m", 0, 100, 10, 100, 100)
}

// TestProcessSingleGap drives enqueue via the real queue manager and exercises
// the interval-determination path.
func TestProcessSingleGap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	h := backfillHandler(ctrl, 100, 0, nil, true, true)
	harness := newCoordHarness(t)

	trans := &testutil.FakeTransformation{ID: "db.model", Handler: h}
	harness.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
		return validation.Result{CanProcess: true}, nil
	}

	enqueued := harness.svc.processSingleGap(context.Background(), trans, admin.GapInfo{StartPos: 100, EndPos: 200}, 0)
	assert.True(t, enqueued)
}

// TestCheckBackfillOpportunities exercises the full opportunity scan including
// the not-enough-data, no-gaps, and gaps-found paths.
func TestCheckBackfillOpportunities(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("nil handler returns early", func(t *testing.T) {
		svc := &service{log: logrus.NewEntry(logrus.New())}
		assert.NotPanics(t, func() {
			svc.checkBackfillOpportunities(context.Background(), &testutil.FakeTransformation{ID: "m"})
		})
	})

	t.Run("no data hasData false returns early", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, true, true)
		svc := &service{
			log: logrus.NewEntry(logrus.New()),
			admin: &adminfake.FakeAdminService{
				GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
					return 0, errBoom
				},
			},
		}
		assert.NotPanics(t, func() {
			svc.checkBackfillOpportunities(context.Background(), &testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})

	t.Run("not enough data to scan", func(t *testing.T) {
		h := backfillHandler(ctrl, 1000, 0, nil, false, true)
		svc := &service{
			log:   logrus.NewEntry(logrus.New()),
			admin: &adminfake.FakeAdminService{LastPositions: map[string]uint64{"m": 500}},
		}
		// lastEndPos(500) < maxInterval(1000) -> early return
		assert.NotPanics(t, func() {
			svc.checkBackfillOpportunities(context.Background(), &testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})

	t.Run("gaps found and processed", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 2000, nil
		}
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		harness := newCoordHarness(t)
		harness.svc.validator = validator
		harness.admin.LastPositions = map[string]uint64{"db.model": 1000}
		harness.admin.Gaps = map[string][]admin.GapInfo{
			"db.model": {{StartPos: 100, EndPos: 200}},
		}
		harness.svc.checkBackfillOpportunities(context.Background(), &testutil.FakeTransformation{ID: "db.model", Handler: h})
	})

	t.Run("no gaps found returns early", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 2000, nil
		}
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin: &adminfake.FakeAdminService{
				LastPositions: map[string]uint64{"m": 1000},
				Gaps:          map[string][]admin.GapInfo{},
			},
		}
		assert.NotPanics(t, func() {
			svc.checkBackfillOpportunities(context.Background(), &testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})

	t.Run("findGaps nil (scan range error) returns early", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		// GetValidRange errors inside findGaps -> findGaps returns nil.
		validator.GetValidRangeFunc = func(_ context.Context, _ string, _ validation.RangeSemantics) (uint64, uint64, error) {
			return 0, 0, errBoom
		}
		svc := &service{
			log:       logrus.NewEntry(logrus.New()),
			validator: validator,
			admin: &adminfake.FakeAdminService{
				LastPositions: map[string]uint64{"m": 1000},
			},
		}
		assert.NotPanics(t, func() {
			svc.checkBackfillOpportunities(context.Background(), &testutil.FakeTransformation{ID: "m", Handler: h})
		})
	})
}

// TestProcessGaps covers the max-check limit, unfillable, and fillable-then-break
// branches.
func TestProcessGaps(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("first gap fillable enqueues and breaks", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		harness := newCoordHarness(t)
		harness.svc.validator = validator
		trans := &testutil.FakeTransformation{ID: "db.model", Handler: h}
		gaps := []admin.GapInfo{{StartPos: 100, EndPos: 200}}
		harness.svc.processGaps(context.Background(), trans, gaps)
	})

	t.Run("all gaps unfillable logs summary", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: false}, nil
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		gaps := []admin.GapInfo{
			{StartPos: 100, EndPos: 200},
			{StartPos: 300, EndPos: 400},
		}
		assert.NotPanics(t, func() {
			svc.processGaps(context.Background(), trans, gaps)
		})
	})

	t.Run("reaches max gap check limit", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: false}, nil
		}
		svc := &service{log: logrus.NewEntry(logrus.New()), validator: validator}
		trans := &testutil.FakeTransformation{ID: "m", Handler: h}
		gaps := make([]admin.GapInfo, 0, 12)
		for i := range uint64(12) {
			gaps = append(gaps, admin.GapInfo{StartPos: i * 100, EndPos: i*100 + 50})
		}
		assert.NotPanics(t, func() {
			svc.processGaps(context.Background(), trans, gaps)
		})
	})
}

// TestHandleFillableGap covers the enqueue-success (returns true) and the
// enqueue-skipped (returns false) scenarios.
func TestHandleFillableGap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("enqueue success returns true", func(t *testing.T) {
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		harness := newCoordHarness(t)
		harness.svc.validator = validator
		trans := &testutil.FakeTransformation{ID: "db.model", Handler: h}
		stats := &gapProcessingStats{}
		broke := harness.svc.handleFillableGap(context.Background(), trans, admin.GapInfo{StartPos: 100, EndPos: 200}, 0, stats, 1)
		assert.True(t, broke)
		assert.Equal(t, 1, stats.enqueued)
	})

	t.Run("enqueue skipped returns false", func(t *testing.T) {
		// Dependencies not satisfied at enqueue time -> processSingleGap returns
		// false -> handleFillableGap returns false without incrementing enqueued.
		h := backfillHandler(ctrl, 100, 0, nil, false, true)
		validator := validation.NewMockValidator()
		validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: false}, nil
		}
		harness := newCoordHarness(t)
		harness.svc.validator = validator
		trans := &testutil.FakeTransformation{ID: "db.model", Handler: h}
		stats := &gapProcessingStats{}
		broke := harness.svc.handleFillableGap(context.Background(), trans, admin.GapInfo{StartPos: 100, EndPos: 200}, 0, stats, 1)
		assert.False(t, broke)
		assert.Equal(t, 0, stats.enqueued)
	})
}

var _ = logrus.DebugLevel
