package validation_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errValidateFailed = errors.New("validate failed")
	errRangeFailed    = errors.New("range failed")
	errStartFailed    = errors.New("start failed")
)

func TestMockValidator_DefaultBehavior(t *testing.T) {
	ctx := context.Background()
	m := validation.NewMockValidator()

	// Default ValidateDependencies returns CanProcess=true, nil error.
	result, err := m.ValidateDependencies(ctx, "model.a", 100, 10)
	require.NoError(t, err)
	assert.True(t, result.CanProcess)

	// Default GetValidRange returns 0, 0, nil.
	minPos, maxPos, err := m.GetValidRange(ctx, "model.a", validation.Union)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), minPos)
	assert.Equal(t, uint64(0), maxPos)

	// Default GetStartPosition returns 0, nil.
	pos, err := m.GetStartPosition(ctx, "model.a")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), pos)

	// Calls are recorded.
	assert.Equal(t, 1, m.GetValidateCallCount())
	require.Len(t, m.ValidateCalls, 1)
	assert.Equal(t, validation.ValidateCall{ModelID: "model.a", Position: 100, Interval: 10}, m.ValidateCalls[0])
	require.Len(t, m.ValidRangeCalls, 1)
	assert.Equal(t, validation.ValidRangeCall{ModelID: "model.a", Semantics: validation.Union}, m.ValidRangeCalls[0])
	require.Len(t, m.StartPosCalls, 1)
	assert.Equal(t, "model.a", m.StartPosCalls[0])
}

func TestMockValidator_CustomFuncs(t *testing.T) {
	ctx := context.Background()
	m := validation.NewMockValidator()

	m.ValidateDependenciesFunc = func(_ context.Context, modelID string, position, interval uint64) (validation.Result, error) {
		assert.Equal(t, "model.b", modelID)
		assert.Equal(t, uint64(200), position)
		assert.Equal(t, uint64(20), interval)
		return validation.Result{CanProcess: false, NextValidPos: 300}, errValidateFailed
	}
	m.GetValidRangeFunc = func(_ context.Context, modelID string, semantics validation.RangeSemantics) (uint64, uint64, error) {
		assert.Equal(t, "model.b", modelID)
		assert.Equal(t, validation.Intersection, semantics)
		return 5, 50, errRangeFailed
	}
	m.GetStartPositionFunc = func(_ context.Context, modelID string) (uint64, error) {
		assert.Equal(t, "model.b", modelID)
		return 42, errStartFailed
	}

	result, err := m.ValidateDependencies(ctx, "model.b", 200, 20)
	require.ErrorIs(t, err, errValidateFailed)
	assert.False(t, result.CanProcess)
	assert.Equal(t, uint64(300), result.NextValidPos)

	minPos, maxPos, err := m.GetValidRange(ctx, "model.b", validation.Intersection)
	require.ErrorIs(t, err, errRangeFailed)
	assert.Equal(t, uint64(5), minPos)
	assert.Equal(t, uint64(50), maxPos)

	pos, err := m.GetStartPosition(ctx, "model.b")
	require.ErrorIs(t, err, errStartFailed)
	assert.Equal(t, uint64(42), pos)
}

func TestMockValidator_Reset(t *testing.T) {
	ctx := context.Background()
	m := validation.NewMockValidator()

	_, errValidate := m.ValidateDependencies(ctx, "model.c", 1, 1)
	require.NoError(t, errValidate)
	_, _, errRange := m.GetValidRange(ctx, "model.c", validation.Union)
	require.NoError(t, errRange)
	_, errStart := m.GetStartPosition(ctx, "model.c")
	require.NoError(t, errStart)

	require.Equal(t, 1, m.GetValidateCallCount())
	require.Len(t, m.ValidRangeCalls, 1)
	require.Len(t, m.StartPosCalls, 1)

	m.Reset()

	assert.Equal(t, 0, m.GetValidateCallCount())
	assert.Empty(t, m.ValidateCalls)
	assert.Empty(t, m.ValidRangeCalls)
	assert.Empty(t, m.StartPosCalls)
}
