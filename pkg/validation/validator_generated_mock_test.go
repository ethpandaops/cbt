package validation_test

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/pkg/validation"
	validationmock "github.com/ethpandaops/cbt/pkg/validation/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestMockValidator_Interface demonstrates using the generated Validator mock
// for testing code that depends on the Validator interface.
// This is the primary use case for the generated mocks - when other packages
// need to mock the Validator interface for their own tests.
func TestMockValidator_Interface(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockValidator := validationmock.NewMockValidator(ctrl)

	// Setup expectations for ValidateDependencies
	mockValidator.EXPECT().
		ValidateDependencies(gomock.Any(), "test.model", uint64(1000), uint64(100)).
		Return(validation.Result{CanProcess: true}, nil)

	// Setup expectations for GetValidRange with Union semantics
	mockValidator.EXPECT().
		GetValidRange(gomock.Any(), "test.model", validation.Union).
		Return(uint64(0), uint64(5000), nil)

	// Setup expectations for GetStartPosition
	mockValidator.EXPECT().
		GetStartPosition(gomock.Any(), "test.model").
		Return(uint64(100), nil)

	// Test code that uses the Validator interface
	result, err := mockValidator.ValidateDependencies(context.Background(), "test.model", 1000, 100)
	require.NoError(t, err)
	assert.True(t, result.CanProcess)

	minPos, maxPos, err := mockValidator.GetValidRange(context.Background(), "test.model", validation.Union)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), minPos)
	assert.Equal(t, uint64(5000), maxPos)

	startPos, err := mockValidator.GetStartPosition(context.Background(), "test.model")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), startPos)
}

// TestMockValidator_IntersectionSemantics tests the mock with Intersection semantics
func TestMockValidator_IntersectionSemantics(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockValidator := validationmock.NewMockValidator(ctrl)

	// Setup for Intersection semantics (backfill)
	mockValidator.EXPECT().
		GetValidRange(gomock.Any(), "test.model", validation.Intersection).
		Return(uint64(500), uint64(2000), nil)

	minPos, maxPos, err := mockValidator.GetValidRange(context.Background(), "test.model", validation.Intersection)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), minPos)
	assert.Equal(t, uint64(2000), maxPos)
}

// TestMockValidator_ValidationFailure tests validation failure scenarios
func TestMockValidator_ValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockValidator := validationmock.NewMockValidator(ctrl)

	// Setup for validation that returns CanProcess=false with NextValidPos
	mockValidator.EXPECT().
		ValidateDependencies(gomock.Any(), "test.model", uint64(500), uint64(100)).
		Return(validation.Result{
			CanProcess:   false,
			NextValidPos: 1000,
		}, nil)

	result, err := mockValidator.ValidateDependencies(context.Background(), "test.model", 500, 100)
	require.NoError(t, err)
	assert.False(t, result.CanProcess)
	assert.Equal(t, uint64(1000), result.NextValidPos)
}

// TestMockExternalValidator_Interface demonstrates using the generated ExternalValidator mock
func TestMockExternalValidator_Interface(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockExtVal := validationmock.NewMockExternalValidator(ctrl)

	// We can't easily create a models.External here without importing the models mock,
	// but this shows the pattern for setting up expectations
	mockExtVal.EXPECT().
		GetMinMax(gomock.Any(), gomock.Any()).
		Return(uint64(100), uint64(5000), nil)

	// In real usage, you'd pass an actual External model
	minPos, maxPos, err := mockExtVal.GetMinMax(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), minPos)
	assert.Equal(t, uint64(5000), maxPos)
}

// TestMockValidator_MultipleCallsWithDifferentArgs demonstrates setting up
// multiple expectations with different arguments
func TestMockValidator_MultipleCallsWithDifferentArgs(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockValidator := validationmock.NewMockValidator(ctrl)

	// Setup different responses for different models
	mockValidator.EXPECT().
		GetStartPosition(gomock.Any(), "model.a").
		Return(uint64(100), nil)

	mockValidator.EXPECT().
		GetStartPosition(gomock.Any(), "model.b").
		Return(uint64(500), nil)

	mockValidator.EXPECT().
		GetStartPosition(gomock.Any(), "model.c").
		Return(uint64(0), validation.ErrModelNotFound)

	// Test each model
	posA, err := mockValidator.GetStartPosition(context.Background(), "model.a")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), posA)

	posB, err := mockValidator.GetStartPosition(context.Background(), "model.b")
	require.NoError(t, err)
	assert.Equal(t, uint64(500), posB)

	_, err = mockValidator.GetStartPosition(context.Background(), "model.c")
	require.ErrorIs(t, err, validation.ErrModelNotFound)
}
