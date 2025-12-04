package validation

import (
	"context"
	"sync"
)

// MockValidator is a mock implementation of Validator for testing
type MockValidator struct {
	mu sync.Mutex

	// Control behavior
	ValidateDependenciesFunc func(ctx context.Context, modelID string, position, interval uint64) (Result, error)
	GetValidRangeFunc        func(ctx context.Context, modelID string, semantics RangeSemantics) (uint64, uint64, error)
	GetStartPositionFunc     func(ctx context.Context, modelID string) (uint64, error)

	// Track calls for assertions
	ValidateCalls   []ValidateCall
	ValidRangeCalls []ValidRangeCall
	StartPosCalls   []string
}

// ValidateCall records a ValidateDependencies call
type ValidateCall struct {
	ModelID  string
	Position uint64
	Interval uint64
}

// ValidRangeCall records a GetValidRange call
type ValidRangeCall struct {
	ModelID   string
	Semantics RangeSemantics
}

// NewMockValidator creates a new mock validator
func NewMockValidator() *MockValidator {
	return &MockValidator{
		ValidateCalls:   make([]ValidateCall, 0),
		ValidRangeCalls: make([]ValidRangeCall, 0),
		StartPosCalls:   make([]string, 0),
	}
}

// ValidateDependencies implements Validator
func (m *MockValidator) ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ValidateCalls = append(m.ValidateCalls, ValidateCall{
		ModelID:  modelID,
		Position: position,
		Interval: interval,
	})

	if m.ValidateDependenciesFunc != nil {
		return m.ValidateDependenciesFunc(ctx, modelID, position, interval)
	}

	return Result{CanProcess: true}, nil
}

// GetValidRange implements Validator
func (m *MockValidator) GetValidRange(ctx context.Context, modelID string, semantics RangeSemantics) (minPos, maxPos uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ValidRangeCalls = append(m.ValidRangeCalls, ValidRangeCall{
		ModelID:   modelID,
		Semantics: semantics,
	})

	if m.GetValidRangeFunc != nil {
		return m.GetValidRangeFunc(ctx, modelID, semantics)
	}

	return 0, 0, nil
}

// GetStartPosition implements Validator
func (m *MockValidator) GetStartPosition(ctx context.Context, modelID string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StartPosCalls = append(m.StartPosCalls, modelID)

	if m.GetStartPositionFunc != nil {
		return m.GetStartPositionFunc(ctx, modelID)
	}

	return 0, nil
}

// Reset clears all recorded calls
func (m *MockValidator) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ValidateCalls = make([]ValidateCall, 0)
	m.ValidRangeCalls = make([]ValidRangeCall, 0)
	m.StartPosCalls = make([]string, 0)
}

// GetValidateCallCount returns the number of ValidateDependencies calls
func (m *MockValidator) GetValidateCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.ValidateCalls)
}

// Ensure mock implements the interface
var _ Validator = (*MockValidator)(nil)
