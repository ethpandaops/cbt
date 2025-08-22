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
	GetInitialPositionFunc   func(ctx context.Context, modelID string) (uint64, error)
	GetEarliestPositionFunc  func(ctx context.Context, modelID string) (uint64, error)
	GetValidRangeFunc        func(ctx context.Context, modelID string) (uint64, uint64, error)

	// Track calls for assertions
	ValidateCalls   []ValidateCall
	InitialCalls    []string
	EarliestCalls   []string
	ValidRangeCalls []string
}

// ValidateCall records a ValidateDependencies call
type ValidateCall struct {
	ModelID  string
	Position uint64
	Interval uint64
}

// NewMockValidator creates a new mock validator
func NewMockValidator() *MockValidator {
	return &MockValidator{
		ValidateCalls: make([]ValidateCall, 0),
		InitialCalls:  make([]string, 0),
		EarliestCalls: make([]string, 0),
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

	// Default: dependencies satisfied
	return Result{
		CanProcess: true,
	}, nil
}

// GetInitialPosition implements Validator
func (m *MockValidator) GetInitialPosition(ctx context.Context, modelID string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.InitialCalls = append(m.InitialCalls, modelID)

	if m.GetInitialPositionFunc != nil {
		return m.GetInitialPositionFunc(ctx, modelID)
	}
	return 0, nil
}

// GetEarliestPosition implements Validator
func (m *MockValidator) GetEarliestPosition(ctx context.Context, modelID string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.EarliestCalls = append(m.EarliestCalls, modelID)

	if m.GetEarliestPositionFunc != nil {
		return m.GetEarliestPositionFunc(ctx, modelID)
	}
	return 0, nil
}

// GetValidRange implements Validator
func (m *MockValidator) GetValidRange(ctx context.Context, modelID string) (minPos, maxPos uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ValidRangeCalls = append(m.ValidRangeCalls, modelID)

	if m.GetValidRangeFunc != nil {
		return m.GetValidRangeFunc(ctx, modelID)
	}
	return 0, 0, nil
}

// Reset clears all recorded calls
func (m *MockValidator) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ValidateCalls = make([]ValidateCall, 0)
	m.InitialCalls = make([]string, 0)
	m.EarliestCalls = make([]string, 0)
}

// AssertValidateCalledWith checks if ValidateDependencies was called with specific args
func (m *MockValidator) AssertValidateCalledWith(modelID string, position, interval uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, call := range m.ValidateCalls {
		if call.ModelID == modelID && call.Position == position && call.Interval == interval {
			return true
		}
	}
	return false
}

// GetValidateCallCount returns the number of ValidateDependencies calls
func (m *MockValidator) GetValidateCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.ValidateCalls)
}

// Ensure mock implements the interface
var _ Validator = (*MockValidator)(nil)
