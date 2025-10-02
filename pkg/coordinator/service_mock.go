package coordinator

import (
	"context"
	"sync"

	"github.com/ethpandaops/cbt/pkg/models"
)

// MockService is a mock implementation of Service for testing
type MockService struct {
	mu sync.Mutex

	// Control behavior
	StartFunc               func(ctx context.Context) error
	StopFunc                func() error
	ProcessFunc             func(transformation models.Transformation, direction Direction)
	ProcessExternalScanFunc func(modelID, scanType string)

	// Track calls for assertions
	StartCalls               []context.Context
	StopCalls                int
	ProcessCalls             []ProcessCall
	ProcessExternalScanCalls []ExternalScanCall
}

// ProcessCall records a Process method call
type ProcessCall struct {
	Transformation models.Transformation
	Direction      Direction
}

// ExternalScanCall records a ProcessExternalScan method call
type ExternalScanCall struct {
	ModelID  string
	ScanType string
}

// NewMockService creates a new mock coordinator service
func NewMockService() *MockService {
	return &MockService{
		StartCalls:               make([]context.Context, 0),
		ProcessCalls:             make([]ProcessCall, 0),
		ProcessExternalScanCalls: make([]ExternalScanCall, 0),
	}
}

// Start implements Service
func (m *MockService) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StartCalls = append(m.StartCalls, ctx)

	if m.StartFunc != nil {
		return m.StartFunc(ctx)
	}
	return nil
}

// Stop implements Service
func (m *MockService) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StopCalls++

	if m.StopFunc != nil {
		return m.StopFunc()
	}
	return nil
}

// Process implements Service
func (m *MockService) Process(transformation models.Transformation, direction Direction) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ProcessCalls = append(m.ProcessCalls, ProcessCall{
		Transformation: transformation,
		Direction:      direction,
	})

	if m.ProcessFunc != nil {
		m.ProcessFunc(transformation, direction)
	}
}

// ProcessExternalScan implements Service
func (m *MockService) ProcessExternalScan(modelID, scanType string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ProcessExternalScanCalls = append(m.ProcessExternalScanCalls, ExternalScanCall{
		ModelID:  modelID,
		ScanType: scanType,
	})

	if m.ProcessExternalScanFunc != nil {
		m.ProcessExternalScanFunc(modelID, scanType)
	}
}

// Reset clears all recorded calls
func (m *MockService) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StartCalls = make([]context.Context, 0)
	m.StopCalls = 0
	m.ProcessCalls = make([]ProcessCall, 0)
	m.ProcessExternalScanCalls = make([]ExternalScanCall, 0)
}

// AssertStartCalled returns true if Start was called
func (m *MockService) AssertStartCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.StartCalls) > 0
}

// AssertProcessCalledWith checks if Process was called with specific args
func (m *MockService) AssertProcessCalledWith(modelID string, direction Direction) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, call := range m.ProcessCalls {
		if call.Transformation.GetID() == modelID && call.Direction == direction {
			return true
		}
	}
	return false
}

// Ensure mock implements the interface
var _ Service = (*MockService)(nil)
