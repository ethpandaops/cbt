// Package clickhouse provides test mocks for the ClickHouse client.
// This file should only be imported in test files.
package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
)

// Define static errors
var (
	ErrBothMustBeSlices = errors.New("both dest and data must be slices")
)

// MockClient is a mock implementation of ClientInterface for testing.
// It should only be used in test files, not in production code.
type MockClient struct {
	// Function fields that can be set by tests
	QueryOneFunc   func(ctx context.Context, query string, dest interface{}) error
	QueryManyFunc  func(ctx context.Context, query string, dest interface{}) error
	ExecuteFunc    func(ctx context.Context, query string) error
	BulkInsertFunc func(ctx context.Context, table string, data interface{}) error
	StartFunc      func() error
	StopFunc       func() error

	// Track calls for assertions
	Calls []MockCall
}

// MockCall represents a method call made to the mock
type MockCall struct {
	Method string
	Args   []interface{}
}

// NewMockClient creates a new mock client with default implementations
func NewMockClient() *MockClient {
	return &MockClient{
		QueryOneFunc: func(_ context.Context, _ string, _ interface{}) error {
			return nil
		},
		QueryManyFunc: func(_ context.Context, _ string, _ interface{}) error {
			return nil
		},
		ExecuteFunc: func(_ context.Context, _ string) error {
			return nil
		},
		BulkInsertFunc: func(_ context.Context, _ string, _ interface{}) error {
			return nil
		},
		StartFunc: func() error {
			return nil
		},
		StopFunc: func() error {
			return nil
		},
		Calls: make([]MockCall, 0),
	}
}

// QueryOne implements ClientInterface
func (m *MockClient) QueryOne(ctx context.Context, query string, dest interface{}) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "QueryOne",
		Args:   []interface{}{ctx, query, dest},
	})

	if m.QueryOneFunc != nil {
		return m.QueryOneFunc(ctx, query, dest)
	}

	return nil
}

// QueryMany implements ClientInterface
func (m *MockClient) QueryMany(ctx context.Context, query string, dest interface{}) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "QueryMany",
		Args:   []interface{}{ctx, query, dest},
	})

	if m.QueryManyFunc != nil {
		return m.QueryManyFunc(ctx, query, dest)
	}

	return nil
}

// Execute implements ClientInterface
func (m *MockClient) Execute(ctx context.Context, query string) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "Execute",
		Args:   []interface{}{ctx, query},
	})

	if m.ExecuteFunc != nil {
		return m.ExecuteFunc(ctx, query)
	}

	return nil
}

// BulkInsert implements ClientInterface
func (m *MockClient) BulkInsert(ctx context.Context, table string, data interface{}) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "BulkInsert",
		Args:   []interface{}{ctx, table, data},
	})

	if m.BulkInsertFunc != nil {
		return m.BulkInsertFunc(ctx, table, data)
	}

	return nil
}

// Start implements ClientInterface
func (m *MockClient) Start() error {
	m.Calls = append(m.Calls, MockCall{
		Method: "Start",
		Args:   []interface{}{},
	})

	if m.StartFunc != nil {
		return m.StartFunc()
	}

	return nil
}

// Stop implements ClientInterface
func (m *MockClient) Stop() error {
	m.Calls = append(m.Calls, MockCall{
		Method: "Stop",
		Args:   []interface{}{},
	})

	if m.StopFunc != nil {
		return m.StopFunc()
	}

	return nil
}

// GetCallCount returns the number of times a method was called
func (m *MockClient) GetCallCount(method string) int {
	count := 0

	for _, call := range m.Calls {
		if call.Method == method {
			count++
		}
	}

	return count
}

// WasCalled returns true if the specified method was called
func (m *MockClient) WasCalled(method string) bool {
	return m.GetCallCount(method) > 0
}

// Reset clears all recorded calls
func (m *MockClient) Reset() {
	m.Calls = make([]MockCall, 0)
}

// Helper functions for common test scenarios

// SetQueryOneResponse sets up the mock to return specific data for QueryOne
func (m *MockClient) SetQueryOneResponse(data interface{}) {
	m.QueryOneFunc = func(_ context.Context, _ string, dest interface{}) error {
		// Marshal the data to JSON and then unmarshal into dest
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}

		return json.Unmarshal(jsonData, dest)
	}
}

// SetQueryManyResponse sets up the mock to return specific data for QueryMany
func (m *MockClient) SetQueryManyResponse(data interface{}) {
	m.QueryManyFunc = func(_ context.Context, _ string, dest interface{}) error {
		// Use reflection to set the slice
		destValue := reflect.ValueOf(dest).Elem()
		srcValue := reflect.ValueOf(data)

		if destValue.Kind() != reflect.Slice || srcValue.Kind() != reflect.Slice {
			return ErrBothMustBeSlices
		}

		destValue.Set(srcValue)

		return nil
	}
}

// SetError sets all functions to return the specified error
func (m *MockClient) SetError(err error) {
	m.QueryOneFunc = func(_ context.Context, _ string, _ interface{}) error {
		return err
	}
	m.QueryManyFunc = func(_ context.Context, _ string, _ interface{}) error {
		return err
	}
	m.ExecuteFunc = func(_ context.Context, _ string) error {
		return err
	}
	m.BulkInsertFunc = func(_ context.Context, _ string, _ interface{}) error {
		return err
	}
	m.StartFunc = func() error {
		return err
	}
	m.StopFunc = func() error {
		return err
	}
}
