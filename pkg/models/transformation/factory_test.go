package transformation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errFactoryError = errors.New("factory error")

func TestRegisterHandler(t *testing.T) {
	// Create a test registry to avoid polluting global state
	testRegistry := &Registry{
		factories: make(map[Type]HandlerFactory),
	}

	// Save original registry and restore after test
	originalRegistry := globalRegistry
	globalRegistry = testRegistry
	defer func() {
		globalRegistry = originalRegistry
	}()

	// Test handler factory
	testFactory := func(_ []byte, _ AdminTable) (Handler, error) {
		return &mockHandler{}, nil
	}

	// Register handler
	RegisterHandler("test-type", testFactory)

	// Verify registration
	assert.NotNil(t, testRegistry.factories["test-type"])
	assert.Len(t, testRegistry.factories, 1)

	// Register another handler
	RegisterHandler("another-type", testFactory)
	assert.Len(t, testRegistry.factories, 2)

	// Overwrite existing handler
	newFactory := func(_ []byte, _ AdminTable) (Handler, error) {
		return &mockHandler{configValue: "new"}, nil
	}
	RegisterHandler("test-type", newFactory)

	// Should still have 2 handlers
	assert.Len(t, testRegistry.factories, 2)

	// Verify the handler was overwritten
	handler, err := CreateHandler("test-type", []byte{}, AdminTable{})
	require.NoError(t, err)
	mockH := handler.(*mockHandler)
	assert.Equal(t, "new", mockH.configValue)
}

func TestCreateHandler(t *testing.T) {
	// Create a test registry
	testRegistry := &Registry{
		factories: make(map[Type]HandlerFactory),
	}

	// Save original registry and restore after test
	originalRegistry := globalRegistry
	globalRegistry = testRegistry
	defer func() {
		globalRegistry = originalRegistry
	}()

	adminTable := AdminTable{
		Database: "admin",
		Table:    "cbt_test",
	}

	tests := []struct {
		name        string
		setupFunc   func()
		handlerType Type
		data        []byte
		wantErr     bool
		errType     error
		checkFunc   func(t *testing.T, h Handler)
	}{
		{
			name: "create handler for registered type",
			setupFunc: func() {
				RegisterHandler(TypeIncremental, func(data []byte, _ AdminTable) (Handler, error) {
					return &mockHandler{
						typeValue:   TypeIncremental,
						configValue: string(data),
					}, nil
				})
			},
			handlerType: TypeIncremental,
			data:        []byte("test-config"),
			wantErr:     false,
			checkFunc: func(t *testing.T, h Handler) {
				assert.NotNil(t, h)
				assert.Equal(t, TypeIncremental, h.Type())
				mockH := h.(*mockHandler)
				assert.Equal(t, "test-config", mockH.configValue)
			},
		},
		{
			name:        "create handler for unregistered type",
			setupFunc:   func() {}, // Don't register anything
			handlerType: Type("unregistered"),
			data:        []byte("test-config"),
			wantErr:     true,
			errType:     ErrHandlerNotRegistered,
		},
		{
			name: "factory returns error",
			setupFunc: func() {
				RegisterHandler(TypeScheduled, func(_ []byte, _ AdminTable) (Handler, error) {
					return nil, errFactoryError
				})
			},
			handlerType: TypeScheduled,
			data:        []byte("test-config"),
			wantErr:     true,
		},
		{
			name: "factory processes admin table correctly",
			setupFunc: func() {
				RegisterHandler(TypeIncremental, func(_ []byte, at AdminTable) (Handler, error) {
					return &mockHandler{
						adminTable: at,
					}, nil
				})
			},
			handlerType: TypeIncremental,
			data:        []byte("test-config"),
			wantErr:     false,
			checkFunc: func(t *testing.T, h Handler) {
				mockH := h.(*mockHandler)
				assert.Equal(t, adminTable, mockH.adminTable)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear registry before each test
			testRegistry.factories = make(map[Type]HandlerFactory)

			// Run setup
			tt.setupFunc()

			// Create handler
			handler, err := CreateHandler(tt.handlerType, tt.data, adminTable)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
			} else {
				require.NoError(t, err)
				if tt.checkFunc != nil {
					tt.checkFunc(t, handler)
				}
			}
		})
	}
}

func TestConcurrentRegistration(t *testing.T) {
	// Create a test registry
	testRegistry := &Registry{
		factories: make(map[Type]HandlerFactory),
	}

	// Save original registry and restore after test
	originalRegistry := globalRegistry
	globalRegistry = testRegistry
	defer func() {
		globalRegistry = originalRegistry
	}()

	// Test concurrent registration
	done := make(chan bool)
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			typeName := Type(string(rune('a' + id)))
			RegisterHandler(typeName, func(_ []byte, _ AdminTable) (Handler, error) {
				return &mockHandler{typeValue: typeName}, nil
			})
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all handlers were registered
	assert.Len(t, testRegistry.factories, numGoroutines)
}

func TestConcurrentCreateHandler(t *testing.T) {
	// Create a test registry
	testRegistry := &Registry{
		factories: make(map[Type]HandlerFactory),
	}

	// Save original registry and restore after test
	originalRegistry := globalRegistry
	globalRegistry = testRegistry
	defer func() {
		globalRegistry = originalRegistry
	}()

	// Register a test handler
	RegisterHandler(TypeIncremental, func(_ []byte, _ AdminTable) (Handler, error) {
		return &mockHandler{typeValue: TypeIncremental}, nil
	})

	// Test concurrent handler creation
	done := make(chan bool)
	numGoroutines := 10
	adminTable := AdminTable{Database: "admin", Table: "cbt"}

	for i := 0; i < numGoroutines; i++ {
		go func() {
			handler, err := CreateHandler(TypeIncremental, []byte("test"), adminTable)
			assert.NoError(t, err)
			assert.NotNil(t, handler)
			assert.Equal(t, TypeIncremental, handler.Type())
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// mockHandler is a test implementation of Handler interface
type mockHandler struct {
	typeValue          Type
	configValue        string
	adminTable         AdminTable
	shouldTrackPos     bool
	templateVars       map[string]any
	recordCompletionFn func() error
}

func (m *mockHandler) Type() Type {
	return m.typeValue
}

func (m *mockHandler) Config() any {
	return m.configValue
}

func (m *mockHandler) Validate() error {
	return nil
}

func (m *mockHandler) ShouldTrackPosition() bool {
	return m.shouldTrackPos
}

func (m *mockHandler) GetTemplateVariables(_ context.Context, _ TaskInfo) map[string]any {
	if m.templateVars != nil {
		return m.templateVars
	}
	return make(map[string]any)
}

func (m *mockHandler) GetAdminTable() AdminTable {
	return m.adminTable
}

func (m *mockHandler) RecordCompletion(_ context.Context, _ any, _ string, _ TaskInfo) error {
	if m.recordCompletionFn != nil {
		return m.recordCompletionFn()
	}
	return nil
}
