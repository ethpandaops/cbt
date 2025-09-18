package transformation

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrHandlerNotRegistered is returned when no handler is registered for a type
	ErrHandlerNotRegistered = errors.New("no handler registered for type")
)

// HandlerFactory is a function that creates a handler from YAML data
type HandlerFactory func(data []byte, adminTable AdminTable) (Handler, error)

// Registry manages handler factories for different transformation types
type Registry struct {
	mu        sync.RWMutex
	factories map[Type]HandlerFactory
}

// globalRegistry is the singleton registry for transformation handlers
//
//nolint:gochecknoglobals // Required for the factory registration pattern
var globalRegistry = &Registry{
	factories: make(map[Type]HandlerFactory),
}

// RegisterHandler registers a handler factory for a transformation type
func RegisterHandler(transformationType Type, factory HandlerFactory) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.factories[transformationType] = factory
}

// CreateHandler creates a handler for the given type
func CreateHandler(transformationType Type, data []byte, adminTable AdminTable) (Handler, error) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	factory, exists := globalRegistry.factories[transformationType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrHandlerNotRegistered, transformationType)
	}

	return factory(data, adminTable)
}
