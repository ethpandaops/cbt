package validation

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
)

// DependencyStatus represents the status of a single dependency
type DependencyStatus struct {
	ModelID   string
	Available bool
	MinPos    uint64
	MaxPos    uint64
	Error     error
}

// Result contains the result of dependency validation
type Result struct {
	CanProcess   bool
	Dependencies []DependencyStatus
	Errors       []error
}

// DependencyValidator validates that dependencies are satisfied before processing
type DependencyValidator interface {
	ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (Result, error)
	GetInitialPosition(ctx context.Context, modelID string) (uint64, error)
}

// AdminTableManagerInterface defines the interface for admin table operations
type AdminTableManagerInterface interface {
	RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error
	GetFirstPosition(ctx context.Context, modelID string) (uint64, error)
	GetLastPosition(ctx context.Context, modelID string) (uint64, error)
	GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error)
	DeleteRange(ctx context.Context, modelID string, startPos, endPos uint64) error
	FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]clickhouse.GapInfo, error)
}
