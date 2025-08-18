package clickhouse

import (
	"context"
	"time"
)

// AdminEntry represents a record in the admin tracking table
type AdminEntry struct {
	UpdatedDateTime time.Time `ch:"updated_date_time"`
	Database        string    `ch:"database"`
	Table           string    `ch:"table"`
	Position        uint64    `ch:"position"`
	Interval        uint64    `ch:"interval"`
}

// GapInfo represents a gap in the processed data
type GapInfo struct {
	StartPos uint64
	EndPos   uint64
}

// ExternalModelCache represents cached min/max bounds for external models
type ExternalModelCache struct {
	ModelID   string        `json:"model_id"`
	Min       uint64        `json:"min"`
	Max       uint64        `json:"max"`
	UpdatedAt time.Time     `json:"updated_at"`
	TTL       time.Duration `json:"ttl"`
}

// ExternalModelCacheManager manages cached external model min/max values
type ExternalModelCacheManager interface {
	Get(ctx context.Context, modelID string) (*ExternalModelCache, error)
	Set(ctx context.Context, cache ExternalModelCache) error
	Invalidate(ctx context.Context, modelID string) error
}
