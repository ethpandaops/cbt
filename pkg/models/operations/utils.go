package operations

import (
	"strings"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/hibiken/asynq"
)

// Interval represents a time interval
type Interval struct {
	Start uint64
	End   uint64
}

// CalculateIntervals calculates the intervals to process within the given range
func CalculateIntervals(start, end, intervalSize uint64) []Interval {
	// Align start to interval boundary
	alignedStart := (start / intervalSize) * intervalSize

	// Pre-allocate the intervals slice
	numIntervals := (end - alignedStart + intervalSize - 1) / intervalSize
	intervals := make([]Interval, 0, numIntervals)

	for pos := alignedStart; pos < end; pos += intervalSize {
		intervals = append(intervals, Interval{
			Start: pos,
			End:   pos + intervalSize,
		})
	}

	return intervals
}

// FindDependentModels finds all models that depend on the given model
func FindDependentModels(modelID string, modelConfigs map[string]models.ModelConfig) []string {
	var dependents []string

	for id := range modelConfigs {
		modelCfg := modelConfigs[id]
		if id == modelID {
			continue
		}

		for _, dep := range modelCfg.Dependencies {
			if dep == modelID {
				dependents = append(dependents, id)
				// Recursively find dependents of this model
				subDependents := FindDependentModels(id, modelConfigs)
				dependents = append(dependents, subDependents...)
				break
			}
		}
	}

	// Deduplicate
	seen := make(map[string]bool)
	var unique []string
	for _, id := range dependents {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}

	return unique
}

// ParseRedisConfig parses Redis URL and returns Asynq Redis configuration
func ParseRedisConfig(redisURL string) *asynq.RedisClientOpt {
	asynqRedis := &asynq.RedisClientOpt{Addr: redisURL}
	if strings.HasPrefix(redisURL, "redis://") {
		// Parse Redis URL for Asynq
		addr := strings.TrimPrefix(redisURL, "redis://")
		asynqRedis = &asynq.RedisClientOpt{Addr: addr}
	}
	return asynqRedis
}
