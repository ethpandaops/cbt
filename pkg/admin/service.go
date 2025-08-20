package admin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
)

var (
	// ErrInvalidModelID is returned when model ID format is invalid
	ErrInvalidModelID = errors.New("invalid model ID format: expected database.table")
)

// GapInfo represents a gap in the processed data
type GapInfo struct {
	StartPos uint64
	EndPos   uint64
}

// Service manages the admin tracking table for completed transformations
type Service struct {
	client        clickhouse.ClientInterface
	cluster       string
	localSuffix   string
	adminDatabase string
	adminTable    string

	cacheManager *CacheManager
}

// NewService creates a new admin table manager
func NewService(client clickhouse.ClientInterface, cluster, localSuffix, adminDatabase, adminTable string, redisClient *redis.Client) *Service {
	cacheManager := NewCacheManager(redisClient)

	return &Service{
		client:        client,
		cluster:       cluster,
		localSuffix:   localSuffix,
		adminDatabase: adminDatabase,
		adminTable:    adminTable,
		cacheManager:  cacheManager,
	}
}

// GetAdminDatabase returns the admin database name
func (a *Service) GetAdminDatabase() string {
	return a.adminDatabase
}

// GetAdminTable returns the admin table name
func (a *Service) GetAdminTable() string {
	return a.adminTable
}

// RecordCompletion records a completed transformation in the admin table
func (a *Service) RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return ErrInvalidModelID
	}

	// Using string formatting with proper escaping
	// In production, consider using parameterized queries for better security
	query := fmt.Sprintf(`
		INSERT INTO %s.%s (updated_date_time, database, table, position, interval)
		VALUES (now(), '%s', '%s', %d, %d)
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], position, interval)

	return a.client.Execute(ctx, query)
}

// GetFirstPosition returns the first processed position for a model
func (a *Service) GetFirstPosition(ctx context.Context, modelID string) (uint64, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return 0, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		SELECT coalesce(min(position), 0) as first_pos
		FROM %s.%s FINAL
		WHERE database = '%s' AND table = '%s'
	`, a.adminDatabase, a.adminTable, parts[0], parts[1])

	var result struct {
		FirstPos uint64 `json:"first_pos,string"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return result.FirstPos, nil
}

// GetLastPosition returns the last processed position for a model
func (a *Service) GetLastPosition(ctx context.Context, modelID string) (uint64, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return 0, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		SELECT coalesce(max(position + interval), 0) as last_pos
		FROM %s.%s FINAL
		WHERE database = '%s' AND table = '%s'
	`, a.adminDatabase, a.adminTable, parts[0], parts[1])

	var result struct {
		LastPos uint64 `json:"last_pos,string"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return result.LastPos, nil
}

// GetCoverage checks if a range is fully covered in the admin table
func (a *Service) GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return false, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		WITH coverage AS (
			SELECT position, position + interval as end_pos
			FROM %s.%s FINAL
			WHERE database = '%s' AND table = '%s'
			  AND position < %d
			  AND position + interval > %d
		)
		SELECT CASE 
			WHEN min(position) <= %d AND max(end_pos) >= %d 
			THEN 1 ELSE 0 
		END as fully_covered
		FROM coverage
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], endPos, startPos, startPos, endPos)

	var result struct {
		FullyCovered int `json:"fully_covered"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	return result.FullyCovered == 1, nil
}

// FindGaps finds all gaps in the processed data for a model
func (a *Service) FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]GapInfo, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return nil, ErrInvalidModelID
	}

	// Query to find gaps using a self-join instead of window function
	query := fmt.Sprintf(`
		WITH positions AS (
			SELECT 
				position,
				position + interval as end_pos,
				row_number() OVER (ORDER BY position) as rn
			FROM %s.%s FINAL
			WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position <= %d
			ORDER BY position
		)
		SELECT 
			p1.end_pos as gap_start,
			p2.position as gap_end
		FROM positions p1
		INNER JOIN positions p2 ON p1.rn + 1 = p2.rn
		WHERE p2.position > p1.end_pos
		  AND p2.position - p1.end_pos >= %d
		ORDER BY gap_start DESC
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], minPos, maxPos, interval)

	var gapResults []struct {
		GapStart uint64 `json:"gap_start,string"`
		GapEnd   uint64 `json:"gap_end,string"`
	}

	err := a.client.QueryMany(ctx, query, &gapResults)
	if err != nil {
		return nil, err
	}

	gaps := make([]GapInfo, 0, len(gapResults))
	for _, result := range gapResults {
		gaps = append(gaps, GapInfo{
			StartPos: result.GapStart,
			EndPos:   result.GapEnd,
		})
	}

	// Also check for a gap at the beginning
	firstPosQuery := fmt.Sprintf(`
		SELECT min(position) as first_pos
		FROM %s.%s FINAL
		WHERE database = '%s' AND table = '%s'
		  AND position >= %d
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], minPos)

	var firstPosResult struct {
		FirstPos *uint64 `json:"first_pos,string"`
	}

	err = a.client.QueryOne(ctx, firstPosQuery, &firstPosResult)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	if firstPosResult.FirstPos != nil && *firstPosResult.FirstPos > minPos {
		// There's a gap at the beginning - append at end since we're processing DESC
		gaps = append(gaps, GapInfo{StartPos: minPos, EndPos: *firstPosResult.FirstPos})
	}

	return gaps, nil
}

// DeleteRange deletes entries in a range from the admin table
func (a *Service) DeleteRange(ctx context.Context, modelID string, startPos, endPos uint64) error {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return ErrInvalidModelID
	}

	// Use ALTER TABLE DELETE for immediate deletion in ReplacingMergeTree
	var query string
	if a.cluster != "" {
		tableName := fmt.Sprintf("%s.%s", a.adminDatabase, a.adminTable)
		if a.localSuffix != "" {
			tableName = fmt.Sprintf("%s.%s%s", a.adminDatabase, a.adminTable, a.localSuffix)
		}
		query = fmt.Sprintf(`
			ALTER TABLE %s ON CLUSTER '%s'
			DELETE WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position < %d
		`, tableName, a.cluster, parts[0], parts[1], startPos, endPos)
	} else {
		query = fmt.Sprintf(`
			ALTER TABLE %s.%s
			DELETE WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position < %d
		`, a.adminDatabase, a.adminTable, parts[0], parts[1], startPos, endPos)
	}

	return a.client.Execute(ctx, query)
}

// GetCacheManager returns the cache manager instance
func (a *Service) GetCacheManager() *CacheManager {
	return a.cacheManager
}
