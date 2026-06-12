package admin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/sirupsen/logrus"
)

// RecordScheduledCompletion records a completed scheduled transformation
func (a *service) RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return err
	}

	a.log.WithFields(logrus.Fields{
		"model_id":        modelID,
		"start_date_time": startDateTime,
	}).Debug("Recording scheduled task completion in admin table")

	query := fmt.Sprintf(`
		INSERT INTO %s (updated_date_time, database, table, start_date_time)
		VALUES (now(), '%s', '%s', '%s')
	`, buildTableRef(a.scheduledAdminDatabase, a.scheduledAdminTable), database, table, startDateTime.UTC().Format("2006-01-02 15:04:05.000"))

	if err := a.client.Execute(ctx, query); err != nil {
		return fmt.Errorf("failed to insert scheduled admin record: %w", err)
	}

	return nil
}

// GetLastScheduledExecution returns the last execution time for a scheduled transformation
func (a *service) GetLastScheduledExecution(ctx context.Context, modelID string) (*time.Time, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT max(start_date_time) as last_execution
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
	`, buildTableRef(a.scheduledAdminDatabase, a.scheduledAdminTable), database, table)

	var result struct {
		LastExecution *time.Time `ch:"last_execution"`
	}

	err = a.client.QueryOne(ctx, query, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to query last scheduled execution: %w", err)
	}

	// Check for nil, Go's zero time, or Unix epoch (ClickHouse's "zero" DateTime)
	if result.LastExecution == nil || result.LastExecution.IsZero() || result.LastExecution.Unix() == 0 {
		return nil, nil
	}

	// Ensure the timestamp is in UTC to match how we store it
	utcTime := result.LastExecution.UTC()

	return &utcTime, nil
}

// GetAllLastScheduledExecutions returns the last execution time for multiple scheduled
// models in a single query. This avoids the N+1 query problem when listing runs.
func (a *service) GetAllLastScheduledExecutions(ctx context.Context, modelIDs []string) (map[string]*time.Time, error) {
	if len(modelIDs) == 0 {
		return make(map[string]*time.Time), nil
	}

	// Build the WHERE IN clause with (database, table) tuples
	tuples := make([]string, 0, len(modelIDs))

	for _, id := range modelIDs {
		database, table, err := modelid.Parse(id)
		if err != nil {
			return nil, err
		}

		tuples = append(tuples, fmt.Sprintf("('%s', '%s')", database, table))
	}

	query := fmt.Sprintf(`
		SELECT database, table, max(start_date_time) as last_execution
		FROM %s FINAL
		WHERE (database, table) IN (%s)
		GROUP BY database, table
	`, buildTableRef(a.scheduledAdminDatabase, a.scheduledAdminTable), strings.Join(tuples, ", "))

	var rows []struct {
		Database      string    `ch:"database"`
		Table         string    `ch:"table"`
		LastExecution time.Time `ch:"last_execution"`
	}

	if err := a.client.QueryMany(ctx, query, &rows); err != nil {
		return nil, fmt.Errorf("failed to batch query scheduled executions: %w", err)
	}

	result := make(map[string]*time.Time, len(modelIDs))
	for _, row := range rows {
		// Skip zero/epoch timestamps (ClickHouse "zero" DateTime)
		if row.LastExecution.IsZero() || row.LastExecution.Unix() == 0 {
			continue
		}

		id := modelid.Format(row.Database, row.Table)
		utcTime := row.LastExecution.UTC()
		result[id] = &utcTime
	}

	a.log.WithFields(logrus.Fields{
		"model_count":  len(modelIDs),
		"result_count": len(result),
	}).Debug("Batch retrieved last scheduled executions from admin table")

	return result, nil
}
