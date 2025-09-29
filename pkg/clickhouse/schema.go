package clickhouse

import (
	"context"
	"fmt"
)

// EnsureDatabase creates a database if it doesn't exist
func EnsureDatabase(ctx context.Context, client ClientInterface, database, cluster string) error {
	var query string
	if cluster != "" {
		query = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER '%s'", database, cluster)
	} else {
		query = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	}
	_, err := client.Execute(ctx, query)
	return err
}

// TableExists checks if a table exists in the given database
func TableExists(ctx context.Context, client ClientInterface, database, table string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT count() as count
		FROM system.tables 
		WHERE database = '%s' AND name = '%s'
	`, database, table)

	var result struct {
		Count uint64 `json:"count,string"`
	}

	err := client.QueryOne(ctx, query, &result)
	if err != nil {
		return false, err
	}

	return result.Count > 0, nil
}
