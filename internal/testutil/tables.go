//go:build integration

package testutil

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/stretchr/testify/require"
)

// CreateSourceTable creates a source table with position column for testing.
// Inserts rows with positions from 0 to maxPos (exclusive), incrementing by step.
func CreateSourceTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
	maxPos, step uint64,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create source table
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			position UInt64,
			value String
		)
		ENGINE = MergeTree()
		ORDER BY position
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)

	// Insert data if maxPos > 0
	if maxPos > 0 {
		// Build batch insert values
		insertSQL := "INSERT INTO " + database + "." + table + " (position, value) VALUES "
		first := true
		for pos := uint64(0); pos < maxPos; pos += step {
			if !first {
				insertSQL += ", "
			}
			insertSQL += "(" + uintToStr(pos) + ", 'test_value_" + uintToStr(pos) + "')"
			first = false
		}

		err = client.Execute(ctx, insertSQL)
		require.NoError(t, err)
	}
}

// CreateTargetTable creates an empty target table for transformation output.
func CreateTargetTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create target table
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			position UInt64,
			value String,
			processed_at DateTime DEFAULT now()
		)
		ENGINE = MergeTree()
		ORDER BY position
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)
}

// CreateAdminTables creates the admin.cbt_incremental and admin.cbt_scheduled tables.
func CreateAdminTables(t *testing.T, client clickhouse.ClientInterface) {
	t.Helper()
	ctx := context.Background()

	// Create admin database
	err := client.Execute(ctx, "CREATE DATABASE IF NOT EXISTS admin")
	require.NoError(t, err)

	// Create incremental admin table
	incrementalTable := `
		CREATE TABLE IF NOT EXISTS admin.cbt_incremental (
			updated_date_time DateTime(3),
			database LowCardinality(String),
			table LowCardinality(String),
			position UInt64,
			interval UInt64
		)
		ENGINE = ReplacingMergeTree(updated_date_time)
		ORDER BY (database, table, position, interval)
	`
	err = client.Execute(ctx, incrementalTable)
	require.NoError(t, err)

	// Create scheduled admin table
	scheduledTable := `
		CREATE TABLE IF NOT EXISTS admin.cbt_scheduled (
			updated_date_time DateTime(3),
			database LowCardinality(String),
			table LowCardinality(String),
			start_date_time DateTime(3)
		)
		ENGINE = ReplacingMergeTree(updated_date_time)
		ORDER BY (database, table)
	`
	err = client.Execute(ctx, scheduledTable)
	require.NoError(t, err)
}

// GetRowCount returns the number of rows in a table.
func GetRowCount(t *testing.T, client clickhouse.ClientInterface, database, table string) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		Count uint64 `ch:"count"`
	}

	query := "SELECT count() as count FROM " + database + "." + table
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.Count
}

// GetMaxPosition returns the maximum position value in a table.
func GetMaxPosition(t *testing.T, client clickhouse.ClientInterface, database, table string) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		MaxPos uint64 `ch:"max_pos"`
	}

	query := "SELECT max(position) as max_pos FROM " + database + "." + table
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.MaxPos
}

// GetMinPosition returns the minimum position value in a table.
func GetMinPosition(t *testing.T, client clickhouse.ClientInterface, database, table string) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		MinPos uint64 `ch:"min_pos"`
	}

	query := "SELECT min(position) as min_pos FROM " + database + "." + table
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.MinPos
}

// TruncateTable removes all data from a table.
func TruncateTable(t *testing.T, client clickhouse.ClientInterface, database, table string) {
	t.Helper()
	ctx := context.Background()

	err := client.Execute(ctx, "TRUNCATE TABLE "+database+"."+table)
	require.NoError(t, err)
}

// DropTable drops a table if it exists.
func DropTable(t *testing.T, client clickhouse.ClientInterface, database, table string) {
	t.Helper()
	ctx := context.Background()

	err := client.Execute(ctx, "DROP TABLE IF EXISTS "+database+"."+table)
	require.NoError(t, err)
}

// uintToStr converts a uint64 to string without importing strconv
func uintToStr(n uint64) string {
	if n == 0 {
		return "0"
	}
	var result []byte
	for n > 0 {
		result = append([]byte{byte('0' + n%10)}, result...)
		n /= 10
	}
	return string(result)
}

// CreateEventsSourceTable creates a source table for event-based testing scenarios.
// This simulates a raw events table with positions, account IDs, and values.
func CreateEventsSourceTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
	maxPos, step uint64,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create events source table with realistic schema
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			position UInt64,
			account_id String,
			value Int64,
			event_type String
		)
		ENGINE = MergeTree()
		ORDER BY (position, account_id)
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)

	// Insert data with multiple accounts and varying values
	if maxPos > 0 {
		insertSQL := "INSERT INTO " + database + "." + table +
			" (position, account_id, value, event_type) VALUES "
		first := true
		accountCount := 5 // Multiple accounts for realistic scenarios
		for pos := uint64(0); pos < maxPos; pos += step {
			for acc := 0; acc < accountCount; acc++ {
				if !first {
					insertSQL += ", "
				}
				accountID := "account_" + uintToStr(uint64(acc))
				// Vary values by position and account for testing
				value := int64((pos+1)*(uint64(acc)+1)) % 1000
				eventType := "transfer"
				if pos%3 == 0 {
					eventType = "deposit"
				} else if pos%3 == 1 {
					eventType = "withdraw"
				}
				insertSQL += "(" + uintToStr(pos) + ", '" + accountID + "', " +
					intToStr(value) + ", '" + eventType + "')"
				first = false
			}
		}
		err = client.Execute(ctx, insertSQL)
		require.NoError(t, err)
	}
}

// CreateEventsAggregatedTable creates an aggregated events table for transformation output.
// Uses ReplacingMergeTree for handling updates during backfills.
func CreateEventsAggregatedTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create aggregated table with ReplacingMergeTree
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			updated_at DateTime,
			position UInt64,
			account_id String,
			event_count UInt64,
			total_value Int64
		)
		ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (position, account_id)
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)
}

// CreateEventsByAccountTable creates a cumulative state table for testing running totals.
// This pattern is common in blockchain analytics for tracking account balances over time.
func CreateEventsByAccountTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create cumulative state table
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			updated_at DateTime,
			position UInt64,
			account_id String,
			delta Int64,
			running_total Int64
		)
		ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (account_id, position)
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)
}

// CreateEventsWithNextTable creates a table for testing next-position calculations.
// Used for multi-statement transformation tests.
func CreateEventsWithNextTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create events with next position table
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			updated_at DateTime,
			position UInt64,
			account_id String,
			next_position Nullable(UInt64)
		)
		ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (account_id, position)
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)
}

// CreateHelperLatestStateTable creates a helper table for tracking latest state per account.
// Used in multi-statement transformation tests.
func CreateHelperLatestStateTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create helper table for latest state
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			account_id String,
			latest_position UInt64,
			updated_at DateTime DEFAULT now()
		)
		ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY account_id
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)
}

// GetAggregatedRowCount returns the count of distinct (position, account_id) in an aggregated table.
func GetAggregatedRowCount(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		Count uint64 `ch:"count"`
	}

	query := "SELECT count() as count FROM " + database + "." + table + " FINAL"
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.Count
}

// GetRunningTotal returns the running total for a specific account at a position.
func GetRunningTotal(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table, accountID string,
	position uint64,
) int64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		RunningTotal int64 `ch:"running_total"`
	}

	query := "SELECT running_total FROM " + database + "." + table + " FINAL " +
		"WHERE account_id = '" + accountID + "' AND position = " + uintToStr(position)
	err := client.QueryOne(ctx, query, &result)
	if err != nil {
		// Return 0 if no row found
		return 0
	}

	return result.RunningTotal
}

// GetDistinctAccountCount returns the number of distinct accounts in a table.
func GetDistinctAccountCount(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		Count uint64 `ch:"count"`
	}

	query := "SELECT count(DISTINCT account_id) as count FROM " + database + "." + table
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.Count
}

// intToStr converts an int64 to string without importing strconv
func intToStr(n int64) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var result []byte
	for n > 0 {
		result = append([]byte{byte('0' + n%10)}, result...)
		n /= 10
	}
	if negative {
		result = append([]byte{'-'}, result...)
	}
	return string(result)
}

// CreateNetworkEventsTable creates a table with network filtering for testing
// environment variable injection. Each network gets positions from 0 to positionsPerNetwork.
func CreateNetworkEventsTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
	networks []string,
	positionsPerNetwork uint64,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create network events table with network filtering support
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			position UInt64,
			network String,
			timestamp DateTime DEFAULT now(),
			value String
		)
		ENGINE = MergeTree()
		ORDER BY (network, position)
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)

	// Insert data for each network
	if positionsPerNetwork > 0 && len(networks) > 0 {
		insertSQL := "INSERT INTO " + database + "." + table +
			" (position, network, value) VALUES "
		first := true
		for _, network := range networks {
			for pos := uint64(0); pos < positionsPerNetwork; pos++ {
				if !first {
					insertSQL += ", "
				}
				insertSQL += "(" + uintToStr(pos) + ", '" + network + "', 'value_" + network + "_" + uintToStr(pos) + "')"
				first = false
			}
		}

		err = client.Execute(ctx, insertSQL)
		require.NoError(t, err)
	}
}

// GetNetworkRowCount returns the number of rows for a specific network in a table.
func GetNetworkRowCount(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table, network string,
) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		Count uint64 `ch:"count"`
	}

	query := "SELECT count() as count FROM " + database + "." + table +
		" WHERE network = '" + network + "'"
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.Count
}

// GetNetworkMaxPosition returns the max position for a specific network.
func GetNetworkMaxPosition(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table, network string,
) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		MaxPos uint64 `ch:"max_pos"`
	}

	query := "SELECT max(position) as max_pos FROM " + database + "." + table +
		" WHERE network = '" + network + "'"
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.MaxPos
}

// GetNetworkMinPosition returns the min position for a specific network.
func GetNetworkMinPosition(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table, network string,
) uint64 {
	t.Helper()
	ctx := context.Background()

	var result struct {
		MinPos uint64 `ch:"min_pos"`
	}

	query := "SELECT min(position) as min_pos FROM " + database + "." + table +
		" WHERE network = '" + network + "'"
	err := client.QueryOne(ctx, query, &result)
	require.NoError(t, err)

	return result.MinPos
}

// CreateSlowSourceTable creates a source table with many rows for slow scan testing.
func CreateSlowSourceTable(
	t *testing.T,
	client clickhouse.ClientInterface,
	database, table string,
	rowCount uint64,
) {
	t.Helper()
	ctx := context.Background()

	// Create database if not exists
	createDB := "CREATE DATABASE IF NOT EXISTS " + database
	err := client.Execute(ctx, createDB)
	require.NoError(t, err)

	// Create source table
	createTable := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			position UInt64,
			value String
		)
		ENGINE = MergeTree()
		ORDER BY position
	`
	err = client.Execute(ctx, createTable)
	require.NoError(t, err)

	// Insert data using numbers() for efficient bulk insert
	if rowCount > 0 {
		insertSQL := `INSERT INTO ` + database + `.` + table + ` (position, value)
			SELECT number, concat('test_value_', toString(number))
			FROM numbers(` + uintToStr(rowCount) + `)`
		err = client.Execute(ctx, insertSQL)
		require.NoError(t, err)
	}
}
