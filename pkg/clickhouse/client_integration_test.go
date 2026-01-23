//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupIntegrationClient(t *testing.T) (clickhouse.ClientInterface, *testutil.ClickHouseConnection) {
	t.Helper()

	conn := testutil.NewClickHouseContainer(t)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	cfg := &clickhouse.Config{
		URL:          conn.URL,
		QueryTimeout: 30 * time.Second,
	}
	cfg.SetDefaults()

	client, err := clickhouse.NewClient(logger, cfg)
	require.NoError(t, err)

	err = client.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := client.Stop(); err != nil {
			t.Logf("failed to stop client: %v", err)
		}
	})

	return client, conn
}

func TestIntegration_NewClient(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	require.NotNil(t, client)
}

func TestIntegration_Execute(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	ctx := context.Background()

	// Create a test table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_db.test_table (
			id UInt64,
			name String,
			created_at DateTime
		) ENGINE = MergeTree()
		ORDER BY id
	`
	err := client.Execute(ctx, createTableQuery)
	require.NoError(t, err)

	// Verify table exists
	var result struct {
		Count uint64 `ch:"count"`
	}
	checkQuery := `SELECT count() as count FROM system.tables WHERE database = 'test_db' AND name = 'test_table'`
	err = client.QueryOne(ctx, checkQuery, &result)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), result.Count)
}

func TestIntegration_QueryOne(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	ctx := context.Background()

	// Test simple query with explicit UInt64 cast
	var result struct {
		Value uint64 `ch:"value"`
	}
	err := client.QueryOne(ctx, "SELECT toUInt64(42) as value", &result)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), result.Value)
}

func TestIntegration_QueryMany(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	ctx := context.Background()

	// Create and populate a test table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_db.numbers_table (
			id UInt64,
			value String
		) ENGINE = MergeTree()
		ORDER BY id
	`
	err := client.Execute(ctx, createTableQuery)
	require.NoError(t, err)

	// Insert test data
	insertQuery := `
		INSERT INTO test_db.numbers_table (id, value) VALUES
		(1, 'one'), (2, 'two'), (3, 'three')
	`
	err = client.Execute(ctx, insertQuery)
	require.NoError(t, err)

	// Query many rows
	var results []struct {
		ID    uint64 `ch:"id"`
		Value string `ch:"value"`
	}
	err = client.QueryMany(ctx, "SELECT id, value FROM test_db.numbers_table ORDER BY id", &results)
	require.NoError(t, err)

	assert.Len(t, results, 3)
	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, "one", results[0].Value)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, "two", results[1].Value)
	assert.Equal(t, uint64(3), results[2].ID)
	assert.Equal(t, "three", results[2].Value)
}

func TestIntegration_BulkInsert(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	ctx := context.Background()

	// Create test table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_db.bulk_table (
			id UInt64,
			name String,
			score Float64
		) ENGINE = MergeTree()
		ORDER BY id
	`
	err := client.Execute(ctx, createTableQuery)
	require.NoError(t, err)

	// Prepare bulk data with pointers (required by AppendStruct)
	type TestRow struct {
		ID    uint64  `ch:"id"`
		Name  string  `ch:"name"`
		Score float64 `ch:"score"`
	}

	data := []*TestRow{
		{ID: 1, Name: "Alice", Score: 95.5},
		{ID: 2, Name: "Bob", Score: 87.3},
		{ID: 3, Name: "Charlie", Score: 92.1},
	}

	// Bulk insert
	err = client.BulkInsert(ctx, "test_db.bulk_table", data)
	require.NoError(t, err)

	// Verify data was inserted
	var count struct {
		Count uint64 `ch:"count"`
	}
	err = client.QueryOne(ctx, "SELECT toUInt64(count()) as count FROM test_db.bulk_table", &count)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), count.Count)

	// Verify specific row
	var row TestRow
	err = client.QueryOne(ctx, "SELECT id, name, score FROM test_db.bulk_table WHERE id = 1", &row)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), row.ID)
	assert.Equal(t, "Alice", row.Name)
	assert.InDelta(t, 95.5, row.Score, 0.01)
}

func TestIntegration_QueryOne_EmptyResult(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	ctx := context.Background()

	// Create an empty table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_db.empty_table (
			id UInt64
		) ENGINE = MergeTree()
		ORDER BY id
	`
	err := client.Execute(ctx, createTableQuery)
	require.NoError(t, err)

	// Query from empty table
	var result struct {
		ID uint64 `ch:"id"`
	}
	err = client.QueryOne(ctx, "SELECT id FROM test_db.empty_table", &result)
	require.NoError(t, err)
	// Result should be zero value when no rows found
	assert.Equal(t, uint64(0), result.ID)
}

func TestIntegration_QueryMany_EmptyResult(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	ctx := context.Background()

	// Create an empty table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_db.empty_many_table (
			id UInt64
		) ENGINE = MergeTree()
		ORDER BY id
	`
	err := client.Execute(ctx, createTableQuery)
	require.NoError(t, err)

	// Query from empty table
	var results []struct {
		ID uint64 `ch:"id"`
	}
	err = client.QueryMany(ctx, "SELECT id FROM test_db.empty_many_table", &results)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestIntegration_BulkInsert_EmptySlice(t *testing.T) {
	client, _ := setupIntegrationClient(t)
	ctx := context.Background()

	// Create test table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_db.empty_insert_table (
			id UInt64
		) ENGINE = MergeTree()
		ORDER BY id
	`
	err := client.Execute(ctx, createTableQuery)
	require.NoError(t, err)

	// Bulk insert with empty slice
	type TestRow struct {
		ID uint64 `ch:"id"`
	}

	err = client.BulkInsert(ctx, "test_db.empty_insert_table", []*TestRow{})
	require.NoError(t, err) // Should succeed without error
}
