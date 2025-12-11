package admin

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test service creation (ethPandaOps requirement)
func TestNewService(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	mockClient := &mockClickhouseClient{}
	cluster := "test_cluster"
	localSuffix := "_local"
	adminDatabase := "admin_db"
	adminTable := "admin_table"
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	config := TableConfig{
		IncrementalDatabase: adminDatabase,
		IncrementalTable:    adminTable,
		ScheduledDatabase:   adminDatabase,
		ScheduledTable:      "cbt_scheduled",
	}
	svc := NewService(log, mockClient, cluster, localSuffix, config, redisClient)
	assert.NotNil(t, svc)

	// Verify it implements the Service interface
	var _ = svc

	// Verify admin database and table are set
	assert.Equal(t, adminDatabase, svc.GetIncrementalAdminDatabase())
	assert.Equal(t, adminTable, svc.GetIncrementalAdminTable())
}

// Test RecordCompletion
func TestRecordCompletion(t *testing.T) {
	tests := []struct {
		name     string
		modelID  string
		position uint64
		interval uint64
		wantErr  bool
		errMatch error
	}{
		{
			name:     "valid model ID",
			modelID:  "database.table",
			position: 1000,
			interval: 100,
			wantErr:  false,
		},
		{
			name:     "invalid model ID - no dot",
			modelID:  "invalidmodel",
			position: 1000,
			interval: 100,
			wantErr:  true,
			errMatch: modelid.ErrInvalidModelID,
		},
		{
			name:     "invalid model ID - multiple dots",
			modelID:  "database.schema.table",
			position: 1000,
			interval: 100,
			wantErr:  true,
			errMatch: modelid.ErrInvalidModelID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			err := svc.RecordCompletion(ctx, tt.modelID, tt.position, tt.interval)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMatch != nil {
					assert.ErrorIs(t, err, tt.errMatch)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test GetNextUnprocessedPosition
func TestGetNextUnprocessedPosition(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		queryResult uint64
		queryError  error
		expectedPos uint64
		wantErr     bool
	}{
		{
			name:        "valid model with data",
			modelID:     "database.table",
			queryResult: 5000,
			expectedPos: 5000,
			wantErr:     false,
		},
		{
			name:        "valid model no data",
			modelID:     "database.empty",
			queryResult: 0,
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			expectedPos: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				queryResult: tt.queryResult,
				queryError:  tt.queryError,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			pos, err := svc.GetNextUnprocessedPosition(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, pos)
			}
		})
	}
}

// Test new position tracking functions
func TestPositionTrackingFunctions(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		queryResult uint64
		queryError  error
		expectedPos uint64
		wantErr     bool
	}{
		{
			name:        "valid model with data",
			modelID:     "database.table",
			queryResult: 10000,
			expectedPos: 10000,
			wantErr:     false,
		},
		{
			name:        "valid model no data",
			modelID:     "database.empty",
			queryResult: 0,
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			expectedPos: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				queryResult: tt.queryResult,
				queryError:  tt.queryError,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()

			// Test GetNextUnprocessedPosition
			nextPos, err := svc.GetNextUnprocessedPosition(ctx, tt.modelID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, nextPos)
			}

			// Test GetLastProcessedPosition (returns max(position) not max(position+interval))
			lastPos, err := svc.GetLastProcessedPosition(ctx, tt.modelID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// For this mock, it will return the same value since we're using a simple mock
				assert.Equal(t, tt.expectedPos, lastPos)
			}
		})
	}
}

// Test GetFirstPosition
func TestGetFirstPosition(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		queryResult uint64
		queryError  error
		expectedPos uint64
		wantErr     bool
	}{
		{
			name:        "valid model with data",
			modelID:     "database.table",
			queryResult: 100,
			expectedPos: 100,
			wantErr:     false,
		},
		{
			name:        "valid model no data",
			modelID:     "database.empty",
			queryResult: 0,
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			expectedPos: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				queryResult: tt.queryResult,
				queryError:  tt.queryError,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			pos, err := svc.GetFirstPosition(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, pos)
			}
		})
	}
}

// Test FindGaps
func TestFindGaps(t *testing.T) {
	tests := []struct {
		name         string
		modelID      string
		minPos       uint64
		maxPos       uint64
		interval     uint64
		mockGaps     []GapInfo
		expectedGaps int
		wantErr      bool
	}{
		{
			name:     "finds large gaps",
			modelID:  "database.table",
			minPos:   0,
			maxPos:   1000,
			interval: 384,
			mockGaps: []GapInfo{
				{StartPos: 200, EndPos: 300},
				{StartPos: 500, EndPos: 600},
			},
			expectedGaps: 2,
			wantErr:      false,
		},
		{
			name:     "finds micro-gaps (12s)",
			modelID:  "database.table",
			minPos:   0,
			maxPos:   1000,
			interval: 384,
			mockGaps: []GapInfo{
				{StartPos: 100, EndPos: 112}, // 12s gap
				{StartPos: 200, EndPos: 224}, // 24s gap
				{StartPos: 300, EndPos: 336}, // 36s gap
			},
			expectedGaps: 3,
			wantErr:      false,
		},
		{
			name:     "finds all gap sizes",
			modelID:  "database.table",
			minPos:   0,
			maxPos:   2000,
			interval: 384,
			mockGaps: []GapInfo{
				{StartPos: 100, EndPos: 112},   // 12s
				{StartPos: 200, EndPos: 224},   // 24s
				{StartPos: 300, EndPos: 360},   // 60s
				{StartPos: 400, EndPos: 520},   // 120s
				{StartPos: 1000, EndPos: 1384}, // 384s (full interval)
			},
			expectedGaps: 5,
			wantErr:      false,
		},
		{
			name:         "no gaps found",
			modelID:      "database.table",
			minPos:       0,
			maxPos:       1000,
			interval:     384,
			mockGaps:     []GapInfo{},
			expectedGaps: 0,
			wantErr:      false,
		},
		{
			name:         "invalid model ID",
			modelID:      "invalid",
			minPos:       0,
			maxPos:       1000,
			interval:     384,
			mockGaps:     []GapInfo{},
			expectedGaps: 0,
			wantErr:      true,
		},
		{
			name:     "no false positive when scanning range overlapping with existing interval",
			modelID:  "database.table",
			minPos:   1760414678,
			maxPos:   1760414690,
			interval: 60,
			mockGaps: []GapInfo{
				// Existing interval: position=1760414666, interval=24, end=1760414690
				// This covers the scan range [1760414678, 1760414690]
				// Should NOT report a false positive gap
			},
			expectedGaps: 0,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				gaps: tt.mockGaps,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			gaps, err := svc.FindGaps(ctx, tt.modelID, tt.minPos, tt.maxPos, tt.interval)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, gaps, tt.expectedGaps)
				if tt.expectedGaps > 0 {
					// Verify first gap matches expected
					assert.Equal(t, tt.mockGaps[0].StartPos, gaps[0].StartPos)
					assert.Equal(t, tt.mockGaps[0].EndPos, gaps[0].EndPos)
				}
			}
		})
	}
}

// Test GetCoverage
func TestGetCoverage(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		startPos    uint64
		endPos      uint64
		queryResult bool
		expectedCov bool
		wantErr     bool
	}{
		{
			name:        "fully covered range",
			modelID:     "database.table",
			startPos:    100,
			endPos:      200,
			queryResult: true,
			expectedCov: true,
			wantErr:     false,
		},
		{
			name:        "not covered range",
			modelID:     "database.table",
			startPos:    100,
			endPos:      200,
			queryResult: false,
			expectedCov: false,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			startPos:    100,
			endPos:      200,
			expectedCov: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				coverageResult: tt.queryResult,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			covered, err := svc.GetCoverage(ctx, tt.modelID, tt.startPos, tt.endPos)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCov, covered)
			}
		})
	}
}

// Mock ClickHouse client for testing
type mockClickhouseClient struct {
	queryResult    uint64
	queryError     error
	gaps           []GapInfo
	coverageResult bool
	executeError   error
}

func (m *mockClickhouseClient) Execute(_ context.Context, _ string) error {
	return m.executeError
}

func (m *mockClickhouseClient) QueryOne(_ context.Context, _ string, result interface{}) error {
	if m.queryError != nil {
		return m.queryError
	}

	// Use reflection to set fields by name, avoiding struct tag mismatches
	v := reflect.ValueOf(result).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		switch field.Name {
		case "LastPos", "LastEndPos", "FirstPos":
			if fieldValue.Kind() == reflect.Uint64 {
				fieldValue.SetUint(m.queryResult)
			}
		case "FullyCovered":
			if fieldValue.Kind() == reflect.Int {
				if m.coverageResult {
					fieldValue.SetInt(1)
				} else {
					fieldValue.SetInt(0)
				}
			}
		}
	}

	return nil
}

func (m *mockClickhouseClient) QueryMany(_ context.Context, _ string, result interface{}) error {
	if m.queryError != nil {
		return m.queryError
	}

	// Use reflection to handle gap results regardless of struct tags
	slicePtr := reflect.ValueOf(result)
	if slicePtr.Kind() != reflect.Ptr || slicePtr.Elem().Kind() != reflect.Slice {
		return nil
	}

	slice := slicePtr.Elem()
	elemType := slice.Type().Elem()

	// Check if this is a gap-like struct (has GapStart and GapEnd fields)
	if _, hasGapStart := elemType.FieldByName("GapStart"); hasGapStart {
		if _, hasGapEnd := elemType.FieldByName("GapEnd"); hasGapEnd {
			for _, gap := range m.gaps {
				newElem := reflect.New(elemType).Elem()
				newElem.FieldByName("GapStart").SetUint(gap.StartPos)
				newElem.FieldByName("GapEnd").SetUint(gap.EndPos)
				slice.Set(reflect.Append(slice, newElem))
			}
		}
	}

	return nil
}

func (m *mockClickhouseClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}

func (m *mockClickhouseClient) Start() error {
	return nil
}

func (m *mockClickhouseClient) Stop() error {
	return nil
}

// Ensure mock implements the interface
var _ clickhouse.ClientInterface = (*mockClickhouseClient)(nil)

// Test ConsolidateHistoricalData
func TestConsolidateHistoricalData(t *testing.T) {
	tests := []struct {
		name                string
		modelID             string
		rangeStartPos       uint64
		rangeEndPos         uint64
		rangeRowCount       uint64
		mockRangeError      error
		expectedRowsDeleted uint64
		wantErr             bool
		errMatch            error
	}{
		{
			name:                "successful consolidation",
			modelID:             "database.table",
			rangeStartPos:       0,
			rangeEndPos:         400,
			rangeRowCount:       4,
			expectedRowsDeleted: 4,
			wantErr:             false,
		},
		{
			name:                "no consolidation needed (no ranges found)",
			modelID:             "database.empty",
			rangeStartPos:       0,
			rangeEndPos:         0,
			rangeRowCount:       0,
			expectedRowsDeleted: 0,
			wantErr:             false,
		},
		{
			name:                "single row found (should not consolidate)",
			modelID:             "database.single",
			rangeStartPos:       100,
			rangeEndPos:         200,
			rangeRowCount:       1,
			expectedRowsDeleted: 0,
			wantErr:             false,
		},
		{
			name:                "invalid model ID",
			modelID:             "invalid",
			expectedRowsDeleted: 0,
			wantErr:             true,
			errMatch:            modelid.ErrInvalidModelID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &consolidationMockClient{
				rangeStartPos: tt.rangeStartPos,
				rangeEndPos:   tt.rangeEndPos,
				rangeRowCount: tt.rangeRowCount,
				rangeError:    tt.mockRangeError,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			rowsDeleted, err := svc.ConsolidateHistoricalData(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMatch != nil {
					assert.ErrorIs(t, err, tt.errMatch)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedRowsDeleted, rowsDeleted)
			}
		})
	}
}

// Test ConsolidateHistoricalData query structure
func TestConsolidateHistoricalDataQueryStructure(t *testing.T) {
	tests := []struct {
		name             string
		modelID          string
		expectedInQuery  []string // Strings that should appear in the consolidation query
		expectedInDelete []string // Strings that should appear in the DELETE query
		useCluster       bool
		clusterName      string
		localSuffix      string
	}{
		{
			name:    "non-cluster mode - query structure",
			modelID: "mydb.mytable",
			expectedInQuery: []string{
				"WITH ordered_rows AS",
				"island_groups AS",
				"consolidated_ranges AS",
				"MAX(position + interval) OVER",
				"COALESCE(prev_max_end, 0)",
				"HAVING COUNT(*) > 1",
				"WHERE database = 'mydb' AND table = 'mytable'",
			},
			expectedInDelete: []string{
				"DELETE FROM",
				"WHERE database = 'mydb' AND table = 'mytable'",
				"AND position >=",
				"AND position <",
				"AND interval !=",
			},
			useCluster:  false,
			clusterName: "",
			localSuffix: "",
		},
		{
			name:    "cluster mode - query structure",
			modelID: "mydb.mytable",
			expectedInQuery: []string{
				"WITH ordered_rows AS",
				"island_groups AS",
				"consolidated_ranges AS",
			},
			expectedInDelete: []string{
				"DELETE FROM",
				"ON CLUSTER 'test_cluster'",
				"WHERE database = 'mydb' AND table = 'mytable'",
				"AND interval !=",
			},
			useCluster:  true,
			clusterName: "test_cluster",
			localSuffix: "_local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &queryCapturingClient{
				queries: []string{},
			}

			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "cbt_incremental",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}

			var svc Service
			if tt.useCluster {
				svc = NewService(log, mockClient, tt.clusterName, tt.localSuffix, config, nil)
			} else {
				svc = NewService(log, mockClient, "", "", config, nil)
			}

			ctx := context.Background()
			_, err := svc.ConsolidateHistoricalData(ctx, tt.modelID)
			require.NoError(t, err)

			// Should have 3 queries: SELECT (range query), INSERT (consolidated row), DELETE (rows in range)
			require.Len(t, mockClient.queries, 3, "Expected 3 queries: SELECT, INSERT, DELETE")

			consolidationQuery := mockClient.queries[0]
			insertQuery := mockClient.queries[1]
			deleteQuery := mockClient.queries[2]

			// Verify consolidation query structure (should be 3-CTE)
			for _, expected := range tt.expectedInQuery {
				assert.Contains(t, consolidationQuery, expected,
					"Consolidation query should contain: %s", expected)
			}

			// Verify it uses the 3-CTE structure
			assert.NotContains(t, consolidationQuery, "with_lag AS",
				"Should NOT use lagInFrame approach")
			assert.NotContains(t, consolidationQuery, "with_max AS",
				"Should NOT use complex multi-CTE structure")
			assert.NotContains(t, consolidationQuery, "row_number() OVER",
				"Should NOT use row numbering approach")

			// Verify INSERT query has consolidated row
			assert.Contains(t, insertQuery, "INSERT INTO",
				"Should have INSERT statement")

			// Verify DELETE query structure (should use interval-based deletion)
			for _, expected := range tt.expectedInDelete {
				assert.Contains(t, deleteQuery, expected,
					"DELETE query should contain: %s", expected)
			}

			// Verify interval-based deletion strategy
			assert.Contains(t, deleteQuery, "AND interval !=",
				"DELETE should use interval-based exclusion")
			assert.NotContains(t, deleteQuery, "updated_date_time <",
				"DELETE should NOT use timestamp-based deletion")
		})
	}
}

// Test consolidation with cluster configuration
func TestConsolidateHistoricalDataClusterMode(t *testing.T) {
	mockClient := &queryCapturingClient{
		queries: []string{},
	}

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	config := TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}

	svc := NewService(log, mockClient, "my_cluster", "_local", config, nil)

	ctx := context.Background()
	_, err := svc.ConsolidateHistoricalData(ctx, "database.table")
	require.NoError(t, err)

	// Check DELETE query uses cluster syntax
	require.Len(t, mockClient.queries, 3)
	deleteQuery := mockClient.queries[2]

	assert.Contains(t, deleteQuery, "ON CLUSTER 'my_cluster'",
		"DELETE in cluster mode should use ON CLUSTER")
	assert.Contains(t, deleteQuery, "`admin`.`cbt_incremental_local`",
		"DELETE in cluster mode should use local suffix")
}

// consolidationMockClient is a specialized mock for consolidation testing
type consolidationMockClient struct {
	rangeStartPos uint64
	rangeEndPos   uint64
	rangeRowCount uint64
	rangeError    error
	executeError  error
	queries       []string
}

func (m *consolidationMockClient) Execute(_ context.Context, query string) error {
	m.queries = append(m.queries, query)
	return m.executeError
}

func (m *consolidationMockClient) QueryOne(_ context.Context, query string, result interface{}) error {
	m.queries = append(m.queries, query)

	if m.rangeError != nil {
		return m.rangeError
	}

	// Use reflection to set fields by name
	v := reflect.ValueOf(result).Elem()

	if startPosField := v.FieldByName("StartPos"); startPosField.IsValid() && startPosField.CanSet() {
		startPosField.SetUint(m.rangeStartPos)
	}

	if endPosField := v.FieldByName("EndPos"); endPosField.IsValid() && endPosField.CanSet() {
		endPosField.SetUint(m.rangeEndPos)
	}

	if rowCountField := v.FieldByName("RowCount"); rowCountField.IsValid() && rowCountField.CanSet() {
		rowCountField.SetUint(m.rangeRowCount)
	}

	return nil
}

func (m *consolidationMockClient) QueryMany(_ context.Context, query string, _ interface{}) error {
	m.queries = append(m.queries, query)
	return nil
}

func (m *consolidationMockClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}

func (m *consolidationMockClient) Start() error {
	return nil
}

func (m *consolidationMockClient) Stop() error {
	return nil
}

// Ensure consolidation mock implements the interface
var _ clickhouse.ClientInterface = (*consolidationMockClient)(nil)

// TestHyphenatedDatabaseNamesInQueries tests that SQL queries properly escape hyphenated database names
func TestHyphenatedDatabaseNamesInQueries(t *testing.T) {
	tests := []struct {
		name          string
		adminDatabase string
		adminTable    string
		modelID       string
		position      uint64
		interval      uint64
		expectedInSQL []string // Strings that should appear in the SQL
	}{
		{
			name:          "simple hyphenated database",
			adminDatabase: "admin-db",
			adminTable:    "cbt",
			modelID:       "source-database.events",
			position:      1000,
			interval:      100,
			expectedInSQL: []string{
				"`admin-db`.`cbt`",
				"'source-database'",
				"'events'",
			},
		},
		{
			name:          "hyphenated admin and model databases",
			adminDatabase: "fusake-devnet-3",
			adminTable:    "admin_cbt",
			modelID:       "analytics-db.aggregates",
			position:      2000,
			interval:      200,
			expectedInSQL: []string{
				"`fusake-devnet-3`.`admin_cbt`",
				"'analytics-db'",
				"'aggregates'",
			},
		},
		{
			name:          "multiple hyphens in names",
			adminDatabase: "my-super-admin-db",
			adminTable:    "tracking-table",
			modelID:       "data-lake-2024.user-events",
			position:      3000,
			interval:      300,
			expectedInSQL: []string{
				"`my-super-admin-db`.`tracking-table`",
				"'data-lake-2024'",
				"'user-events'",
			},
		},
		{
			name:          "numeric with hyphens",
			adminDatabase: "admin-2024-01",
			adminTable:    "cbt-v2",
			modelID:       "db-2024-01.metrics",
			position:      4000,
			interval:      400,
			expectedInSQL: []string{
				"`admin-2024-01`.`cbt-v2`",
				"'db-2024-01'",
				"'metrics'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock client that captures queries
			mockClient := &queryCapturingClient{
				queries: []string{},
			}

			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			// Create service with hyphenated admin database/table
			config := TableConfig{
				IncrementalDatabase: tt.adminDatabase,
				IncrementalTable:    tt.adminTable,
				ScheduledDatabase:   tt.adminDatabase,
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)
			ctx := context.Background()

			// Test RecordCompletion (INSERT query)
			err := svc.RecordCompletion(ctx, tt.modelID, tt.position, tt.interval)
			require.NoError(t, err)

			// Check that the INSERT query has proper escaping
			require.Len(t, mockClient.queries, 1, "Expected one INSERT query")
			insertQuery := mockClient.queries[0]

			for _, expected := range tt.expectedInSQL {
				assert.Contains(t, insertQuery, expected,
					"INSERT query should contain %s", expected)
			}

			// Reset queries
			mockClient.queries = []string{}

			// Test GetFirstPosition (SELECT query)
			_, err = svc.GetFirstPosition(ctx, tt.modelID)
			require.NoError(t, err)

			require.Len(t, mockClient.queries, 1, "Expected one SELECT query")
			selectQuery := mockClient.queries[0]

			// Check for proper escaping in SELECT
			assert.Contains(t, selectQuery, fmt.Sprintf("`%s`.`%s`", tt.adminDatabase, tt.adminTable),
				"SELECT query should have escaped admin table reference")

			// Check SELECT query has proper WHERE clause
			parts := strings.Split(tt.modelID, ".")
			assert.Contains(t, selectQuery, fmt.Sprintf("database = '%s'", parts[0]),
				"SELECT query should have database in WHERE clause")
			assert.Contains(t, selectQuery, fmt.Sprintf("table = '%s'", parts[1]),
				"SELECT query should have table in WHERE clause")

			// Reset queries
			mockClient.queries = []string{}

			// Test GetNextUnprocessedPosition
			_, err = svc.GetNextUnprocessedPosition(ctx, tt.modelID)
			require.NoError(t, err)

			require.Len(t, mockClient.queries, 1, "Expected one SELECT query")
			selectQuery2 := mockClient.queries[0]

			assert.Contains(t, selectQuery2, fmt.Sprintf("`%s`.`%s`", tt.adminDatabase, tt.adminTable),
				"SELECT query should have escaped admin table reference")
			assert.Contains(t, selectQuery2, fmt.Sprintf("database = '%s'", parts[0]),
				"SELECT query should have database in WHERE clause")
			assert.Contains(t, selectQuery2, fmt.Sprintf("table = '%s'", parts[1]),
				"SELECT query should have table in WHERE clause")
		})
	}
}

// queryCapturingClient is a mock ClickHouse client that captures queries for inspection
type queryCapturingClient struct {
	queries     []string
	mockResults []interface{}
	resultIndex int
}

func (m *queryCapturingClient) Execute(_ context.Context, query string) error {
	m.queries = append(m.queries, query)
	return nil
}

func (m *queryCapturingClient) QueryOne(_ context.Context, query string, result interface{}) error {
	m.queries = append(m.queries, query)

	// Use reflection to set fields by name, avoiding struct tag mismatches
	v := reflect.ValueOf(result).Elem()

	// Handle consolidation range query fields
	if startPosField := v.FieldByName("StartPos"); startPosField.IsValid() && startPosField.CanSet() {
		startPosField.SetUint(1000)
	}

	if endPosField := v.FieldByName("EndPos"); endPosField.IsValid() && endPosField.CanSet() {
		endPosField.SetUint(2000)
	}

	if rowCountField := v.FieldByName("RowCount"); rowCountField.IsValid() && rowCountField.CanSet() {
		rowCountField.SetUint(5)
	}

	// Handle other query result fields
	if firstPosField := v.FieldByName("FirstPos"); firstPosField.IsValid() && firstPosField.CanSet() {
		firstPosField.SetUint(1000)
	}

	if lastEndPosField := v.FieldByName("LastEndPos"); lastEndPosField.IsValid() && lastEndPosField.CanSet() {
		lastEndPosField.SetUint(2000)
	}

	if lastPosField := v.FieldByName("LastPos"); lastPosField.IsValid() && lastPosField.CanSet() {
		lastPosField.SetUint(1500)
	}

	m.resultIndex++

	return nil
}

func (m *queryCapturingClient) QueryMany(_ context.Context, query string, result interface{}) error {
	m.queries = append(m.queries, query)

	if m.resultIndex < len(m.mockResults) {
		// Return the mock result based on the actual type expected
		switch v := result.(type) {
		case *[]struct {
			GapStart uint64 `json:"gap_start,string"`
			GapEnd   uint64 `json:"gap_end,string"`
		}:
			// For FindGaps query
			*v = []struct {
				GapStart uint64 `json:"gap_start,string"`
				GapEnd   uint64 `json:"gap_end,string"`
			}{
				{GapStart: 1000, GapEnd: 1100},
				{GapStart: 2000, GapEnd: 2200},
			}
		case *[]GapInfo:
			// For other queries that might use GapInfo directly
			if mockGaps, ok := m.mockResults[m.resultIndex].([]GapInfo); ok {
				*v = mockGaps
			}
		}
		m.resultIndex++
	}

	return nil
}

func (m *queryCapturingClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}

func (m *queryCapturingClient) Start() error {
	return nil
}

func (m *queryCapturingClient) Stop() error {
	return nil
}

func TestParseModelID(t *testing.T) {
	tests := []struct {
		name             string
		modelID          string
		expectedDatabase string
		expectedTable    string
		expectError      bool
	}{
		{
			name:             "valid model ID",
			modelID:          "my_database.my_table",
			expectedDatabase: "my_database",
			expectedTable:    "my_table",
			expectError:      false,
		},
		{
			name:             "valid model ID with underscores",
			modelID:          "analytics_db.block_propagation",
			expectedDatabase: "analytics_db",
			expectedTable:    "block_propagation",
			expectError:      false,
		},
		{
			name:        "invalid - no dot",
			modelID:     "no_dot_here",
			expectError: true,
		},
		{
			name:        "invalid - multiple dots",
			modelID:     "db.schema.table",
			expectError: true,
		},
		{
			name:        "invalid - empty string",
			modelID:     "",
			expectError: true,
		},
		{
			name:        "invalid - just a dot",
			modelID:     ".",
			expectError: false, // Actually produces empty strings, but modelid.Parse doesn't validate empty parts
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database, table, err := modelid.Parse(tt.modelID)
			if tt.expectError {
				assert.Error(t, err)
				assert.ErrorIs(t, err, modelid.ErrInvalidModelID)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedDatabase, database)
				assert.Equal(t, tt.expectedTable, table)
			}
		})
	}
}

func TestBuildTableRef(t *testing.T) {
	tests := []struct {
		name     string
		database string
		table    string
		expected string
	}{
		{
			name:     "standard names",
			database: "my_database",
			table:    "my_table",
			expected: "`my_database`.`my_table`",
		},
		{
			name:     "names with special characters would be escaped",
			database: "db",
			table:    "table",
			expected: "`db`.`table`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildTableRef(tt.database, tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}
