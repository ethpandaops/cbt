package external

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validFrontmatter = `---
database: test_db
table: test_table
lag: 5
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT max(slot) FROM test_db.test_table`

func TestNewSQL(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		wantErr     error
		wantContent string
		assertFn    func(t *testing.T, sql *SQL)
	}{
		{
			name:        "valid frontmatter and content",
			content:     validFrontmatter,
			wantContent: "SELECT max(slot) FROM test_db.test_table",
			assertFn: func(t *testing.T, sql *SQL) {
				t.Helper()
				assert.Equal(t, "test_db", sql.Database)
				assert.Equal(t, "test_table", sql.Table)
				assert.Equal(t, uint64(5), sql.Lag)
				require.NotNil(t, sql.Interval)
				assert.Equal(t, "second", sql.Interval.Type)
				require.NotNil(t, sql.Cache)
				assert.Equal(t, time.Minute, sql.Cache.IncrementalScanInterval)
				assert.Equal(t, time.Hour, sql.Cache.FullScanInterval)
			},
		},
		{
			name:    "missing frontmatter separator",
			content: "SELECT 1",
			wantErr: ErrInvalidFrontmatter,
		},
		{
			name: "invalid yaml frontmatter",
			content: `---
database: [unclosed
---
SELECT 1`,
			wantErr: nil, // wrapped error, checked separately
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := NewSQL([]byte(tt.content))

			switch {
			case tt.name == "invalid yaml frontmatter":
				require.Error(t, err)
				assert.Contains(t, err.Error(), "failed to parse frontmatter")
				assert.Nil(t, sql)
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, sql)
			default:
				require.NoError(t, err)
				require.NotNil(t, sql)
				assert.Equal(t, tt.wantContent, sql.GetValue())
				if tt.assertFn != nil {
					tt.assertFn(t, sql)
				}
			}
		})
	}
}

func TestSQLValidate(t *testing.T) {
	newValidSQL := func() *SQL {
		return &SQL{
			Config: Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{Type: "second"},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			Content: "SELECT 1",
		}
	}

	tests := []struct {
		name    string
		mutate  func(s *SQL)
		wantErr error
	}{
		{
			name:    "valid sql model",
			mutate:  func(_ *SQL) {},
			wantErr: nil,
		},
		{
			name:    "empty content",
			mutate:  func(s *SQL) { s.Content = "" },
			wantErr: ErrSQLContentRequired,
		},
		{
			name:    "content set but config invalid",
			mutate:  func(s *SQL) { s.Database = "" },
			wantErr: ErrDatabaseRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := newValidSQL()
			tt.mutate(sql)

			err := sql.Validate()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSQLGetters(t *testing.T) {
	sql := &SQL{
		Config: Config{
			Cluster:  "test_cluster",
			Database: "test_db",
			Table:    "test_table",
			Interval: &IntervalConfig{Type: "slot"},
		},
		Content: "SELECT 1",
	}

	assert.Equal(t, TypeSQL, sql.GetType())
	assert.Equal(t, "sql", sql.GetType())
	assert.Equal(t, "SELECT 1", sql.GetValue())
	assert.Equal(t, "slot", sql.GetIntervalType())

	cfg := sql.GetConfig()
	assert.Equal(t, "test_db", cfg.Database)
	assert.Equal(t, "test_table", cfg.Table)

	mutable := sql.GetConfigMutable()
	require.NotNil(t, mutable)
	assert.Same(t, &sql.Config, mutable)
	mutable.Table = "changed_table"
	assert.Equal(t, "changed_table", sql.Table)
}

func TestSQLGetIntervalTypeNilInterval(t *testing.T) {
	sql := &SQL{
		Config:  Config{Database: "test_db", Table: "test_table"},
		Content: "SELECT 1",
	}

	assert.Empty(t, sql.GetIntervalType())
}

func TestSQLSetDefaults(t *testing.T) {
	tests := []struct {
		name            string
		sql             *SQL
		defaultCluster  string
		defaultDB       string
		expectedCluster string
		expectedDB      string
	}{
		{
			name:            "applies defaults when empty",
			sql:             &SQL{Config: Config{Table: "test_table"}, Content: "SELECT 1"},
			defaultCluster:  "default_cluster",
			defaultDB:       "default_db",
			expectedCluster: "default_cluster",
			expectedDB:      "default_db",
		},
		{
			name: "keeps existing values",
			sql: &SQL{
				Config:  Config{Cluster: "existing", Database: "existing_db", Table: "test_table"},
				Content: "SELECT 1",
			},
			defaultCluster:  "default_cluster",
			defaultDB:       "default_db",
			expectedCluster: "existing",
			expectedDB:      "existing_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.sql.SetDefaults(tt.defaultCluster, tt.defaultDB)
			assert.Equal(t, tt.expectedCluster, tt.sql.Cluster)
			assert.Equal(t, tt.expectedDB, tt.sql.Database)
		})
	}
}
