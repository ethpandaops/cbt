package external

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigSetDefaults(t *testing.T) {
	tests := []struct {
		name            string
		config          *Config
		defaultCluster  string
		defaultDatabase string
		expectedCluster string
		expectedDB      string
	}{
		{
			name: "apply defaults when cluster and database are empty",
			config: &Config{
				Table: "test_table",
				Interval: &IntervalConfig{
					Type: "second",
				},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			defaultCluster:  "default_cluster",
			defaultDatabase: "default_db",
			expectedCluster: "default_cluster",
			expectedDB:      "default_db",
		},
		{
			name: "keep existing cluster and database when already set",
			config: &Config{
				Cluster:  "existing_cluster",
				Database: "existing_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Type: "second",
				},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			defaultCluster:  "default_cluster",
			defaultDatabase: "default_db",
			expectedCluster: "existing_cluster",
			expectedDB:      "existing_db",
		},
		{
			name: "no change when defaults are empty",
			config: &Config{
				Table: "test_table",
				Interval: &IntervalConfig{
					Type: "second",
				},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			defaultCluster:  "",
			defaultDatabase: "",
			expectedCluster: "",
			expectedDB:      "",
		},
		{
			name: "apply only cluster default when database is set",
			config: &Config{
				Database: "existing_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Type: "second",
				},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			defaultCluster:  "default_cluster",
			defaultDatabase: "default_db",
			expectedCluster: "default_cluster",
			expectedDB:      "existing_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.config.SetDefaults(tt.defaultCluster, tt.defaultDatabase)
			assert.Equal(t, tt.expectedCluster, tt.config.Cluster)
			assert.Equal(t, tt.expectedDB, tt.config.Database)
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  error
	}{
		{
			name: "valid config with database set",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Type: "second",
				},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid config without database",
			config: &Config{
				Table: "test_table",
				Interval: &IntervalConfig{
					Type: "second",
				},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: true,
			errMsg:  ErrDatabaseRequired,
		},
		{
			name: "invalid config without table",
			config: &Config{
				Database: "test_db",
				Interval: &IntervalConfig{
					Type: "second",
				},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: true,
			errMsg:  ErrTableRequired,
		},
		{
			name: "invalid config without interval",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: true,
			errMsg:  ErrIntervalRequired,
		},
		{
			name: "invalid config with empty interval type",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{Type: ""},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: true,
			errMsg:  ErrIntervalTypeRequired,
		},
		{
			name: "invalid config without cache",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{Type: "second"},
			},
			wantErr: true,
			errMsg:  ErrCacheConfigRequired,
		},
		{
			name: "invalid config with zero incremental scan interval",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{Type: "second"},
				Cache: &CacheConfig{
					IncrementalScanInterval: 0,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: true,
			errMsg:  ErrInvalidIncrementalInterval,
		},
		{
			name: "invalid config with zero full scan interval",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{Type: "second"},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        0,
				},
			},
			wantErr: true,
			errMsg:  ErrInvalidFullInterval,
		},
		{
			name: "invalid config with incremental interval not less than full interval",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{Type: "second"},
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Hour,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: true,
			errMsg:  ErrInvalidIntervalOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != nil {
					require.ErrorIs(t, err, tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestGetIDWithHyphenatedDatabases tests that GetID correctly handles database names with hyphens
func TestGetIDWithHyphenatedDatabases(t *testing.T) {
	tests := []struct {
		name       string
		database   string
		table      string
		expectedID string
	}{
		{
			name:       "simple hyphenated database",
			database:   "some-database",
			table:      "my_table",
			expectedID: "some-database.my_table",
		},
		{
			name:       "multiple hyphens in database",
			database:   "my-super-long-database",
			table:      "users",
			expectedID: "my-super-long-database.users",
		},
		{
			name:       "hyphen at start and end",
			database:   "-database-",
			table:      "data",
			expectedID: "-database-.data",
		},
		{
			name:       "hyphenated database and table with underscore",
			database:   "analytics-db",
			table:      "user_events",
			expectedID: "analytics-db.user_events",
		},
		{
			name:       "numeric with hyphens",
			database:   "db-2024-01",
			table:      "metrics",
			expectedID: "db-2024-01.metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				Database: tt.database,
				Table:    tt.table,
			}

			// Test GetID
			assert.Equal(t, tt.expectedID, config.GetID())
		})
	}
}
