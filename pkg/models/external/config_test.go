package external

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfigSetDefaults(t *testing.T) {
	tests := []struct {
		name            string
		config          *Config
		defaultDatabase string
		expectedDB      string
	}{
		{
			name: "apply default when database is empty",
			config: &Config{
				Table: "test_table",
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			defaultDatabase: "default_db",
			expectedDB:      "default_db",
		},
		{
			name: "keep existing database when already set",
			config: &Config{
				Database: "existing_db",
				Table:    "test_table",
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			defaultDatabase: "default_db",
			expectedDB:      "existing_db",
		},
		{
			name: "no change when default is empty",
			config: &Config{
				Table: "test_table",
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			defaultDatabase: "",
			expectedDB:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.config.SetDefaults(tt.defaultDatabase)
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
				Cache: &CacheConfig{
					IncrementalScanInterval: time.Minute,
					FullScanInterval:        time.Hour,
				},
			},
			wantErr: true,
			errMsg:  ErrTableRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != nil {
					assert.Equal(t, tt.errMsg, err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
