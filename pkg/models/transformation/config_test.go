package transformation

import (
	"testing"

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
				Type:  TypeIncremental,
				Table: "test_table",
			},
			defaultDatabase: "default_db",
			expectedDB:      "default_db",
		},
		{
			name: "keep existing database when already set",
			config: &Config{
				Type:     TypeScheduled,
				Database: "existing_db",
				Table:    "test_table",
			},
			defaultDatabase: "default_db",
			expectedDB:      "existing_db",
		},
		{
			name: "no change when default is empty",
			config: &Config{
				Type:  TypeIncremental,
				Table: "test_table",
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
		errMsg  string
	}{
		{
			name: "valid incremental config",
			config: &Config{
				Type:     TypeIncremental,
				Database: "test_db",
				Table:    "test_table",
			},
			wantErr: false,
		},
		{
			name: "valid scheduled config",
			config: &Config{
				Type:     TypeScheduled,
				Database: "test_db",
				Table:    "test_table",
			},
			wantErr: false,
		},
		{
			name: "missing database",
			config: &Config{
				Type:  TypeIncremental,
				Table: "test_table",
			},
			wantErr: true,
			errMsg:  "database is required",
		},
		{
			name: "missing table",
			config: &Config{
				Type:     TypeIncremental,
				Database: "test_db",
			},
			wantErr: true,
			errMsg:  "table is required",
		},
		{
			name: "invalid type",
			config: &Config{
				Type:     "invalid",
				Database: "test_db",
				Table:    "test_table",
			},
			wantErr: true,
			errMsg:  "unknown transformation type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigGetID(t *testing.T) {
	config := &Config{
		Type:     TypeIncremental,
		Database: "test_db",
		Table:    "test_table",
	}

	assert.Equal(t, "test_db.test_table", config.GetID())
}

func TestConfigIsScheduledType(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected bool
	}{
		{
			name: "scheduled type",
			config: &Config{
				Type: TypeScheduled,
			},
			expected: true,
		},
		{
			name: "incremental type",
			config: &Config{
				Type: TypeIncremental,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.IsScheduledType())
		})
	}
}
