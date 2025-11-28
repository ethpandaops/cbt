package transformation

import (
	"reflect"
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

func TestApplyTagsOverride(t *testing.T) {
	// Test struct that mimics the override structure used in handlers
	type override struct {
		Tags []string
	}

	tests := []struct {
		name         string
		existingTags []string
		override     override
		expected     []string
	}{
		{
			name:         "empty override tags returns existing",
			existingTags: []string{"tag1", "tag2"},
			override:     override{Tags: []string{}},
			expected:     []string{"tag1", "tag2"},
		},
		{
			name:         "nil existing tags with override",
			existingTags: nil,
			override:     override{Tags: []string{"new1", "new2"}},
			expected:     []string{"new1", "new2"},
		},
		{
			name:         "appends override tags to existing",
			existingTags: []string{"existing1"},
			override:     override{Tags: []string{"override1", "override2"}},
			expected:     []string{"existing1", "override1", "override2"},
		},
		{
			name:         "empty existing and empty override",
			existingTags: []string{},
			override:     override{Tags: []string{}},
			expected:     []string{},
		},
		{
			name:         "multiple existing with single override",
			existingTags: []string{"a", "b", "c"},
			override:     override{Tags: []string{"d"}},
			expected:     []string{"a", "b", "c", "d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := reflect.ValueOf(tt.override)
			result := ApplyTagsOverride(tt.existingTags, v)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyTagsOverride_InvalidReflectValue(t *testing.T) {
	existingTags := []string{"tag1", "tag2"}

	// Test with struct that has no Tags field
	type noTagsStruct struct {
		Other string
	}

	v := reflect.ValueOf(noTagsStruct{Other: "value"})
	result := ApplyTagsOverride(existingTags, v)

	assert.Equal(t, existingTags, result)
}
