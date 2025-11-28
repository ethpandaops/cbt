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

func TestApplyScheduleOverride(t *testing.T) {
	// Test struct that mimics the override structure used in handlers
	type override struct {
		Schedule *string
	}

	newSchedule := func(s string) *string { return &s }

	tests := []struct {
		name             string
		existingSchedule string
		override         override
		expected         string
	}{
		{
			name:             "nil override returns existing",
			existingSchedule: "*/5 * * * *",
			override:         override{Schedule: nil},
			expected:         "*/5 * * * *",
		},
		{
			name:             "override replaces existing",
			existingSchedule: "*/5 * * * *",
			override:         override{Schedule: newSchedule("@hourly")},
			expected:         "@hourly",
		},
		{
			name:             "override with empty existing",
			existingSchedule: "",
			override:         override{Schedule: newSchedule("@daily")},
			expected:         "@daily",
		},
		{
			name:             "nil override with empty existing",
			existingSchedule: "",
			override:         override{Schedule: nil},
			expected:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := reflect.ValueOf(tt.override)
			result := ApplyScheduleOverride(tt.existingSchedule, v)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyScheduleOverride_InvalidReflectValue(t *testing.T) {
	existingSchedule := "*/5 * * * *"

	// Test with struct that has no Schedule field
	type noScheduleStruct struct {
		Other string
	}

	v := reflect.ValueOf(noScheduleStruct{Other: "value"})
	result := ApplyScheduleOverride(existingSchedule, v)

	assert.Equal(t, existingSchedule, result)
}

func TestApplyMinMaxOverride(t *testing.T) {
	type minMaxStruct struct {
		Min *uint64
		Max *uint64
	}
	type override struct {
		Interval *minMaxStruct
		Limits   *minMaxStruct
	}

	newUint64 := func(v uint64) *uint64 { return &v }

	tests := []struct {
		name        string
		fieldName   string
		existingMin uint64
		existingMax uint64
		override    override
		wantMin     uint64
		wantMax     uint64
		wantFound   bool
	}{
		{
			name:        "nil override returns existing",
			fieldName:   "Interval",
			existingMin: 100,
			existingMax: 1000,
			override:    override{Interval: nil},
			wantMin:     100,
			wantMax:     1000,
			wantFound:   false,
		},
		{
			name:        "override both min and max",
			fieldName:   "Interval",
			existingMin: 100,
			existingMax: 1000,
			override:    override{Interval: &minMaxStruct{Min: newUint64(50), Max: newUint64(500)}},
			wantMin:     50,
			wantMax:     500,
			wantFound:   true,
		},
		{
			name:        "override only min",
			fieldName:   "Interval",
			existingMin: 100,
			existingMax: 1000,
			override:    override{Interval: &minMaxStruct{Min: newUint64(50)}},
			wantMin:     50,
			wantMax:     1000,
			wantFound:   true,
		},
		{
			name:        "override only max",
			fieldName:   "Interval",
			existingMin: 100,
			existingMax: 1000,
			override:    override{Interval: &minMaxStruct{Max: newUint64(500)}},
			wantMin:     100,
			wantMax:     500,
			wantFound:   true,
		},
		{
			name:        "limits field override",
			fieldName:   "Limits",
			existingMin: 0,
			existingMax: 0,
			override:    override{Limits: &minMaxStruct{Min: newUint64(10), Max: newUint64(20)}},
			wantMin:     10,
			wantMax:     20,
			wantFound:   true,
		},
		{
			name:        "empty struct override (no min/max set)",
			fieldName:   "Interval",
			existingMin: 100,
			existingMax: 1000,
			override:    override{Interval: &minMaxStruct{}},
			wantMin:     100,
			wantMax:     1000,
			wantFound:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := reflect.ValueOf(tt.override)
			gotMin, gotMax, gotFound := ApplyMinMaxOverride(tt.fieldName, tt.existingMin, tt.existingMax, v)
			assert.Equal(t, tt.wantMin, gotMin)
			assert.Equal(t, tt.wantMax, gotMax)
			assert.Equal(t, tt.wantFound, gotFound)
		})
	}
}

func TestApplySchedulesOverride(t *testing.T) {
	type schedulesStruct struct {
		ForwardFill *string
		Backfill    *string
	}
	type override struct {
		Schedules *schedulesStruct
	}

	newString := func(s string) *string { return &s }

	tests := []struct {
		name               string
		existingForward    string
		existingBackfill   string
		override           override
		wantForward        string
		wantBackfill       string
	}{
		{
			name:             "nil override returns existing",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         override{Schedules: nil},
			wantForward:      "*/5 * * * *",
			wantBackfill:     "*/10 * * * *",
		},
		{
			name:             "override both schedules",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         override{Schedules: &schedulesStruct{ForwardFill: newString("@hourly"), Backfill: newString("@daily")}},
			wantForward:      "@hourly",
			wantBackfill:     "@daily",
		},
		{
			name:             "override only forward fill",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         override{Schedules: &schedulesStruct{ForwardFill: newString("@hourly")}},
			wantForward:      "@hourly",
			wantBackfill:     "*/10 * * * *",
		},
		{
			name:             "override only backfill",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         override{Schedules: &schedulesStruct{Backfill: newString("@daily")}},
			wantForward:      "*/5 * * * *",
			wantBackfill:     "@daily",
		},
		{
			name:             "empty struct override",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         override{Schedules: &schedulesStruct{}},
			wantForward:      "*/5 * * * *",
			wantBackfill:     "*/10 * * * *",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := reflect.ValueOf(tt.override)
			gotForward, gotBackfill := ApplySchedulesOverride(tt.existingForward, tt.existingBackfill, v)
			assert.Equal(t, tt.wantForward, gotForward)
			assert.Equal(t, tt.wantBackfill, gotBackfill)
		})
	}
}
