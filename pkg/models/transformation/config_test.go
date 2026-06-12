package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				require.Error(t, err)
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

func TestConfigIsIncrementalType(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected bool
	}{
		{
			name:     "incremental type",
			config:   &Config{Type: TypeIncremental},
			expected: true,
		},
		{
			name:     "scheduled type",
			config:   &Config{Type: TypeScheduled},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.IsIncrementalType())
		})
	}
}

func TestApplyTagsOverride(t *testing.T) {
	tests := []struct {
		name         string
		existingTags []string
		overrideTags []string
		expected     []string
	}{
		{
			name:         "empty override tags returns existing",
			existingTags: []string{"tag1", "tag2"},
			overrideTags: []string{},
			expected:     []string{"tag1", "tag2"},
		},
		{
			name:         "nil existing tags with override",
			existingTags: nil,
			overrideTags: []string{"new1", "new2"},
			expected:     []string{"new1", "new2"},
		},
		{
			name:         "appends override tags to existing",
			existingTags: []string{"existing1"},
			overrideTags: []string{"override1", "override2"},
			expected:     []string{"existing1", "override1", "override2"},
		},
		{
			name:         "empty existing and empty override",
			existingTags: []string{},
			overrideTags: []string{},
			expected:     []string{},
		},
		{
			name:         "multiple existing with single override",
			existingTags: []string{"a", "b", "c"},
			overrideTags: []string{"d"},
			expected:     []string{"a", "b", "c", "d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyTagsOverride(tt.existingTags, tt.overrideTags)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyScheduleOverride(t *testing.T) {
	newSchedule := func(s string) *string { return &s }

	tests := []struct {
		name             string
		existingSchedule string
		override         *string
		expected         string
	}{
		{
			name:             "nil override returns existing",
			existingSchedule: "*/5 * * * *",
			override:         nil,
			expected:         "*/5 * * * *",
		},
		{
			name:             "override replaces existing",
			existingSchedule: "*/5 * * * *",
			override:         newSchedule("@hourly"),
			expected:         "@hourly",
		},
		{
			name:             "override with empty existing",
			existingSchedule: "",
			override:         newSchedule("@daily"),
			expected:         "@daily",
		},
		{
			name:             "nil override with empty existing",
			existingSchedule: "",
			override:         nil,
			expected:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyScheduleOverride(tt.existingSchedule, tt.override)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyMinMaxOverride(t *testing.T) {
	newUint64 := func(v uint64) *uint64 { return &v }

	tests := []struct {
		name        string
		existingMin uint64
		existingMax uint64
		override    *LimitsOverride
		wantMin     uint64
		wantMax     uint64
		wantFound   bool
	}{
		{
			name:        "nil override returns existing",
			existingMin: 100,
			existingMax: 1000,
			override:    nil,
			wantMin:     100,
			wantMax:     1000,
			wantFound:   false,
		},
		{
			name:        "override both min and max",
			existingMin: 100,
			existingMax: 1000,
			override:    &LimitsOverride{Min: newUint64(50), Max: newUint64(500)},
			wantMin:     50,
			wantMax:     500,
			wantFound:   true,
		},
		{
			name:        "override only min",
			existingMin: 100,
			existingMax: 1000,
			override:    &LimitsOverride{Min: newUint64(50)},
			wantMin:     50,
			wantMax:     1000,
			wantFound:   true,
		},
		{
			name:        "override only max",
			existingMin: 100,
			existingMax: 1000,
			override:    &LimitsOverride{Max: newUint64(500)},
			wantMin:     100,
			wantMax:     500,
			wantFound:   true,
		},
		{
			name:        "limits field override",
			existingMin: 0,
			existingMax: 0,
			override:    &LimitsOverride{Min: newUint64(10), Max: newUint64(20)},
			wantMin:     10,
			wantMax:     20,
			wantFound:   true,
		},
		{
			name:        "empty struct override (no min/max set)",
			existingMin: 100,
			existingMax: 1000,
			override:    &LimitsOverride{},
			wantMin:     100,
			wantMax:     1000,
			wantFound:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMin, gotMax, gotFound := ApplyMinMaxOverride(tt.override, tt.existingMin, tt.existingMax)
			assert.Equal(t, tt.wantMin, gotMin)
			assert.Equal(t, tt.wantMax, gotMax)
			assert.Equal(t, tt.wantFound, gotFound)
		})
	}
}

func TestApplyIntervalOverride(t *testing.T) {
	newUint64 := func(v uint64) *uint64 { return &v }

	gotMin, gotMax, gotFound := ApplyIntervalOverride(
		&IntervalOverride{Min: newUint64(50), Max: newUint64(500)}, 100, 1000,
	)
	assert.Equal(t, uint64(50), gotMin)
	assert.Equal(t, uint64(500), gotMax)
	assert.True(t, gotFound)

	gotMin, gotMax, gotFound = ApplyIntervalOverride(nil, 100, 1000)
	assert.Equal(t, uint64(100), gotMin)
	assert.Equal(t, uint64(1000), gotMax)
	assert.False(t, gotFound)
}

func TestApplySchedulesOverride(t *testing.T) {
	newString := func(s string) *string { return &s }

	tests := []struct {
		name             string
		existingForward  string
		existingBackfill string
		override         *SchedulesOverride
		wantForward      string
		wantBackfill     string
	}{
		{
			name:             "nil override returns existing",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         nil,
			wantForward:      "*/5 * * * *",
			wantBackfill:     "*/10 * * * *",
		},
		{
			name:             "override both schedules",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         &SchedulesOverride{ForwardFill: newString("@hourly"), Backfill: newString("@daily")},
			wantForward:      "@hourly",
			wantBackfill:     "@daily",
		},
		{
			name:             "override only forward fill",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         &SchedulesOverride{ForwardFill: newString("@hourly")},
			wantForward:      "@hourly",
			wantBackfill:     "*/10 * * * *",
		},
		{
			name:             "override only backfill",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         &SchedulesOverride{Backfill: newString("@daily")},
			wantForward:      "*/5 * * * *",
			wantBackfill:     "@daily",
		},
		{
			name:             "empty struct override",
			existingForward:  "*/5 * * * *",
			existingBackfill: "*/10 * * * *",
			override:         &SchedulesOverride{},
			wantForward:      "*/5 * * * *",
			wantBackfill:     "*/10 * * * *",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotForward, gotBackfill := ApplySchedulesOverride(tt.existingForward, tt.existingBackfill, tt.override)
			assert.Equal(t, tt.wantForward, gotForward)
			assert.Equal(t, tt.wantBackfill, gotBackfill)
		})
	}
}
