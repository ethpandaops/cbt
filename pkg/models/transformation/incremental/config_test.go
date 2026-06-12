package incremental

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  IntervalConfig
		wantErr bool
		errIs   error
	}{
		{
			name:    "valid",
			config:  IntervalConfig{Min: 10, Max: 100, Type: "slot"},
			wantErr: false,
		},
		{
			name:    "max zero",
			config:  IntervalConfig{Min: 0, Max: 0, Type: "slot"},
			wantErr: true,
			errIs:   ErrIntervalMaxRequired,
		},
		{
			name:    "min exceeds max",
			config:  IntervalConfig{Min: 200, Max: 100, Type: "slot"},
			wantErr: true,
			errIs:   ErrInvalidInterval,
		},
		{
			name:    "missing type",
			config:  IntervalConfig{Min: 10, Max: 100, Type: ""},
			wantErr: true,
			errIs:   ErrIntervalTypeRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSchedulesConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  SchedulesConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "both valid",
			config:  SchedulesConfig{ForwardFill: "@every 10s", Backfill: "@every 30s"},
			wantErr: false,
		},
		{
			name:    "both empty",
			config:  SchedulesConfig{},
			wantErr: false,
		},
		{
			name:    "invalid forwardfill",
			config:  SchedulesConfig{ForwardFill: "not a cron"},
			wantErr: true,
			errMsg:  "invalid forwardfill schedule",
		},
		{
			name:    "invalid backfill",
			config:  SchedulesConfig{Backfill: "not a cron"},
			wantErr: true,
			errMsg:  "invalid backfill schedule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestLimitsConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  LimitsConfig
		wantErr bool
	}{
		{name: "both zero", config: LimitsConfig{Min: 0, Max: 0}, wantErr: false},
		{name: "valid min < max", config: LimitsConfig{Min: 10, Max: 100}, wantErr: false},
		{name: "min only", config: LimitsConfig{Min: 10, Max: 0}, wantErr: false},
		{name: "max only", config: LimitsConfig{Min: 0, Max: 100}, wantErr: false},
		{name: "min greater than max", config: LimitsConfig{Min: 100, Max: 10}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidLimits)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestFillConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  FillConfig
		wantErr bool
	}{
		{name: "empty direction", config: FillConfig{}, wantErr: false},
		{name: "head direction", config: FillConfig{Direction: "head"}, wantErr: false},
		{name: "tail direction", config: FillConfig{Direction: "tail"}, wantErr: false},
		{name: "invalid direction", config: FillConfig{Direction: "diagonal"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidFillDirection)

				return
			}

			require.NoError(t, err)
		})
	}
}
