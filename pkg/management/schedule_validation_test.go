package management

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string { return &s }

func TestParseScheduleInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schedule string
		wantErr  bool
		wantDur  string
	}{
		{name: "every valid", schedule: "@every 1h30m", wantDur: "1h30m0s"},
		{name: "every invalid duration", schedule: "@every notaduration", wantErr: true},
		{name: "empty string", schedule: "", wantErr: true},
		{name: "plain cron", schedule: "0 0 * * *"},
		{name: "short non-every", schedule: "@daily"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dur, err := parseScheduleInterval(tc.schedule)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tc.wantDur != "" {
				require.Equal(t, tc.wantDur, dur.String())
			}
		})
	}
}

func TestValidateOptionalSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   *string
		wantErr bool
	}{
		{name: "nil pointer", value: nil},
		{name: "empty value", value: strPtr("")},
		{name: "valid", value: strPtr("@every 1h")},
		{name: "invalid", value: strPtr("@every bogus"), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateOptionalSchedule(tc.value, "forwardfill")
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "forwardfill")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateTransformationSchedules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ov      models.TransformationOverride
		wantErr bool
	}{
		{
			name: "no schedules valid top-level schedule",
			ov:   models.TransformationOverride{Schedule: strPtr("@every 1h")},
		},
		{
			name:    "schedules nil top-level invalid",
			ov:      models.TransformationOverride{Schedule: strPtr("@every bad")},
			wantErr: true,
		},
		{
			name: "forwardfill invalid",
			ov: models.TransformationOverride{
				Schedules: &models.SchedulesOverride{ForwardFill: strPtr("@every bad")},
			},
			wantErr: true,
		},
		{
			name: "backfill invalid",
			ov: models.TransformationOverride{
				Schedules: &models.SchedulesOverride{
					ForwardFill: strPtr("@every 1h"),
					Backfill:    strPtr("@every bad"),
				},
			},
			wantErr: true,
		},
		{
			name: "all schedules valid",
			ov: models.TransformationOverride{
				Schedules: &models.SchedulesOverride{
					ForwardFill: strPtr("@every 1h"),
					Backfill:    strPtr("@every 2h"),
				},
				Schedule: strPtr("0 0 * * *"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ov := tc.ov
			err := validateTransformationSchedules(&ov)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestValidateOverrideConfigUnmarshalError exercises the transformation
// unmarshal error branch in validateOverrideConfig.
func TestValidateOverrideConfigUnmarshalError(t *testing.T) {
	t.Parallel()

	h := &Handlers{}

	err := h.validateOverrideConfig([]byte(`{"schedule":123}`), models.TypeTransformation)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid transformation override")
}
