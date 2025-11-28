package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateScheduleFormat(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		wantErr  bool
	}{
		{
			name:     "valid standard cron - every minute",
			schedule: "* * * * *",
			wantErr:  false,
		},
		{
			name:     "valid standard cron - every 5 minutes",
			schedule: "*/5 * * * *",
			wantErr:  false,
		},
		{
			name:     "valid standard cron - daily at midnight",
			schedule: "0 0 * * *",
			wantErr:  false,
		},
		{
			name:     "valid @every syntax",
			schedule: "@every 1h30m",
			wantErr:  false,
		},
		{
			name:     "valid @hourly",
			schedule: "@hourly",
			wantErr:  false,
		},
		{
			name:     "valid @daily",
			schedule: "@daily",
			wantErr:  false,
		},
		{
			name:     "valid @weekly",
			schedule: "@weekly",
			wantErr:  false,
		},
		{
			name:     "valid @monthly",
			schedule: "@monthly",
			wantErr:  false,
		},
		{
			name:     "valid @yearly",
			schedule: "@yearly",
			wantErr:  false,
		},
		{
			name:     "invalid - empty string",
			schedule: "",
			wantErr:  true,
		},
		{
			name:     "invalid - random text",
			schedule: "not a cron",
			wantErr:  true,
		},
		{
			name:     "invalid - too many fields",
			schedule: "* * * * * *",
			wantErr:  true,
		},
		{
			name:     "invalid - too few fields",
			schedule: "* * *",
			wantErr:  true,
		},
		{
			name:     "invalid @every syntax",
			schedule: "@every invalid",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateScheduleFormat(tt.schedule)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid cron expression")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
