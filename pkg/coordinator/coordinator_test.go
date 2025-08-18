package coordinator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCoordinator_parseSchedule(t *testing.T) {
	c := &Coordinator{}

	tests := []struct {
		name         string
		schedule     string
		wantDuration time.Duration
		wantError    bool
	}{
		{
			name:         "numeric seconds",
			schedule:     "300",
			wantDuration: 5 * time.Minute,
			wantError:    false,
		},
		{
			name:         "hourly schedule",
			schedule:     "@hourly",
			wantDuration: time.Hour,
			wantError:    false,
		},
		{
			name:         "daily schedule",
			schedule:     "@daily",
			wantDuration: 24 * time.Hour,
			wantError:    false,
		},
		{
			name:         "weekly schedule",
			schedule:     "@weekly",
			wantDuration: 7 * 24 * time.Hour,
			wantError:    false,
		},
		{
			name:         "@every format",
			schedule:     "@every 30m",
			wantDuration: 30 * time.Minute,
			wantError:    false,
		},
		{
			name:         "@every with hours",
			schedule:     "@every 2h",
			wantDuration: 2 * time.Hour,
			wantError:    false,
		},
		{
			name:         "cron expression defaults to 5min",
			schedule:     "*/5 * * * *",
			wantDuration: 5 * time.Minute,
			wantError:    false,
		},
		{
			name:         "invalid schedule",
			schedule:     "invalid",
			wantDuration: 0,
			wantError:    true,
		},
		{
			name:         "invalid @every format",
			schedule:     "@every invalid",
			wantDuration: 0,
			wantError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration, err := c.parseSchedule(tt.schedule)

			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantDuration, duration)
			}
		})
	}
}
