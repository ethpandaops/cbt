package transformation

import (
	"fmt"

	"github.com/robfig/cron/v3"
)

// ValidateScheduleFormat validates a cron schedule expression.
func ValidateScheduleFormat(schedule string) error {
	_, err := cron.ParseStandard(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	return nil
}
