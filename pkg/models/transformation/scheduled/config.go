// Package scheduled provides the scheduled transformation type handler
package scheduled

import (
	"errors"
	"fmt"

	"github.com/robfig/cron/v3"
)

var (
	// ErrScheduleRequired is returned when schedule is not specified
	ErrScheduleRequired = errors.New("schedule is required for scheduled transformations")
	// ErrIntervalNotAllowed is returned when interval is specified for scheduled transformations
	ErrIntervalNotAllowed = errors.New("interval cannot be specified for scheduled transformations")
	// ErrSchedulesNotAllowed is returned when schedules are specified for scheduled transformations
	ErrSchedulesNotAllowed = errors.New("schedules (forwardfill/backfill) cannot be specified for scheduled transformations, use schedule field instead")
	// ErrDependenciesNotAllowed is returned when dependencies are specified for scheduled transformations
	ErrDependenciesNotAllowed = errors.New("dependencies cannot be specified for scheduled transformations")
	// ErrAdminServiceInvalid is returned when admin service doesn't implement required interface
	ErrAdminServiceInvalid = errors.New("admin service does not implement RecordScheduledCompletion")
)

// Config defines the configuration for scheduled transformation models
type Config struct {
	Type     string   `yaml:"type"`
	Database string   `yaml:"database"`
	Table    string   `yaml:"table"`
	Schedule string   `yaml:"schedule"` // Cron expression for scheduling
	Tags     []string `yaml:"tags,omitempty"`
	Exec     string   `yaml:"exec,omitempty"`
	SQL      string   `yaml:"-"` // SQL content from separate file
}

// ValidateScheduleFormat validates a cron schedule expression
func ValidateScheduleFormat(schedule string) error {
	_, err := cron.ParseStandard(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	return nil
}
