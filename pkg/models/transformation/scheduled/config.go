// Package scheduled provides the scheduled transformation type handler
package scheduled

import (
	"errors"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

var (
	// ErrScheduleRequired is returned when schedule is not specified
	ErrScheduleRequired = errors.New("schedule is required for scheduled transformations")
	// ErrAdminServiceInvalid is returned when admin service doesn't implement required interface
	ErrAdminServiceInvalid = errors.New("admin service does not implement RecordScheduledCompletion")
)

// Config defines the configuration for scheduled transformation models
type Config struct {
	Type         string                      `yaml:"type"`
	Database     string                      `yaml:"database"`
	Table        string                      `yaml:"table"`
	Schedule     string                      `yaml:"schedule"` // Cron expression for scheduling
	Dependencies []transformation.Dependency `yaml:"dependencies,omitempty"`
	Tags         []string                    `yaml:"tags,omitempty"`
	// Env and Exec are never read from this struct; they exist only so the strict
	// YAML decoder (KnownFields) accepts those keys. Consumption happens via
	// transformation.Config and transformation.Exec.
	Env  map[string]string `yaml:"env,omitempty"`
	Exec string            `yaml:"exec,omitempty"`

	// OriginalDependencies stores the dependencies before placeholder substitution
	OriginalDependencies []transformation.Dependency `yaml:"-"`
}
