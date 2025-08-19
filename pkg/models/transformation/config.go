package transformation

import (
	"fmt"

	"github.com/robfig/cron/v3"
)

type TransformationConfig struct {
	Database     string                        `yaml:"database"`
	Table        string                        `yaml:"table"`
	Partition    string                        `yaml:"partition"`
	Interval     uint64                        `yaml:"interval"`
	Schedule     string                        `yaml:"schedule"`
	Backfill     *TransformationBackfillConfig `yaml:"backfill,omitempty"`
	Dependencies []string                      `yaml:"dependencies"`
	Tags         []string                      `yaml:"tags"`
}

type TransformationBackfillConfig struct {
	Enabled  bool   `yaml:"enabled,omitempty"`
	Schedule string `yaml:"schedule,omitempty"`
	Minimum  uint64 `yaml:"minimum,omitempty"`
}

func (c *TransformationConfig) Validate() error {
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	if c.Table == "" {
		return fmt.Errorf("table is required")
	}

	if c.Partition == "" {
		return fmt.Errorf("partition is required")
	}

	if c.Interval == 0 {
		return fmt.Errorf("interval is required")
	}

	if err := ValidateScheduleFormat(c.Schedule); err != nil {
		return err
	}

	if c.Backfill == nil {
		return fmt.Errorf("backfill is required")
	}

	if len(c.Dependencies) == 0 {
		return fmt.Errorf("dependencies is required")
	}

	return nil
}

func (c *TransformationConfig) GetID() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}

func (c *TransformationBackfillConfig) Validate() error {
	if c.Enabled && c.Schedule == "" {
		return fmt.Errorf("schedule is required when backfill is enabled")
	}

	if err := ValidateScheduleFormat(c.Schedule); err != nil {
		return err
	}

	return nil
}

func ValidateScheduleFormat(schedule string) error {
	_, err := cron.ParseStandard(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	return nil
}
