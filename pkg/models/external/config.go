package external

import (
	"fmt"
	"time"
)

type ExternalConfig struct {
	Database  string         `yaml:"database" validate:"required"`
	Table     string         `yaml:"table" validate:"required"`
	Partition string         `yaml:"partition" validate:"required"`
	TTL       *time.Duration `yaml:"ttl"`
	Lag       uint64         `yaml:"lag"`
}

func (c *ExternalConfig) Validate() error {
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	if c.Table == "" {
		return fmt.Errorf("table is required")
	}

	if c.Partition == "" {
		return fmt.Errorf("partition is required")
	}

	if c.TTL == nil {
		return fmt.Errorf("ttl is required")
	}

	return nil
}

func (c *ExternalConfig) GetID() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}
