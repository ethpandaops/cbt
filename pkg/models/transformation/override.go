package transformation

// Override contains override values for transformation model configurations.
// All fields are optional - nil means keep existing value.
// The structure is type-agnostic - fields are applied based on the actual transformation type:
//   - Incremental transformations: interval, schedules (forwardfill/backfill), limits, tags
//   - Scheduled transformations: schedule, tags
type Override struct {
	// Incremental transformation fields
	Interval  *IntervalOverride  `yaml:"interval,omitempty"`
	Schedules *SchedulesOverride `yaml:"schedules,omitempty"`
	Limits    *LimitsOverride    `yaml:"limits,omitempty"`

	// Scheduled transformation fields
	Schedule *string `yaml:"schedule,omitempty"`

	// Common fields (both types)
	Tags []string `yaml:"tags,omitempty"`
}

// IntervalOverride allows overriding interval configuration
type IntervalOverride struct {
	Max *uint64 `yaml:"max,omitempty"`
	Min *uint64 `yaml:"min,omitempty"`
}

// SchedulesOverride allows overriding schedule configuration
// nil = keep existing, empty string = disable, non-empty = new schedule
type SchedulesOverride struct {
	ForwardFill *string `yaml:"forwardfill"`
	Backfill    *string `yaml:"backfill"`
}

// LimitsOverride allows overriding position limits
type LimitsOverride struct {
	Min *uint64 `yaml:"min,omitempty"`
	Max *uint64 `yaml:"max,omitempty"`
}
