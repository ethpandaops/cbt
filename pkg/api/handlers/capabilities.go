package handlers

// This file declares the capability interfaces the handlers assert against when
// inspecting domain model handlers. Several of these shapes are NOT declared in
// pkg/models/transformation/types.go (the package exposes IntervalHandler with
// GetMinInterval/GetMaxInterval rather than GetInterval, and DependencyHandler
// with GetDependencies() []transformation.Dependency rather than the anonymous
// struct shape used here). Declaring them once here removes the repeated inline
// interface literals while preserving the exact assertion semantics.

// intervalTypeProvider exposes the interval type string for a model or handler.
type intervalTypeProvider interface {
	GetIntervalType() string
}

// intervalProvider exposes the min/max interval for an incremental handler.
type intervalProvider interface {
	GetInterval() (minInterval, maxInterval uint64)
}

// scheduleProvider exposes the schedule and tags for a scheduled handler.
type scheduleProvider interface {
	GetSchedule() string
	GetTags() []string
}

// incrementalProvider exposes the full incremental handler surface used when
// populating an incremental transformation model.
type incrementalProvider interface {
	GetInterval() (minInterval, maxInterval uint64)
	GetSchedules() (forwardfill, backfill string)
	GetTags() []string
	GetFlattenedDependencies() []string
}

// schedulesProvider exposes the forward/backfill schedules for a handler.
type schedulesProvider interface {
	GetSchedules() (forwardfill, backfill string)
}

// tagsProvider exposes the tags for a handler.
type tagsProvider interface {
	GetTags() []string
}

// positionTracker reports whether a handler tracks positions (is incremental).
type positionTracker interface {
	ShouldTrackPosition() bool
}

// structuredDep mirrors the structured dependency shape exposed by handlers via
// reflection-free assertion. Note this anonymous struct shape intentionally
// matches the historical inline assertion; it does NOT match
// transformation.Dependency, so the assertion only succeeds for handlers that
// expose this exact shape.
type structuredDep = struct {
	IsGroup   bool
	SingleDep string
	GroupDeps []string
}

// structuredDepProvider exposes structured dependencies (with OR group support).
type structuredDepProvider interface {
	GetDependencies() []structuredDep
}

// flatDepProvider exposes a flattened list of dependency IDs.
type flatDepProvider interface {
	GetFlattenedDependencies() []string
}
