package validation

import "errors"

// Validation-specific errors
var (
	ErrDependencyNotFound = errors.New("dependency model not found")
	ErrRangeNotAvailable  = errors.New("required range not available")
	ErrRangeNotCovered    = errors.New("range not fully covered")
)
