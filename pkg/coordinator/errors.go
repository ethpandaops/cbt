package coordinator

import "errors"

// Coordinator-specific errors
var (
	ErrUnsupportedSchedule = errors.New("unsupported schedule format")
)
