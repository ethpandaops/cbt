package models

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidModelID is returned when a model ID is not in the expected format.
var ErrInvalidModelID = errors.New("invalid model ID format, expected database.table")

// FormatModelID creates a standardized model ID from database and table names (format: "database.table").
func FormatModelID(database, table string) string {
	return fmt.Sprintf("%s.%s", database, table)
}

// ParseModelID splits a model ID into database and table components.
// Returns an error if the format is invalid.
func ParseModelID(modelID string) (database, table string, err error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: %s", ErrInvalidModelID, modelID)
	}

	return parts[0], parts[1], nil
}
