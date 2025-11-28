package models

import (
	"github.com/ethpandaops/cbt/pkg/models/modelid"
)

// ErrInvalidModelID is returned when a model ID is not in the expected format.
var ErrInvalidModelID = modelid.ErrInvalidModelID

// FormatModelID creates a standardized model ID from database and table names (format: "database.table").
func FormatModelID(database, table string) string {
	return modelid.Format(database, table)
}

// ParseModelID splits a model ID into database and table components.
// Returns an error if the format is invalid.
func ParseModelID(modelID string) (database, table string, err error) {
	return modelid.Parse(modelID)
}
