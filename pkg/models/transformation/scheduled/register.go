package scheduled

import (
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// Register registers the scheduled handler with the global registry
func Register() {
	transformation.RegisterHandler(transformation.TypeScheduled, func(data []byte, adminTable transformation.AdminTable) (transformation.Handler, error) {
		return NewHandler(data, adminTable)
	})
}
