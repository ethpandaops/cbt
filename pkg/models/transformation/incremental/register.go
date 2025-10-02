package incremental

import (
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// Register registers the incremental handler with the global registry
func Register() {
	transformation.RegisterHandler(transformation.TypeIncremental, func(data []byte, adminTable transformation.AdminTable) (transformation.Handler, error) {
		return NewHandler(data, adminTable)
	})
}
