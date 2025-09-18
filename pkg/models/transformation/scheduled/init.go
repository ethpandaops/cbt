package scheduled

import (
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

func init() {
	// Register the scheduled handler factory
	transformation.RegisterHandler(transformation.TypeScheduled, func(data []byte, adminTable transformation.AdminTable) (transformation.Handler, error) {
		return NewHandler(data, adminTable)
	})
}
