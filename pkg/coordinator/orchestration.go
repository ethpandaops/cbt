package coordinator

import (
	"context"

	"github.com/sirupsen/logrus"
)

// RunConsolidation performs admin table consolidation for all models
func (s *service) RunConsolidation(ctx context.Context) {
	transformations := s.dag.GetTransformationNodes()
	for _, transformation := range transformations {
		modelID := transformation.GetID()

		// Try to consolidate
		consolidated, err := s.admin.ConsolidateHistoricalData(ctx, modelID)
		if err != nil {
			if consolidated > 0 {
				s.log.WithError(err).WithField("model_id", modelID).Debug("Consolidation partially succeeded")
			}
			continue
		}

		if consolidated > 0 {
			s.log.WithFields(logrus.Fields{
				"model_id":          modelID,
				"rows_consolidated": consolidated,
			}).Info("Consolidated admin.cbt rows")
		}
	}
}

