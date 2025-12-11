package coordinator

import (
	"context"

	"github.com/sirupsen/logrus"
)

// RunConsolidation performs admin table consolidation for all models
func (s *service) RunConsolidation(ctx context.Context) {
	transformations := s.dag.GetTransformationNodes()

	var (
		totalModels          int
		modelsConsolidated   int
		modelsFailed         int
		totalRowsConsolidate uint64
	)

	for _, transformation := range transformations {
		modelID := transformation.GetID()
		totalModels++

		// Try to consolidate
		consolidated, err := s.admin.ConsolidateHistoricalData(ctx, modelID)
		if err != nil {
			modelsFailed++

			s.log.WithError(err).WithFields(logrus.Fields{
				"model_id":          modelID,
				"rows_consolidated": consolidated,
			}).Warn("Consolidation failed")

			continue
		}

		if consolidated > 0 {
			modelsConsolidated++
			totalRowsConsolidate += consolidated

			s.log.WithFields(logrus.Fields{
				"model_id":          modelID,
				"rows_consolidated": consolidated,
			}).Info("Consolidated admin.cbt rows")
		}
	}

	s.log.WithFields(logrus.Fields{
		"total_models":        totalModels,
		"models_consolidated": modelsConsolidated,
		"models_failed":       modelsFailed,
		"total_rows":          totalRowsConsolidate,
	}).Info("Consolidation run completed")
}
