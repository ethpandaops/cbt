package management

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"
)

// Handlers implements management action endpoints.
type Handlers struct {
	adminService  admin.Service
	modelsService models.Service
	coordinator   coordinator.Service
	log           logrus.FieldLogger
}

// deletePeriodRequest is the JSON body for DeletePeriod.
type deletePeriodRequest struct {
	StartPos uint64 `json:"start_pos"`
	EndPos   uint64 `json:"end_pos"`
	Cascade  bool   `json:"cascade"`
}

// cascadeResult holds the per-model result of a cascaded delete.
type cascadeResult struct {
	ModelID     string `json:"model_id"`
	DeletedRows uint64 `json:"deleted_rows"`
}

// NewHandlers creates a new management Handlers instance.
func NewHandlers(
	adminService admin.Service,
	modelsService models.Service,
	coord coordinator.Service,
	log logrus.FieldLogger,
) *Handlers {
	return &Handlers{
		adminService:  adminService,
		modelsService: modelsService,
		coordinator:   coord,
		log:           log,
	}
}

// DeletePeriod removes tracking rows overlapping a position range and
// optionally cascades the deletion to all transitive incremental dependents.
func (h *Handlers) DeletePeriod(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	var req deletePeriodRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}

	if req.StartPos >= req.EndPos {
		return fiber.NewError(
			fiber.StatusBadRequest, "start_pos must be less than end_pos",
		)
	}

	deleted, err := h.adminService.DeletePeriod(
		c.Context(), id, req.StartPos, req.EndPos,
	)
	if err != nil {
		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to delete period")

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("delete period failed: %s", err.Error()),
		})
	}

	var cascadeResults []cascadeResult

	if req.Cascade {
		dag := h.modelsService.GetDAG()
		dependents := dag.GetAllDependents(id)

		for _, depID := range dependents {
			node, nodeErr := dag.GetTransformationNode(depID)
			if nodeErr != nil {
				continue
			}

			handler := node.GetHandler()
			if handler == nil || !handler.ShouldTrackPosition() {
				continue
			}

			depDeleted, depErr := h.adminService.DeletePeriod(
				c.Context(), depID, req.StartPos, req.EndPos,
			)
			if depErr != nil {
				h.log.WithError(depErr).
					WithField("model_id", depID).
					Warn("Failed to cascade delete period")

				continue
			}

			cascadeResults = append(cascadeResults, cascadeResult{
				ModelID:     depID,
				DeletedRows: depDeleted,
			})
		}
	}

	return c.JSON(fiber.Map{
		"model_id":        id,
		"deleted_rows":    deleted,
		"cascade_results": cascadeResults,
	})
}

// Consolidate triggers historical data consolidation for a model.
func (h *Handlers) Consolidate(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	merged, err := h.adminService.ConsolidateHistoricalData(c.Context(), id)
	if err != nil {
		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to consolidate historical data")

		return fiber.NewError(
			fiber.StatusInternalServerError,
			"consolidation failed",
		)
	}

	return c.JSON(fiber.Map{
		"model_id":      id,
		"ranges_merged": merged,
	})
}

// updateBoundsRequest is the JSON body for UpdateBounds.
type updateBoundsRequest struct {
	Min uint64 `json:"min"`
	Max uint64 `json:"max"`
}

// UpdateBounds overwrites external model bounds in Redis cache.
func (h *Handlers) UpdateBounds(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	var req updateBoundsRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}

	lock, err := h.adminService.AcquireBoundsLock(c.Context(), id)
	if err != nil {
		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to acquire bounds lock")

		return fiber.NewError(
			fiber.StatusInternalServerError,
			"failed to acquire lock",
		)
	}
	defer func() { _ = lock.Unlock(c.Context()) }()

	// Get existing bounds to preserve metadata
	existing, err := h.adminService.GetExternalBounds(c.Context(), id)
	if err != nil {
		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to get existing bounds")

		return fiber.NewError(
			fiber.StatusInternalServerError,
			"failed to read existing bounds",
		)
	}

	cache := &admin.BoundsCache{
		ModelID:   id,
		Min:       req.Min,
		Max:       req.Max,
		UpdatedAt: time.Now().UTC(),
	}

	// Preserve metadata from existing bounds
	if existing != nil {
		cache.PreviousMin = existing.Min
		cache.PreviousMax = existing.Max
		cache.LastIncrementalScan = existing.LastIncrementalScan
		cache.LastFullScan = existing.LastFullScan
		cache.InitialScanComplete = existing.InitialScanComplete
		cache.InitialScanStarted = existing.InitialScanStarted
	}

	if err := h.adminService.SetExternalBounds(c.Context(), cache); err != nil {
		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to set bounds")

		return fiber.NewError(
			fiber.StatusInternalServerError,
			"failed to save bounds",
		)
	}

	return c.JSON(fiber.Map{
		"model_id": id,
		"min":      req.Min,
		"max":      req.Max,
	})
}

// DeleteBounds removes external model bounds from Redis cache.
func (h *Handlers) DeleteBounds(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	lock, err := h.adminService.AcquireBoundsLock(c.Context(), id)
	if err != nil {
		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to acquire bounds lock")

		return fiber.NewError(
			fiber.StatusInternalServerError,
			"failed to acquire lock",
		)
	}
	defer func() { _ = lock.Unlock(c.Context()) }()

	if err := h.adminService.DeleteExternalBounds(c.Context(), id); err != nil {
		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to delete bounds")

		return fiber.NewError(
			fiber.StatusInternalServerError,
			"failed to delete bounds",
		)
	}

	return c.JSON(fiber.Map{
		"model_id": id,
		"deleted":  true,
	})
}

// TriggerRefreshBounds enqueues a full external scan via the coordinator.
func (h *Handlers) TriggerRefreshBounds(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	if err := h.coordinator.TriggerBoundsRefresh(c.Context(), id); err != nil {
		if errors.Is(err, coordinator.ErrRefreshInProgress) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "Bounds refresh already in progress",
			})
		}

		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to trigger bounds refresh")

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf(
				"failed to refresh bounds: %s", err.Error(),
			),
		})
	}

	return c.JSON(fiber.Map{
		"model_id":  id,
		"scan_type": "full",
	})
}
