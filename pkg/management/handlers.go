package management

import (
	"encoding/json"
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

// BaseConfigProvider returns original (pre-override) config for a model.
type BaseConfigProvider interface {
	GetBaseConfig(modelID string) (json.RawMessage, error)
}

// Handlers implements management action endpoints.
type Handlers struct {
	adminService       admin.Service
	modelsService      models.Service
	coordinator        coordinator.Service
	log                logrus.FieldLogger
	baseConfigProvider BaseConfigProvider
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

var errEmptySchedule = errors.New("empty schedule")

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

// SetBaseConfigProvider sets the provider used to retrieve original config snapshots.
func (h *Handlers) SetBaseConfigProvider(p BaseConfigProvider) {
	h.baseConfigProvider = p
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

// TriggerScheduledRun enqueues an immediate run for a scheduled transformation.
func (h *Handlers) TriggerScheduledRun(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	if err := h.coordinator.TriggerScheduledRun(c.Context(), id); err != nil {
		if errors.Is(err, coordinator.ErrScheduledRunInProgress) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "Scheduled run already in progress",
			})
		}

		if errors.Is(err, coordinator.ErrNotScheduledModel) {
			return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
				"error": "Model is not a scheduled transformation",
			})
		}

		h.log.WithError(err).
			WithField("model_id", id).
			Error("Failed to trigger scheduled run")

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf(
				"failed to trigger scheduled run: %s", err.Error(),
			),
		})
	}

	return c.JSON(fiber.Map{
		"model_id": id,
		"status":   "enqueued",
	})
}

// configOverrideRequest is the JSON body for SetConfigOverride.
type configOverrideRequest struct {
	Enabled *bool           `json:"enabled,omitempty"`
	Config  json.RawMessage `json:"config,omitempty"`
}

// GetConfigOverride returns the live override for a specific model,
// along with the original base config snapshot for comparison.
func (h *Handlers) GetConfigOverride(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	override, err := h.adminService.GetConfigOverride(c.Context(), id)
	if err != nil {
		h.log.WithError(err).WithField("model_id", id).Error("Failed to get config override")

		return fiber.NewError(fiber.StatusInternalServerError, "failed to get config override")
	}

	response := fiber.Map{}

	// Include base_config if provider is available
	if h.baseConfigProvider != nil {
		baseJSON, bcErr := h.baseConfigProvider.GetBaseConfig(id)
		if bcErr == nil && baseJSON != nil {
			response["base_config"] = baseJSON
		}
	}

	if override == nil {
		// Return just base_config with 200 (no override exists)
		return c.JSON(response)
	}

	response["model_id"] = override.ModelID
	response["model_type"] = override.ModelType

	if override.Enabled != nil {
		response["enabled"] = override.Enabled
	}

	response["override"] = override.Override
	response["updated_at"] = override.UpdatedAt

	return c.JSON(response)
}

// SetConfigOverride creates or updates a live override for a model.
func (h *Handlers) SetConfigOverride(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	var req configOverrideRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}

	// Determine model type from DAG
	dag := h.modelsService.GetDAG()
	node, err := dag.GetNode(id)
	if err != nil {
		return fiber.NewError(fiber.StatusNotFound, "model not found in DAG")
	}

	var modelType models.ModelType

	switch node.NodeType {
	case models.NodeTypeTransformation:
		modelType = models.ModelTypeTransformation
	case models.NodeTypeExternal:
		modelType = models.ModelTypeExternal

		// External models don't support enable/disable
		if req.Enabled != nil {
			return fiber.NewError(fiber.StatusBadRequest, "enabled field not supported for external models")
		}
	default:
		return fiber.NewError(fiber.StatusBadRequest, "unknown model type")
	}

	// Validate config if provided
	if len(req.Config) > 0 {
		if err := h.validateOverrideConfig(req.Config, modelType); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("invalid config: %s", err.Error()))
		}
	}

	override := &admin.ConfigOverride{
		ModelID:   id,
		ModelType: string(modelType),
		Enabled:   req.Enabled,
		Override:  req.Config,
		UpdatedAt: time.Now().UTC(),
	}

	if err := h.adminService.SetConfigOverride(c.Context(), override); err != nil {
		h.log.WithError(err).WithField("model_id", id).Error("Failed to set config override")

		return fiber.NewError(fiber.StatusInternalServerError, "failed to set config override")
	}

	return c.JSON(fiber.Map{
		"model_id": id,
		"updated":  true,
	})
}

// DeleteConfigOverride removes the live override for a model (reverts to base config).
func (h *Handlers) DeleteConfigOverride(c fiber.Ctx) error {
	id := c.Params("id")

	if _, _, err := modelid.Parse(id); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid model ID")
	}

	if err := h.adminService.DeleteConfigOverride(c.Context(), id); err != nil {
		h.log.WithError(err).WithField("model_id", id).Error("Failed to delete config override")

		return fiber.NewError(fiber.StatusInternalServerError, "failed to delete config override")
	}

	return c.JSON(fiber.Map{
		"model_id": id,
		"deleted":  true,
	})
}

// ListConfigOverrides returns all live overrides.
func (h *Handlers) ListConfigOverrides(c fiber.Ctx) error {
	overrides, err := h.adminService.GetAllConfigOverrides(c.Context())
	if err != nil {
		h.log.WithError(err).Error("Failed to list config overrides")

		return fiber.NewError(fiber.StatusInternalServerError, "failed to list config overrides")
	}

	return c.JSON(fiber.Map{
		"overrides": overrides,
	})
}

// ClearAllConfigOverrides removes all live overrides.
func (h *Handlers) ClearAllConfigOverrides(c fiber.Ctx) error {
	if err := h.adminService.DeleteAllConfigOverrides(c.Context()); err != nil {
		h.log.WithError(err).Error("Failed to clear all config overrides")

		return fiber.NewError(fiber.StatusInternalServerError, "failed to clear config overrides")
	}

	return c.JSON(fiber.Map{
		"cleared": true,
	})
}

// validateOverrideConfig validates the override config JSON based on model type.
func (h *Handlers) validateOverrideConfig(raw json.RawMessage, modelType models.ModelType) error {
	switch modelType {
	case models.ModelTypeTransformation:
		var tOv models.TransformationOverride
		if err := json.Unmarshal(raw, &tOv); err != nil {
			return fmt.Errorf("invalid transformation override: %w", err)
		}

		if err := validateTransformationSchedules(&tOv); err != nil {
			return err
		}

	case models.ModelTypeExternal:
		var eOv models.ExternalOverride
		if err := json.Unmarshal(raw, &eOv); err != nil {
			return fmt.Errorf("invalid external override: %w", err)
		}
	}

	return nil
}

// validateTransformationSchedules validates schedule formats in a transformation override.
func validateTransformationSchedules(tOv *models.TransformationOverride) error {
	if tOv.Schedules != nil {
		if err := validateOptionalSchedule(tOv.Schedules.ForwardFill, "forwardfill"); err != nil {
			return err
		}

		if err := validateOptionalSchedule(tOv.Schedules.Backfill, "backfill"); err != nil {
			return err
		}
	}

	return validateOptionalSchedule(tOv.Schedule, "schedule")
}

// validateOptionalSchedule validates a single optional schedule string pointer.
func validateOptionalSchedule(s *string, name string) error {
	if s == nil || *s == "" {
		return nil
	}

	if _, err := parseScheduleInterval(*s); err != nil {
		return fmt.Errorf("invalid %s schedule: %w", name, err)
	}

	return nil
}

// parseScheduleInterval validates a cron/every schedule format.
// We duplicate the validation here to avoid a circular dependency.
func parseScheduleInterval(schedule string) (time.Duration, error) {
	// For @every format, extract and validate the duration
	if len(schedule) > 7 && schedule[:6] == "@every" {
		durationStr := schedule[7:]

		duration, err := time.ParseDuration(durationStr)
		if err != nil {
			return 0, fmt.Errorf("invalid @every duration: %w", err)
		}

		return duration, nil
	}

	// For standard cron, just validate it parses
	// We don't need the robfig parser here — basic validation is sufficient
	// since the scheduler will do full validation when it picks up the override
	if schedule == "" {
		return 0, errEmptySchedule
	}

	return 0, nil
}
