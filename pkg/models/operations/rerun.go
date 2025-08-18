// Package operations provides write operations for models including rerun functionality
package operations

import (
	"context"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/sirupsen/logrus"
)

// Manager handles rerun operations
type Manager struct {
	chClient     clickhouse.ClientInterface
	adminManager *clickhouse.AdminTableManager
	queueManager *tasks.QueueManager
	modelConfigs map[string]models.ModelConfig
	logger       *logrus.Entry
}

// Options contains options for rerun operations
type Options struct {
	ModelID string
	Start   uint64
	End     uint64
	Cascade bool
	Force   bool
}

// NewManager creates a new rerun manager
func NewManager(chConfig *clickhouse.Config, redisURL string, logger *logrus.Logger) (*Manager, error) {
	log := logger.WithField("component", "rerun-manager")

	// Initialize ClickHouse client with admin manager
	chClient, adminManager, err := clickhouse.SetupClientWithAdmin(chConfig, logger)
	if err != nil {
		return nil, err
	}

	// Initialize components
	queueManager := tasks.NewQueueManager(ParseRedisConfig(redisURL))

	// Load model configurations
	modelConfigs, err := models.LoadAllModels(log)
	if err != nil {
		_ = chClient.Stop()
		_ = queueManager.Close()
		return nil, err
	}

	return &Manager{
		chClient:     chClient,
		adminManager: adminManager,
		queueManager: queueManager,
		modelConfigs: modelConfigs,
		logger:       log,
	}, nil
}

// Execute performs the rerun operation
func (m *Manager) Execute(ctx context.Context, opts *Options) error {
	// Validate model exists
	_, exists := m.modelConfigs[opts.ModelID]
	if !exists {
		return fmt.Errorf("%w: %s", models.ErrModelNotFound, opts.ModelID)
	}

	m.logger.WithFields(logrus.Fields{
		"model":   opts.ModelID,
		"start":   opts.Start,
		"end":     opts.End,
		"cascade": opts.Cascade,
		"force":   opts.Force,
	}).Info("Starting rerun")

	// Build list of models to rerun
	modelsToRerun := m.buildModelsToRerun(opts.ModelID, opts.Cascade)

	// Process each model
	totalTasks := 0
	for _, modelID := range modelsToRerun {
		cfg := m.modelConfigs[modelID]
		count := m.processModel(ctx, modelID, &cfg, opts)
		totalTasks += count
	}

	m.logger.WithField("total_tasks", totalTasks).Info("Rerun completed")
	return nil
}

// Close cleanly shuts down the manager
func (m *Manager) Close() error {
	chErr := m.chClient.Stop()
	if chErr != nil {
		m.logger.WithError(chErr).Error("Failed to stop ClickHouse client")
	}

	qErr := m.queueManager.Close()
	if qErr != nil {
		m.logger.WithError(qErr).Error("Failed to close queue manager")
	}

	// Return first error encountered
	if chErr != nil {
		return fmt.Errorf("failed to stop ClickHouse client: %w", chErr)
	}
	if qErr != nil {
		return fmt.Errorf("failed to close queue manager: %w", qErr)
	}

	return nil
}

func (m *Manager) buildModelsToRerun(modelID string, cascade bool) []string {
	modelsToRerun := []string{modelID}

	if cascade {
		// Find all dependent models
		dependents := FindDependentModels(modelID, m.modelConfigs)
		modelsToRerun = append(modelsToRerun, dependents...)
		m.logger.WithField("models", modelsToRerun).Info("Will rerun models with cascade")
	}

	return modelsToRerun
}

func (m *Manager) processModel(ctx context.Context, modelID string, modelCfg *models.ModelConfig, opts *Options) int {
	intervals := CalculateIntervals(opts.Start, opts.End, modelCfg.Interval)

	m.logger.WithFields(logrus.Fields{
		"model":     modelID,
		"intervals": len(intervals),
	}).Info("Processing model")

	// Clear completions if force flag is set
	if opts.Force {
		m.clearCompletions(ctx, modelID, intervals, modelCfg.Interval)
	}

	// Enqueue tasks
	return m.enqueueTasks(ctx, modelID, modelCfg, intervals, opts.Force)
}

func (m *Manager) clearCompletions(ctx context.Context, modelID string, intervals []Interval, intervalSize uint64) {
	for _, interval := range intervals {
		if err := m.adminManager.DeleteRange(ctx, modelID, interval.Start, interval.Start+intervalSize); err != nil {
			m.logger.WithError(err).WithFields(logrus.Fields{
				"model":    modelID,
				"position": interval.Start,
			}).Error("Failed to clear completion")
		}
	}
}

func (m *Manager) enqueueTasks(ctx context.Context, modelID string, modelCfg *models.ModelConfig, intervals []Interval, force bool) int {
	count := 0

	for _, interval := range intervals {
		if m.shouldEnqueue(ctx, modelID, interval, force) {
			if m.enqueueTask(modelID, interval.Start, modelCfg.Interval) {
				count++
			}
		}
	}

	return count
}

func (m *Manager) shouldEnqueue(ctx context.Context, modelID string, interval Interval, force bool) bool {
	if force {
		return true
	}

	// Check coverage to see if this interval is already completed
	covered, err := m.adminManager.GetCoverage(ctx, modelID, interval.Start, interval.End)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to check completion status")
	}

	if covered {
		m.logger.WithFields(logrus.Fields{
			"model":    modelID,
			"position": interval.Start,
		}).Debug("Skipping already completed interval")
		return false
	}

	return true
}

func (m *Manager) enqueueTask(modelID string, position, interval uint64) bool {
	payload := tasks.TaskPayload{
		ModelID:  modelID,
		Position: position,
		Interval: interval,
	}

	if err := m.queueManager.EnqueueTransformation(payload); err != nil {
		m.logger.WithError(err).WithFields(logrus.Fields{
			"model":    modelID,
			"position": position,
		}).Error("Failed to enqueue task")
		return false
	}

	m.logger.WithFields(logrus.Fields{
		"model":    modelID,
		"position": position,
		"interval": interval,
		"task_id":  payload.UniqueID(),
	}).Info("Enqueued task")

	return true
}
