package coordinator

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// ArchiveHandler manages archive monitoring and cleanup
type ArchiveHandler interface {
	// Start begins monitoring archived tasks
	Start(ctx context.Context) error
	// Stop gracefully shuts down the handler
	Stop() error
}

// archiveHandler implements the ArchiveHandler interface
type archiveHandler struct {
	log       logrus.FieldLogger
	inspector *asynq.Inspector
	dag       models.DAGReader

	// Synchronization - per ethPandaOps standards
	done chan struct{}  // Signal shutdown
	wg   sync.WaitGroup // Track goroutines

	// Config
	checkInterval time.Duration
	batchSize     int
}

// NewArchiveHandler creates a new archive handler
func NewArchiveHandler(log logrus.FieldLogger, redisOpt *redis.Options, dag models.DAGReader) (ArchiveHandler, error) {
	inspector := asynq.NewInspector(r.NewAsynqRedisOptions(redisOpt))

	return &archiveHandler{
		log:           log.WithField("service", "archive-handler"),
		inspector:     inspector,
		dag:           dag,
		done:          make(chan struct{}),
		checkInterval: time.Second * 30,
		batchSize:     100,
	}, nil
}

// Start begins monitoring archived tasks
func (h *archiveHandler) Start(_ context.Context) error {
	h.log.WithFields(logrus.Fields{
		"check_interval": h.checkInterval,
		"batch_size":     h.batchSize,
	}).Info("Starting archive handler")

	h.wg.Add(1)
	go h.monitorArchive()

	return nil
}

// Stop gracefully shuts down the handler
func (h *archiveHandler) Stop() error {
	h.log.Info("Stopping archive handler")

	close(h.done)
	h.wg.Wait()

	h.log.Info("Archive handler stopped")
	return nil
}

// monitorArchive continuously monitors and processes archived tasks
func (h *archiveHandler) monitorArchive() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			h.processArchivedTasks()
		}
	}
}

// processArchivedTasks processes all archived tasks across all queues
func (h *archiveHandler) processArchivedTasks() {
	// Get all transformation nodes to know which queues to check
	transformations := h.dag.GetTransformationNodes()

	for _, transformation := range transformations {
		queueName := transformation.GetID()

		// List archived tasks for this queue
		archivedTasks, err := h.inspector.ListArchivedTasks(
			queueName,
			asynq.PageSize(h.batchSize),
		)
		if err != nil {
			h.log.WithError(err).WithField("queue", queueName).Warn("Failed to list archived tasks")
			continue
		}

		if len(archivedTasks) == 0 {
			continue
		}

		h.log.WithFields(logrus.Fields{
			"queue":          queueName,
			"archived_count": len(archivedTasks),
		}).Info("Found archived tasks")

		// Process each archived task
		for _, taskInfo := range archivedTasks {
			h.processArchivedTask(queueName, taskInfo)
		}
	}
}

// processArchivedTask processes a single archived task
func (h *archiveHandler) processArchivedTask(queueName string, taskInfo *asynq.TaskInfo) {
	// Parse the task payload to get details
	var payload tasks.TaskPayload
	modelID := "unknown"

	if err := json.Unmarshal(taskInfo.Payload, &payload); err != nil {
		h.log.WithError(err).WithFields(logrus.Fields{
			"queue":   queueName,
			"task_id": taskInfo.ID,
		}).Error("Failed to unmarshal archived task payload")
	} else {
		modelID = payload.ModelID
		// Log detailed error information about the archived task
		h.log.WithFields(logrus.Fields{
			"queue":          queueName,
			"task_id":        taskInfo.ID,
			"model_id":       payload.ModelID,
			"position":       payload.Position,
			"interval":       payload.Interval,
			"retry_count":    taskInfo.Retried,
			"max_retry":      taskInfo.MaxRetry,
			"last_error":     taskInfo.LastErr,
			"last_failed_at": taskInfo.LastFailedAt,
			"next_process":   taskInfo.NextProcessAt,
		}).Error("Task archived after exhausting retries")
	}

	// Delete the archived task
	if err := h.inspector.DeleteTask(queueName, taskInfo.ID); err != nil {
		h.log.WithError(err).WithFields(logrus.Fields{
			"queue":   queueName,
			"task_id": taskInfo.ID,
		}).Error("Failed to delete archived task")
		observability.RecordError("archive-handler", "delete_error")
	} else {
		h.log.WithFields(logrus.Fields{
			"queue":    queueName,
			"task_id":  taskInfo.ID,
			"model_id": modelID,
		}).Info("Deleted archived task")
		observability.RecordArchivedTaskDeleted(queueName, modelID)
	}
}

// noopHandler is a no-op implementation when archive handling is disabled
type noopHandler struct{}

func (n *noopHandler) Start(_ context.Context) error { return nil }
func (n *noopHandler) Stop() error                   { return nil }

var _ ArchiveHandler = (*archiveHandler)(nil)
var _ ArchiveHandler = (*noopHandler)(nil)
