package coordinator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// ProcessExternalScan handles processing external model scans
// This is called by scheduled tasks for each external model
func (s *service) ProcessExternalScan(modelID, scanType string) {
	// Create unique task ID
	taskID := fmt.Sprintf("external:%s:%s", modelID, scanType)

	// Create task payload
	payload := map[string]string{
		"model_id":  modelID,
		"scan_type": scanType,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id":  modelID,
			"scan_type": scanType,
		}).Error("Failed to marshal external scan task payload")
		return
	}

	// Use the appropriate task type based on scan type
	var taskType string
	if scanType == "incremental" {
		taskType = ExternalIncrementalTaskType
	} else {
		taskType = ExternalFullTaskType
	}

	// Create task with unique ID to prevent duplicates
	task := asynq.NewTask(taskType, payloadBytes,
		asynq.TaskID(taskID),
		asynq.Queue(modelID),
		asynq.MaxRetry(0),
		asynq.Timeout(30*time.Minute),
	)

	// Enqueue the task
	if _, err := s.queueManager.Enqueue(task); err != nil {
		// Check if task already exists (not an error, just skip)
		if err.Error() == "task ID already exists" {
			s.log.WithFields(logrus.Fields{
				"model_id":  modelID,
				"scan_type": scanType,
			}).Debug("External scan task already exists, skipping")
		} else {
			s.log.WithError(err).WithFields(logrus.Fields{
				"model_id":  modelID,
				"scan_type": scanType,
			}).Error("Failed to enqueue external scan task")
		}
		return
	}

	s.log.WithFields(logrus.Fields{
		"model_id":  modelID,
		"scan_type": scanType,
		"task_id":   taskID,
	}).Debug("Enqueued external scan task")
}
