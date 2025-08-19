// Package worker implements the worker functionality for CBT
package worker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/sirupsen/logrus"
)

// Define static errors
var (
	ErrInvalidTaskContext = errors.New("invalid task context type")
)

// ModelExecutor implements the execution of model transformations
type ModelExecutor struct {
	logger   *logrus.Logger
	chClient clickhouse.ClientInterface
	models   *models.Service
	admin    *admin.Service
}

// NewModelExecutor creates a new model executor
func NewModelExecutor(logger *logrus.Logger, chClient clickhouse.ClientInterface, modelsService *models.Service, adminManager *admin.Service) *ModelExecutor {
	return &ModelExecutor{
		logger:   logger,
		chClient: chClient,
		models:   modelsService,
		admin:    adminManager,
	}
}

// Execute runs the model transformation
func (e *ModelExecutor) Execute(ctx context.Context, taskCtxInterface interface{}) error {
	taskCtx, ok := taskCtxInterface.(*tasks.TaskContext)
	if !ok {
		return ErrInvalidTaskContext
	}

	// Validate first
	if err := e.Validate(ctx, taskCtx); err != nil {
		return err
	}

	config := taskCtx.Transformation.GetConfig()

	e.logger.WithFields(logrus.Fields{
		"model_id": fmt.Sprintf("%s.%s", config.Database, config.Table),
		"position": taskCtx.Position,
		"interval": taskCtx.Interval,
	}).Info("Executing model transformation")

	switch taskCtx.Transformation.GetType() {
	case transformation.TransformationTypeExec:
		if err := e.executeCommand(ctx, taskCtx); err != nil {
			return err
		}
	case transformation.TransformationTypeSQL:
		if err := e.executeSQL(ctx, taskCtx); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid transformation type: %s", taskCtx.Transformation.GetType())
	}

	// Record completion in admin table
	if err := e.admin.RecordCompletion(ctx, taskCtx.Transformation.GetID(), taskCtx.Position, taskCtx.Interval); err != nil {
		return fmt.Errorf("failed to record completion: %w", err)
	}

	return nil
}

// Validate checks if the model can be executed
func (e *ModelExecutor) Validate(ctx context.Context, taskCtxInterface interface{}) error {
	taskCtx, ok := taskCtxInterface.(*tasks.TaskContext)
	if !ok {
		return ErrInvalidTaskContext
	}

	config := taskCtx.Transformation.GetConfig()

	exists, err := clickhouse.TableExists(ctx, e.chClient, config.Database, config.Table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("table %s.%s does not exist", config.Database, config.Table)
	}

	return nil
}

func (e *ModelExecutor) executeSQL(ctx context.Context, taskCtx *tasks.TaskContext) error {
	config := taskCtx.Transformation.GetConfig()

	renderedSQL, err := e.models.RenderTransformation(taskCtx.Transformation, taskCtx.Position, taskCtx.Interval, taskCtx.StartTime)
	if err != nil {
		return fmt.Errorf("failed to render SQL template: %w", err)
	}

	// Simple split by semicolon
	statements := strings.Split(renderedSQL, ";")

	e.logger.WithFields(logrus.Fields{
		"count":    len(statements),
		"model_id": fmt.Sprintf("%s.%s", config.Database, config.Table),
	}).Info("Split SQL into statements")

	// Execute each statement
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Log first 500 chars of SQL for visibility
		logSQL := stmt
		if len(stmt) > 500 {
			logSQL = stmt[:500] + "..."
		}

		e.logger.WithFields(logrus.Fields{
			"statement_num": i + 1,
			"total":         len(statements),
			"model_id":      fmt.Sprintf("%s.%s", config.Database, config.Table),
			"sql_preview":   logSQL,
		}).Info("Executing SQL statement")

		if err := e.chClient.Execute(ctx, stmt); err != nil {
			e.logger.WithFields(logrus.Fields{
				"statement": i + 1,
				"sql":       logSQL,
				"error":     err.Error(),
			}).Error("SQL execution failed")
			return fmt.Errorf("failed to execute statement %d: %w", i+1, err)
		}
	}

	e.logger.WithFields(logrus.Fields{
		"model_id":   fmt.Sprintf("%s.%s", config.Database, config.Table),
		"position":   taskCtx.Position,
		"interval":   taskCtx.Interval,
		"statements": len(statements),
	}).Info("Model transformation completed successfully")

	return nil
}

func (e *ModelExecutor) executeCommand(ctx context.Context, taskCtx *tasks.TaskContext) error {
	config := taskCtx.Transformation.GetConfig()
	command := taskCtx.Transformation.GetValue()

	env, err := e.models.GetTransformationEnvironmentVariables(taskCtx.Transformation, taskCtx.Position, taskCtx.Interval, taskCtx.StartTime)
	if err != nil {
		return fmt.Errorf("failed to render SQL template: %w", err)
	}

	// Execute command
	// #nosec G204 -- Model exec commands are defined by trusted model files
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Env = append(cmd.Env, *env...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		e.logger.WithFields(logrus.Fields{
			"command": command,
			"output":  string(output),
			"error":   err,
		}).Error("Command execution failed")
		return fmt.Errorf("command execution failed: %w", err)
	}

	e.logger.WithFields(logrus.Fields{
		"model_id": fmt.Sprintf("%s.%s", config.Database, config.Table),
		"position": taskCtx.Position,
		"interval": taskCtx.Interval,
	}).Info("Model command executed successfully")

	return nil
}
