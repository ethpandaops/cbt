// Package worker implements the worker functionality for CBT
package worker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/rendering"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/sirupsen/logrus"
)

// Define static errors
var (
	ErrInvalidTaskContext = errors.New("invalid task context type")
)

// ModelExecutor implements the execution of model transformations
type ModelExecutor struct {
	chConfig    *clickhouse.Config
	workerCfg   *Settings
	chClient    clickhouse.ClientInterface
	templateEng *rendering.TemplateEngine
	logger      *logrus.Logger
}

// NewModelExecutor creates a new model executor
func NewModelExecutor(chConfig *clickhouse.Config, workerCfg *Settings, chClient clickhouse.ClientInterface, templateEng *rendering.TemplateEngine, logger *logrus.Logger) *ModelExecutor {
	return &ModelExecutor{
		chConfig:    chConfig,
		workerCfg:   workerCfg,
		chClient:    chClient,
		templateEng: templateEng,
		logger:      logger,
	}
}

// Execute runs the model transformation
func (e *ModelExecutor) Execute(ctx context.Context, taskCtxInterface interface{}) error {
	taskCtx, ok := taskCtxInterface.(*tasks.TaskContext)
	if !ok {
		return ErrInvalidTaskContext
	}

	// Create log fields with basic info
	logFields := logrus.Fields{
		"model_id": fmt.Sprintf("%s.%s", taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table),
		"position": taskCtx.Position,
		"interval": taskCtx.Interval,
	}

	// Add lookback information if present
	effectiveLookback := taskCtx.ModelConfig.GetEffectiveLookback()
	if effectiveLookback > 0 {
		logFields["lookback"] = effectiveLookback
		if !taskCtx.ModelConfig.External {
			logFields["lookback_inherited"] = true
			logFields["lookback_reason"] = taskCtx.ModelConfig.LookbackReason
		}
	}

	e.logger.WithFields(logFields).Info("Executing model transformation")

	if taskCtx.ModelConfig.Exec != "" {
		return e.executeCommand(ctx, taskCtx)
	}
	return e.executeSQL(ctx, taskCtx)
}

// Validate checks if the model can be executed
func (e *ModelExecutor) Validate(ctx context.Context, taskCtxInterface interface{}) error {
	taskCtx, ok := taskCtxInterface.(*tasks.TaskContext)
	if !ok {
		return ErrInvalidTaskContext
	}
	// Check if target table exists
	exists, err := clickhouse.TableExists(ctx, e.chClient, taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		// Create table if it doesn't exist
		if err := e.createTargetTable(ctx, taskCtx); err != nil {
			return fmt.Errorf("failed to create target table: %w", err)
		}
	}

	return nil
}

func (e *ModelExecutor) executeSQL(ctx context.Context, taskCtx *tasks.TaskContext) error {
	// Build template variables
	variables := e.templateEng.BuildVariables(e.chConfig, &taskCtx.ModelConfig, taskCtx.Position, taskCtx.Interval, taskCtx.StartTime)

	// Render SQL template
	renderedSQL, err := e.templateEng.Render(taskCtx.ModelConfig.Content, variables)
	if err != nil {
		return fmt.Errorf("failed to render SQL template: %w", err)
	}

	// Simple split by semicolon
	statements := strings.Split(renderedSQL, ";")

	e.logger.WithFields(logrus.Fields{
		"count":    len(statements),
		"model_id": fmt.Sprintf("%s.%s", taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table),
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
			"model_id":      fmt.Sprintf("%s.%s", taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table),
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

	logFields := logrus.Fields{
		"model_id":   fmt.Sprintf("%s.%s", taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table),
		"position":   taskCtx.Position,
		"interval":   taskCtx.Interval,
		"statements": len(statements),
	}

	// Add lookback information to completion log
	effectiveLookback := taskCtx.ModelConfig.GetEffectiveLookback()
	if effectiveLookback > 0 {
		logFields["lookback"] = effectiveLookback
	}

	e.logger.WithFields(logFields).Info("Model transformation completed successfully")

	return nil
}

func (e *ModelExecutor) executeCommand(ctx context.Context, taskCtx *tasks.TaskContext) error {
	// Build environment variables
	env := rendering.BuildEnvironmentVariables(e.chConfig, &taskCtx.ModelConfig, taskCtx.Position, taskCtx.Interval, taskCtx.StartTime)

	// Execute command
	// #nosec G204 -- Model exec commands are defined by trusted model files
	cmd := exec.CommandContext(ctx, "sh", "-c", taskCtx.ModelConfig.Exec)
	cmd.Env = append(cmd.Env, env...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		e.logger.WithFields(logrus.Fields{
			"command": taskCtx.ModelConfig.Exec,
			"output":  string(output),
			"error":   err,
		}).Error("Command execution failed")
		return fmt.Errorf("command execution failed: %w", err)
	}

	logFields := logrus.Fields{
		"model_id": fmt.Sprintf("%s.%s", taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table),
		"position": taskCtx.Position,
		"interval": taskCtx.Interval,
	}

	// Add lookback information to completion log
	effectiveLookback := taskCtx.ModelConfig.GetEffectiveLookback()
	if effectiveLookback > 0 {
		logFields["lookback"] = effectiveLookback
	}

	e.logger.WithFields(logFields).Info("Model command executed successfully")

	return nil
}

func (e *ModelExecutor) createTargetTable(ctx context.Context, taskCtx *tasks.TaskContext) error {
	// This is a simplified table creation - in production, you'd want to use the model's schema definition
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			%s DateTime,
			position UInt64 DEFAULT %d,
			_updated_at DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(_updated_at)
		PARTITION BY toYYYYMM(%s)
		ORDER BY (%s, position)
	`, taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table,
		taskCtx.ModelConfig.Partition, taskCtx.Position,
		taskCtx.ModelConfig.Partition, taskCtx.ModelConfig.Partition)

	return e.chClient.Execute(ctx, query)
}

// TransformationModelExecutor specifically handles transformation models
type TransformationModelExecutor struct {
	*ModelExecutor
	adminManager *clickhouse.AdminTableManager
}

// NewTransformationModelExecutor creates a new transformation model executor
func NewTransformationModelExecutor(
	chConfig *clickhouse.Config,
	workerCfg *Settings,
	chClient clickhouse.ClientInterface,
	templateEng *rendering.TemplateEngine,
	adminManager *clickhouse.AdminTableManager,
	logger *logrus.Logger,
) *TransformationModelExecutor {
	return &TransformationModelExecutor{
		ModelExecutor: NewModelExecutor(chConfig, workerCfg, chClient, templateEng, logger),
		adminManager:  adminManager,
	}
}

// Execute runs the transformation and records completion
func (t *TransformationModelExecutor) Execute(ctx context.Context, taskCtxInterface interface{}) error {
	taskCtx, ok := taskCtxInterface.(*tasks.TaskContext)
	if !ok {
		return ErrInvalidTaskContext
	}
	// Validate first
	if err := t.Validate(ctx, taskCtx); err != nil {
		return err
	}

	// Execute the transformation
	if err := t.ModelExecutor.Execute(ctx, taskCtx); err != nil {
		return err
	}

	// Record completion in admin table
	modelID := fmt.Sprintf("%s.%s", taskCtx.ModelConfig.Database, taskCtx.ModelConfig.Table)
	if err := t.adminManager.RecordCompletion(ctx, modelID, taskCtx.Position, taskCtx.Interval); err != nil {
		return fmt.Errorf("failed to record completion: %w", err)
	}

	return nil
}
