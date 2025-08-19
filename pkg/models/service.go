package models

import (
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Service encapsulates the worker application logic
type Service struct {
	config *Config
	log    *logrus.Logger

	dag                  *DependencyGraph
	templateEngine       *TemplateEngine
	transformationModels []Transformation
	externalModels       []External
}

// NewService creates a new worker application
func NewService(log *logrus.Logger, cfg *Config, redisClient *redis.Client, clickhouseCfg *clickhouse.Config) (*Service, error) {
	dag := NewDependencyGraph()
	templateEngine := NewTemplateEngine(clickhouseCfg, dag)

	return &Service{
		config: cfg,
		log:    log,

		dag:            dag,
		templateEngine: templateEngine,
	}, nil
}

func (s *Service) Start() error {
	if err := s.parseModels(); err != nil {
		return err
	}

	if err := s.buildDAG(); err != nil {
		return err
	}

	s.log.Info("Models service started successfully")

	return nil
}

func (s *Service) Stop() error {
	return nil
}

func (s *Service) parseModels() error {
	externalFiles, err := DiscoverPaths(s.config.External.Paths)
	if err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	if len(externalFiles) > 0 {
		for _, file := range externalFiles {
			externalModel, err := NewExternal(file.Content, file.FilePath)
			if err != nil {
				return err
			}

			s.externalModels = append(s.externalModels, externalModel)
		}
	}

	transformationFiles, err := DiscoverPaths(s.config.Transformation.Paths)
	if err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	if len(transformationFiles) > 0 {
		for _, file := range transformationFiles {
			transformationModel, err := NewTransformation(file.Content, file.FilePath)
			if err != nil {
				return err
			}

			s.transformationModels = append(s.transformationModels, transformationModel)
		}
	}

	return nil
}

func (s *Service) buildDAG() error {
	if err := s.dag.BuildGraph(s.transformationModels, s.externalModels); err != nil {
		return err
	}

	return nil
}

func (s *Service) GetDAG() *DependencyGraph {
	return s.dag
}

func (s *Service) RenderTransformation(model Transformation, position, interval uint64, startTime time.Time) (string, error) {
	return s.templateEngine.RenderTransformation(model, position, interval, startTime)
}

func (s *Service) GetTransformationEnvironmentVariables(model Transformation, position, interval uint64, startTime time.Time) (*[]string, error) {
	return s.templateEngine.GetTransformationEnvironmentVariables(model, position, interval, startTime)
}

func (s *Service) RenderExternal(model External) (string, error) {
	return s.templateEngine.RenderExternal(model)
}
