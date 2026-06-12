package testutil

import (
	"time"

	"github.com/ethpandaops/cbt/pkg/models"
)

// FakeModelsService is a configurable fake implementing models.Service for tests.
//
// The zero value returns an empty DAG and empty render results. Set DAG to control
// GetDAG; when DAG is nil and Transformations is set, GetDAG returns a FakeDAGReader
// backed by Transformations. Render results and errors are driven by the matching fields.
type FakeModelsService struct {
	DAG             models.DAGReader
	Transformations []models.Transformation

	RenderedSQL string
	RenderErr   error
	EnvVars     *[]string

	// RenderExternalFn overrides RenderExternal when set.
	RenderExternalFn func(model models.External, cacheState map[string]any) (string, error)
}

// Start is a no-op.
func (f *FakeModelsService) Start() error { return nil }

// Stop is a no-op.
func (f *FakeModelsService) Stop() error { return nil }

// GetDAG returns the configured DAG, or a FakeDAGReader backed by Transformations.
func (f *FakeModelsService) GetDAG() models.DAGReader {
	if f.DAG != nil {
		return f.DAG
	}

	return &FakeDAGReader{Transformations: f.Transformations}
}

// RenderTransformation returns RenderedSQL, or RenderErr when set.
func (f *FakeModelsService) RenderTransformation(_ models.Transformation, _, _ uint64, _ time.Time) (string, error) {
	if f.RenderErr != nil {
		return "", f.RenderErr
	}

	return f.RenderedSQL, nil
}

// RenderExternal returns an empty string by default.
func (f *FakeModelsService) RenderExternal(model models.External, cacheState map[string]any) (string, error) {
	if f.RenderExternalFn != nil {
		return f.RenderExternalFn(model, cacheState)
	}

	return "", nil
}

// GetTransformationEnvironmentVariables returns EnvVars when set, otherwise an empty slice.
func (f *FakeModelsService) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	if f.EnvVars != nil {
		return f.EnvVars, nil
	}

	vars := []string{}

	return &vars, nil
}

var _ models.Service = (*FakeModelsService)(nil)
