package testutil

import (
	"errors"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// ErrFakeNodeNotFound is the default error returned by FakeDAGReader lookups that miss.
var ErrFakeNodeNotFound = errors.New("node not found")

// FakeDAGReader is a configurable fake implementing models.DAGReader for tests.
//
// The zero value returns empty results from every method. It supports two lookup styles
// that the hand-rolled copies relied on:
//   - Slice style: set Transformations / Externals; GetTransformationNode linear-searches
//     Transformations by ID and, on a miss, calls TransformationFallbackFn (if set) so a
//     default node can be synthesized the way some callers expect.
//   - Map style: set TransformationByID / ExternalByID / NodeByID for direct keyed lookups
//     that return ErrFakeNodeNotFound (or the configured *Err) on a miss.
//
// Dependency relationships are read from the Dependencies / Dependents / AllDependencies /
// AllDependents / PathBetween maps. Inject lookup errors via the *Err fields, or force a
// miss on GetTransformationNode with NodeNotFound.
type FakeDAGReader struct {
	Transformations []models.Transformation
	Externals       []models.Node

	TransformationByID map[string]models.Transformation
	ExternalByID       map[string]models.External
	NodeByID           map[string]models.Node

	Dependencies    map[string][]string
	Dependents      map[string][]string
	AllDependencies map[string][]string
	AllDependents   map[string][]string
	PathBetween     map[string]map[string]bool

	// NotFoundErr is returned by GetTransformationNode when NodeNotFound is set; defaults
	// to ErrFakeNodeNotFound.
	NodeNotFound bool
	NotFoundErr  error

	// Single external-node conveniences used by the executor tests.
	ExternalNode    models.External
	ExternalNodeErr error

	// Error injection for keyed lookups.
	GetTransformationErr error
	GetExternalErr       error
	GetNodeErr           error

	// TransformationFallbackFn supplies a default node when a slice/map lookup misses and no
	// error is configured. When nil, GetTransformationNode returns a not-found error on a miss.
	TransformationFallbackFn func(id string) models.Transformation
}

func (f *FakeDAGReader) notFoundErr() error {
	if f.NotFoundErr != nil {
		return f.NotFoundErr
	}

	return ErrFakeNodeNotFound
}

// GetNode returns the node stored under id, or an error/empty node.
func (f *FakeDAGReader) GetNode(id string) (models.Node, error) {
	if f.GetNodeErr != nil {
		return models.Node{}, f.GetNodeErr
	}

	if node, ok := f.NodeByID[id]; ok {
		return node, nil
	}

	if f.NodeByID != nil {
		return models.Node{}, f.notFoundErr()
	}

	return models.Node{}, nil
}

// GetTransformationNode returns the transformation registered under id.
//
// Resolution order: configured GetTransformationErr -> NodeNotFound flag -> TransformationByID
// map -> Transformations slice (linear search) -> a transformation stored in NodeByID[id]
// -> TransformationFallbackFn -> not-found error.
func (f *FakeDAGReader) GetTransformationNode(id string) (models.Transformation, error) {
	if f.GetTransformationErr != nil {
		return nil, f.GetTransformationErr
	}

	if f.NodeNotFound {
		return nil, f.notFoundErr()
	}

	if node, ok := f.TransformationByID[id]; ok {
		return node, nil
	}

	for _, t := range f.Transformations {
		if t.GetID() == id {
			return t, nil
		}
	}

	if node, ok := f.NodeByID[id]; ok {
		if trans, ok := node.Model.(models.Transformation); ok {
			return trans, nil
		}
	}

	if f.TransformationFallbackFn != nil {
		return f.TransformationFallbackFn(id), nil
	}

	return nil, f.notFoundErr()
}

// GetExternalNode returns the external registered under id.
//
// Resolution order: configured GetExternalErr/ExternalNodeErr -> ExternalByID map ->
// single ExternalNode convenience -> not-found error.
func (f *FakeDAGReader) GetExternalNode(id string) (models.External, error) {
	if f.GetExternalErr != nil {
		return nil, f.GetExternalErr
	}

	if f.ExternalNodeErr != nil {
		return nil, f.ExternalNodeErr
	}

	if node, ok := f.ExternalByID[id]; ok {
		return node, nil
	}

	if f.ExternalNode != nil {
		return f.ExternalNode, nil
	}

	if f.ExternalByID != nil {
		return nil, f.notFoundErr()
	}

	return nil, nil
}

// GetDependencies returns the configured direct dependencies of id.
func (f *FakeDAGReader) GetDependencies(id string) []string {
	if deps, ok := f.Dependencies[id]; ok {
		return deps
	}

	return []string{}
}

// GetStructuredDependencies returns nil; structured dependencies are not modeled by the fake.
func (f *FakeDAGReader) GetStructuredDependencies(_ string) []transformation.Dependency {
	return nil
}

// GetDependents returns the configured direct dependents of id.
func (f *FakeDAGReader) GetDependents(id string) []string {
	if deps, ok := f.Dependents[id]; ok {
		return deps
	}

	return []string{}
}

// GetAllDependencies returns the configured transitive dependencies of id.
func (f *FakeDAGReader) GetAllDependencies(id string) []string {
	if deps, ok := f.AllDependencies[id]; ok {
		return deps
	}

	return []string{}
}

// GetAllDependents returns the configured transitive dependents of id.
func (f *FakeDAGReader) GetAllDependents(id string) []string {
	if deps, ok := f.AllDependents[id]; ok {
		return deps
	}

	return []string{}
}

// GetTransformationNodes returns the configured Transformations.
func (f *FakeDAGReader) GetTransformationNodes() []models.Transformation {
	return f.Transformations
}

// GetExternalNodes returns the configured Externals.
func (f *FakeDAGReader) GetExternalNodes() []models.Node {
	if f.Externals != nil {
		return f.Externals
	}

	return []models.Node{}
}

// IsPathBetween reports whether a path between from and to was configured in PathBetween.
func (f *FakeDAGReader) IsPathBetween(from, to string) bool {
	if paths, ok := f.PathBetween[from]; ok {
		return paths[to]
	}

	return false
}

var _ models.DAGReader = (*FakeDAGReader)(nil)
