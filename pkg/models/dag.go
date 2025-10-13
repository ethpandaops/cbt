package models

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/heimdalr/dag"
)

var (
	// ErrNonExistentDependency is returned when a model depends on a non-existent model
	ErrNonExistentDependency = errors.New("model depends on non-existent model")
	// ErrNotExternalModel is returned when a model is not an external model
	ErrNotExternalModel = errors.New("model is not an external model")
	// ErrNotTransformationModel is returned when a model is not a transformation model
	ErrNotTransformationModel = errors.New("model is not a transformation model")
	// ErrInvalidNodeType is returned when a node has an invalid type
	ErrInvalidNodeType = errors.New("invalid node type")
	// ErrInvalidExternalModelType is returned when an external model has an invalid type
	ErrInvalidExternalModelType = errors.New("invalid external model type")
	// ErrInvalidTransformationModelType is returned when a transformation model has an invalid type
	ErrInvalidTransformationModelType = errors.New("invalid transformation model type")
	// ErrIncompatibleIntervalType is returned when incremental model cannot depend on model with different interval_type
	ErrIncompatibleIntervalType = errors.New("incremental model cannot depend on model with different interval_type")
)

// DAGReader provides read-only access to the dependency graph (ethPandaOps pattern)
type DAGReader interface {
	// GetNode retrieves a node by its ID
	GetNode(id string) (Node, error)

	// GetTransformationNode retrieves a transformation node by its ID
	GetTransformationNode(id string) (Transformation, error)

	// GetExternalNode retrieves an external node by its ID
	GetExternalNode(id string) (External, error)

	// GetDependencies returns direct dependencies of a node (flattened)
	GetDependencies(id string) []string

	// GetStructuredDependencies returns direct dependencies preserving OR groups
	GetStructuredDependencies(id string) []transformation.Dependency

	// GetDependents returns nodes that depend on the given node
	GetDependents(id string) []string

	// GetAllDependencies returns all transitive dependencies
	GetAllDependencies(id string) []string

	// GetAllDependents returns all transitive dependents
	GetAllDependents(id string) []string

	// GetTransformationNodes returns all transformation nodes
	GetTransformationNodes() []Transformation

	// GetExternalNodes returns all external nodes
	GetExternalNodes() []Node

	// IsPathBetween checks if there's a path between two nodes
	IsPathBetween(from, to string) bool
}

// DependencyGraph manages the dependency graph for models
type DependencyGraph struct {
	dag   *dag.DAG
	mutex sync.RWMutex
}

// NodeType represents the type of a node in the dependency graph
type NodeType string

const (
	// NodeTypeTransformation represents a transformation model node
	NodeTypeTransformation NodeType = "transformation"
	// NodeTypeExternal represents an external model node
	NodeTypeExternal NodeType = "external"
)

// Node represents a node in the dependency graph
type Node struct {
	NodeType NodeType
	Model    interface{}
}

// NewDependencyGraph creates a new dependency graph
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		dag:   dag.NewDAG(),
		mutex: sync.RWMutex{},
	}
}

// BuildGraph builds the dependency graph from model configurations
func (d *DependencyGraph) BuildGraph(transformationModels []Transformation, externalModels []External) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Reset DAG
	d.dag = dag.NewDAG()

	if err := d.AddTransformationModels(transformationModels); err != nil {
		return err
	}

	if err := d.AddExternalModels(externalModels); err != nil {
		return err
	}

	return d.AddTransformationEdges(transformationModels)
}

// AddTransformationModels adds transformation models to the dependency graph
func (d *DependencyGraph) AddTransformationModels(models []Transformation) error {
	for _, model := range models {
		if model != nil {
			node := Node{
				NodeType: NodeTypeTransformation,
				Model:    model,
			}

			if err := d.dag.AddVertexByID(model.GetID(), node); err != nil {
				return fmt.Errorf("failed to add vertex %s: %w", model.GetID(), err)
			}
		}
	}

	return nil
}

// AddExternalModels adds external models to the dependency graph
func (d *DependencyGraph) AddExternalModels(models []External) error {
	for _, model := range models {
		if model != nil {
			node := Node{
				NodeType: NodeTypeExternal,
				Model:    model,
			}

			if err := d.dag.AddVertexByID(model.GetID(), node); err != nil {
				return fmt.Errorf("failed to add vertex %s: %w", model.GetID(), err)
			}
		}
	}

	return nil
}

// AddTransformationEdges adds edges between transformation models based on dependencies
func (d *DependencyGraph) AddTransformationEdges(models []Transformation) error {
	for _, model := range models {
		if model == nil {
			continue
		}

		if err := d.addModelDependencies(model); err != nil {
			return err
		}
	}
	return nil
}

// addModelDependencies adds dependency edges for a single model
func (d *DependencyGraph) addModelDependencies(model Transformation) error {
	handler := model.GetHandler()
	if handler == nil {
		return nil
	}

	// Check if handler implements dependency provider interface
	depProvider, ok := handler.(interface{ GetFlattenedDependencies() []string })
	if !ok {
		return nil
	}

	allDeps := depProvider.GetFlattenedDependencies()
	for _, depID := range allDeps {
		// Validate dependency exists
		depVertex, err := d.dag.GetVertex(depID)
		if err != nil {
			return fmt.Errorf("%w: %s depends on %s", ErrNonExistentDependency, model.GetID(), depID)
		}

		// Validate interval type compatibility for incremental models
		if err := d.validateIntervalTypeCompatibility(model, depVertex, depID); err != nil {
			return err
		}

		// AddEdge returns error if it would create a cycle
		if err := d.dag.AddEdge(depID, model.GetID()); err != nil {
			return fmt.Errorf("invalid dependency %s → %s: %w", depID, model.GetID(), err)
		}
	}
	return nil
}

// validateIntervalTypeCompatibility ensures incremental models only depend on models with matching interval_type
func (d *DependencyGraph) validateIntervalTypeCompatibility(model Transformation, depVertex any, depID string) error {
	modelIntervalType := d.getModelIntervalType(model)
	if modelIntervalType == "" {
		return nil // Not an incremental model or doesn't support interval types
	}

	depIntervalType := d.getDependencyIntervalType(depVertex)
	if depIntervalType == "" {
		return nil // Scheduled dependency or validation will be caught by config validation
	}

	if modelIntervalType != depIntervalType {
		return fmt.Errorf("%w: %s (interval_type=%s) cannot depend on %s (interval_type=%s)",
			ErrIncompatibleIntervalType,
			model.GetID(),
			modelIntervalType,
			depID,
			depIntervalType,
		)
	}

	return nil
}

// getModelIntervalType extracts interval type from a transformation model
func (d *DependencyGraph) getModelIntervalType(model Transformation) string {
	handler := model.GetHandler()
	if handler == nil || !handler.ShouldTrackPosition() {
		return "" // Not an incremental transformation
	}

	type intervalTypeProvider interface{ GetIntervalType() string }
	if provider, ok := handler.(intervalTypeProvider); ok {
		return provider.GetIntervalType()
	}

	return ""
}

// getDependencyIntervalType extracts interval type from a dependency vertex
func (d *DependencyGraph) getDependencyIntervalType(depVertex any) string {
	node, ok := depVertex.(Node)
	if !ok {
		return ""
	}

	switch node.NodeType {
	case NodeTypeExternal:
		return d.getExternalIntervalType(node.Model)
	case NodeTypeTransformation:
		return d.getTransformationIntervalType(node.Model)
	}

	return ""
}

// getExternalIntervalType extracts interval type from an external model
func (d *DependencyGraph) getExternalIntervalType(model any) string {
	ext, ok := model.(External)
	if !ok {
		return ""
	}

	type intervalTypeProvider interface{ GetIntervalType() string }
	if provider, ok := ext.(intervalTypeProvider); ok {
		return provider.GetIntervalType()
	}

	return ""
}

// getTransformationIntervalType extracts interval type from a transformation model
func (d *DependencyGraph) getTransformationIntervalType(model any) string {
	trans, ok := model.(Transformation)
	if !ok {
		return ""
	}

	handler := trans.GetHandler()
	if handler == nil || !handler.ShouldTrackPosition() {
		return "" // Scheduled transformation - no interval type restrictions
	}

	type intervalTypeProvider interface{ GetIntervalType() string }
	if provider, ok := handler.(intervalTypeProvider); ok {
		return provider.GetIntervalType()
	}

	return ""
}

// GetNode retrieves a node from the dependency graph by model ID
func (d *DependencyGraph) GetNode(modelID string) (Node, error) {
	vertex, err := d.dag.GetVertex(modelID)
	if err != nil {
		return Node{}, err
	}
	node, ok := vertex.(Node)
	if !ok {
		return Node{}, fmt.Errorf("%w for model %s", ErrInvalidNodeType, modelID)
	}
	return node, nil
}

// GetExternalNode retrieves an external model node from the dependency graph
func (d *DependencyGraph) GetExternalNode(modelID string) (External, error) {
	vertex, err := d.dag.GetVertex(modelID)
	if err != nil {
		return nil, err
	}

	node, ok := vertex.(Node)
	if !ok {
		return nil, fmt.Errorf("%w for model %s", ErrInvalidNodeType, modelID)
	}

	if node.NodeType == NodeTypeExternal {
		external, ok := node.Model.(External)
		if !ok {
			return nil, fmt.Errorf("%w for %s", ErrInvalidExternalModelType, modelID)
		}
		return external, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrNotExternalModel, modelID)
}

// GetTransformationNode retrieves a transformation model node from the dependency graph
func (d *DependencyGraph) GetTransformationNode(modelID string) (Transformation, error) {
	vertex, err := d.dag.GetVertex(modelID)
	if err != nil {
		return nil, err
	}

	node, ok := vertex.(Node)
	if !ok {
		return nil, fmt.Errorf("%w for model %s", ErrInvalidNodeType, modelID)
	}

	if node.NodeType == NodeTypeTransformation {
		trans, ok := node.Model.(Transformation)
		if !ok {
			return nil, fmt.Errorf("%w for %s", ErrInvalidTransformationModelType, modelID)
		}
		return trans, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrNotTransformationModel, modelID)
}

// GetDependents returns the direct dependents of a model
func (d *DependencyGraph) GetDependents(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	children, err := d.dag.GetChildren(modelID)
	if err != nil {
		return nil
	}

	dependents := make([]string, 0, len(children))
	for id := range children {
		dependents = append(dependents, id)
	}

	return dependents
}

// GetDependencies returns the direct dependencies of a model
// Note: This returns all dependencies flattened (including those in OR groups)
func (d *DependencyGraph) GetDependencies(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	parents, err := d.dag.GetParents(modelID)
	if err != nil {
		return nil
	}

	dependencies := make([]string, 0, len(parents))
	for id := range parents {
		dependencies = append(dependencies, id)
	}

	return dependencies
}

// GetStructuredDependencies returns the direct dependencies preserving OR groups
func (d *DependencyGraph) GetStructuredDependencies(modelID string) []transformation.Dependency {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Get the transformation node
	node, err := d.GetNode(modelID)
	if err != nil {
		return nil
	}

	// Only transformation models have structured dependencies
	if node.NodeType != NodeTypeTransformation {
		return nil
	}

	transformModel, ok := node.Model.(Transformation)
	if !ok {
		return nil
	}

	// Get handler
	handler := transformModel.GetHandler()
	if handler == nil {
		return nil
	}

	// Check if handler provides GetDependencies method
	type depProvider interface {
		GetDependencies() []transformation.Dependency
	}

	provider, ok := handler.(depProvider)
	if !ok {
		return nil
	}

	return provider.GetDependencies()
}

// GetAllDependents returns all dependents (recursive) of a model
func (d *DependencyGraph) GetAllDependents(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	descendants, err := d.dag.GetDescendants(modelID)
	if err != nil {
		return nil
	}

	allDependents := make([]string, 0, len(descendants))
	for id := range descendants {
		allDependents = append(allDependents, id)
	}

	return allDependents
}

// GetAllDependencies returns all dependencies (recursive) of a model
func (d *DependencyGraph) GetAllDependencies(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	ancestors, err := d.dag.GetAncestors(modelID)
	if err != nil {
		return nil
	}

	allDependencies := make([]string, 0, len(ancestors))
	for id := range ancestors {
		allDependencies = append(allDependencies, id)
	}

	return allDependencies
}

// IsPathBetween checks if there's a path from one model to another
func (d *DependencyGraph) IsPathBetween(fromModelID, toModelID string) bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Check if toModelID is in the descendants of fromModelID
	descendants, err := d.dag.GetDescendants(fromModelID)
	if err != nil {
		return false
	}

	_, exists := descendants[toModelID]
	return exists
}

// GetTransformationNodes returns all transformation nodes from the dependency graph
func (d *DependencyGraph) GetTransformationNodes() []Transformation {
	vertices := d.dag.GetVertices()

	transformationNodes := make([]Transformation, 0, len(vertices))
	for _, vertex := range vertices {
		node, ok := vertex.(Node)
		if !ok {
			continue
		}
		if node.NodeType == NodeTypeTransformation {
			model, ok := node.Model.(Transformation)
			if ok {
				transformationNodes = append(transformationNodes, model)
			}
		}
	}

	return transformationNodes
}

// GetExternalNodes returns all external nodes from the dependency graph
func (d *DependencyGraph) GetExternalNodes() []Node {
	vertices := d.dag.GetVertices()

	externalNodes := make([]Node, 0, len(vertices))
	for _, vertex := range vertices {
		node, ok := vertex.(Node)
		if !ok {
			continue
		}
		if node.NodeType == NodeTypeExternal {
			externalNodes = append(externalNodes, node)
		}
	}

	return externalNodes
}

// Ensure DependencyGraph implements DAGReader
var _ DAGReader = (*DependencyGraph)(nil)
