package models

import (
	"errors"
	"fmt"
	"sync"

	"github.com/heimdalr/dag"
)

var (
	// ErrNonExistentDependency is returned when a model depends on a non-existent model
	ErrNonExistentDependency = errors.New("model depends on non-existent model")
	// ErrInconsistentGraph is returned when the dependency graph is inconsistent
	ErrInconsistentGraph = errors.New("dependency graph is inconsistent: vertex count mismatch")
)

// DependencyGraph manages the dependency graph for models
type DependencyGraph struct {
	dag   *dag.DAG
	mutex sync.RWMutex
}

type NodeType string

const (
	NodeTypeTransformation NodeType = "transformation"
	NodeTypeExternal       NodeType = "external"
)

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

	if err := d.AddTransformationEdges(transformationModels); err != nil {
		return err
	}

	return nil
}

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

func (d *DependencyGraph) AddTransformationEdges(models []Transformation) error {
	for _, model := range models {
		if model != nil {
			for _, depID := range model.GetConfig().Dependencies {
				// Validate dependency exists
				if _, err := d.dag.GetVertex(depID); err != nil {
					return fmt.Errorf("%w: %s depends on %s", ErrNonExistentDependency, model.GetID(), depID)
				}

				// AddEdge returns error if it would create a cycle
				if err := d.dag.AddEdge(depID, model.GetID()); err != nil {
					return fmt.Errorf("invalid dependency %s → %s: %w", depID, model.GetID(), err)
				}
			}
		}
	}

	return nil
}

func (d *DependencyGraph) GetNode(modelID string) (Node, error) {
	vertex, err := d.dag.GetVertex(modelID)
	if err != nil {
		return Node{}, err
	}
	return vertex.(Node), nil
}

func (d *DependencyGraph) GetExternalNode(modelID string) (External, error) {
	vertex, err := d.dag.GetVertex(modelID)
	if err != nil {
		return nil, err
	}

	node := vertex.(Node)

	if node.NodeType == NodeTypeExternal {
		return node.Model.(External), nil
	}

	return nil, fmt.Errorf("model %s is not an external model", modelID)
}

func (d *DependencyGraph) GetTransformationNode(modelID string) (Transformation, error) {
	vertex, err := d.dag.GetVertex(modelID)
	if err != nil {
		return nil, err
	}

	node := vertex.(Node)

	if node.NodeType == NodeTypeTransformation {
		return node.Model.(Transformation), nil
	}

	return nil, fmt.Errorf("model %s is not a transformation model", modelID)
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

func (d *DependencyGraph) GetTransformationNodes() []Transformation {
	vertices := d.dag.GetVertices()

	transformationNodes := make([]Transformation, 0)
	for _, vertex := range vertices {
		node := vertex.(Node)
		if node.NodeType == NodeTypeTransformation {
			model, ok := node.Model.(Transformation)
			if ok {
				transformationNodes = append(transformationNodes, model)
			}
		}
	}

	return transformationNodes
}
