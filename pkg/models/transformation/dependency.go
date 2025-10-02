package transformation

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

var (
	// ErrInvalidDependencyType is returned when dependency has invalid YAML type
	ErrInvalidDependencyType = errors.New("dependency must be a string or array of strings")
	// ErrInvalidDependencyArrayItem is returned when dependency array contains non-string
	ErrInvalidDependencyArrayItem = errors.New("expected string in dependency array")
	// ErrEmptyDependencyGroup is returned when dependency group is empty
	ErrEmptyDependencyGroup = errors.New("dependency group cannot be empty")
)

// Dependency represents a dependency that can be either a string (AND) or an array of strings (OR)
type Dependency struct {
	// IsGroup indicates if this is an OR group (array) or a single dependency (string)
	IsGroup bool
	// SingleDep holds the dependency ID for single dependencies
	SingleDep string
	// GroupDeps holds multiple dependency IDs for OR groups
	GroupDeps []string
}

// UnmarshalYAML implements custom YAML unmarshaling for mixed dependency types
func (d *Dependency) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		// Single string dependency
		if node.Tag != "!!str" && node.Tag != "" && node.Tag != "!" {
			return fmt.Errorf("%w: expected string but got %s", ErrInvalidDependencyType, node.Tag)
		}
		d.IsGroup = false
		d.SingleDep = node.Value
		return nil
	case yaml.SequenceNode:
		// Array of dependencies (OR group)
		d.IsGroup = true
		d.GroupDeps = make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			if item.Kind != yaml.ScalarNode {
				return fmt.Errorf("%w: got %v", ErrInvalidDependencyArrayItem, item.Kind)
			}
			if item.Tag != "!!str" && item.Tag != "" && item.Tag != "!" {
				return fmt.Errorf("%w: expected string but got %s", ErrInvalidDependencyArrayItem, item.Tag)
			}
			d.GroupDeps = append(d.GroupDeps, item.Value)
		}
		if len(d.GroupDeps) == 0 {
			return ErrEmptyDependencyGroup
		}
		return nil
	case yaml.DocumentNode, yaml.MappingNode, yaml.AliasNode:
		return fmt.Errorf("%w: got %v", ErrInvalidDependencyType, node.Kind)
	default:
		return fmt.Errorf("%w: got %v", ErrInvalidDependencyType, node.Kind)
	}
}

// MarshalYAML implements custom YAML marshaling for mixed dependency types
func (d Dependency) MarshalYAML() (interface{}, error) {
	if d.IsGroup {
		return d.GroupDeps, nil
	}
	return d.SingleDep, nil
}

// GetAllDependencies returns all dependency IDs from this dependency (flattened)
func (d *Dependency) GetAllDependencies() []string {
	if d.IsGroup {
		return d.GroupDeps
	}
	return []string{d.SingleDep}
}
