package transformation

import (
	"bytes"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"gopkg.in/yaml.v3"
)

// DecodeStrict unmarshals YAML data into a value of type T using strict
// decoding (KnownFields), so unknown keys are rejected. This is shared by the
// transformation type handlers to enforce the same parsing semantics.
func DecodeStrict[T any](data []byte) (*T, error) {
	var config T

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

// CopyTags returns a shallow copy of the provided tags slice, preserving a nil
// input as a nil output.
func CopyTags(tags []string) []string {
	if tags == nil {
		return nil
	}

	cp := make([]string, len(tags))
	copy(cp, tags)

	return cp
}

// DependencyBase provides the dependency and identity behavior shared by the
// transformation type handlers. The concrete handlers embed it and wire it to
// their own config fields at construction time via accessor functions, since
// the handler config types differ.
type DependencyBase struct {
	adminTable AdminTable

	identity     func() (database, table string)
	tags         func() []string
	dependencies func() []Dependency
	original     func() []Dependency
	setOriginal  func([]Dependency)
}

// NewDependencyBase wires a DependencyBase to a handler's config accessors.
func NewDependencyBase(
	adminTable AdminTable,
	identity func() (database, table string),
	tags func() []string,
	dependencies func() []Dependency,
	original func() []Dependency,
	setOriginal func([]Dependency),
) DependencyBase {
	return DependencyBase{
		adminTable:   adminTable,
		identity:     identity,
		tags:         tags,
		dependencies: dependencies,
		original:     original,
		setOriginal:  setOriginal,
	}
}

// GetID returns the unique identifier for the transformation model.
func (b *DependencyBase) GetID() string {
	database, table := b.identity()

	return modelid.Format(database, table)
}

// GetTags returns the tags for this transformation.
func (b *DependencyBase) GetTags() []string {
	return b.tags()
}

// GetDependencies returns the dependencies (after placeholder substitution).
func (b *DependencyBase) GetDependencies() []Dependency {
	return b.dependencies()
}

// GetOriginalDependencies returns the original dependencies before placeholder substitution.
func (b *DependencyBase) GetOriginalDependencies() []Dependency {
	return b.original()
}

// GetFlattenedDependencies returns all dependencies as a flat string array.
func (b *DependencyBase) GetFlattenedDependencies() []string {
	deps := b.dependencies()

	result := make([]string, 0, len(deps))
	for i := range deps {
		result = append(result, deps[i].GetAllDependencies()...)
	}

	return result
}

// SubstituteDependencyPlaceholders replaces {{external}} and {{transformation}} placeholders.
func (b *DependencyBase) SubstituteDependencyPlaceholders(externalDefaultDB, transformationDefaultDB string) {
	b.setOriginal(SubstituteDependencyPlaceholders(
		b.dependencies(),
		externalDefaultDB,
		transformationDefaultDB,
	))
}

// GetAdminTable returns the admin table configuration.
func (b *DependencyBase) GetAdminTable() AdminTable {
	return b.adminTable
}
