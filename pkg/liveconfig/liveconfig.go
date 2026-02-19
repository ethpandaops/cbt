// Package liveconfig manages live configuration overrides for model handlers.
package liveconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// configSnapshotRestorer is implemented by transformation handlers that support
// snapshot/restore of their overridable config fields. This avoids importing
// concrete handler packages (incremental, scheduled) which would create an
// import cycle.
type configSnapshotRestorer interface {
	SnapshotConfig() any
	RestoreConfig(snapshot any)
}

// ErrSnapshotNotSerializable is returned when a snapshot type does not
// implement the baseConfigSerializer interface.
var ErrSnapshotNotSerializable = errors.New("snapshot does not implement baseConfigSerializer")

// baseConfigSerializer is implemented by snapshot types that can serialize
// themselves to JSON for the management API's base_config response.
type baseConfigSerializer interface {
	ToBaseConfigJSON() (json.RawMessage, error)
}

// overrideApplier is implemented by transformation handlers that accept
// override structs via reflection.
type overrideApplier interface {
	ApplyOverrides(override any)
}

// Applier manages live configuration overrides via Redis.
// It polls a version counter for cheap change detection and applies
// overrides to handler configs. It maintains a set of live-disabled
// model IDs for the scheduler to check.
type Applier struct {
	dag   models.DAGReader
	cache *admin.CacheManager
	log   logrus.FieldLogger

	lastVersion int64

	disabledMu     sync.RWMutex
	disabledModels map[string]bool

	// Base config snapshots for revert when overrides are deleted.
	// Keyed by model ID.
	transformationBases map[string]any // from configSnapshotRestorer.SnapshotConfig()
	externalBases       map[string]*externalBaseConfig
}

// externalBaseConfig stores the baseline config for an external model.
type externalBaseConfig struct {
	Lag                     uint64
	IncrementalScanInterval time.Duration
	FullScanInterval        time.Duration
}

// NewApplier creates a new Applier and takes a snapshot of every
// handler's current config (after yaml overrides).
func NewApplier(
	dag models.DAGReader,
	cache *admin.CacheManager,
	log logrus.FieldLogger,
) *Applier {
	l := &Applier{
		dag:                 dag,
		cache:               cache,
		log:                 log.WithField("component", "live-overrides"),
		disabledModels:      make(map[string]bool),
		transformationBases: make(map[string]any, 16),
		externalBases:       make(map[string]*externalBaseConfig, 8),
	}

	l.snapshotBaseConfigs()

	return l
}

// snapshotBaseConfigs captures the current handler config as the base
// (frontmatter + yaml overrides). Called once at construction time.
func (l *Applier) snapshotBaseConfigs() {
	// Snapshot transformation handlers
	for _, trans := range l.dag.GetTransformationNodes() {
		handler := trans.GetHandler()
		if handler == nil {
			continue
		}

		snapshotter, ok := handler.(configSnapshotRestorer)
		if !ok {
			continue
		}

		l.transformationBases[trans.GetID()] = snapshotter.SnapshotConfig()
	}

	// Snapshot external model configs
	for _, node := range l.dag.GetExternalNodes() {
		ext, ok := node.Model.(models.External)
		if !ok {
			continue
		}

		cfg := ext.GetConfig()
		modelID := ext.GetID()
		base := &externalBaseConfig{
			Lag: cfg.Lag,
		}

		if cfg.Cache != nil {
			base.IncrementalScanInterval = cfg.Cache.IncrementalScanInterval
			base.FullScanInterval = cfg.Cache.FullScanInterval
		}

		l.externalBases[modelID] = base
	}

	l.log.WithFields(logrus.Fields{
		"transformation_count": len(l.transformationBases),
		"external_count":       len(l.externalBases),
	}).Info("Captured base config snapshots for live overrides")
}

// CheckAndApply checks the version counter and applies overrides if changed.
// Returns true if overrides were (re-)applied.
func (l *Applier) CheckAndApply(ctx context.Context) (bool, error) {
	version, err := l.cache.GetConfigOverrideVersion(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check override version: %w", err)
	}

	if version == l.lastVersion {
		return false, nil
	}

	overrides, err := l.cache.GetAllConfigOverrides(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to fetch overrides: %w", err)
	}

	l.applyAll(overrides)
	l.lastVersion = version

	return true, nil
}

// applyAll rebuilds the disabled set and applies all overrides.
// Models NOT in the override set are reverted to base config.
func (l *Applier) applyAll(overrides []admin.ConfigOverride) {
	overriddenIDs := make(map[string]bool, len(overrides))
	newDisabled := make(map[string]bool, len(overrides))

	for _, ov := range overrides {
		overriddenIDs[ov.ModelID] = true

		// Track disabled state
		if ov.Enabled != nil && !*ov.Enabled {
			newDisabled[ov.ModelID] = true
		}

		l.applySingleOverride(&ov)
	}

	// Revert models that no longer have overrides
	l.revertNonOverridden(overriddenIDs)

	// Swap disabled set atomically
	l.disabledMu.Lock()
	l.disabledModels = newDisabled
	l.disabledMu.Unlock()

	l.log.WithFields(logrus.Fields{
		"override_count": len(overrides),
		"disabled_count": len(newDisabled),
	}).Debug("Applied live overrides")
}

// applySingleOverride applies one ConfigOverride to the appropriate handler.
func (l *Applier) applySingleOverride(ov *admin.ConfigOverride) {
	switch ov.ModelType {
	case string(models.ModelTypeTransformation):
		l.applyTransformationOverride(ov)
	case string(models.ModelTypeExternal):
		l.applyExternalOverride(ov)
	default:
		l.log.WithField("model_id", ov.ModelID).Warn("Unknown model type in override")
	}
}

// applyTransformationOverride restores base config then applies the override.
func (l *Applier) applyTransformationOverride(ov *admin.ConfigOverride) {
	trans, err := l.dag.GetTransformationNode(ov.ModelID)
	if err != nil {
		l.log.WithError(err).WithField("model_id", ov.ModelID).Debug("Transformation not found in DAG for override")

		return
	}

	handler := trans.GetHandler()
	if handler == nil {
		return
	}

	// Restore base config first via snapshot/restore interface
	if restorer, ok := handler.(configSnapshotRestorer); ok {
		if snapshot, exists := l.transformationBases[ov.ModelID]; exists {
			restorer.RestoreConfig(snapshot)
		}
	}

	// Parse and apply the override on top
	if len(ov.Override) > 0 {
		var tOv models.TransformationOverride
		if err := json.Unmarshal(ov.Override, &tOv); err != nil {
			l.log.WithError(err).WithField("model_id", ov.ModelID).Warn("Failed to unmarshal transformation override")

			return
		}

		if applier, ok := handler.(overrideApplier); ok {
			applier.ApplyOverrides(&tOv)
		}
	}

	l.log.WithField("model_id", ov.ModelID).Debug("Applied transformation live override")
}

// applyExternalOverride restores base config then applies the override.
func (l *Applier) applyExternalOverride(ov *admin.ConfigOverride) {
	ext, err := l.dag.GetExternalNode(ov.ModelID)
	if err != nil {
		l.log.WithError(err).WithField("model_id", ov.ModelID).Debug("External model not found in DAG for override")

		return
	}

	cfg := ext.GetConfigMutable()

	// Restore base config first
	if base, ok := l.externalBases[ov.ModelID]; ok {
		cfg.Lag = base.Lag
		if cfg.Cache != nil {
			cfg.Cache.IncrementalScanInterval = base.IncrementalScanInterval
			cfg.Cache.FullScanInterval = base.FullScanInterval
		}
	}

	// Apply the override on top
	if len(ov.Override) > 0 {
		var eOv models.ExternalOverride
		if err := json.Unmarshal(ov.Override, &eOv); err != nil {
			l.log.WithError(err).WithField("model_id", ov.ModelID).Warn("Failed to unmarshal external override")

			return
		}

		eOv.ApplyToExternalConfig(cfg)
	}

	l.log.WithField("model_id", ov.ModelID).Debug("Applied external live override")
}

// revertNonOverridden restores base config for models that no longer have overrides.
func (l *Applier) revertNonOverridden(overriddenIDs map[string]bool) {
	// Revert transformations
	for modelID, snapshot := range l.transformationBases {
		if overriddenIDs[modelID] {
			continue
		}

		trans, err := l.dag.GetTransformationNode(modelID)
		if err != nil {
			continue
		}

		handler := trans.GetHandler()
		if handler == nil {
			continue
		}

		if restorer, ok := handler.(configSnapshotRestorer); ok {
			restorer.RestoreConfig(snapshot)
		}
	}

	// Revert external models
	for modelID, base := range l.externalBases {
		if overriddenIDs[modelID] {
			continue
		}

		ext, err := l.dag.GetExternalNode(modelID)
		if err != nil {
			continue
		}

		cfg := ext.GetConfigMutable()
		cfg.Lag = base.Lag

		if cfg.Cache != nil {
			cfg.Cache.IncrementalScanInterval = base.IncrementalScanInterval
			cfg.Cache.FullScanInterval = base.FullScanInterval
		}
	}
}

// GetBaseConfig returns the original (pre-override) config for a model as JSON.
// Returns nil, nil if the model has no base config snapshot.
func (l *Applier) GetBaseConfig(modelID string) (json.RawMessage, error) {
	// Check transformation bases
	if snap, ok := l.transformationBases[modelID]; ok {
		serializer, ok := snap.(baseConfigSerializer)
		if !ok {
			return nil, ErrSnapshotNotSerializable
		}

		return serializer.ToBaseConfigJSON()
	}

	// Check external bases
	if base, ok := l.externalBases[modelID]; ok {
		return json.Marshal(map[string]any{
			"lag": base.Lag,
			"cache": map[string]any{
				"incremental_scan_interval": base.IncrementalScanInterval.String(),
				"full_scan_interval":        base.FullScanInterval.String(),
			},
		})
	}

	return nil, nil
}

// IsModelDisabled returns true if the model is currently live-disabled.
func (l *Applier) IsModelDisabled(modelID string) bool {
	l.disabledMu.RLock()
	defer l.disabledMu.RUnlock()

	return l.disabledModels[modelID]
}
