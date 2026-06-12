package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/stretchr/testify/assert"
)

// taskIDs returns the set of scheduled-task IDs as a lookup map.
func taskIDs(tasks []scheduledTask) map[string]scheduledTask {
	out := make(map[string]scheduledTask, len(tasks))
	for _, task := range tasks {
		out[task.ID] = task
	}

	return out
}

func TestBuildSystemScheduledTasks(t *testing.T) {
	tests := []struct {
		name          string
		consolidation string
		wantCount     int
	}{
		{
			name:          "valid consolidation schedule",
			consolidation: "@every 10m",
			wantCount:     1,
		},
		{
			name:          "empty consolidation schedule",
			consolidation: "",
			wantCount:     0,
		},
		{
			name:          "invalid consolidation schedule is logged and skipped",
			consolidation: "not-a-schedule",
			wantCount:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService(&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{})
			svc.cfg.Consolidation = tt.consolidation

			tasks := svc.buildSystemScheduledTasks()
			assert.Len(t, tasks, tt.wantCount)

			if tt.wantCount == 1 {
				assert.Equal(t, ConsolidationTaskType, tasks[0].ID)
				assert.Equal(t, 10*time.Minute, tasks[0].Interval)
			}
		})
	}
}

func TestBuildExternalScheduledTasks(t *testing.T) {
	tests := []struct {
		name      string
		externals []models.Node
		wantIDs   []string
	}{
		{
			name:      "no externals",
			externals: nil,
			wantIDs:   nil,
		},
		{
			name: "non-external node is skipped",
			externals: []models.Node{
				{NodeType: models.NodeTypeExternal, Model: "not-an-external"},
			},
			wantIDs: nil,
		},
		{
			name: "external without cache config is skipped",
			externals: []models.Node{
				{NodeType: models.NodeTypeExternal, Model: &testutil.FakeExternal{
					ID:     "ext.nocache",
					Config: external.Config{Cache: nil},
				}},
			},
			wantIDs: nil,
		},
		{
			name: "external with both intervals",
			externals: []models.Node{
				{NodeType: models.NodeTypeExternal, Model: &testutil.FakeExternal{
					ID: "ext.both",
					Config: external.Config{Cache: &external.CacheConfig{
						IncrementalScanInterval: time.Minute,
						FullScanInterval:        5 * time.Minute,
					}},
				}},
			},
			wantIDs: []string{"external:ext.both:incremental", "external:ext.both:full"},
		},
		{
			name: "external with only incremental interval",
			externals: []models.Node{
				{NodeType: models.NodeTypeExternal, Model: &testutil.FakeExternal{
					ID: "ext.inc",
					Config: external.Config{Cache: &external.CacheConfig{
						IncrementalScanInterval: time.Minute,
					}},
				}},
			},
			wantIDs: []string{"external:ext.inc:incremental"},
		},
		{
			name: "external with only full interval",
			externals: []models.Node{
				{NodeType: models.NodeTypeExternal, Model: &testutil.FakeExternal{
					ID: "ext.full",
					Config: external.Config{Cache: &external.CacheConfig{
						FullScanInterval: 5 * time.Minute,
					}},
				}},
			},
			wantIDs: []string{"external:ext.full:full"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := &testutil.FakeDAGReader{Externals: tt.externals}
			svc := newTestService(dag, &coordinatorfake.FakeCoordinator{})

			tasks := svc.buildExternalScheduledTasks()
			ids := taskIDs(tasks)

			assert.Len(t, tasks, len(tt.wantIDs))
			for _, id := range tt.wantIDs {
				_, ok := ids[id]
				assert.True(t, ok, "expected task %s", id)
			}
		})
	}
}

func TestBuildTransformationScheduledTasks(t *testing.T) {
	tests := []struct {
		name            string
		transformations []models.Transformation
		wantIDs         []string
	}{
		{
			name:            "no transformations",
			transformations: nil,
			wantIDs:         nil,
		},
		{
			name: "scheduled transformation with valid schedule",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:     "sched.model",
					Config: transformation.Config{Type: TransformationTypeScheduled},
					Handler: &testutil.FakeHandler{
						HandlerType: transformation.TypeScheduled,
						Schedule:    "@every 1m",
					},
				},
			},
			wantIDs: []string{"transformation:sched.model:scheduled"},
		},
		{
			name: "scheduled transformation with handler that is not a scheduleProvider",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:      "sched.noprovider",
					Config:  transformation.Config{Type: TransformationTypeScheduled},
					Handler: notAScheduleProvider{},
				},
			},
			wantIDs: nil,
		},
		{
			name: "scheduled transformation with empty schedule",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:     "sched.empty",
					Config: transformation.Config{Type: TransformationTypeScheduled},
					Handler: &testutil.FakeHandler{
						HandlerType: transformation.TypeScheduled,
						Schedule:    "",
					},
				},
			},
			wantIDs: nil,
		},
		{
			name: "scheduled transformation with invalid schedule",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:     "sched.invalid",
					Config: transformation.Config{Type: TransformationTypeScheduled},
					Handler: &testutil.FakeHandler{
						HandlerType: transformation.TypeScheduled,
						Schedule:    "not-a-schedule",
					},
				},
			},
			wantIDs: nil,
		},
		{
			name: "incremental transformation with nil handler",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:      "inc.nilhandler",
					Config:  transformation.Config{Type: transformation.TypeIncremental},
					Handler: nil,
				},
			},
			wantIDs: nil,
		},
		{
			name: "incremental transformation with non-incremental handler config",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:      "inc.badcfg",
					Config:  transformation.Config{Type: transformation.TypeIncremental},
					Handler: &testutil.FakeHandler{HandlerConfig: "not-incremental"},
				},
			},
			wantIDs: nil,
		},
		{
			name: "incremental transformation with nil schedules",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:     "inc.nilsched",
					Config: transformation.Config{Type: transformation.TypeIncremental},
					Handler: &testutil.FakeHandler{
						HandlerConfig: &incremental.Config{Type: transformation.TypeIncremental, Schedules: nil},
					},
				},
			},
			wantIDs: nil,
		},
		{
			name: "incremental transformation with forward and backfill schedules",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:     "inc.both",
					Config: transformation.Config{Type: transformation.TypeIncremental},
					Handler: &testutil.FakeHandler{
						HandlerConfig: &incremental.Config{
							Type:      transformation.TypeIncremental,
							Schedules: &incremental.SchedulesConfig{ForwardFill: "@every 30s", Backfill: "@every 1m"},
						},
					},
				},
			},
			wantIDs: []string{"transformation:inc.both:forward", "transformation:inc.both:back"},
		},
		{
			name: "incremental transformation with invalid forward and backfill schedules",
			transformations: []models.Transformation{
				&testutil.FakeTransformation{
					ID:     "inc.badsched",
					Config: transformation.Config{Type: transformation.TypeIncremental},
					Handler: &testutil.FakeHandler{
						HandlerConfig: &incremental.Config{
							Type:      transformation.TypeIncremental,
							Schedules: &incremental.SchedulesConfig{ForwardFill: "bad", Backfill: "also-bad"},
						},
					},
				},
			},
			wantIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := &testutil.FakeDAGReader{Transformations: tt.transformations}
			svc := newTestService(dag, &coordinatorfake.FakeCoordinator{})

			tasks := svc.buildTransformationScheduledTasks()
			ids := taskIDs(tasks)

			assert.Len(t, tasks, len(tt.wantIDs))
			for _, id := range tt.wantIDs {
				_, ok := ids[id]
				assert.True(t, ok, "expected task %s", id)
			}
		})
	}
}

// notAScheduleProvider is a handler whose only purpose is to NOT implement the
// scheduleProvider interface (no GetSchedule method), forcing the type assertion
// in buildTransformationScheduledTasks to fail.
type notAScheduleProvider struct{}

func (notAScheduleProvider) Type() transformation.Type { return transformation.TypeScheduled }
func (notAScheduleProvider) Config() any               { return nil }
func (notAScheduleProvider) Validate() error           { return nil }
func (notAScheduleProvider) ShouldTrackPosition() bool { return false }
func (notAScheduleProvider) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}

func (notAScheduleProvider) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (notAScheduleProvider) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

var _ transformation.Handler = notAScheduleProvider{}
