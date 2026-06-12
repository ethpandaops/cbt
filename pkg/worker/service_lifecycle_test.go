package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errRunServer is returned by the stubbed server runner to exercise the error branch.
var errRunServer = errors.New("server run failed")

// minimalHandler implements transformation.Handler but NOT the tagsProvider interface
// (no GetTags method), so it exercises the type-assertion-miss branch in
// filteredTransformations.
type minimalHandler struct{}

func (minimalHandler) Type() transformation.Type { return transformation.TypeIncremental }
func (minimalHandler) Config() any               { return nil }
func (minimalHandler) Validate() error           { return nil }
func (minimalHandler) ShouldTrackPosition() bool { return true }
func (minimalHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (minimalHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return map[string]any{}
}

func (minimalHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

// newWorkerService builds a worker service backed by miniredis with the given models.
func newWorkerService(t *testing.T, modelsService models.Service, cfg *Config) *service {
	t.Helper()

	mr := miniredis.RunT(t)
	redisOpt := &redis.Options{Addr: mr.Addr()}

	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	svc, err := NewService(log, cfg, &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		modelsService, redisOpt, validation.NewMockValidator())
	require.NoError(t, err)

	return svc.(*service)
}

// TestService_StartStop runs the full Start/Stop lifecycle with a stubbed server runner
// that returns immediately, exercising the server-run error log branch and the clean
// shutdown path of Stop.
func TestService_StartStop(t *testing.T) {
	original := runServer
	t.Cleanup(func() { runServer = original })

	done := make(chan struct{})
	runServer = func(_ *asynq.Server, _ *asynq.ServeMux) error {
		close(done)
		return errRunServer // exercise the error-log branch in the goroutine
	}

	external1 := &testutil.FakeExternal{
		ID:     "src.external",
		Config: external.Config{Database: "src", Table: "external"},
	}
	trans := &testutil.FakeTransformation{
		ID:   "db.model",
		Tags: []string{"tag1"},
	}

	modelsService := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{
			Transformations: []models.Transformation{trans},
			Externals:       []models.Node{{Model: external1}},
		},
	}

	cfg := &Config{Concurrency: 1, ShutdownTimeout: 5}
	svc := newWorkerService(t, modelsService, cfg)

	require.NoError(t, svc.Start(context.Background()))
	require.NotNil(t, svc.server)

	// Wait for the stubbed runner to execute so the error branch is hit.
	<-done

	// The goroutine returns promptly, so Stop takes the clean (non-timeout) path.
	require.NoError(t, svc.Stop())
}

// TestService_Stop_Timeout covers the shutdown-timeout branch of Stop. asynq's
// Server.Run blocks on OS signals and is not unblocked by Shutdown(), so the background
// goroutine outlives Stop and the bounded timeout fires.
func TestService_Stop_Timeout(t *testing.T) {
	trans := &testutil.FakeTransformation{ID: "db.model", Tags: []string{"tag1"}}
	modelsService := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{Transformations: []models.Transformation{trans}},
	}

	cfg := &Config{Concurrency: 1, ShutdownTimeout: 1}
	svc := newWorkerService(t, modelsService, cfg)

	require.NoError(t, svc.Start(context.Background()))
	time.Sleep(50 * time.Millisecond)

	err := svc.Stop()
	require.ErrorIs(t, err, ErrShutdownTimeout)
}

// TestService_Stop_NoServer covers Stop when Start was never called (server is nil).
func TestService_Stop_NoServer(t *testing.T) {
	modelsService := &testutil.FakeModelsService{Transformations: []models.Transformation{}}
	cfg := &Config{Concurrency: 1, ShutdownTimeout: 5}
	svc := newWorkerService(t, modelsService, cfg)

	require.NoError(t, svc.Stop())
}

// TestService_Start_ExternalNodeNotExternalModel covers the branch where an external
// DAG node's Model does not satisfy models.External (the type assertion fails).
func TestService_Start_ExternalNodeNotExternalModel(t *testing.T) {
	original := runServer
	t.Cleanup(func() { runServer = original })
	runServer = func(_ *asynq.Server, _ *asynq.ServeMux) error { return nil }

	// A node whose Model is a Transformation, not an External, so the
	// `external, ok := node.Model.(models.External)` assertion is false.
	notExternal := &testutil.FakeTransformation{ID: "not.external"}

	modelsService := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{
			Transformations: []models.Transformation{},
			Externals:       []models.Node{{Model: notExternal}},
		},
	}

	cfg := &Config{Concurrency: 1, ShutdownTimeout: 5}
	svc := newWorkerService(t, modelsService, cfg)

	require.NoError(t, svc.Start(context.Background()))
	require.NoError(t, svc.Stop())
}

// TestFilteredTransformations_NilHandlerAndNoTagsProvider covers the two skip branches:
// a transformation whose handler is nil, and one whose handler does not implement
// the tagsProvider interface.
func TestFilteredTransformations_NilHandlerAndNoTagsProvider(t *testing.T) {
	// nilHandlerTrans.GetHandler() returns nil because both Handler and Tags are nil.
	nilHandlerTrans := &testutil.FakeTransformation{ID: "model.nil_handler"}

	// noTagsTrans has a handler that does not implement GetTags.
	noTagsTrans := &testutil.FakeTransformation{
		ID:      "model.no_tags_provider",
		Handler: minimalHandler{},
	}

	// matching has tags so it is selected, confirming filtering still works.
	matching := &testutil.FakeTransformation{
		ID:   "model.match",
		Tags: []string{"tag1"},
	}

	modelsService := &testutil.FakeModelsService{
		Transformations: []models.Transformation{nilHandlerTrans, noTagsTrans, matching},
	}

	result := filteredTransformations(modelsService, []string{"tag1"})

	require.Len(t, result, 1)
	assert.Equal(t, "model.match", result[0].GetID())
}
