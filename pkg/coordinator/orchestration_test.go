package coordinator

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestRunConsolidation exercises the success, zero-rows, and failure branches.
func TestRunConsolidation(t *testing.T) {
	consolidated := &testutil.FakeTransformation{ID: "db.consolidated"}
	zeroRows := &testutil.FakeTransformation{ID: "db.zero"}
	failing := &testutil.FakeTransformation{ID: "db.failing"}

	adminSvc := &adminfake.FakeAdminService{
		ConsolidateHistoricalDataFn: func(_ context.Context, modelID string) (uint64, error) {
			switch modelID {
			case "db.consolidated":
				return 42, nil
			case "db.failing":
				return 0, errBoom
			default:
				return 0, nil
			}
		},
	}

	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{consolidated, zeroRows, failing},
	}

	svc := &service{
		log:   logrus.NewEntry(logrus.New()),
		dag:   dag,
		admin: adminSvc,
	}

	assert.NotPanics(t, func() {
		svc.RunConsolidation(context.Background())
	})
}

// TestRunConsolidationEmpty covers the empty-DAG path.
func TestRunConsolidationEmpty(t *testing.T) {
	svc := &service{
		log:   logrus.NewEntry(logrus.New()),
		dag:   &testutil.FakeDAGReader{},
		admin: &adminfake.FakeAdminService{},
	}

	assert.NotPanics(t, func() {
		svc.RunConsolidation(context.Background())
	})
}
