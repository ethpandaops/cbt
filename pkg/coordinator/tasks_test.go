package coordinator

import (
	"fmt"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The task tracker caps its memory by cleaning up processedTasks once it
// exceeds maxProcessedTaskEntries. The cleanup must keep recently marked
// tasks: forgetting them makes checkCompletedTasks treat still-listed
// completed tasks as new and re-trigger their dependents.
func TestTaskTrackerRetainsRecentTasksAfterCleanup(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	svc, err := NewService(log, nil, &testutil.FakeDAGReader{}, &adminfake.FakeAdminService{}, validation.NewMockValidator())
	require.NoError(t, err)

	sc, ok := svc.(*service)
	require.True(t, ok)

	sc.wg.Add(1)
	go sc.taskTracker()
	t.Cleanup(func() {
		close(sc.done)
		sc.wg.Wait()
	})

	// Overflow the tracker so the cleanup path runs.
	for i := 0; i <= maxProcessedTaskEntries; i++ {
		sc.markTaskProcessed(fmt.Sprintf("task-%d", i))
	}

	lastTask := fmt.Sprintf("task-%d", maxProcessedTaskEntries)
	assert.Eventually(t, func() bool {
		return sc.isTaskProcessed(lastTask)
	}, 2*time.Second, 10*time.Millisecond,
		"most recently marked task was forgotten by the tracker cleanup")
}
