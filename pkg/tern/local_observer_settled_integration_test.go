//go:build integration

package tern

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// recordingTerminalObserver counts progress and terminal notifications so a
// test can prove which ones were delivered, and exactly-once terminal delivery.
type recordingTerminalObserver struct {
	mu        sync.Mutex
	progress  []*storage.Apply
	terminals []*storage.Apply
}

func (o *recordingTerminalObserver) OnProgress(apply *storage.Apply, _ []*storage.Task) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.progress = append(o.progress, apply)
}

func (o *recordingTerminalObserver) progressCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.progress)
}

func (o *recordingTerminalObserver) OnTerminal(apply *storage.Apply, _ []*storage.Task) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.terminals = append(o.terminals, apply)
}

func (o *recordingTerminalObserver) terminalCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.terminals)
}

func (o *recordingTerminalObserver) terminalStates() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	states := make([]string, 0, len(o.terminals))
	for _, apply := range o.terminals {
		states = append(states, apply.State)
	}
	return states
}

func createObserverTestApply(t *testing.T, stor storage.Storage, applyState string, completedAt *time.Time) *storage.Apply {
	t.Helper()
	return createObserverTestApplyWithOperations(t, stor, applyState, completedAt, 1)
}

// observerPollDeadline bounds how long a test waits for a storage-polled
// observer notification.
const observerPollDeadline = 5 * time.Second

// createObserverTestApplyWithOperations creates an apply whose work runs as
// operationCount operations, one per deployment.
func createObserverTestApplyWithOperations(t *testing.T, stor storage.Storage, applyState string, completedAt *time.Time, operationCount int) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-observer-%s-%d", applyState, time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-observer-%s-%d", applyState, now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Environment:     localClientTestEnvironment,
		Engine:          storage.EngineSpirit,
		State:           applyState,
		CompletedAt:     completedAt,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	ops := make([]*storage.ApplyOperation, 0, operationCount)
	for i := range operationCount {
		deployment := apply.Deployment
		if i > 0 {
			deployment = fmt.Sprintf("%s-%d", apply.Deployment, i)
		}
		ops = append(ops, &storage.ApplyOperation{
			Deployment: deployment,
			State:      state.ApplyOperation.Pending,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, nil, ops)
	require.NoError(t, err)
	apply.ID = applyID
	return apply
}

// An apply is claimable the moment its create transaction commits — before
// Apply registers the dispatch's observer — so a fast drive can settle before
// the registration and deliver its terminal notification to nobody. The
// post-registration re-check must hand that missed notification to the late
// observer exactly once and clear the registration; a non-terminal apply must
// keep its observer for the drive's own progress and terminal events.
func TestLocalClient_DeliverTerminalIfSettled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, _ := newTasklessControlClient(t, dsn, stor)

	t.Run("already-terminal apply notifies the late observer exactly once", func(t *testing.T) {
		now := time.Now()
		apply := createObserverTestApply(t, stor, state.Apply.Completed, &now)

		obs := &recordingTerminalObserver{}
		client.SetObserver(apply.ID, obs)
		client.deliverTerminalIfSettled(ctx, apply.ID)

		require.Equal(t, 1, obs.terminalCount(), "the missed terminal notification must be delivered to the late observer")
		assert.Nil(t, client.getObserver(apply.ID), "delivery must clear the registration")

		// A second re-check (or a racing drive-side notify) finds no observer.
		client.deliverTerminalIfSettled(ctx, apply.ID)
		client.notifyTerminalObserver(apply, nil)
		assert.Equal(t, 1, obs.terminalCount(), "terminal delivery must be exactly-once")
	})

	t.Run("non-terminal apply keeps its observer for the drive", func(t *testing.T) {
		apply := createObserverTestApply(t, stor, state.Apply.Pending, nil)

		obs := &recordingTerminalObserver{}
		client.SetObserver(apply.ID, obs)
		client.deliverTerminalIfSettled(ctx, apply.ID)

		assert.Equal(t, 0, obs.terminalCount(), "a queued apply has no terminal notification to deliver")
		assert.NotNil(t, client.getObserver(apply.ID), "the observer must stay registered for the coming drive")
	})
}

// A rollout whose work runs as several operations has no drive that reports
// apply-level progress: each operation's drive holds only its own lease and
// leaves the parent apply to the operator's projection. The observer still
// sees the rollout's progress while it runs and its terminal state exactly
// once when it settles, read back from storage. A single-operation apply's
// drive notifies the observer itself, so storage polling adds nothing for it.
func TestLocalClient_ObserverFollowsMultiOperationRolloutFromStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, _ := newTasklessControlClient(t, dsn, stor)
	client.observerPollIntervalOverride = 20 * time.Millisecond
	t.Cleanup(func() { _ = client.Close() })

	t.Run("multi-operation rollout reports progress, then its terminal state once", func(t *testing.T) {
		apply := createObserverTestApplyWithOperations(t, stor, state.Apply.Running, nil, 3)

		obs := &recordingTerminalObserver{}
		client.SetObserver(apply.ID, obs)

		require.Eventually(t, func() bool { return obs.progressCount() > 0 },
			observerPollDeadline, 10*time.Millisecond, "a running rollout must report progress to its observer")
		assert.Equal(t, 0, obs.terminalCount(), "a running rollout has no terminal state to report")

		completedAt := time.Now()
		swapped, err := stor.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, &completedAt)
		require.NoError(t, err)
		require.True(t, swapped)

		require.Eventually(t, func() bool { return obs.terminalCount() == 1 },
			observerPollDeadline, 10*time.Millisecond, "the settled rollout must report its terminal state")
		assert.Equal(t, []string{state.Apply.Completed}, obs.terminalStates())
		assert.Nil(t, client.getObserver(apply.ID), "terminal delivery must clear the registration")

		// The poller stopped with the terminal delivery; a later re-check finds
		// no observer to notify again.
		client.deliverTerminalIfSettled(ctx, apply.ID)
		assert.Equal(t, 1, obs.terminalCount(), "terminal delivery must be exactly-once")
	})

	t.Run("single-operation apply is left to its drive", func(t *testing.T) {
		apply := createObserverTestApplyWithOperations(t, stor, state.Apply.Running, nil, 1)

		obs := &recordingTerminalObserver{}
		client.SetObserver(apply.ID, obs)

		assert.Never(t, func() bool { return obs.progressCount() > 0 },
			20*client.observerPollIntervalOverride, 10*time.Millisecond, "a single-operation apply's progress comes from its drive")
		assert.NotNil(t, client.getObserver(apply.ID), "the observer must stay registered for the drive")
	})
}
