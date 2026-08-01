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

// recordingTerminalObserver counts terminal notifications so a test can prove
// exactly-once delivery.
type recordingTerminalObserver struct {
	mu        sync.Mutex
	terminals []*storage.Apply
}

func (o *recordingTerminalObserver) OnProgress(*storage.Apply, []*storage.Task) {}

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

func createObserverTestApply(t *testing.T, stor storage.Storage, applyState string, completedAt *time.Time) *storage.Apply {
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
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, nil, []*storage.ApplyOperation{{
		Deployment: apply.Deployment,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}})
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
