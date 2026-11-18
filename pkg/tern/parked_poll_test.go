package tern

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// hasPendingStore answers ListPending from fixed values, so a test can put a
// pending request in front of a held drive or make the check fail. The request
// it lists is not stored, so a full poll that goes on to load it finds nothing
// to act on.
type hasPendingStore struct {
	*testControlRequestStore
	pending bool
	// operation is the pending request's operation, a cutover when unset.
	operation storage.ControlOperation
	err       error
	reads     int
}

func (s *hasPendingStore) ListPending(_ context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.reads++
	if s.err != nil {
		return nil, s.err
	}
	if !s.pending {
		return nil, nil
	}
	operation := s.operation
	if operation == "" {
		operation = storage.ControlOperationCutover
	}
	return []*storage.ApplyControlRequest{{ApplyID: applyID, Operation: operation, Status: storage.ControlRequestPending}}, nil
}

// A drive held at a deferred cutover runs its full poll only when the refresh
// interval has passed or the apply has a pending control request. Any other
// tick costs one storage read. A drive that is not held, or whose check fails,
// runs the full poll every tick.
func TestParkedPoll(t *testing.T) {
	apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-held"}
	start := time.Now()
	newStore := func(controls storage.ControlRequestStore) storage.Storage {
		return &mockStorage{controlRequests: controls}
	}

	t.Run("a drive that is not held polls every tick", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}}
		p := newParkedPoll()
		for i := range 3 {
			assert.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start.Add(time.Duration(i)*time.Second), slog.Default()))
		}
		assert.Zero(t, controls.reads, "only a held drive checks for pending requests")
	})

	t.Run("a held drive skips ticks until the refresh interval passes", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}}
		store := newStore(controls)
		p := newParkedPoll()
		require.True(t, p.runFullPoll(t.Context(), store, apply, start, slog.Default()))
		p.hold()

		assert.False(t, p.runFullPoll(t.Context(), store, apply, start.Add(time.Second), slog.Default()))
		assert.False(t, p.runFullPoll(t.Context(), store, apply, start.Add(parkedFullPollInterval-time.Second), slog.Default()))
		assert.Equal(t, 2, controls.reads, "each skipped tick makes one check")
		assert.True(t, p.runFullPoll(t.Context(), store, apply, start.Add(parkedFullPollInterval), slog.Default()),
			"the refresh poll keeps the task rows' liveness writes landing")
		assert.True(t, p.runFullPoll(t.Context(), store, apply, start.Add(parkedFullPollInterval+time.Second), slog.Default()),
			"the hold lasts only until a full poll that does not find the drive held again")
	})

	t.Run("a pending request wakes a held drive at once", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}, pending: true}
		p := newParkedPoll()
		require.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start, slog.Default()))
		p.hold()
		assert.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start.Add(time.Second), slog.Default()))

		// The poll that consumed the request can still find the engine holding
		// while it starts the swap. The drive keeps polling every tick so it
		// sees the swap finish, though nothing is pending any more.
		controls.pending = false
		p.hold()
		reads := controls.reads
		for i := 2; i < 5; i++ {
			assert.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start.Add(time.Duration(i)*time.Second), slog.Default()),
				"a woken drive runs the full poll on tick %d", i)
			p.hold()
		}
		assert.Equal(t, reads, controls.reads, "a woken drive no longer checks for pending requests")
	})

	t.Run("a request the refresh poll consumes keeps the drive at full cadence", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}}
		store := newStore(controls)
		p := newParkedPoll()
		require.True(t, p.runFullPoll(t.Context(), store, apply, start, slog.Default()))
		p.hold()

		// The refresh runs the full poll without a check, so the request it
		// consumes is seen only by the poll's own control read.
		require.True(t, p.runFullPoll(t.Context(), store, apply, start.Add(parkedFullPollInterval), slog.Default()))
		p.observe(pendingControlOperations{storage.ControlOperationCutover: {}})
		p.hold()

		reads := controls.reads
		assert.True(t, p.runFullPoll(t.Context(), store, apply, start.Add(parkedFullPollInterval+time.Second), slog.Default()),
			"the drive polls the next tick to see the swap the consumed request started")
		assert.Equal(t, reads, controls.reads, "a woken drive no longer checks for pending requests")
	})

	t.Run("a control read that finds nothing leaves the drive held", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}}
		store := newStore(controls)
		p := newParkedPoll()
		require.True(t, p.runFullPoll(t.Context(), store, apply, start, slog.Default()))
		p.observe(pendingControlOperations{})
		p.hold()
		assert.False(t, p.runFullPoll(t.Context(), store, apply, start.Add(time.Second), slog.Default()))
	})

	for _, operation := range []storage.ControlOperation{storage.ControlOperationStop, storage.ControlOperationCancel} {
		t.Run("a pending "+string(operation)+" wakes a held drive", func(t *testing.T) {
			controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}, pending: true, operation: operation}
			p := newParkedPoll()
			require.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start, slog.Default()))
			p.hold()
			assert.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start.Add(time.Second), slog.Default()))
		})
	}

	// A request that does not end a hold, such as a skip-revert queued for
	// the revert window, stays pending for the whole hold. It neither wakes
	// the drive between refreshes nor keeps it at full cadence after one.
	t.Run("a request that does not end the hold leaves the drive held", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}, pending: true, operation: storage.ControlOperationSkipRevert}
		store := newStore(controls)
		p := newParkedPoll()
		require.True(t, p.runFullPoll(t.Context(), store, apply, start, slog.Default()))
		p.observe(pendingControlOperations{storage.ControlOperationSkipRevert: {}})
		p.hold()
		assert.False(t, p.runFullPoll(t.Context(), store, apply, start.Add(time.Second), slog.Default()))
		assert.Equal(t, 1, controls.reads)
		assert.True(t, p.runFullPoll(t.Context(), store, apply, start.Add(parkedFullPollInterval), slog.Default()),
			"the refresh still runs, and resolves the request as a full-cadence poll would")
	})

	t.Run("a held drive whose context ended skips the check", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}, err: assert.AnError}
		p := newParkedPoll()
		require.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start, slog.Default()))
		p.hold()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.False(t, p.runFullPoll(ctx, newStore(controls), apply, start.Add(time.Second), slog.Default()))
		assert.Zero(t, controls.reads, "no storage read runs on an ended context")
	})

	t.Run("a failed check runs the full poll", func(t *testing.T) {
		controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}, err: assert.AnError}
		p := newParkedPoll()
		require.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start, slog.Default()))
		p.hold()
		assert.True(t, p.runFullPoll(t.Context(), newStore(controls), apply, start.Add(time.Second), slog.Default()))
	})
}

// A control-plane drive held at a deferred cutover stops polling the data
// plane between refreshes: it polls once, finds the apply waiting for its
// cutover, and then only checks storage for an operator's request. Once a
// request is pending it polls the data plane every tick again, so the request
// lands as fast as it would have at full cadence.
func TestGRPCClient_PollForCompletionSlowsWhileHeldAtDeferredCutover(t *testing.T) {
	for _, tc := range []struct {
		name          string
		pending       bool
		wantMinPolls  int
		wantMaxPolls  int
		wantMinChecks int
	}{
		{name: "nothing pending", wantMinPolls: 1, wantMaxPolls: 1, wantMinChecks: 2},
		{name: "a request pending", pending: true, wantMinPolls: 3, wantMaxPolls: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &capturingTernServer{
				progressState:    ternv1.State_STATE_WAITING_FOR_CUTOVER,
				progressStateSet: true,
				progressTables: []*ternv1.TableProgress{{
					Namespace:       "default",
					TableName:       "users",
					Status:          state.Task.WaitingForCutover,
					PercentComplete: 100,
				}},
			}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()

			apply := &storage.Apply{
				ID:              64,
				ApplyIdentifier: "apply-deferred-hold",
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeMySQL,
				Deployment:      "eu",
				Environment:     "production",
				ExternalID:      "remote-deferred-hold",
				State:           state.Apply.Running,
			}
			apply.SetOptions(storage.ApplyOptions{DeferCutover: true})
			storedApply := *apply
			task := &storage.Task{
				ID:             65,
				TaskIdentifier: "task-deferred-hold",
				ApplyID:        apply.ID,
				Namespace:      "default",
				TableName:      "users",
				State:          state.Task.Running,
			}
			controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}, pending: tc.pending}
			client.storage = &mockStorage{
				applies:         &mockApplyStore{apply: &storedApply},
				tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
				logs:            &mockApplyLogStore{},
				controlRequests: controls,
			}

			// Three poll ticks fit before the deadline.
			ctx, cancel := context.WithTimeout(t.Context(), 3*grpcProgressPollInterval+grpcProgressPollInterval/2)
			defer cancel()
			err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
			require.ErrorIs(t, err, context.DeadlineExceeded)

			server.mu.Lock()
			polls := server.progressCalls
			server.mu.Unlock()
			assert.GreaterOrEqual(t, polls, tc.wantMinPolls)
			assert.LessOrEqual(t, polls, tc.wantMaxPolls)
			assert.GreaterOrEqual(t, controls.reads, tc.wantMinChecks)
			assert.Equal(t, state.Apply.WaitingForCutover, apply.State)
		})
	}
}

// A data-plane drive held at a deferred cutover stops polling its engine
// between refreshes and checks storage for an operator's request instead. A
// pending request puts it back on every tick.
func TestPollForCompletionAtomicSlowsWhileHeldAtDeferredCutover(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pending      bool
		wantMaxPolls int
		wantMinPolls int
	}{
		{name: "nothing pending", wantMinPolls: 1, wantMaxPolls: 1},
		{name: "a request pending", pending: true, wantMinPolls: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRevertWindowTickFixture(storage.ApplyOptions{DeferCutover: true})
			f.tasks[0].State = state.Task.WaitingForCutover
			progress := &engine.ProgressResult{
				State:  engine.StateWaitingForCutover,
				Tables: []engine.TableProgress{{Namespace: "testdb", Table: "users", State: state.Task.WaitingForCutover, Progress: 100}},
			}
			f.eng.progressResult = progress
			controls := &hasPendingStore{testControlRequestStore: f.controlRequests, pending: tc.pending}
			f.client.storage.(*exactProgressStorage).controlRequests = controls
			f.client.config = LocalConfig{Database: "testdb", Type: storage.DatabaseTypeVitess}
			f.client.planetscaleEngine = f.eng
			f.client.taskPollIntervalOverride = 5 * time.Millisecond

			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			require.NoError(t, f.client.pollForCompletionAtomic(ctx, f.apply, f.tasks, &engine.Credentials{}, nil, f.apply.GetOptions().Map(), false))

			assert.GreaterOrEqual(t, f.eng.progressCalls, tc.wantMinPolls)
			if tc.wantMaxPolls > 0 {
				assert.LessOrEqual(t, f.eng.progressCalls, tc.wantMaxPolls, "a held drive polls its engine only to refresh")
				assert.Positive(t, controls.reads, "a held drive checks storage for a request on every other tick")
			}
			assert.Zero(t, f.eng.cutoverCount, "a deferred cutover waits for the operator")
		})
	}
}

// A sequential drive whose stored task row waits for a deferred cutover stops
// polling its engine between refreshes and checks storage for an operator's
// request instead. A pending request puts it back on every tick.
func TestPollTaskToCompletionSlowsWhileHeldAtDeferredCutover(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pending      bool
		wantMaxPolls int
		wantMinPolls int
	}{
		{name: "nothing pending", wantMinPolls: 1, wantMaxPolls: 1},
		{name: "a request pending", pending: true, wantMinPolls: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &storage.Task{
				ID: 1, ApplyID: 1, TaskIdentifier: "task-held",
				Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
				Namespace: "appdb", TableName: "users", State: state.Task.Running,
			}
			apply := &storage.Apply{
				ID: 1, ApplyIdentifier: "apply-held", Database: "appdb",
				DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging",
			}
			apply.SetOptions(storage.ApplyOptions{DeferCutover: true})
			eng := &fakeControlEngine{progressResult: &engine.ProgressResult{
				State:  engine.StateWaitingForCutover,
				Tables: []engine.TableProgress{{Namespace: "appdb", Table: "users", State: state.Task.WaitingForCutover, Progress: 100}},
			}}
			controls := &hasPendingStore{testControlRequestStore: &testControlRequestStore{}, pending: tc.pending}
			client := &LocalClient{
				config:       LocalConfig{Database: "appdb", Type: storage.DatabaseTypeMySQL},
				spiritEngine: eng,
				storage: &exactProgressStorage{
					tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
					controlRequests: controls,
					logs:            &mockApplyLogStore{},
				},
				logger:                   slog.Default(),
				taskPollIntervalOverride: 5 * time.Millisecond,
			}

			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			assert.Equal(t, taskHandover, client.pollTaskToCompletion(ctx, apply, task, nil, nil))

			assert.Equal(t, state.Task.WaitingForCutover, task.State)
			assert.GreaterOrEqual(t, eng.progressCalls, tc.wantMinPolls)
			if tc.wantMaxPolls > 0 {
				assert.LessOrEqual(t, eng.progressCalls, tc.wantMaxPolls, "a held drive polls its engine only to refresh")
				assert.Positive(t, controls.reads, "a held drive checks storage for a request on every other tick")
			}
			assert.Zero(t, eng.cutoverCount, "a deferred cutover waits for the operator")
		})
	}
}

// A grouped drive holding several tables at a deferred cutover slows its poll,
// unless the tick is the one on which the hold times out: that tick stops the
// engine, and the drive stays at full cadence to see the stop land rather than
// learning of it on the next refresh.
func TestAtomicProgressTickHoldsUnlessTheCutoverTimesOut(t *testing.T) {
	for _, tc := range []struct {
		name      string
		heldFor   time.Duration
		wantHeld  bool
		wantStops int
	}{
		{name: "within the timeout", heldFor: time.Minute, wantHeld: true},
		{name: "past the timeout", heldFor: waitingForManualActionTimeout + time.Minute, wantStops: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRevertWindowTickFixture(storage.ApplyOptions{DeferCutover: true})
			opID := *f.tasks[0].ApplyOperationID
			f.tasks = []*storage.Task{
				{TaskIdentifier: "task-users", ApplyID: f.apply.ID, ApplyOperationID: &opID, State: state.Task.WaitingForCutover, TableName: "users", Namespace: "testdb"},
				{TaskIdentifier: "task-orders", ApplyID: f.apply.ID, ApplyOperationID: &opID, State: state.Task.WaitingForCutover, TableName: "orders", Namespace: "testdb"},
			}
			f.client.storage.(*exactProgressStorage).tasks = &exactProgressTaskStore{tasks: f.tasks}
			progress := &engine.ProgressResult{
				State: engine.StateWaitingForCutover,
				Tables: []engine.TableProgress{
					{Namespace: "testdb", Table: "users", State: state.Task.WaitingForCutover, Progress: 100},
					{Namespace: "testdb", Table: "orders", State: state.Task.WaitingForCutover, Progress: 100},
				},
			}
			f.eng.progressResult = progress
			f.ps.lastTaskState = taskStateFromProgressResult(progress)
			f.ps.stateEnteredAt = time.Now().Add(-tc.heldFor)
			f.ps.parked = newParkedPoll()

			f.tick(t)

			assert.Equal(t, tc.wantHeld, f.ps.parked.held)
			assert.Equal(t, tc.wantStops, f.eng.stopCount)
			for _, task := range f.tasks {
				assert.Equal(t, state.Task.WaitingForCutover, task.State, "task %s", task.TableName)
			}
		})
	}
}

// A data-plane drive whose full poll consumes a stored cutover can find its
// engine still waiting for the cutover on the polls that follow, since the
// engine takes a poll or more to start its swap. The request is no longer
// pending to wake the drive, so the drive must stay at full cadence on what its
// own control reads found, or the swap would go unobserved until a refresh.
// That holds whether the cutover was recorded before the poll began or while it
// ran, between the poll's first control read and its second.
func TestPollForCompletionAtomicStaysAtFullCadenceAfterConsumingACutover(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arriveAfterReads is how many control reads pass before the
		// operator's cutover is recorded.
		arriveAfterReads int
	}{
		{name: "recorded before the poll"},
		{name: "recorded while the poll runs", arriveAfterReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRevertWindowTickFixture(storage.ApplyOptions{DeferCutover: true})
			f.tasks[0].State = state.Task.WaitingForCutover
			f.eng.progressResult = &engine.ProgressResult{
				State:  engine.StateWaitingForCutover,
				Tables: []engine.TableProgress{{Namespace: "testdb", Table: "users", State: state.Task.WaitingForCutover, Progress: 100}},
			}
			controls := &arrivingControlRequestStore{
				testControlRequestStore: f.controlRequests,
				arriveAfterReads:        tc.arriveAfterReads,
				arrive: func() {
					f.recordControlRequest(storage.ControlOperationCutover, storage.ControlRequestPending)
				},
			}
			stored := f.client.storage.(*exactProgressStorage)
			stored.controlRequests = controls
			stored.applyOperations = noResumeStateOperationStore{stored.applyOperations}
			f.client.config = LocalConfig{Database: "testdb", Type: storage.DatabaseTypeVitess}
			f.client.planetscaleEngine = f.eng
			f.client.taskPollIntervalOverride = 5 * time.Millisecond

			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			require.NoError(t, f.client.pollForCompletionAtomic(ctx, f.apply, f.tasks, &engine.Credentials{}, nil, f.apply.GetOptions().Map(), false))

			require.Equal(t, 1, f.eng.cutoverCount, "the drive consumes the operator's cutover")
			assert.NotEqual(t, storage.ControlRequestPending, f.controlRequestStatus(t, storage.ControlOperationCutover),
				"nothing is left pending to wake the drive")
			assert.GreaterOrEqual(t, f.eng.progressCalls, 5, "the drive keeps polling its engine every tick to see the swap")
		})
	}
}

// arrivingControlRequestStore records an operator's request once a set number
// of pending-request reads have run, so a test can land the request partway
// through a drive's poll.
type arrivingControlRequestStore struct {
	*testControlRequestStore
	arriveAfterReads int
	arrive           func()
	reads            int
}

func (s *arrivingControlRequestStore) ListPending(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	if s.reads == s.arriveAfterReads && s.arrive != nil {
		s.arrive()
		s.arrive = nil
	}
	s.reads++
	return s.testControlRequestStore.ListPending(ctx, applyID)
}

// noResumeStateOperationStore answers every operation's engine resume state as
// not yet recorded, so a cutover builds its request from the apply alone.
type noResumeStateOperationStore struct {
	storage.ApplyOperationStore
}

func (noResumeStateOperationStore) GetEngineResumeState(context.Context, int64) (*storage.EngineResumeState, error) {
	return nil, storage.ErrEngineResumeStateNotFound
}
