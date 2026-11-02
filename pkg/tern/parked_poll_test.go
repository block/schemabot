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
	err     error
	reads   int
}

func (s *hasPendingStore) ListPending(_ context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.reads++
	if s.err != nil {
		return nil, s.err
	}
	if !s.pending {
		return nil, nil
	}
	return []*storage.ApplyControlRequest{{ApplyID: applyID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending}}, nil
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
