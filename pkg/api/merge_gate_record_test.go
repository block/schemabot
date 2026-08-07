package api

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type staticGetApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *staticGetApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.apply, nil
}

// capturingMergeGateStore records requests and serves per-kind rows, so
// both the drive tail's settle recording and the preflight gate's
// record-then-poll loop can run against it. Mutating a stored row's state
// from the consumer callback stands in for the processor completing the
// fan-out; the mutex keeps that write safe against the gate's poll reads.
type capturingMergeGateStore struct {
	storage.MergeGateRequestStore
	mu       sync.Mutex
	rows     map[string]*storage.MergeGateRequest
	recorded []*storage.MergeGateRequest
	reopened int
}

func (s *capturingMergeGateStore) Record(_ context.Context, req *storage.MergeGateRequest) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recorded = append(s.recorded, req)
	cp := *req
	cp.State = storage.MergeGatePending
	if s.rows == nil {
		s.rows = map[string]*storage.MergeGateRequest{}
	}
	s.rows[req.Kind] = &cp
	return true, nil
}

func (s *capturingMergeGateStore) GetByApplyAndKind(_ context.Context, _ int64, kind string) (*storage.MergeGateRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows[kind] == nil {
		return nil, nil
	}
	cp := *s.rows[kind]
	return &cp, nil
}

func (s *capturingMergeGateStore) ReopenForRetry(context.Context, int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reopened++
	row := s.rows[storage.MergeGateKindPreflight]
	if row == nil || row.State != storage.MergeGateFailed {
		return false, nil
	}
	row.State = storage.MergeGatePending
	row.RetryAfter = nil
	return true, nil
}

// setRowState mutates a stored row the way the processor's finish would.
func (s *capturingMergeGateStore) setRowState(kind, st string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[kind].State = st
}

// setRowHoldsRecorded stamps a stored row the way the processor's hold phase
// would: holds recorded while the request itself is still being driven.
func (s *capturingMergeGateStore) setRowHoldsRecorded(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.rows[kind].State = storage.MergeGateProcessing
	s.rows[kind].HoldsRecordedAt = &now
}

func (s *capturingMergeGateStore) recordedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.recorded)
}

type staticTaskCountStore struct {
	storage.TaskStore
	count int64
}

func (s *staticTaskCountStore) CountByApplyID(context.Context, int64) (int64, error) {
	return s.count, nil
}

type mockStorageWithMergeGate struct {
	mockStorage
	applies   storage.ApplyStore
	tasks     storage.TaskStore
	mergeGate storage.MergeGateRequestStore
}

func (m *mockStorageWithMergeGate) Applies() storage.ApplyStore { return m.applies }
func (m *mockStorageWithMergeGate) Tasks() storage.TaskStore    { return m.tasks }
func (m *mockStorageWithMergeGate) MergeGateRequests() storage.MergeGateRequestStore {
	return m.mergeGate
}

// newMergeGateTestService builds a Service over the capturing merge gate
// store with a single apply in the given state that owns taskCount task rows.
func newMergeGateTestService(applyState string, taskCount int64) (*Service, *capturingMergeGateStore) {
	gateStore := &capturingMergeGateStore{}
	st := &mockStorageWithMergeGate{
		applies: &staticGetApplyStore{apply: &storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply-gate-test",
			Database:        "gate_db",
			DatabaseType:    "mysql",
			Environment:     "staging",
			Repository:      "octocat/hello-world",
			PullRequest:     1,
			Caller:          "cli:tester@host",
			State:           applyState,
		}},
		tasks:     &staticTaskCountStore{count: taskCount},
		mergeGate: gateStore,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(st, testServerConfig(), nil, logger), gateStore
}

// TestRecordMergeGateGatedOnConsumer verifies the drive tail records a
// settle request only when a merge gate consumer is registered. A server with no
// GitHub runtime — a gRPC/CLI-only deployment — has no PR check state to
// refresh and no processor to drain requests, so a recorded row would sit
// pending forever; the drive tail must skip recording entirely there. With a
// consumer registered, the settle is recorded with the apply's target and
// attribution and the consumer is woken.
func TestRecordMergeGateGatedOnConsumer(t *testing.T) {
	t.Run("no consumer registered skips recording", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Completed, 1)

		svc.recordMergeGateIfApplyResolved(t.Context(), 0, 7)

		assert.Empty(t, gateStore.recorded,
			"a server without a merge gate consumer must not record requests nothing will drain")
	})

	t.Run("registered consumer records a settle and is woken", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Completed, 1)
		woken := 0
		svc.OnMergeGateRecorded = func() { woken++ }

		svc.recordMergeGateIfApplyResolved(t.Context(), 0, 7)

		require.Len(t, gateStore.recorded, 1)
		recorded := gateStore.recorded[0]
		assert.Equal(t, storage.MergeGateKindSettle, recorded.Kind)
		assert.Equal(t, "apply-gate-test", recorded.ApplyIdentifier)
		assert.Equal(t, "gate_db", recorded.DatabaseName)
		assert.Equal(t, "mysql", recorded.DatabaseType)
		assert.Equal(t, "staging", recorded.Environment)
		assert.Equal(t, "octocat/hello-world", recorded.Repository)
		assert.Equal(t, "1", recorded.ChangeKey)
		assert.Equal(t, "cli:tester@host", recorded.RequestedBy)
		assert.Equal(t, 1, woken, "the drive tail wakes the consumer exactly once per recording")
	})
}

// TestRecordMergeGateOnTerminalStates verifies which terminal outcomes get
// a settle. A completed apply always does — it changed the live schema. A
// failed apply changed nothing, so it needs a settle only when its preflight
// held sibling PR checks: the settle's re-plan is what releases those holds.
// A non-terminal apply never records one.
func TestRecordMergeGateOnTerminalStates(t *testing.T) {
	t.Run("non-terminal apply records nothing", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Running, 1)
		svc.OnMergeGateRecorded = func() {}

		svc.recordMergeGateIfApplyResolved(t.Context(), 0, 7)

		assert.Empty(t, gateStore.recorded)
	})

	t.Run("failed apply without a preflight records nothing", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Failed, 1)
		svc.OnMergeGateRecorded = func() {}

		svc.recordMergeGateIfApplyResolved(t.Context(), 0, 7)

		assert.Empty(t, gateStore.recorded,
			"a failed apply that never held sibling checks has nothing to release or refresh")
	})

	t.Run("failed apply with a preflight records the releasing settle", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Failed, 1)
		svc.OnMergeGateRecorded = func() {}
		_, err := gateStore.Record(t.Context(), &storage.MergeGateRequest{
			ApplyID: 7,
			Kind:    storage.MergeGateKindPreflight,
		})
		require.NoError(t, err)

		svc.recordMergeGateIfApplyResolved(t.Context(), 0, 7)

		require.Len(t, gateStore.recorded, 2)
		assert.Equal(t, storage.MergeGateKindSettle, gateStore.recorded[1].Kind,
			"the settle releases the holds the preflight placed")
	})
}

// TestGateApplyStartOnCheckPreflight verifies the hard gate in front of an
// apply's engine work: the drive may start only once the preflight fan-out
// has confirmed sibling PR check holds. The gate does not apply on servers
// with no merge gate consumer or to applies that own no schema change tasks; it
// passes immediately on an already-completed preflight, records and waits
// otherwise, re-arms a terminally failed request, and fails closed when the
// drive context ends first.
func TestGateApplyStartOnCheckPreflight(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-gate-test",
		Database:        "gate_db",
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Caller:          "cli:tester@host",
		State:           state.Apply.Pending,
	}

	t.Run("no consumer: the apply starts ungated", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 1)

		err := svc.gateApplyStartOnCheckPreflight(t.Context(), 0, apply, "default")

		require.NoError(t, err)
		assert.Zero(t, gateStore.recordedCount())
	})

	t.Run("task-less apply skips the preflight", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 0)
		svc.OnMergeGateRecorded = func() {}

		err := svc.gateApplyStartOnCheckPreflight(t.Context(), 0, apply, "default")

		require.NoError(t, err)
		assert.Zero(t, gateStore.recordedCount(),
			"an apply with no schema change tasks cannot invalidate sibling verdicts")
	})

	t.Run("records the preflight, wakes the consumer, and passes once it completes", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 1)
		woken := 0
		svc.OnMergeGateRecorded = func() {
			woken++
			// Stand in for the processor: the wake-up drains the request.
			gateStore.setRowState(storage.MergeGateKindPreflight, storage.MergeGateCompleted)
		}

		err := svc.gateApplyStartOnCheckPreflight(t.Context(), 0, apply, "default")

		require.NoError(t, err)
		require.Equal(t, 1, gateStore.recordedCount())
		assert.Equal(t, storage.MergeGateKindPreflight, gateStore.recorded[0].Kind)
		assert.Equal(t, "cli:tester@host", gateStore.recorded[0].RequestedBy)
		assert.Equal(t, 1, woken)
	})

	t.Run("already-completed preflight passes without recording", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 1)
		svc.OnMergeGateRecorded = func() {}
		_, err := gateStore.Record(t.Context(), &storage.MergeGateRequest{
			ApplyID: 7,
			Kind:    storage.MergeGateKindPreflight,
		})
		require.NoError(t, err)
		gateStore.setRowState(storage.MergeGateKindPreflight, storage.MergeGateCompleted)
		gateStore.mu.Lock()
		gateStore.recorded = nil
		gateStore.mu.Unlock()

		err = svc.gateApplyStartOnCheckPreflight(t.Context(), 0, apply, "default")

		require.NoError(t, err)
		assert.Zero(t, gateStore.recordedCount(),
			"a resume or cutover drive pays one read, not a new request")
	})

	t.Run("stored holds pass the gate while the render is still retrying", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 1)
		svc.OnMergeGateRecorded = func() {
			// Stand in for the processor's hold phase: the stored holds land
			// but the code-host rendering has not completed — the shape of a
			// code-host outage. The gate must start the apply on the holds
			// alone.
			gateStore.setRowHoldsRecorded(storage.MergeGateKindPreflight)
		}

		err := svc.gateApplyStartOnCheckPreflight(t.Context(), 0, apply, "default")

		require.NoError(t, err, "a code-host outage must never block an apply on the rendering of its own holds")
	})

	t.Run("holds recorded on an earlier drive pass without recording", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 1)
		svc.OnMergeGateRecorded = func() {}
		_, err := gateStore.Record(t.Context(), &storage.MergeGateRequest{
			ApplyID: 7,
			Kind:    storage.MergeGateKindPreflight,
		})
		require.NoError(t, err)
		gateStore.setRowHoldsRecorded(storage.MergeGateKindPreflight)
		gateStore.mu.Lock()
		gateStore.recorded = nil
		gateStore.mu.Unlock()

		err = svc.gateApplyStartOnCheckPreflight(t.Context(), 0, apply, "default")

		require.NoError(t, err)
		assert.Zero(t, gateStore.recordedCount(),
			"a resume drive pays one read against the recorded holds, not a new request")
	})

	t.Run("terminally failed preflight is re-armed and the gate keeps waiting", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 1)
		_, err := gateStore.Record(t.Context(), &storage.MergeGateRequest{
			ApplyID: 7,
			Kind:    storage.MergeGateKindPreflight,
		})
		require.NoError(t, err)
		gateStore.setRowState(storage.MergeGateKindPreflight, storage.MergeGateFailed)
		svc.OnMergeGateRecorded = func() {
			// Stand in for the processor draining the re-armed request.
			gateStore.setRowState(storage.MergeGateKindPreflight, storage.MergeGateCompleted)
		}

		err = svc.gateApplyStartOnCheckPreflight(t.Context(), 0, apply, "default")

		require.NoError(t, err)
		gateStore.mu.Lock()
		reopened := gateStore.reopened
		gateStore.mu.Unlock()
		assert.Equal(t, 1, reopened,
			"a preflight that exhausted its retries during an outage must self-heal, not block the apply forever")
	})

	t.Run("fails closed when the drive context ends before the holds land", func(t *testing.T) {
		svc, gateStore := newMergeGateTestService(state.Apply.Pending, 1)
		svc.OnMergeGateRecorded = func() {} // no processor: the request stays pending

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := svc.gateApplyStartOnCheckPreflight(ctx, 0, apply, "default")

		require.Error(t, err, "unconfirmed holds must abandon the drive attempt, never start the engine")
		assert.Equal(t, 1, gateStore.recordedCount())
	})
}
