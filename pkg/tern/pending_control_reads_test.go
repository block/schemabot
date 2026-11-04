package tern

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// sharedReadControlRequestStore counts ListPending calls from any goroutine.
// A test can fail the next calls, or hold a call until it releases it, to see
// what the other drives of the apply do meanwhile.
type sharedReadControlRequestStore struct {
	*testControlRequestStore

	mu       sync.Mutex
	reads    int
	failNext int
	// hold, when set, blocks each call until it is closed. entered receives
	// once per call that is blocked.
	hold    chan struct{}
	entered chan struct{}
}

func (s *sharedReadControlRequestStore) ListPending(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.mu.Lock()
	s.reads++
	fail := s.failNext > 0
	if fail {
		s.failNext--
	}
	hold, entered := s.hold, s.entered
	s.mu.Unlock()
	if hold != nil {
		entered <- struct{}{}
		<-hold
	}
	if fail {
		return nil, errors.New("storage unavailable")
	}
	return s.testControlRequestStore.ListPending(ctx, applyID)
}

func (s *sharedReadControlRequestStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// manualClock is a clock a test moves by hand.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// The drives of one apply share their checks for pending control requests:
// a drive answers its check from a read another drive started recently, or
// one still in flight, instead of querying again. A drive never answers two
// checks from the same read, so one drive alone queries on every check, and
// a check that must see everything recorded before it always queries. A
// failed read is never handed to a drive that did not make it.
func TestSharedPendingControlReads(t *testing.T) {
	const maxAge = 250 * time.Millisecond
	apply := &storage.Apply{ID: 11, ApplyIdentifier: "apply-shared-reads"}
	setup := func() (*SharedPendingControlReads, *manualClock, *sharedReadControlRequestStore, storage.Storage) {
		clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
		controls := &sharedReadControlRequestStore{testControlRequestStore: &testControlRequestStore{requests: []*storage.ApplyControlRequest{
			{ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending},
		}}}
		return &SharedPendingControlReads{now: clock.read}, clock, controls, &mockStorage{controlRequests: controls}
	}
	load := func(t *testing.T, d *drivePendingControlReads, store storage.Storage, apply *storage.Apply) pendingControlOperations {
		t.Helper()
		pending, err := d.load(t.Context(), store, apply, slog.Default())
		require.NoError(t, err)
		return pending
	}

	t.Run("drives of one apply share a recent read", func(t *testing.T) {
		shared, clock, controls, store := setup()
		first, second := shared.forDrive(maxAge), shared.forDrive(maxAge)

		assert.True(t, load(t, first, store, apply).has(storage.ControlOperationCutover))
		clock.advance(maxAge / 2)
		assert.True(t, load(t, second, store, apply).has(storage.ControlOperationCutover), "the joined read carries the pending set")
		assert.Equal(t, 1, controls.readCount(), "the second drive answers from the first drive's read")

		load(t, second, store, apply)
		assert.Equal(t, 2, controls.readCount(), "a drive never answers two checks from the same read")
		load(t, first, store, apply)
		assert.Equal(t, 2, controls.readCount(), "the first drive answers from the second drive's newer read")
	})

	t.Run("a read older than the drive's max age is not shared", func(t *testing.T) {
		shared, clock, controls, store := setup()
		load(t, shared.forDrive(maxAge), store, apply)
		clock.advance(maxAge + time.Millisecond)
		load(t, shared.forDrive(maxAge), store, apply)
		assert.Equal(t, 2, controls.readCount())
	})

	t.Run("a fresh check always reads, and other drives can join it", func(t *testing.T) {
		shared, _, controls, store := setup()
		first, second, third := shared.forDrive(maxAge), shared.forDrive(maxAge), shared.forDrive(maxAge)
		load(t, first, store, apply)

		pending, err := second.loadFresh(t.Context(), store, apply, slog.Default())
		require.NoError(t, err)
		assert.True(t, pending.has(storage.ControlOperationCutover))
		assert.Equal(t, 2, controls.readCount(), "a fresh check does not answer from a read that started before it")

		load(t, third, store, apply)
		assert.Equal(t, 2, controls.readCount(), "a later check joins the fresh read")
	})

	t.Run("different applies do not share", func(t *testing.T) {
		shared, _, controls, store := setup()
		other := &storage.Apply{ID: apply.ID + 1, ApplyIdentifier: "apply-shared-reads-other"}
		load(t, shared.forDrive(maxAge), store, apply)
		assert.False(t, load(t, shared.forDrive(maxAge), store, other).has(storage.ControlOperationCutover),
			"the other apply's check sees only its own requests")
		assert.Equal(t, 2, controls.readCount())
	})

	t.Run("a drive joins a read still in flight", func(t *testing.T) {
		shared, _, controls, store := setup()
		controls.hold, controls.entered = make(chan struct{}), make(chan struct{}, 1)
		leader := make(chan pendingControlOperations, 1)
		go func() {
			pending, _ := shared.forDrive(maxAge).load(t.Context(), store, apply, slog.Default())
			leader <- pending
		}()
		<-controls.entered

		joined := make(chan pendingControlOperations, 1)
		go func() {
			pending, _ := shared.forDrive(maxAge).load(t.Context(), store, apply, slog.Default())
			joined <- pending
		}()
		close(controls.hold)

		assert.True(t, receiveWithin(t, leader).has(storage.ControlOperationCutover))
		assert.True(t, receiveWithin(t, joined).has(storage.ControlOperationCutover))
		assert.Equal(t, 1, controls.readCount(), "the second drive waited for the first drive's read")
	})

	t.Run("a read still in flight is joined however long ago it started", func(t *testing.T) {
		shared, clock, controls, store := setup()
		controls.hold, controls.entered = make(chan struct{}), make(chan struct{}, 1)
		leaderDone := make(chan struct{})
		go func() {
			defer close(leaderDone)
			_, _ = shared.forDrive(maxAge).load(t.Context(), store, apply, slog.Default())
		}()
		<-controls.entered
		clock.advance(2 * maxAge)

		read, leader := shared.join(apply.ID, nil, clock.read().Add(-maxAge), false)
		assert.False(t, leader, "a check made while storage is slow waits for the read in flight instead of sending another")
		assert.True(t, read.inFlight())

		close(controls.hold)
		receiveWithin(t, leaderDone)
		assert.Equal(t, 1, controls.readCount())
		_, leader = shared.join(apply.ID, nil, clock.read().Add(-maxAge), false)
		assert.True(t, leader, "once that read has finished, its age applies again")
	})

	t.Run("a failed read is not handed to another drive", func(t *testing.T) {
		shared, _, controls, store := setup()
		controls.failNext = 1
		_, err := shared.forDrive(maxAge).load(t.Context(), store, apply, slog.Default())
		require.Error(t, err, "the drive that made the read sees its failure")

		assert.True(t, load(t, shared.forDrive(maxAge), store, apply).has(storage.ControlOperationCutover),
			"another drive reads for itself instead of taking the failure")
		assert.Equal(t, 2, controls.readCount())
	})

	t.Run("a cancelled drive stops waiting for another drive's read", func(t *testing.T) {
		shared, _, controls, store := setup()
		controls.hold, controls.entered = make(chan struct{}), make(chan struct{}, 1)
		leaderDone := make(chan struct{})
		go func() {
			defer close(leaderDone)
			_, _ = shared.forDrive(maxAge).load(t.Context(), store, apply, slog.Default())
		}()
		<-controls.entered

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := shared.forDrive(maxAge).load(ctx, store, apply, slog.Default())
		require.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "apply-shared-reads")

		close(controls.hold)
		receiveWithin(t, leaderDone)
	})

	t.Run("a nil handle reads on every check", func(t *testing.T) {
		_, _, controls, store := setup()
		var d *drivePendingControlReads
		load(t, d, store, apply)
		load(t, d, store, apply)
		_, err := d.loadFresh(t.Context(), store, apply, slog.Default())
		require.NoError(t, err)
		assert.Equal(t, 3, controls.readCount())
	})

	t.Run("reads of applies no drive checks any more are swept", func(t *testing.T) {
		shared, clock, _, store := setup()
		idle := &storage.Apply{ID: apply.ID + 1, ApplyIdentifier: "apply-shared-reads-idle"}
		load(t, shared.forDrive(maxAge), store, idle)
		clock.advance(pendingControlReadSweepAfter)
		load(t, shared.forDrive(maxAge), store, apply)

		shared.mu.Lock()
		defer shared.mu.Unlock()
		assert.NotContains(t, shared.reads, idle.ID)
		assert.Contains(t, shared.reads, apply.ID)
	})
}

// Two drives of one remote apply, each driving a rollout member on its own
// deployment and so through its own client, answer their control checks at
// each poll from one read of the apply's pending requests, because the
// clients were built with the same SharedPendingControlReads.
func TestGRPCClient_DrivesOfOneApplyShareTheirControlRead(t *testing.T) {
	shared := &SharedPendingControlReads{}
	var clients []*GRPCClient
	for range 2 {
		client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{})
		defer cleanup()
		client.pendingControlReads = shared
		clients = append(clients, client)
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-shared-control-read",
		Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL,
		Environment: "staging", Engine: storage.EngineSpirit,
		ExternalID: "remote-shared-control-read", State: state.Apply.Running,
	}
	stored := *apply
	controls := &pollReadCountingControlRequestStore{testControlRequestStore: &testControlRequestStore{}}
	store := &mockStorage{applies: &mockApplyStore{apply: &stored}, tasks: &mockTaskStore{}, logs: &mockApplyLogStore{}, controlRequests: controls}

	for _, client := range clients {
		client.storage = store
		standDown, err := client.processPendingControlRequestsAtPoll(t.Context(), apply, applyTaskScope{},
			client.pendingControlReads.forDrive(grpcProgressPollInterval/2), nil)
		require.NoError(t, err)
		assert.False(t, standDown)
	}
	assert.Equal(t, 1, controls.listReads, "both drives' checks are answered by one read")
}

// sharedReadTestDeadline bounds each wait for a drive's check to return.
const sharedReadTestDeadline = 5 * time.Second

// receiveWithin returns the next value from ch, failing the test if none
// arrives within sharedReadTestDeadline.
func receiveWithin[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(sharedReadTestDeadline):
		require.FailNow(t, "timed out waiting for a drive's check to return")
		var zero T
		return zero
	}
}
