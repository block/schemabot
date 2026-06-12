package tern

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// stopStartRaceEngine lets the test hold a stop open while a start arrives,
// mirroring how Spirit's teardown takes time after the data plane already
// reports stopped. Progress reports running until the test releases it to
// completed, so a resumed task can be observed mid-flight.
type stopStartRaceEngine struct {
	engine.Engine

	stopEntered chan struct{} // closed when Stop is first called
	stopRelease chan struct{} // Stop blocks until this is closed
	stopOnce    sync.Once

	// stopReturned flips once Stop has returned. Apply records its value so the
	// test can assert that no resume dispatch began while the stop was in flight.
	stopReturned         atomic.Bool
	applyDuringStop      atomic.Bool
	applyCalled          chan struct{}
	applyCalledOnce      sync.Once
	progressCompleted    atomic.Bool
	tableName            string
	progressPollObserved chan struct{}
	progressPollOnce     sync.Once
}

func (e *stopStartRaceEngine) Name() string { return "fake" }

func (e *stopStartRaceEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			TableChanges: []engine.TableChange{{Table: e.tableName, DDL: "ALTER TABLE `" + e.tableName + "` MODIFY COLUMN `id` bigint NOT NULL"}},
		}},
	}, nil
}

func (e *stopStartRaceEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	if !e.stopReturned.Load() {
		e.applyDuringStop.Store(true)
	}
	e.applyCalledOnce.Do(func() { close(e.applyCalled) })
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *stopStartRaceEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	if e.progressCompleted.Load() {
		return &engine.ProgressResult{State: engine.StateCompleted}, nil
	}
	e.progressPollOnce.Do(func() { close(e.progressPollObserved) })
	return &engine.ProgressResult{State: engine.StateRunning}, nil
}

func (e *stopStartRaceEngine) Stop(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.stopOnce.Do(func() { close(e.stopEntered) })
	<-e.stopRelease
	e.stopReturned.Store(true)
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *stopStartRaceEngine) Start(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *stopStartRaceEngine) Volume(context.Context, *engine.VolumeRequest) (*engine.VolumeResult, error) {
	return &engine.VolumeResult{Accepted: true}, nil
}

// syncStopStartApplyStore is a mutex-guarded apply store that returns copies,
// so concurrent stop/start/resume goroutines mirror real row semantics instead
// of sharing one struct pointer.
type syncStopStartApplyStore struct {
	storage.ApplyStore
	mu    sync.Mutex
	apply storage.Apply
}

func (s *syncStopStartApplyStore) get() *storage.Apply {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := s.apply
	return &clone
}

func (s *syncStopStartApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.get(), nil
}

func (s *syncStopStartApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	return s.get(), nil
}

func (s *syncStopStartApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply = *apply
	return nil
}

func (s *syncStopStartApplyStore) Heartbeat(context.Context, int64) error { return nil }

func (s *syncStopStartApplyStore) setState(applyState string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply.State = applyState
}

func (s *syncStopStartApplyStore) currentState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.apply.State
}

type syncStopStartTaskStore struct {
	storage.TaskStore
	mu   sync.Mutex
	task storage.Task
}

func (s *syncStopStartTaskStore) get() *storage.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := s.task
	return &clone
}

func (s *syncStopStartTaskStore) GetByDatabase(context.Context, string) ([]*storage.Task, error) {
	return []*storage.Task{s.get()}, nil
}

func (s *syncStopStartTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	return []*storage.Task{s.get()}, nil
}

func (s *syncStopStartTaskStore) Get(context.Context, string) (*storage.Task, error) {
	return s.get(), nil
}

func (s *syncStopStartTaskStore) Update(_ context.Context, task *storage.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.task = *task
	return nil
}

func (s *syncStopStartTaskStore) setState(taskState string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.task.State = taskState
}

type syncStopStartPlanStore struct {
	storage.PlanStore
	plan *storage.Plan
}

func (s *syncStopStartPlanStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return s.plan, nil
}

type syncStopStartControlRequestStore struct {
	storage.ControlRequestStore
}

func (s *syncStopStartControlRequestStore) GetPending(context.Context, int64, storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	return nil, nil
}

func (s *syncStopStartControlRequestStore) CompletePending(context.Context, int64, storage.ControlOperation) error {
	return nil
}

func (s *syncStopStartControlRequestStore) FailPending(context.Context, int64, storage.ControlOperation, string) error {
	return nil
}

type syncStopStartApplyLogStore struct {
	storage.ApplyLogStore
}

func (s *syncStopStartApplyLogStore) Append(context.Context, *storage.ApplyLog) error { return nil }

type syncStopStartStorage struct {
	storage.Storage
	applies         *syncStopStartApplyStore
	tasks           *syncStopStartTaskStore
	plans           *syncStopStartPlanStore
	controlRequests *syncStopStartControlRequestStore
}

func (s *syncStopStartStorage) Applies() storage.ApplyStore { return s.applies }
func (s *syncStopStartStorage) Tasks() storage.TaskStore    { return s.tasks }
func (s *syncStopStartStorage) Plans() storage.PlanStore    { return s.plans }
func (s *syncStopStartStorage) ApplyLogs() storage.ApplyLogStore {
	return &syncStopStartApplyLogStore{}
}
func (s *syncStopStartStorage) ControlRequests() storage.ControlRequestStore {
	return s.controlRequests
}

// pollStoreState polls fn until it returns want or the deadline expires.
func pollStoreState(t *testing.T, what, want string, fn func() string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		last = fn()
		if state.IsState(last, want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Fail(t, "timeout", "timeout waiting for %s state %q, last state: %q", what, want, last)
}

// A stop is not atomic: the engine tears down (and the data plane reports
// stopped) before the stop handler persists its final task and apply states.
// A start accepted in that window must wait for the stop to finish, and the
// stop's writes must never land on top of the resumed apply. The resumed
// schema change must run to completion and the apply must end completed, not
// stranded in stopped.
func TestStartDuringInFlightStopDoesNotStrandResumedApply(t *testing.T) {
	const database = "testdb"
	const tableName = "users"

	eng := &stopStartRaceEngine{
		stopEntered:          make(chan struct{}),
		stopRelease:          make(chan struct{}),
		applyCalled:          make(chan struct{}),
		progressPollObserved: make(chan struct{}),
		tableName:            tableName,
	}
	applies := &syncStopStartApplyStore{apply: storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-1",
		PlanID:          1,
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		State:           state.Apply.Running,
		Options:         []byte("{}"),
	}}
	tasks := &syncStopStartTaskStore{task: storage.Task{
		ID:             1,
		TaskIdentifier: "task-1",
		ApplyID:        1,
		Database:       database,
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      tableName,
		State:          state.Task.Running,
		UpdatedAt:      time.Now(),
	}}
	client := &LocalClient{
		config: LocalConfig{
			Database: database,
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &syncStopStartStorage{
			applies:         applies,
			tasks:           tasks,
			plans:           &syncStopStartPlanStore{plan: &storage.Plan{ID: 1}},
			controlRequests: &syncStopStartControlRequestStore{},
		},
		spiritEngine:      eng,
		logger:            slog.Default(),
		heartbeatInterval: time.Hour,
	}

	// Stop blocks inside the engine teardown until released.
	stopDone := make(chan error, 1)
	go func() {
		_, err := client.Stop(t.Context(), &ternv1.StopRequest{ApplyId: "apply-1"})
		stopDone <- err
	}()
	select {
	case <-eng.stopEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for engine stop to begin")
	}

	// While the stop handler is still mid-flight, progress polling has already
	// persisted the data plane's stopped state, which is what lets a start
	// request through.
	tasks.setState(state.Task.Stopped)
	applies.setState(state.Apply.Stopped)

	startDone := make(chan error, 1)
	go func() {
		resp, err := client.Start(t.Context(), &ternv1.StartRequest{ApplyId: "apply-1"})
		if err == nil && !resp.Accepted {
			err = assert.AnError
		}
		startDone <- err
	}()

	// Give an unserialized start the window it needs to dispatch its resume
	// while the stop is still in flight; a serialized start stays blocked and
	// this select simply times out.
	select {
	case <-eng.applyCalled:
	case <-time.After(2 * time.Second):
	}

	close(eng.stopRelease)
	select {
	case err := <-stopDone:
		require.NoError(t, err, "stop must succeed")
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for stop to finish")
	}
	select {
	case err := <-startDone:
		require.NoError(t, err, "start must be accepted")
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for start to finish")
	}

	assert.False(t, eng.applyDuringStop.Load(),
		"start dispatched its resume while the stop was still in flight")

	// Let the resumed task be observed running, then complete it. The apply
	// must finish completed — a stop that finished before the start must not
	// leave state that strands the resumed apply in stopped.
	select {
	case <-eng.progressPollObserved:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for resumed task progress poll")
	}
	eng.progressCompleted.Store(true)
	pollStoreState(t, "apply", state.Apply.Completed, applies.currentState)
}
