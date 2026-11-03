package tern

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// getFailingApplyStore serves applies from a map and fails the reads of the
// internal ids and the identifier it is told to, so a test can tell a storage
// failure apart from a row that does not exist.
type getFailingApplyStore struct {
	storage.ApplyStore
	applies     map[int64]*storage.Apply
	failGetByID map[int64]error
	failByIdent error
}

func (s *getFailingApplyStore) Get(_ context.Context, id int64) (*storage.Apply, error) {
	if err := s.failGetByID[id]; err != nil {
		return nil, err
	}
	return s.applies[id], nil
}

func (s *getFailingApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.failByIdent != nil {
		return nil, s.failByIdent
	}
	for _, apply := range s.applies {
		if apply.ApplyIdentifier == applyIdentifier {
			return apply, nil
		}
	}
	return nil, nil
}

func newGetFailingControlClient(applyStore *getFailingApplyStore, tasks []*storage.Task, eng *controlCaptureEngine) *LocalClient {
	client := newMySQLControlTestClient(nil, tasks, eng)
	client.storage.(*controlTestStorage).applies = applyStore
	return client
}

func stoppedApplyWithTask(id int64, identifier string, updatedAt time.Time) (*storage.Apply, *storage.Task) {
	apply := &storage.Apply{
		ID:              id,
		ApplyIdentifier: identifier,
		State:           state.Apply.Stopped,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		UpdatedAt:       updatedAt,
	}
	task := &storage.Task{
		ID:             id + 1000,
		ApplyID:        id,
		TaskIdentifier: "task-" + identifier,
		Database:       "testdb",
		Namespace:      "testdb",
		TableName:      "users",
		State:          state.Task.Stopped,
		UpdatedAt:      updatedAt,
	}
	return apply, task
}

// A start that names an apply while storage cannot answer must report the
// storage failure, not claim the apply does not exist: an operator told "not
// found" stops looking at the database and the retry never happens.
func TestLocalClient_StartNamedApplyReportsStorageFailure(t *testing.T) {
	apply, task := stoppedApplyWithTask(90, "apply-named", time.Now())
	storeErr := errors.New("read applies row: i/o timeout")
	client := newGetFailingControlClient(
		&getFailingApplyStore{applies: map[int64]*storage.Apply{apply.ID: apply}, failByIdent: storeErr},
		[]*storage.Task{task}, &controlCaptureEngine{})

	_, err := client.Start(t.Context(), &ternv1.StartRequest{ApplyId: apply.ApplyIdentifier})

	require.ErrorIs(t, err, storeErr)
	assert.NotErrorIs(t, err, storage.ErrApplyNotFound)
	assert.Contains(t, err.Error(), apply.ApplyIdentifier)
	assert.Empty(t, client.storage.ControlRequests().(*testControlRequestStore).requests,
		"a start that could not load its apply records no request")
}

// A start that names an apply storage has no row for is a missing apply, and
// carries the sentinel so callers can tell it from a storage failure.
func TestLocalClient_StartNamedApplyMissingWrapsNotFound(t *testing.T) {
	_, task := stoppedApplyWithTask(90, "apply-present", time.Now())
	client := newGetFailingControlClient(
		&getFailingApplyStore{applies: map[int64]*storage.Apply{}},
		[]*storage.Task{task}, &controlCaptureEngine{})

	_, err := client.Start(t.Context(), &ternv1.StartRequest{ApplyId: "apply-missing"})

	require.ErrorIs(t, err, storage.ErrApplyNotFound)
	assert.Contains(t, err.Error(), "apply-missing")
}

// A start that names no apply picks the most recently updated stopped apply.
// When storage fails to load that newest apply, resuming the older stopped one
// instead would restart a change the operator did not mean, so the start fails
// with the storage error.
func TestLocalClient_StartDiscoveryDoesNotFallBackToAnOlderApplyOnStorageFailure(t *testing.T) {
	now := time.Now()
	older, olderTask := stoppedApplyWithTask(90, "apply-older", now.Add(-time.Hour))
	newer, newerTask := stoppedApplyWithTask(95, "apply-newer", now)
	storeErr := errors.New("read applies row: i/o timeout")
	client := newGetFailingControlClient(
		&getFailingApplyStore{
			applies:     map[int64]*storage.Apply{older.ID: older, newer.ID: newer},
			failGetByID: map[int64]error{newer.ID: storeErr},
		},
		[]*storage.Task{olderTask, newerTask}, &controlCaptureEngine{})

	resolved, _, _, err := client.resolveStartRequest(t.Context(), &ternv1.StartRequest{})

	require.ErrorIs(t, err, storeErr)
	assert.Nil(t, resolved, "no apply is resolved when the newest candidate could not be loaded")
	assert.Contains(t, err.Error(), newerTask.TaskIdentifier)
}

// A stop that names an apply while storage cannot answer must report the storage
// failure and leave the engine and every stored row untouched.
func TestLocalClient_StopNamedApplyReportsStorageFailure(t *testing.T) {
	apply, task := stoppedApplyWithTask(90, "apply-stop-named", time.Now())
	apply.State = state.Apply.Running
	task.State = state.Task.Running
	storeErr := errors.New("read applies row: i/o timeout")
	eng := &controlCaptureEngine{}
	client := newGetFailingControlClient(
		&getFailingApplyStore{applies: map[int64]*storage.Apply{apply.ID: apply}, failByIdent: storeErr},
		[]*storage.Task{task}, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, storeErr)
	assert.NotErrorIs(t, err, storage.ErrApplyNotFound)
	assert.Contains(t, err.Error(), apply.ApplyIdentifier)
	assert.Nil(t, eng.stopReq, "a stop that could not load its apply does not stop the engine")
	assert.Equal(t, state.Task.Running, task.State)
	assert.Equal(t, state.Apply.Running, apply.State)
}

// A stop that names an apply storage has no row for is a missing apply, and
// carries the sentinel so callers can tell it from a storage failure.
func TestLocalClient_StopNamedApplyMissingWrapsNotFound(t *testing.T) {
	apply, task := stoppedApplyWithTask(90, "apply-stop-present", time.Now())
	task.State = state.Task.Running
	eng := &controlCaptureEngine{}
	client := newGetFailingControlClient(
		&getFailingApplyStore{applies: map[int64]*storage.Apply{}},
		[]*storage.Task{task}, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: "apply-missing"}, "")

	require.ErrorIs(t, err, storage.ErrApplyNotFound)
	assert.Contains(t, err.Error(), "apply-missing")
	assert.Nil(t, eng.stopReq, "a stop for a missing apply does not stop the engine")
	assert.Equal(t, state.Task.Running, task.State)
	assert.Equal(t, state.Apply.Stopped, apply.State, "the unrelated stored apply is untouched")
}
