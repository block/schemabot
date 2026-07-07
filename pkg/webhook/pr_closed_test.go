package webhook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// prClosedTestStorage serves a fixed set of locks and applies and records
// which cleanup operations handlePRClosed performed, so tests can verify
// whether locks were released and check state was deleted.
type prClosedTestStorage struct {
	emptyStorage
	locks   *prClosedLockStore
	applies storage.ApplyStore
	checks  *prClosedCheckStore
}

func (s *prClosedTestStorage) Locks() storage.LockStore { return s.locks }

func (s *prClosedTestStorage) Applies() storage.ApplyStore { return s.applies }

func (s *prClosedTestStorage) Checks() storage.CheckStore { return s.checks }

type prClosedLockStore struct {
	storage.LockStore
	locks    []*storage.Lock
	released []string
}

func (s *prClosedLockStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Lock, error) {
	return s.locks, nil
}

func (s *prClosedLockStore) Release(_ context.Context, database, _, _ string) error {
	s.released = append(s.released, database)
	return nil
}

type prClosedApplyStore struct {
	storage.ApplyStore
	applies []*storage.Apply
}

func (s *prClosedApplyStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Apply, error) {
	return s.applies, nil
}

type failingPRApplyLookupStore struct {
	storage.ApplyStore
}

func (s *failingPRApplyLookupStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Apply, error) {
	return nil, errors.New("storage read failed")
}

type prClosedCheckStore struct {
	storage.CheckStore
	deleteCalls  int
	deleteMerged []bool
}

func (s *prClosedCheckStore) DeleteByPRRetainingBlockingApplyOwned(_ context.Context, _ string, _ int, merged bool) error {
	s.deleteCalls++
	s.deleteMerged = append(s.deleteMerged, merged)
	return nil
}

func prClosedTestHandler(t *testing.T, st storage.Storage) *Handler {
	t.Helper()
	service := api.New(st, &api.ServerConfig{}, nil, testLogger())
	return &Handler{
		service: service,
		logger:  testLogger(),
	}
}

func prClosedLock(database string) *storage.Lock {
	return &storage.Lock{
		DatabaseName: database,
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}
}

func prClosedApply(database, applyState string) *storage.Apply {
	return &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply_" + database,
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		State:           applyState,
	}
}

// TestPRClosedRetainsLockAndChecksForInFlightApply verifies that closing a PR
// while one of its applies is still running does not release the apply's
// database lock and does not delete the PR's stored check state. The lock must
// keep other PRs from starting a concurrent apply on the same database, and the
// stored check state must keep blocking a close-and-reopen until the apply
// reaches a terminal state.
func TestPRClosedRetainsLockAndChecksForInFlightApply(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{
		locks:   lockStore,
		applies: &prClosedApplyStore{applies: []*storage.Apply{prClosedApply("orders", state.Apply.Running)}},
		checks:  checkStore,
	}

	h := prClosedTestHandler(t, st)
	h.handlePRClosed("octocat/hello-world", 1, 12345, true)

	assert.Empty(t, lockStore.released, "lock for a database with an in-flight apply must not be released on PR close")
	assert.Equal(t, 0, checkStore.deleteCalls, "stored check state must not be deleted while an apply is in flight")
}

// TestPRClosedReleasesOnlyLocksWithoutInFlightApply verifies per-database lock
// cleanup on PR close: a lock on a database whose apply finished is released,
// while a lock on a database with a running apply is retained. Check state
// deletion is skipped entirely while any apply is in flight, because stored
// check state is PR-scoped.
func TestPRClosedReleasesOnlyLocksWithoutInFlightApply(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders"), prClosedLock("billing")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{
		locks: lockStore,
		applies: &prClosedApplyStore{applies: []*storage.Apply{
			prClosedApply("orders", state.Apply.Running),
			prClosedApply("billing", state.Apply.Completed),
		}},
		checks: checkStore,
	}

	h := prClosedTestHandler(t, st)
	h.handlePRClosed("octocat/hello-world", 1, 12345, true)

	assert.Equal(t, []string{"billing"}, lockStore.released, "only the lock without an in-flight apply is released")
	assert.Equal(t, 0, checkStore.deleteCalls, "stored check state must not be deleted while any apply is in flight")
}

// TestPRClosedCleansUpWhenAllAppliesTerminal verifies that once every apply for
// the PR has reached a terminal state, closing the PR releases all of its locks
// and deletes its stored check state.
func TestPRClosedCleansUpWhenAllAppliesTerminal(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders"), prClosedLock("billing")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{
		locks: lockStore,
		applies: &prClosedApplyStore{applies: []*storage.Apply{
			prClosedApply("orders", state.Apply.Completed),
			prClosedApply("billing", state.Apply.Failed),
		}},
		checks: checkStore,
	}

	h := prClosedTestHandler(t, st)
	h.handlePRClosed("octocat/hello-world", 1, 12345, true)

	assert.Equal(t, []string{"orders", "billing"}, lockStore.released, "all locks are released when applies are terminal")
	assert.Equal(t, 1, checkStore.deleteCalls, "stored check state is deleted when applies are terminal")
	assert.Equal(t, []bool{true}, checkStore.deleteMerged, "the storage delete must run in merged mode for a merged close")
}

// TestPRClosedCleansUpWhenPRHasNoApplies verifies that a PR with no recorded
// applies (e.g., plan-only PRs) gets full cleanup on close: locks released and
// stored check state deleted. An unmerged close must run the storage delete in
// unmerged mode so apply-owned rows — including success rows whose stored
// conclusion may predate a commit that removed the applied change — are
// retained.
func TestPRClosedCleansUpWhenPRHasNoApplies(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{
		locks:   lockStore,
		applies: &prClosedApplyStore{},
		checks:  checkStore,
	}

	h := prClosedTestHandler(t, st)
	h.handlePRClosed("octocat/hello-world", 1, 12345, false)

	assert.Equal(t, []string{"orders"}, lockStore.released, "locks are released when the PR has no applies")
	assert.Equal(t, 1, checkStore.deleteCalls, "stored check state is deleted when the PR has no applies")
	assert.Equal(t, []bool{false}, checkStore.deleteMerged, "the storage delete must run in unmerged mode for a close without merge")
}

// TestPRClosedSkipsCleanupWhenApplyLookupFails verifies that PR-close cleanup
// fails closed: when apply state cannot be read, no lock is released and no
// stored check state is deleted, because either action could unblock a database
// with an apply still in flight.
func TestPRClosedSkipsCleanupWhenApplyLookupFails(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{
		locks:   lockStore,
		applies: &failingPRApplyLookupStore{},
		checks:  checkStore,
	}

	h := prClosedTestHandler(t, st)
	h.handlePRClosed("octocat/hello-world", 1, 12345, true)

	assert.Empty(t, lockStore.released, "no lock is released when apply state is unknown")
	assert.Equal(t, 0, checkStore.deleteCalls, "no check state is deleted when apply state is unknown")
}
