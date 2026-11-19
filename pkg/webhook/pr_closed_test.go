package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// prClosedTestStorage serves a fixed set of locks and applies and records
// which cleanup operations runPRCloseCleanup performed, so tests can verify
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
	// releasedRows records the row ID and pending plan each release was pinned
	// to, in call order.
	releasedRows []pinnedRelease
	// releaseErr, keyed by database, is what a release of that database's lock
	// returns instead of deleting it: the lock already gone, or held again.
	releaseErr map[string]error
}

type pinnedRelease struct {
	id            int64
	pendingPlanID string
}

func (s *prClosedLockStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Lock, error) {
	return s.locks, nil
}

func (s *prClosedLockStore) ReleaseByID(_ context.Context, id int64, database, _, _, pendingPlanID string) error {
	s.releasedRows = append(s.releasedRows, pinnedRelease{id: id, pendingPlanID: pendingPlanID})
	if err := s.releaseErr[database]; err != nil {
		return err
	}
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

// prClosedTestHandlerWithGitHub is prClosedTestHandler with a GitHub client
// factory, for closes that read the PR's live state.
func prClosedTestHandlerWithGitHub(t *testing.T, st storage.Storage, factory *fakeClientFactory) *Handler {
	t.Helper()
	service := api.New(st, &api.ServerConfig{}, nil, testLogger())
	return NewHandler(service, factory, nil, testLogger())
}

// prClosedGitHub serves octocat/hello-world#1 with the given live state, or
// the given HTTP error status when status is not zero. The returned counter
// reports how many live PR reads cleanup issued.
func prClosedGitHub(t *testing.T, prState string, status int) (*fakeClientFactory, *atomic.Int32) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	fetches := &atomic.Int32{}
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": 1,
			"state":  prState,
			"head":   map[string]any{"sha": "live-head-sha", "ref": "feature"},
		}))
	})
	return &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")}, fetches
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
	ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
	assert.NoError(t, err)
	assert.True(t, ran, "cleanup runs for a PR that is still closed")

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
	ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
	assert.NoError(t, err)
	assert.True(t, ran, "cleanup runs for a PR that is still closed")

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
	ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
	assert.NoError(t, err)
	assert.True(t, ran, "cleanup runs for a PR that is still closed")

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

	factory, fetches := prClosedGitHub(t, "closed", 0)
	h := prClosedTestHandlerWithGitHub(t, st, factory)
	ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, false)
	assert.NoError(t, err)
	assert.True(t, ran, "cleanup runs for a PR that is still closed")

	assert.Equal(t, int32(1), fetches.Load(), "an unmerged close reads the live PR once")
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
	_, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
	assert.Error(t, err, "a failed apply lookup must be returned so the durable driver retries")

	assert.Empty(t, lockStore.released, "no lock is released when apply state is unknown")
	assert.Equal(t, 0, checkStore.deleteCalls, "no check state is deleted when apply state is unknown")
}

// TestPRClosedCleanupSkipsMergedLiveRead verifies that a merged close never
// reads GitHub: a merged PR cannot reopen, so the delivery's own state is
// already final and the close costs no extra request.
func TestPRClosedCleanupSkipsMergedLiveRead(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{locks: lockStore, applies: &prClosedApplyStore{}, checks: checkStore}
	factory, fetches := prClosedGitHub(t, "open", 0)

	h := prClosedTestHandlerWithGitHub(t, st, factory)
	ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
	assert.NoError(t, err)
	assert.True(t, ran, "cleanup runs for a PR that is still closed")

	assert.Equal(t, int32(0), fetches.Load(), "a merged close must not read the live PR")
	assert.Equal(t, []string{"orders"}, lockStore.released, "a merged close releases the lock")
	assert.Equal(t, 1, checkStore.deleteCalls, "a merged close deletes stored check state")
}

// TestPRClosedCleanupSkipsReopenedPR covers a close delivery that runs after
// the PR was reopened: the operator holds a lock owned by repo#pr and plan-only
// check state exists. The live PR is open, so cleanup releases nothing and
// deletes nothing, and the delivery completes without error.
func TestPRClosedCleanupSkipsReopenedPR(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{locks: lockStore, applies: &prClosedApplyStore{}, checks: checkStore}
	factory, fetches := prClosedGitHub(t, "open", 0)

	h := prClosedTestHandlerWithGitHub(t, st, factory)
	ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, false)
	assert.NoError(t, err)
	assert.False(t, ran, "cleanup reports that it did not run for a PR that is open again")

	assert.Equal(t, int32(1), fetches.Load(), "cleanup reads the live PR once")
	assert.Empty(t, lockStore.released, "the lock taken after the reopen must stay held")
	assert.Equal(t, 0, checkStore.deleteCalls, "stored check state must survive a close delivery for a reopened PR")
}

// TestPRClosedCleanupReadsLivePRPastCachedSnapshot covers a close delivery
// whose context already carries a snapshot of the PR taken while it was
// closed. The PR has reopened since, so cleanup must read GitHub instead of
// trusting the snapshot, and it releases nothing and deletes nothing.
func TestPRClosedCleanupReadsLivePRPastCachedSnapshot(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{locks: lockStore, applies: &prClosedApplyStore{}, checks: checkStore}
	factory, fetches := prClosedGitHub(t, "open", 0)
	ctx := ghclient.WithPRInfo(t.Context(), "octocat/hello-world", 1, &ghclient.PullRequestInfo{State: "closed"})

	h := prClosedTestHandlerWithGitHub(t, st, factory)
	ran, err := h.runPRCloseCleanup(ctx, "octocat/hello-world", 1, 12345, false)
	assert.NoError(t, err)
	assert.False(t, ran, "cleanup reports that it did not run for a PR that is open again")

	assert.Equal(t, int32(1), fetches.Load(), "cleanup reads the live PR instead of the cached snapshot")
	assert.Empty(t, lockStore.released, "the lock taken after the reopen must stay held")
	assert.Equal(t, 0, checkStore.deleteCalls, "stored check state must survive a close delivery for a reopened PR")
}

// TestPRClosedReleasesTheListedLockRow verifies that close cleanup deletes the
// lock row it listed, for the pending plan it listed, rather than whatever lock
// the PR holds on the database when the delete runs.
func TestPRClosedReleasesTheListedLockRow(t *testing.T) {
	lock := prClosedLock("orders")
	lock.ID = 314
	lock.PendingPlanID = "plan-closed"
	lockStore := &prClosedLockStore{locks: []*storage.Lock{lock}}
	st := &prClosedTestStorage{locks: lockStore, applies: &prClosedApplyStore{}, checks: &prClosedCheckStore{}}

	h := prClosedTestHandler(t, st)
	_, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
	require.NoError(t, err)

	assert.Equal(t, []pinnedRelease{{id: 314, pendingPlanID: "plan-closed"}}, lockStore.releasedRows)
}

// TestPRClosedTreatsAnAlreadyReleasedLockAsReleased covers a close delivery
// whose listed lock is gone by the time cleanup deletes it, because an unlock
// or an apply's own cleanup released it in between. That is the outcome
// cleanup wanted, so the delivery completes and the PR's stored check state
// is deleted instead of retrying against a lock that will never come back.
func TestPRClosedTreatsAnAlreadyReleasedLockAsReleased(t *testing.T) {
	lockStore := &prClosedLockStore{
		locks:      []*storage.Lock{prClosedLock("orders"), prClosedLock("billing")},
		releaseErr: map[string]error{"orders": storage.ErrLockNotFound},
	}
	checkStore := &prClosedCheckStore{}
	st := &prClosedTestStorage{locks: lockStore, applies: &prClosedApplyStore{}, checks: checkStore}

	h := prClosedTestHandler(t, st)
	ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
	require.NoError(t, err)
	assert.True(t, ran)

	assert.Equal(t, []string{"billing"}, lockStore.released, "the lock still held is released")
	assert.Equal(t, 1, checkStore.deleteCalls, "stored check state is deleted once every listed lock is gone")
}

// TestPRClosedKeepsALockAcquiredAgainAfterTheListing covers a close delivery
// that lists the PR's lock on orders, after which the PR reopens and a new
// plan takes the orders lock again before cleanup deletes it. The new lock
// protects work the closed PR's cleanup never saw, so it stays held. Cleanup
// still releases the PR's other lock, but returns an error and leaves the
// stored check state alone, so the durable retry re-reads the live PR before
// deciding anything about the new lock or the new plan's check state.
func TestPRClosedKeepsALockAcquiredAgainAfterTheListing(t *testing.T) {
	for _, reacquired := range []error{storage.ErrLockIntentChanged, storage.ErrLockReplaced} {
		t.Run(reacquired.Error(), func(t *testing.T) {
			lockStore := &prClosedLockStore{
				locks:      []*storage.Lock{prClosedLock("orders"), prClosedLock("billing")},
				releaseErr: map[string]error{"orders": reacquired},
			}
			checkStore := &prClosedCheckStore{}
			st := &prClosedTestStorage{locks: lockStore, applies: &prClosedApplyStore{}, checks: checkStore}

			h := prClosedTestHandler(t, st)
			ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, 12345, true)
			require.ErrorIs(t, err, reacquired, "a lock taken again must make the delivery retry")
			assert.False(t, ran)

			assert.Equal(t, []string{"billing"}, lockStore.released, "the lock acquired again stays held; the other is released")
			assert.Equal(t, 0, checkStore.deleteCalls, "stored check state must survive while a lock was taken again after the listing")
		})
	}
}

// TestPRClosedCleanupFailsClosedWhenLivePRUnreadable covers a close delivery
// whose live PR read fails, or reports no usable state: cleanup cannot tell
// whether the PR reopened, so it returns an error for the durable driver to
// retry and releases and deletes nothing.
func TestPRClosedCleanupFailsClosedWhenLivePRUnreadable(t *testing.T) {
	tests := []struct {
		name           string
		prState        string
		status         int
		installationID int64
	}{
		{name: "GitHub read fails", prState: "closed", status: http.StatusForbidden, installationID: 12345},
		{name: "unrecognized state", prState: "", installationID: 12345},
		{name: "no installation", prState: "closed", installationID: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
			checkStore := &prClosedCheckStore{}
			st := &prClosedTestStorage{locks: lockStore, applies: &prClosedApplyStore{}, checks: checkStore}
			factory, _ := prClosedGitHub(t, tt.prState, tt.status)

			h := prClosedTestHandlerWithGitHub(t, st, factory)
			ran, err := h.runPRCloseCleanup(t.Context(), "octocat/hello-world", 1, tt.installationID, false)

			assert.False(t, ran, "cleanup does not run on an unverifiable PR state")
			assert.Error(t, err, "an unverifiable PR state must be returned so the durable driver retries")
			assert.Empty(t, lockStore.released, "no lock is released when the live PR state is unknown")
			assert.Equal(t, 0, checkStore.deleteCalls, "no check state is deleted when the live PR state is unknown")
		})
	}
}
