package webhook

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// unlockCommandCore returns a durability disposition (retry, err) that a future
// durable issue_comment driver consumes. The synchronous goSafe wrapper discards
// it, so these tests pin the contract directly on the core.

// unlockContractLockStore serves configurable lock lookups and records release
// calls, with injectable failures for each storage touchpoint the unlock core
// classifies.
type unlockContractLockStore struct {
	storage.LockStore
	locks      []*storage.Lock
	getByPRErr error
	releaseErr error

	releaseCalls      int
	forceReleaseCalls int
}

func (s *unlockContractLockStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Lock, error) {
	if s.getByPRErr != nil {
		return nil, s.getByPRErr
	}
	return s.locks, nil
}

func (s *unlockContractLockStore) List(_ context.Context) ([]*storage.Lock, error) {
	return s.locks, nil
}

func (s *unlockContractLockStore) Release(_ context.Context, _, _, _ string) error {
	s.releaseCalls++
	return s.releaseErr
}

func (s *unlockContractLockStore) ForceRelease(_ context.Context, _, _ string) error {
	s.forceReleaseCalls++
	return s.releaseErr
}

// activeApplyStore reports a non-terminal apply for every database, the shape
// of a lock still protecting an in-flight apply.
type activeApplyStore struct {
	storage.ApplyStore
}

func (s *activeApplyStore) GetByDatabase(_ context.Context, database, _, _ string) ([]*storage.Apply, error) {
	return []*storage.Apply{{
		Database:        database,
		Environment:     "staging",
		ApplyIdentifier: "apply-123",
		State:           state.Apply.Running,
	}}, nil
}

func prOwnedOrdersLock() *storage.Lock {
	return &storage.Lock{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}
}

// Transient infrastructure failures — a storage lock lookup, the active-apply
// verification, or a lock release — are failures the same delivery could clear
// on a later attempt, so the core must report them as retryable with a non-nil
// error. Released locks drop out of the lookup, so a re-drive retries only the
// locks that remain.
func TestUnlockCommandCoreTransientFailuresAreRetryable(t *testing.T) {
	t.Run("lock lookup failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockContractLockStore{getByPRErr: errors.New("storage read failed")}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "a transient lock lookup failure must stay retryable for a durable driver")
		body := requireUnlockComment(t, comments)
		assert.Contains(t, body, "Failed to look up locks")
	})

	t.Run("active-apply verification failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockContractLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &failingApplyLookupStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "an unverifiable active-apply state must be retried, not treated as the command's answer")
		assert.Zero(t, lockStore.releaseCalls, "no lock may be released while apply state is unknown")
		body := requireUnlockComment(t, comments)
		assert.Contains(t, body, "Failed to verify active applies for database `orders`")
	})

	t.Run("lock release failure is retryable", func(t *testing.T) {
		client, _ := setupGitHubServer(t)
		lockStore := &unlockContractLockStore{
			locks:      []*storage.Lock{prOwnedOrdersLock()},
			releaseErr: errors.New("storage write failed"),
		}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "a failed lock release must stay retryable so a re-drive can release the remaining locks")
		assert.Equal(t, 1, lockStore.releaseCalls, "the release must have been attempted")
	})
}

// Terminal outcomes are the command's answer — no locks to release, a lock
// still protecting an active apply, a deterministic force-unlock ownership
// rejection, or a completed release — so the core reports them as
// (retry=false, err=nil): a durable driver must not re-drive them.
func TestUnlockCommandCoreTerminalDispositions(t *testing.T) {
	t.Run("no locks found is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockContractLockStore{}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "an empty lock set is the command's answer, not a failure")
		body := requireUnlockComment(t, comments)
		assert.Contains(t, body, "No Locks Found")
	})

	t.Run("active apply protecting the lock is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockContractLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &activeApplyStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "an active apply verifiably blocking the unlock is the command's answer; re-driving would re-block")
		assert.Zero(t, lockStore.releaseCalls, "a lock protecting an active apply must not be released")
		body := requireUnlockComment(t, comments)
		assert.Contains(t, body, "Cannot Unlock")
		assert.Contains(t, body, "apply-123")
	})

	t.Run("force unlock of a lock held by another PR is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockContractLockStore{locks: []*storage.Lock{{
			DatabaseName: "orders",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "octocat/other-repo",
			PullRequest:  7,
			Owner:        "octocat/other-repo#7",
		}}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true, Database: "orders"})

		require.NoError(t, err)
		assert.False(t, retry, "another PR's lock ownership is a deterministic rejection the same input always reproduces")
		assert.Zero(t, lockStore.forceReleaseCalls, "another PR's lock must not be force-released")
		body := requireUnlockComment(t, comments)
		assert.Contains(t, body, "held by octocat/other-repo#7")
	})

	t.Run("released locks are terminal success", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockContractLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "a completed release is the command's answer; a re-drive would find nothing to release")
		assert.Equal(t, 1, lockStore.releaseCalls, "exactly one owned-lock release expected")
		body := requireUnlockComment(t, comments)
		assert.Contains(t, body, "Lock Released")
	})
}

// requireUnlockComment waits briefly for the next PR comment the unlock core
// posted and returns its body.
func requireUnlockComment(t *testing.T, comments chan string) string {
	t.Helper()
	select {
	case body := <-comments:
		return body
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for unlock comment")
		return ""
	}
}
