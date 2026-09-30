package webhook

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// replacedLockStore holds a single lock row for one database and models the
// real store's release semantics for every release method. The first release
// call swaps the held row for replacement before acting, standing in for a
// concurrent release and re-acquire that lands after unlock has looked up and
// vetted the original row but before its delete.
type replacedLockStore struct {
	storage.LockStore
	held        *storage.Lock
	replacement *storage.Lock
	replaced    bool
}

func (s *replacedLockStore) snapshot() []*storage.Lock {
	if s.held == nil {
		return nil
	}
	lock := *s.held
	return []*storage.Lock{&lock}
}

func (s *replacedLockStore) List(_ context.Context) ([]*storage.Lock, error) {
	return s.snapshot(), nil
}

func (s *replacedLockStore) GetByPR(_ context.Context, repo string, pr int) ([]*storage.Lock, error) {
	if s.held == nil || s.held.Repository != repo || s.held.PullRequest != pr {
		return nil, nil
	}
	return s.snapshot(), nil
}

func (s *replacedLockStore) replaceOnce() {
	if s.replaced {
		return
	}
	s.replaced = true
	s.held = s.replacement
}

func (s *replacedLockStore) ReleaseByID(_ context.Context, id int64, database, dbType, owner, pendingPlanID string) error {
	s.replaceOnce()
	switch {
	case s.held == nil || s.held.DatabaseName != database || s.held.DatabaseType != dbType:
		return storage.ErrLockNotFound
	case s.held.ID != id:
		return storage.ErrLockReplaced
	case s.held.Owner != owner:
		return storage.ErrLockNotOwned
	case s.held.PendingPlanID != pendingPlanID:
		return storage.ErrLockIntentChanged
	}
	s.held = nil
	return nil
}

func (s *replacedLockStore) Release(_ context.Context, database, dbType, owner string) error {
	s.replaceOnce()
	switch {
	case s.held == nil || s.held.DatabaseName != database || s.held.DatabaseType != dbType:
		return storage.ErrLockNotFound
	case s.held.Owner != owner:
		return storage.ErrLockNotOwned
	}
	s.held = nil
	return nil
}

func (s *replacedLockStore) ForceRelease(_ context.Context, database, dbType string) error {
	s.replaceOnce()
	if s.held == nil || s.held.DatabaseName != database || s.held.DatabaseType != dbType {
		return storage.ErrLockNotFound
	}
	s.held = nil
	return nil
}

// An unlock decides what to release against the lock rows it looked up: the
// cross-PR ownership check, the issued-at bound, actor authorization, and the
// active-apply check all read that row. When the row is released and a new
// lock is acquired on the same database while those checks run, the unlock
// must leave the new lock held and answer that the lock it targeted is
// already gone, for a force unlock of a CLI lock that another PR's pending
// apply takes over and for a PR's own unlock racing that PR's fresh re-acquire.
func TestUnlockLeavesLockAcquiredAfterVettingHeld(t *testing.T) {
	cases := []struct {
		name        string
		vetted      *storage.Lock
		replacement *storage.Lock
		result      CommandResult
	}{
		{
			name: "force unlock leaves another PR's new lock held",
			vetted: &storage.Lock{
				ID:           41,
				DatabaseName: "orders",
				DatabaseType: storage.DatabaseTypeMySQL,
				Owner:        "cli:dev@workstation",
			},
			replacement: &storage.Lock{
				ID:            42,
				DatabaseName:  "orders",
				DatabaseType:  storage.DatabaseTypeMySQL,
				Repository:    "octocat/other-repo",
				PullRequest:   7,
				Owner:         "octocat/other-repo#7",
				PendingPlanID: "plan-other-7",
			},
			result: CommandResult{Action: action.Unlock, Force: true, Database: "orders"},
		},
		{
			name:   "PR unlock leaves the PR's re-acquired lock held",
			vetted: withLockID(prOwnedOrdersLock(), 41),
			replacement: func() *storage.Lock {
				lock := withLockID(prOwnedOrdersLock(), 42)
				lock.PendingPlanID = "plan-fresh"
				return lock
			}(),
			result: CommandResult{Action: action.Unlock},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			lockStore := &replacedLockStore{held: tc.vetted, replacement: tc.replacement}
			st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
			h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

			retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", tc.result)

			require.NoError(t, err)
			assert.False(t, retry, "the targeted lock is gone, which is the command's goal; a re-drive would find nothing it vetted")
			require.True(t, lockStore.replaced, "the release must have been attempted after the replacement landed")
			require.NotNil(t, lockStore.held, "the lock acquired after vetting must stay held")
			assert.Equal(t, int64(42), lockStore.held.ID)
			assert.Equal(t, tc.replacement.Owner, lockStore.held.Owner)
			assert.Equal(t, tc.replacement.PendingPlanID, lockStore.held.PendingPlanID)
			body := requireComment(t, comments, "already-released answer")
			assert.Contains(t, body, "Locks Already Released")
			assert.NotContains(t, body, "Released by @testuser", "the command must not claim it released the new lock")
			assert.Empty(t, comments, "the already-released answer must be the command's only comment")
		})
	}
}

// A PR acquires its own still-held lock again for a fresh plan after the
// unlock command was received. The row keeps its ID and creation time, but it
// now protects an apply awaiting confirmation that the command never covered,
// so the unlock leaves it held and answers that the command is stale — whether
// the new plan landed before the unlock looked the lock up or after.
func TestUnlockLeavesLockAcquiredAgainForNewPlanHeld(t *testing.T) {
	issuedAt := time.Now()
	vetted := withLockID(prOwnedOrdersLock(), 41)
	vetted.PendingPlanID = "plan-old"
	vetted.CreatedAt = issuedAt.Add(-time.Hour)
	vetted.UpdatedAt = issuedAt.Add(-time.Hour)
	refreshed := *vetted
	refreshed.PendingPlanID = "plan-fresh"
	refreshed.UpdatedAt = issuedAt.Add(time.Minute)

	cases := []struct {
		name  string
		store *replacedLockStore
	}{
		{name: "new plan before lookup", store: &replacedLockStore{held: &refreshed, replaced: true}},
		{name: "new plan after lookup", store: &replacedLockStore{held: vetted, replacement: &refreshed}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			st := &unlockTestStorage{locks: tc.store, applies: &noActiveAppliesStore{}}
			h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

			retry, err := h.unlockCommandCore(t.Context(), issuedAt, "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

			require.NoError(t, err)
			assert.False(t, retry, "a stale command is terminal; the recovery path is a fresh comment")
			require.NotNil(t, tc.store.held, "the lock protecting the fresh plan must stay held")
			assert.Equal(t, "plan-fresh", tc.store.held.PendingPlanID)
			body := requireComment(t, comments, "stale-command answer")
			assert.Contains(t, body, "acquired after the command was received")
			assert.NotContains(t, body, "Released by @testuser")
		})
	}
}

func withLockID(lock *storage.Lock, id int64) *storage.Lock {
	lock.ID = id
	return lock
}
