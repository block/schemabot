package webhook

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
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

func (s *replacedLockStore) ReleaseByIDIfUnused(_ context.Context, id int64, database, dbType, owner, pendingPlanID string) error {
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

// A PR comments `schemabot unlock` after its apply posted a plan awaiting
// confirmation, so the lock it unlocks carries that pending plan. Nothing
// races the command, and the vetted lock is released along with its plan,
// whether the PR unlocks its own lock or an operator force-unlocks it.
func TestUnlockReleasesLockPinnedToPendingPlan(t *testing.T) {
	cases := []struct {
		name   string
		result CommandResult
	}{
		{name: "PR unlock", result: CommandResult{Action: action.Unlock}},
		{name: "force unlock", result: CommandResult{Action: action.Unlock, Force: true, Database: "orders"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			held := withLockID(prOwnedOrdersLock(), 41)
			held.PendingPlanID = "plan-1"
			lockStore := &replacedLockStore{held: held, replaced: true}
			st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
			h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

			retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", tc.result)

			require.NoError(t, err)
			assert.False(t, retry)
			assert.Nil(t, lockStore.held, "the vetted lock and its pending plan must be released")
			body := requireComment(t, comments, "release answer")
			assert.Contains(t, body, "Lock Released")
			assert.Contains(t, body, "Released by @testuser")
		})
	}
}

// An unlock decides what to release against the lock rows it looked up: the
// cross-PR ownership check, the issued-at bound, actor authorization, and the
// active-apply check all read that row. When the row is released and a new
// lock is acquired on the same database while those checks run, the unlock
// must leave the new lock held. The database is still locked, so the answer
// is the stale-command prompt to comment again, never that nothing is left to
// unlock: for a force unlock of a CLI lock that another PR's pending apply
// takes over, the fresh command then names who holds the lock now; for a PR's
// own unlock racing that PR's fresh re-acquire, it releases the new lock.
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
			assert.False(t, retry, "a stale command is terminal; the recovery path is a fresh comment")
			require.True(t, lockStore.replaced, "the release must have been attempted after the replacement landed")
			require.NotNil(t, lockStore.held, "the lock acquired after vetting must stay held")
			assert.Equal(t, int64(42), lockStore.held.ID)
			assert.Equal(t, tc.replacement.Owner, lockStore.held.Owner)
			assert.Equal(t, tc.replacement.PendingPlanID, lockStore.held.PendingPlanID)
			body := requireComment(t, comments, "stale-command answer")
			assert.Contains(t, body, "acquired after the command was received")
			assert.Contains(t, body, "Comment `schemabot unlock` again")
			assert.NotContains(t, body, "Nothing left to unlock", "the database is still locked")
			assert.NotContains(t, body, "Released by @testuser", "the command must not claim it released the new lock")
			assert.Empty(t, comments, "the stale-command answer must be the command's only comment")
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

// An unlock's active-apply check finds nothing running on orders, and the
// release then finds an apply recording the vetted lock row: one that became
// durable after the check, or a stopped apply the check treats as finished.
// The lock still protects that apply, so it stays held, and the command
// answers with the apply holding it and what to do: wait for or cancel an
// active apply, resume or cancel a stopped one. It is the command's answer,
// so the delivery is not retried and no other answer follows it.
func TestUnlockKeepsLockAnUnsettledApplyRecords(t *testing.T) {
	tests := []struct {
		name       string
		applyState string
		wantBody   []string
	}{
		{
			name:       "apply started after the active-apply check",
			applyState: state.Apply.Running,
			wantBody: []string{
				"An apply is currently active (apply ID: `apply-orders-1`, state: `running`).",
				"Wait for it to complete, or cancel it with `schemabot cancel apply-orders-1 -e staging`, then unlock again.",
			},
		},
		{
			name:       "stopped apply",
			applyState: state.Apply.Stopped,
			wantBody: []string{
				"Apply `apply-orders-1` is stopped and still holds this lock.",
				"Resume it with `schemabot start apply-orders-1 -e staging` or cancel it with `schemabot cancel apply-orders-1 -e staging`, then unlock again.",
			},
		},
		{
			name:       "apply in its revert window",
			applyState: state.Apply.RevertWindow,
			wantBody: []string{
				"Apply `apply-orders-1` has passed its cutover and is in its revert window, so it still holds this lock.",
				"Revert it with `schemabot revert apply-orders-1 -e staging` or keep the change with `schemabot skip-revert apply-orders-1 -e staging`, then unlock again.",
			},
		},
		{
			name:       "revert in progress",
			applyState: state.Apply.Reverting,
			wantBody: []string{
				"Apply `apply-orders-1` is finishing its revert window (state: `reverting`) and still holds this lock.",
				"Wait for it, then unlock again.",
			},
		},
		{
			name:       "skip-revert in progress",
			applyState: state.Apply.SkippingRevert,
			wantBody: []string{
				"(state: `skipping_revert`)",
				"Wait for it, then unlock again.",
			},
		},
		{
			name:       "ended apply with an operation still running",
			applyState: state.Apply.Failed,
			wantBody: []string{
				"Apply `apply-orders-1` has ended (state: `failed`), but one of its operations is still running and holds this lock.",
				"Wait for that operation to finish, then unlock again.",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			lockStore := &unlockTestLockStore{
				locks: []*storage.Lock{withLockID(prOwnedOrdersLock(), 41)},
				releaseErrs: map[string]error{"orders": &storage.LockInUseError{
					LockID: 41,
					Apply: &storage.Apply{
						ApplyIdentifier: "apply-orders-1",
						Database:        "orders",
						DatabaseType:    storage.DatabaseTypeMySQL,
						Environment:     "staging",
						Repository:      "octocat/hello-world",
						PullRequest:     1,
						State:           tc.applyState,
					},
				}},
			}
			st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
			h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

			retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

			require.NoError(t, err)
			assert.False(t, retry, "a lock kept for an apply is the command's answer")
			assert.Equal(t, 1, lockStore.releaseCalls)
			assert.Empty(t, lockStore.released, "the lock the apply records must stay held")
			body := requireComment(t, comments, "cannot-unlock answer")
			assert.Contains(t, body, "Cannot Unlock")
			assert.Contains(t, body, "`orders`")
			for _, want := range tc.wantBody {
				assert.Contains(t, body, want)
			}
			assert.NotContains(t, body, "stop it")
			assert.Empty(t, comments, "the cannot-unlock answer must be the command's only comment")
		})
	}
}

// An unlock matches two locks: one an active apply records, so it is kept,
// and one a concurrent operation already released. The command answers only
// with the refusal for the kept lock, never also with "every lock was already
// released", which would be false for the lock still held.
func TestUnlockKeptLockAndAlreadyReleasedLockAnswersOnlyWithTheRefusal(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	billing := withLockID(prOwnedOrdersLock(), 42)
	billing.DatabaseName = "billing"
	lockStore := &unlockTestLockStore{
		locks: []*storage.Lock{withLockID(prOwnedOrdersLock(), 41), billing},
		releaseErrs: map[string]error{
			"orders": &storage.LockInUseError{LockID: 41, Apply: &storage.Apply{
				ApplyIdentifier: "apply-orders-1",
				Database:        "orders",
				DatabaseType:    storage.DatabaseTypeMySQL,
				Environment:     "staging",
				Repository:      "octocat/hello-world",
				PullRequest:     1,
				State:           state.Apply.Running,
			}},
			"billing": storage.ErrLockNotFound,
		},
	}
	st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

	retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

	require.NoError(t, err)
	assert.False(t, retry)
	assert.Equal(t, 2, lockStore.releaseCalls)
	assert.Empty(t, lockStore.released)
	body := requireComment(t, comments, "cannot-unlock answer")
	assert.Contains(t, body, "Cannot Unlock")
	assert.Contains(t, body, "apply-orders-1")
	assert.Empty(t, comments, "the kept lock must not be followed by 'every lock was already released'")
}

func withLockID(lock *storage.Lock, id int64) *storage.Lock {
	lock.ID = id
	return lock
}
