//go:build integration

// Lock release integration tests for an apply that records the lock and
// becomes durable after the release path last read the applies, and for check
// state written again after close cleanup listed it and released the lock.

package webhook

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// startApplyAfterLockListing runs start on each lock the closed PR's lock
// listing returns, right after the listing, standing in for an apply confirmed
// on another pod after close cleanup read the PR's applies and before it
// releases the lock.
type startApplyAfterLockListing struct {
	storage.LockStore
	start func(ctx context.Context, lock *storage.Lock)
}

func (s *startApplyAfterLockListing) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Lock, error) {
	locks, err := s.LockStore.GetByPR(ctx, repo, pr)
	if err != nil {
		return nil, err
	}
	if s.start != nil {
		for _, lock := range locks {
			s.start(ctx, lock)
		}
	}
	return locks, nil
}

type startApplyAfterLockListingStorage struct {
	storage.Storage
	locks *startApplyAfterLockListing
}

func (s *startApplyAfterLockListingStorage) Locks() storage.LockStore { return s.locks }

// startApplyAfterActiveApplyCheck runs start right after unlock's active-apply
// check reads the database's applies, standing in for an apply confirmed on
// another pod after that check and before unlock releases the lock.
type startApplyAfterActiveApplyCheck struct {
	storage.ApplyStore
	start func(ctx context.Context)
}

func (s *startApplyAfterActiveApplyCheck) GetByDatabase(ctx context.Context, database, dbType, environment string) ([]*storage.Apply, error) {
	applies, err := s.ApplyStore.GetByDatabase(ctx, database, dbType, environment)
	if err != nil {
		return nil, err
	}
	if s.start != nil {
		s.start(ctx)
	}
	return applies, nil
}

type startApplyAfterActiveApplyCheckStorage struct {
	storage.Storage
	applies *startApplyAfterActiveApplyCheck
}

func (s *startApplyAfterActiveApplyCheckStorage) Applies() storage.ApplyStore { return s.applies }

// lockRaceTestLock is the PR's lock on dbName.
func lockRaceTestLock(dbName string) *storage.Lock {
	return &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}
}

// createApplyRecordingLock stores an apply in applyState that records lock as
// the lock it runs under.
func createApplyRecordingLock(ctx context.Context, t *testing.T, svc *api.Service, lock *storage.Lock, identifier, applyState string) {
	t.Helper()
	_, err := svc.Storage().Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: identifier,
		LockID:          lock.ID,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Environment:     "staging",
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		State:           applyState,
		Engine:          "spirit",
	})
	require.NoError(t, err)
}

// A PR closes while nothing is in flight on its database, and an apply that
// records the PR's lock is confirmed on another pod after close cleanup read
// the PR's applies. Or the PR's apply is stopped, which the applies read
// treats as finished. Either way the apply still runs under the lock, so close
// cleanup keeps the lock and the PR's stored check state, and the delivery
// completes as it does for an apply the read found in flight.
func TestE2EPRCloseCleanupKeepsLockAnUnsettledApplyRecords(t *testing.T) {
	tests := []struct {
		name             string
		dbName           string
		applyState       string
		createdAfterRead bool
	}{
		{name: "apply started after the applies read", dbName: "webhook_close_lock_race", applyState: state.Apply.Running, createdAfterRead: true},
		{name: "stopped apply", dbName: "webhook_close_lock_stopped", applyState: state.Apply.Stopped},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			wrapper := &startApplyAfterLockListing{}
			svc := setupE2EServiceOpts(t, tc.dbName, e2eServiceOpts{
				skipOperator: true,
				wrapStorage: func(st storage.Storage) storage.Storage {
					wrapper.LockStore = st.Locks()
					return &startApplyAfterLockListingStorage{Storage: st, locks: wrapper}
				},
			})
			require.NoError(t, svc.Storage().Locks().Acquire(ctx, lockRaceTestLock(tc.dbName)))
			held, err := svc.Storage().Locks().Get(ctx, tc.dbName, storage.DatabaseTypeMySQL)
			require.NoError(t, err)
			require.NotNil(t, held)
			require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
				Repository:   "octocat/hello-world",
				PullRequest:  1,
				HeadSHA:      "abc123",
				Environment:  "staging",
				DatabaseType: storage.DatabaseTypeMySQL,
				DatabaseName: tc.dbName,
				CheckRunID:   42,
				HasChanges:   true,
				Status:       checkStatusCompleted,
				Conclusion:   checkConclusionActionRequired,
			}))
			if tc.createdAfterRead {
				started := false
				wrapper.start = func(ctx context.Context, lock *storage.Lock) {
					if started {
						return
					}
					started = true
					createApplyRecordingLock(ctx, t, svc, lock, "apply-close-race", tc.applyState)
				}
			} else {
				createApplyRecordingLock(ctx, t, svc, held, "apply-close-stopped", tc.applyState)
			}

			factory, _ := prClosedGitHub(t, "closed", 0)
			h := NewHandler(svc, factory, nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
			drainWebhookWorkOnCleanup(t, h)

			ran, err := h.runPRCloseCleanup(ctx, "octocat/hello-world", 1, 12345, false)
			require.NoError(t, err, "a lock kept for an apply is the in-flight outcome, not a failure to retry")
			assert.True(t, ran)

			lock, err := svc.Storage().Locks().Get(ctx, tc.dbName, storage.DatabaseTypeMySQL)
			require.NoError(t, err)
			require.NotNil(t, lock, "the lock an unsettled apply records must stay held")
			assert.Equal(t, held.ID, lock.ID)
			check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", storage.DatabaseTypeMySQL, tc.dbName)
			require.NoError(t, err)
			require.NotNil(t, check, "stored check state must stay while an apply recording the lock has not settled")
			assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
		})
	}
}

// A PR comments `schemabot unlock` while nothing is running on its database,
// and an apply that records the PR's lock is confirmed on another pod after
// unlock's active-apply check. Or the PR's apply is stopped, which that check
// treats as finished. Either way the apply still runs under the lock, so the
// unlock keeps the lock and answers with the apply holding it and the command
// that frees it.
func TestE2EUnlockKeepsLockAnUnsettledApplyRecords(t *testing.T) {
	tests := []struct {
		name             string
		dbName           string
		identifier       string
		applyState       string
		createdAfterRead bool
		wantBody         string
	}{
		{
			name:             "apply started after the active-apply check",
			dbName:           "webhook_unlock_lock_race",
			identifier:       "apply-unlock-race",
			applyState:       state.Apply.Running,
			createdAfterRead: true,
			wantBody:         "Wait for it to complete, or cancel it with `schemabot cancel apply-unlock-race -e staging`, then unlock again.",
		},
		{
			name:       "stopped apply",
			dbName:     "webhook_unlock_lock_stopped",
			identifier: "apply-unlock-stopped",
			applyState: state.Apply.Stopped,
			wantBody:   "Resume it with `schemabot start apply-unlock-stopped -e staging` or cancel it with `schemabot cancel apply-unlock-stopped -e staging`, then unlock again.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			wrapper := &startApplyAfterActiveApplyCheck{}
			svc := setupE2EServiceOpts(t, tc.dbName, e2eServiceOpts{
				skipOperator: true,
				wrapStorage: func(st storage.Storage) storage.Storage {
					wrapper.ApplyStore = st.Applies()
					return &startApplyAfterActiveApplyCheckStorage{Storage: st, applies: wrapper}
				},
			})
			require.NoError(t, svc.Storage().Locks().Acquire(ctx, lockRaceTestLock(tc.dbName)))
			held, err := svc.Storage().Locks().Get(ctx, tc.dbName, storage.DatabaseTypeMySQL)
			require.NoError(t, err)
			require.NotNil(t, held)
			if tc.createdAfterRead {
				started := false
				wrapper.start = func(ctx context.Context) {
					if started {
						return
					}
					started = true
					createApplyRecordingLock(ctx, t, svc, held, tc.identifier, tc.applyState)
				}
			} else {
				createApplyRecordingLock(ctx, t, svc, held, tc.identifier, tc.applyState)
			}

			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
			h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger)
			drainWebhookWorkOnCleanup(t, h)

			// The command is received after the lock was acquired, so the
			// receipt bound covers it.
			issuedAt := time.Now().Add(2 * time.Second)
			retry, err := h.unlockCommandCore(ctx, issuedAt, "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})
			require.NoError(t, err)
			assert.False(t, retry, "a lock kept for an apply is the command's answer")

			body := requireComment(t, comments, "cannot-unlock answer")
			assert.Contains(t, body, "Cannot Unlock")
			assert.Contains(t, body, tc.identifier)
			assert.Contains(t, body, tc.wantBody)
			assert.Empty(t, comments, "the cannot-unlock answer must be the command's only comment")

			lock, err := svc.Storage().Locks().Get(ctx, tc.dbName, storage.DatabaseTypeMySQL)
			require.NoError(t, err)
			require.NotNil(t, lock, "the lock an unsettled apply records must stay held")
			assert.Equal(t, held.ID, lock.ID)
		})
	}
}

// afterLockRelease runs after each lock release that succeeds, standing in
// for work another pod does once close cleanup has released a lock and before
// it deletes the PR's stored check state.
type afterLockRelease struct {
	storage.LockStore
	after func(ctx context.Context, database, dbType string)
}

func (s *afterLockRelease) ReleaseByIDIfUnused(ctx context.Context, id int64, database, dbType, owner, pendingPlanID string) error {
	if err := s.LockStore.ReleaseByIDIfUnused(ctx, id, database, dbType, owner, pendingPlanID); err != nil {
		return err
	}
	if s.after != nil {
		s.after(ctx, database, dbType)
	}
	return nil
}

type afterLockReleaseStorage struct {
	storage.Storage
	locks *afterLockRelease
}

func (s *afterLockReleaseStorage) Locks() storage.LockStore { return s.locks }

// A PR closes without merging, and close cleanup reads it as closed. The PR
// then reopens without a push, and once cleanup has released its lock, an
// `apply` takes the lock again and stores its plan's check state on the same
// head, rewriting the row cleanup listed. Cleanup deletes only what it listed
// and has not changed since: the reopened PR's check state and its new lock
// survive, the row of another database nobody touched is deleted, and the pass
// returns an error so the durable retry starts with a fresh live read.
func TestE2EPRCloseCleanupKeepsCheckStateWrittenAfterTheListing(t *testing.T) {
	dbName := "webhook_close_rewrite_race"
	otherDB := "webhook_close_rewrite_untouched"
	ctx := t.Context()
	wrapper := &afterLockRelease{}
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{
		skipOperator: true,
		wrapStorage: func(st storage.Storage) storage.Storage {
			wrapper.LockStore = st.Locks()
			return &afterLockReleaseStorage{Storage: st, locks: wrapper}
		},
	})
	require.NoError(t, svc.Storage().Locks().Acquire(ctx, lockRaceTestLock(dbName)))
	planCheck := func(database string, checkRunID int64) *storage.Check {
		return &storage.Check{
			Repository:   "octocat/hello-world",
			PullRequest:  1,
			HeadSHA:      "abc123",
			Environment:  "staging",
			DatabaseType: storage.DatabaseTypeMySQL,
			DatabaseName: database,
			CheckRunID:   checkRunID,
			HasChanges:   true,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionActionRequired,
		}
	}
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, planCheck(dbName, 42)))
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, planCheck(otherDB, 43)))

	wrapper.after = func(ctx context.Context, database, dbType string) {
		reacquired := lockRaceTestLock(database)
		reacquired.DatabaseType = dbType
		reacquired.PendingPlanID = "plan-after-reopen"
		require.NoError(t, svc.Storage().Locks().Acquire(ctx, reacquired))
		require.NoError(t, svc.Storage().Checks().Upsert(ctx, planCheck(database, 77)))
	}

	factory, _ := prClosedGitHub(t, "closed", 0)
	h := NewHandler(svc, factory, nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	drainWebhookWorkOnCleanup(t, h)

	ran, err := h.runPRCloseCleanup(ctx, "octocat/hello-world", 1, 12345, false)
	require.Error(t, err, "check state written again after the listing must fail the pass so a retry re-reads the live PR")
	assert.ErrorContains(t, err, "1 stored check rows for closed PR octocat/hello-world#1 were written again after close cleanup listed them and are kept")
	assert.False(t, ran)

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", storage.DatabaseTypeMySQL, dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "the reopened PR's plan check state must survive the close cleanup that listed the row before it was rewritten")
	assert.Equal(t, int64(77), check.CheckRunID)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)

	lock, err := svc.Storage().Locks().Get(ctx, dbName, storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, lock, "the reopened PR's lock stays held")
	assert.Equal(t, "plan-after-reopen", lock.PendingPlanID)

	untouched, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", storage.DatabaseTypeMySQL, otherDB)
	require.NoError(t, err)
	assert.Nil(t, untouched, "a listed row nobody wrote since the listing is deleted")
}
