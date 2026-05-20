//go:build integration

package mysqlstore

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestApplyStore_Create(t *testing.T) {
	clearTables(t)
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	created := createTestApply(t, store, lock, "apply_create_test", 1)

	require.NotZero(t, created.ID)
}

func TestApplyStore_CreateDuplicate(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	apply := &storage.Apply{
		ApplyIdentifier: "apply_dup_test",
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	}
	_, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)

	// Duplicate apply_identifier should fail
	apply2 := &storage.Apply{
		ApplyIdentifier: "apply_dup_test",
		LockID:          lock.ID,
		PlanID:          2,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Completed,
	}
	_, err = store.Applies().Create(ctx, apply2)
	require.ErrorIs(t, err, storage.ErrApplyIDExists)
}

func TestApplyStore_CreateBlocksActiveApplyForSameTarget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	active := createTestApply(t, store, lock, "apply_active", 1)

	_, err := store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_same_target",
		LockID:          lock.ID,
		PlanID:          2,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.ErrorIs(t, err, storage.ErrActiveApplyExists)

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_terminal_same_target",
		LockID:          lock.ID,
		PlanID:          3,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Completed,
	})
	require.NoError(t, err)

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_other_env",
		LockID:          lock.ID,
		PlanID:          4,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "production",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.NoError(t, err)

	active.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, active))

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_same_target_after_terminal",
		LockID:          lock.ID,
		PlanID:          5,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.NoError(t, err)
}

func TestApplyStore_CreateWaitsForApplyTargetLock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Hold the same target lock that the create path must acquire. The creates
	// below use the public store API; a result before release means active
	// apply writes are not serialized by the per-target lock.
	guardConn, guardLockName, err := acquireApplyTargetLockConn(ctx, testDB, "testdb", "mysql", "staging")
	require.NoError(t, err)
	releaseGuard := func() {
		if guardConn == nil {
			return
		}
		releaseApplyTargetLockConn(ctx, guardConn, guardLockName, "test active apply guard")
		guardConn = nil
	}
	t.Cleanup(releaseGuard)

	createCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err = store.Applies().Create(createCtx, &storage.Apply{
		ApplyIdentifier: "apply_waiting_same_target",
		LockID:          lock.ID,
		PlanID:          6,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	applies, err := store.Applies().GetByDatabase(ctx, "testdb", "mysql", "staging")
	require.NoError(t, err)
	assert.Empty(t, applies)

	releaseGuard()

	id, err := store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_after_target_lock_release",
		LockID:          lock.ID,
		PlanID:          7,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.NoError(t, err)
	assert.NotZero(t, id)

	applies, err = store.Applies().GetByDatabase(ctx, "testdb", "mysql", "staging")
	require.NoError(t, err)
	assert.Len(t, applies, 1)
}

func TestApplyStore_CreateAllowsConcurrentActiveAppliesForDifferentTargets(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	type applyTarget struct {
		database    string
		dbType      string
		environment string
		engine      string
	}
	targets := make([]applyTarget, 0, 16)
	for i := range 8 {
		targets = append(targets, applyTarget{
			database:    "testapp",
			dbType:      "mysql",
			environment: "env-" + strconv.Itoa(i),
			engine:      "spirit",
		})
		targets = append(targets, applyTarget{
			database:    "testapp-vitess",
			dbType:      "vitess",
			environment: "env-" + strconv.Itoa(i),
			engine:      "planetscale",
		})
	}

	locks := make(map[string]*storage.Lock)
	for _, target := range targets {
		key := target.database + "/" + target.dbType
		if _, ok := locks[key]; !ok {
			locks[key] = createTestLock(t, store, target.database, target.dbType, target.environment)
		}
	}

	start := make(chan struct{})
	errs := make(chan error, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		lock := locks[target.database+"/"+target.dbType]
		wg.Go(func() {
			<-start
			// These creates all start at the same time, but every apply targets
			// a different database/type/environment. Storage should serialize
			// only same-target active applies, so every independent target can
			// create its first active apply successfully.
			_, err := store.Applies().Create(ctx, &storage.Apply{
				ApplyIdentifier: "apply_concurrent_target_" + strconv.Itoa(i),
				LockID:          lock.ID,
				PlanID:          int64(20 + i),
				Database:        target.database,
				DatabaseType:    target.dbType,
				Repository:      "org/repo",
				PullRequest:     123,
				Environment:     target.environment,
				Engine:          target.engine,
				State:           state.Apply.Pending,
			})
			errs <- err
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	close(start)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "concurrent active apply creates for different targets blocked")
	}
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	for _, target := range targets {
		applies, err := store.Applies().GetByDatabase(ctx, target.database, target.dbType, target.environment)
		require.NoError(t, err)
		assert.Len(t, applies, 1)
	}
}

func TestApplyStore_Get(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Get non-existent should return nil
	apply, err := store.Applies().Get(ctx, 99999)
	require.NoError(t, err)
	require.Nil(t, apply)

	// Create apply
	created := createTestApply(t, store, lock, "apply_get_test", 123)

	// Get should return the apply
	apply, err = store.Applies().Get(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, "apply_get_test", apply.ApplyIdentifier)
	require.Equal(t, "testdb", apply.Database)
}

func TestApplyStore_GetByApplyIdentifier(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Get non-existent should return nil
	apply, err := store.Applies().GetByApplyIdentifier(ctx, "nonexistent")
	require.NoError(t, err)
	require.Nil(t, apply)

	// Create apply
	createTestApply(t, store, lock, "apply_byid_test", 42)

	// GetByApplyIdentifier should return the apply
	apply, err = store.Applies().GetByApplyIdentifier(ctx, "apply_byid_test")
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, int64(42), apply.PlanID)
}

func TestApplyStore_GetByPlan(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Get non-existent should return nil
	apply, err := store.Applies().GetByPlan(ctx, 99999)
	require.NoError(t, err)
	require.Nil(t, apply)

	// Create apply with a specific plan_id
	created := createTestApply(t, store, lock, "apply_byplan", 12345)

	// GetByPlan should return the apply
	apply, err = store.Applies().GetByPlan(ctx, 12345)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, created.ApplyIdentifier, apply.ApplyIdentifier)
}

func TestApplyStore_GetByLock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// GetByLock with no applies should return empty slice
	applies, err := store.Applies().GetByLock(ctx, lock.ID)
	require.NoError(t, err)
	require.Empty(t, applies)

	// Create two applies for the same lock.
	first := createTestApply(t, store, lock, "apply_first", 100)
	first.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, first))
	createTestApply(t, store, lock, "apply_second", 101)

	// GetByLock should return both applies
	applies, err = store.Applies().GetByLock(ctx, lock.ID)
	require.NoError(t, err)
	require.Len(t, applies, 2)
}

func TestApplyStore_GetByDatabase(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create locks for different databases
	lock1 := createTestLock(t, store, "db1", "mysql", "staging")
	lock2 := createTestLock(t, store, "db2", "mysql", "staging")

	// Create applies
	createTestApply(t, store, lock1, "apply_db1", 200)
	createTestApply(t, store, lock2, "apply_db2", 201)

	// GetByDatabase should only return applies for db1
	applies, err := store.Applies().GetByDatabase(ctx, "db1", "mysql", "staging")
	require.NoError(t, err)
	require.Len(t, applies, 1)
	require.Equal(t, "apply_db1", applies[0].ApplyIdentifier)
}

func TestApplyStore_Update(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_update", 300)

	// Update state
	apply.State = state.Apply.Running
	apply.ErrorMessage = ""
	now := time.Now()
	apply.StartedAt = &now

	require.NoError(t, store.Applies().Update(ctx, apply))

	// Verify update
	updated, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.Equal(t, state.Apply.Running, updated.State)
	require.NotNil(t, updated.StartedAt)
}

func TestApplyStore_UpdateBlocksActiveApplyForSameTarget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	active := createTestApply(t, store, lock, "apply_update_active", 301)
	completed := createTestApplyWithStateAndEnv(t, store, lock, "apply_update_completed", 302, state.Apply.Completed, "staging")

	completed.State = state.Apply.Running
	require.ErrorIs(t, store.Applies().Update(ctx, completed), storage.ErrActiveApplyExists)

	active.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, active))
	require.NoError(t, store.Applies().Update(ctx, completed))
}

func TestApplyStore_UpdateNonExistent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := &storage.Apply{
		ID:    99999,
		State: state.Apply.Running,
	}

	// Update on a non-existent row is a no-op (0 rows affected), not an error.
	// MySQL UPDATE with WHERE id=? succeeds even when no row matches.
	require.NoError(t, store.Applies().Update(ctx, apply))
}

func TestApplyStore_GetInProgress(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	pending := createTestApply(t, store, lock, "apply_pending", 400)
	running := createTestApplyWithStateAndEnv(t, store, lock, "apply_running", 401, state.Apply.Running, "production")
	completed := createTestApplyWithStateAndEnv(t, store, lock, "apply_completed", 402, state.Apply.Completed, "staging")
	failed := createTestApplyWithStateAndEnv(t, store, lock, "apply_failed", 403, state.Apply.Failed, "staging")

	require.NotZero(t, completed.ID)
	require.NotZero(t, failed.ID)

	// GetInProgress should return only pending and running
	applies, err := store.Applies().GetInProgress(ctx)
	require.NoError(t, err)
	require.Len(t, applies, 2)

	// Verify we got the right ones
	applyIDs := make(map[string]bool)
	for _, a := range applies {
		applyIDs[a.ApplyIdentifier] = true
	}
	assert.True(t, applyIDs[pending.ApplyIdentifier], "expected pending apply")
	assert.True(t, applyIDs[running.ApplyIdentifier], "expected running apply")
}

// TestApplyStore_FindNextApplyClaimsRetryable verifies the storage-level retry
// claim behavior. The caller sees the retryable state that was claimed, while
// the stored row is leased as running with an incremented apply attempt.
func TestApplyStore_FindNextApplyClaimsRetryable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_claim", 500, state.Apply.FailedRetryable, "staging")
	apply.ErrorMessage = "transient engine failure"
	require.NoError(t, store.Applies().Update(ctx, apply))

	claimed, err := store.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.FailedRetryable, claimed.State)
	assert.Equal(t, 1, claimed.Attempt)
	assert.Empty(t, claimed.ErrorMessage)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)
	assert.Equal(t, 1, persisted.Attempt)
	assert.Empty(t, persisted.ErrorMessage)

	claimedAgain, err := store.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimedAgain)
}

func TestApplyStore_FindNextApplyRequiresTasksForPendingApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_pending_claim", 502, state.Apply.Pending, "staging")

	// A pending apply record can be visible before its task rows are written.
	// The scheduler must wait for the task list so dispatch has concrete table
	// work to run.
	claimedBeforeTasks, err := store.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimedBeforeTasks, "pending applies are not ready for scheduler dispatch until their tasks are persisted")

	now := time.Now()
	_, err = store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_pending_claim",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "users",
		DDL:            "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY)",
		DDLAction:      "CREATE",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	// Once at least one task exists, the pending apply is ready to claim. The
	// caller sees the state it claimed, and the stored row is leased as running.
	claimed, err := store.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.Pending, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)
}

// TestApplyStore_ExpireRetryable verifies retry budget exhaustion at the
// storage layer. A retryable apply that has used all attempts becomes failed,
// and unfinished tasks are finalized as failed with completion timestamps.
func TestApplyStore_ExpireRetryable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_expired", 501, state.Apply.FailedRetryable, "staging")
	apply.Attempt = maxRecoveryAttempts
	require.NoError(t, store.Applies().Update(ctx, apply))

	now := time.Now()
	_, err := store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_retryable_expired",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.FailedRetryable,
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)
	_, err = store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_retryable_pending",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "posts",
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	expired, err := store.Applies().ExpireRetryable(ctx)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	assert.Equal(t, apply.ApplyIdentifier, expired[0].ApplyIdentifier)
	assert.Equal(t, state.Apply.Failed, expired[0].State)
	assert.NotNil(t, expired[0].CompletedAt)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Failed, persisted.State)
	assert.NotNil(t, persisted.CompletedAt)

	task, err := store.Tasks().Get(ctx, "task_retryable_expired")
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, state.Task.Failed, task.State)
	assert.NotNil(t, task.CompletedAt)

	pendingTask, err := store.Tasks().Get(ctx, "task_retryable_pending")
	require.NoError(t, err)
	require.NotNil(t, pendingTask)
	assert.Equal(t, state.Task.Failed, pendingTask.State)
	assert.NotNil(t, pendingTask.CompletedAt)
}

func TestApplyStore_FindMissingSummaryComment_ExcludesAppliesWithoutGitHubDestination(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	now := time.Now()
	startedAt := now.Add(-time.Minute)

	githubLock := createTestLockWithPR(t, store, "github_db", storage.DatabaseTypeMySQL, "staging", "org/repo", 123)
	githubApply := &storage.Apply{
		ApplyIdentifier: "apply_missing_summary_github",
		LockID:          githubLock.ID,
		PlanID:          600,
		Database:        githubLock.DatabaseName,
		DatabaseType:    githubLock.DatabaseType,
		Repository:      githubLock.Repository,
		PullRequest:     githubLock.PullRequest,
		Environment:     "staging",
		Caller:          "org/repo#123",
		InstallationID:  12345,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
	}
	githubApplyID, err := store.Applies().Create(ctx, githubApply)
	require.NoError(t, err)
	githubApply.ID = githubApplyID
	githubApply.StartedAt = &startedAt
	githubApply.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, githubApply))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         githubApply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 1001,
	}))

	cliLock := createTestLockWithPR(t, store, "cli_db", storage.DatabaseTypeMySQL, "staging", "", 0)
	cliApply := &storage.Apply{
		ApplyIdentifier: "apply_missing_summary_cli",
		LockID:          cliLock.ID,
		PlanID:          601,
		Database:        cliLock.DatabaseName,
		DatabaseType:    cliLock.DatabaseType,
		Repository:      cliLock.Repository,
		PullRequest:     cliLock.PullRequest,
		Environment:     "staging",
		Caller:          "cli:user@host",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
	}
	cliApplyID, err := store.Applies().Create(ctx, cliApply)
	require.NoError(t, err)
	cliApply.ID = cliApplyID
	cliApply.StartedAt = &startedAt
	cliApply.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, cliApply))

	// Even if a CLI-style apply somehow has a progress marker, it cannot be
	// reconciled into a GitHub summary without repository, PR, and installation ID.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         cliApply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 1002,
	}))

	applies, err := store.Applies().FindMissingSummaryComment(ctx)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, githubApply.ApplyIdentifier, applies[0].ApplyIdentifier)
}

func TestApplyStore_GetByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create locks for different PRs
	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "staging", "org/repo", 100)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "staging", "org/repo", 200)

	// Create applies
	createTestApply(t, store, lock1, "apply_pr100", 500)
	createTestApply(t, store, lock2, "apply_pr200", 501)

	// GetByPR should only return applies for PR 100
	applies, err := store.Applies().GetByPR(ctx, "org/repo", 100)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	require.Equal(t, 100, applies[0].PullRequest)
}

func TestApplyStore_Delete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_delete", 600)

	// Delete should succeed
	require.NoError(t, store.Applies().Delete(ctx, apply.ID))

	// Verify deleted
	deleted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.Nil(t, deleted)

	// Delete non-existent should fail
	require.ErrorIs(t, store.Applies().Delete(ctx, apply.ID), storage.ErrApplyNotFound)
}

func TestApplyStore_DeleteByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create locks for different PRs
	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "staging", "org/repo", 100)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "staging", "org/repo", 100)
	lock3 := createTestLockWithPR(t, store, "db3", "mysql", "staging", "org/repo", 200)

	// Create applies
	createTestApply(t, store, lock1, "apply_pr100_1", 701)
	createTestApply(t, store, lock2, "apply_pr100_2", 702)
	createTestApply(t, store, lock3, "apply_pr200", 703)

	// DeleteByPR should only delete applies for PR 100
	require.NoError(t, store.Applies().DeleteByPR(ctx, "org/repo", 100))

	// Verify PR 100 applies deleted
	applies, err := store.Applies().GetByPR(ctx, "org/repo", 100)
	require.NoError(t, err)
	require.Empty(t, applies)

	// Verify PR 200 apply still exists
	applies, err = store.Applies().GetByPR(ctx, "org/repo", 200)
	require.NoError(t, err)
	require.Len(t, applies, 1)
}

func TestApplyStore_Options(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Create apply with options
	apply := &storage.Apply{
		ApplyIdentifier: "apply_options_test",
		LockID:          lock.ID,
		PlanID:          800,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{
		AllowUnsafe:  true,
		DeferCutover: true,
		SkipRevert:   false,
		Volume:       5,
	})

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)

	// Retrieve and verify options
	retrieved, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)

	opts := retrieved.GetOptions()
	assert.True(t, opts.AllowUnsafe)
	assert.True(t, opts.DeferCutover)
	assert.False(t, opts.SkipRevert)
	assert.Equal(t, 5, opts.Volume)
}

func TestApplyStore_AllFields(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	now := time.Now().Truncate(time.Second)
	apply := &storage.Apply{
		ApplyIdentifier: "apply_allfields",
		LockID:          lock.ID,
		PlanID:          900,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Caller:          "cli:user@host",
		ExternalID:      "ext_remote_abc123",
		Engine:          "spirit",
		State:           state.Apply.WaitingForCutover,
		ErrorMessage:    "test error",
		Attempt:         3,
	}
	apply.SetOptions(storage.ApplyOptions{
		AllowUnsafe:  true,
		DeferCutover: true,
		SkipRevert:   true,
	})

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = id

	// Update with timestamps
	apply.StartedAt = &now
	completedTime := now.Add(time.Hour)
	apply.CompletedAt = &completedTime
	apply.State = state.Apply.Completed

	require.NoError(t, store.Applies().Update(ctx, apply))

	// Retrieve and verify all fields
	retrieved, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)

	assert.Equal(t, "apply_allfields", retrieved.ApplyIdentifier)
	assert.Equal(t, lock.ID, retrieved.LockID)
	assert.Equal(t, int64(900), retrieved.PlanID)
	assert.Equal(t, "testdb", retrieved.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, retrieved.DatabaseType)
	assert.Equal(t, "org/repo", retrieved.Repository)
	assert.Equal(t, 123, retrieved.PullRequest)
	assert.Equal(t, "staging", retrieved.Environment)
	assert.Equal(t, "cli:user@host", retrieved.Caller)
	assert.Equal(t, "ext_remote_abc123", retrieved.ExternalID)
	assert.Equal(t, "spirit", retrieved.Engine)
	assert.Equal(t, state.Apply.Completed, retrieved.State)
	assert.Equal(t, "test error", retrieved.ErrorMessage)
	assert.Equal(t, 3, retrieved.Attempt)
	assert.NotNil(t, retrieved.StartedAt)
	assert.NotNil(t, retrieved.CompletedAt)

	// Verify options
	opts := retrieved.GetOptions()
	assert.True(t, opts.AllowUnsafe)
	assert.True(t, opts.DeferCutover)
	assert.True(t, opts.SkipRevert)
}

// Helper functions

func createTestLock(t *testing.T, store *Storage, dbName, dbType, env string) *storage.Lock {
	t.Helper()
	return createTestLockWithPR(t, store, dbName, dbType, env, "org/repo", 123)
}

func createTestLockWithPR(t *testing.T, store *Storage, dbName, dbType, env, repo string, pr int) *storage.Lock {
	t.Helper()
	ctx := t.Context()

	_ = env // unused, but kept for API compatibility with tests

	lock := &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: dbType,
		Repository:   repo,
		PullRequest:  pr,
		Owner:        "testuser",
	}

	require.NoError(t, store.Locks().Acquire(ctx, lock))

	lock, err := store.Locks().Get(ctx, dbName, dbType)
	require.NoError(t, err)
	return lock
}

func createTestApply(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64) *storage.Apply {
	t.Helper()
	return createTestApplyWithEnv(t, store, lock, applyID, planID, "staging")
}

func createTestApplyWithEnv(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, env string) *storage.Apply {
	t.Helper()
	return createTestApplyWithStateAndEnv(t, store, lock, applyID, planID, state.Apply.Pending, env)
}

func createTestApplyWithStateAndEnv(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, applyState, env string) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	apply := &storage.Apply{
		ApplyIdentifier: applyID,
		LockID:          lock.ID,
		PlanID:          planID,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     env,
		Engine:          "spirit",
		State:           applyState,
	}

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = id
	return apply
}

// DB error tests

func TestApplyStore_Create_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "test",
		State:           state.Apply.Pending,
	})
	require.Error(t, err)
}

func TestApplyStore_Get_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().Get(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_GetByApplyIdentifier_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByApplyIdentifier(t.Context(), "test")
	require.Error(t, err)
}

func TestApplyStore_GetByLock_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByLock(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_GetInProgress_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetInProgress(t.Context())
	require.Error(t, err)
}

func TestApplyStore_Update_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Applies().Update(t.Context(), &storage.Apply{ID: 1, State: "running"})
	require.Error(t, err)
}

func TestApplyStore_Delete_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Applies().Delete(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_DeleteByPR_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Applies().DeleteByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestApplyStore_GetByDatabase_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByDatabase(t.Context(), "db", "mysql", "staging")
	require.Error(t, err)
}

func TestApplyStore_GetByPR_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestApplyStore_GetByPlan_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByPlan(t.Context(), 123)
	require.Error(t, err)
}
