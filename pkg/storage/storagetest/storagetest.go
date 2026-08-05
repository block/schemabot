// Package storagetest provides a behavioral parity suite for storage.Storage
// implementations. Every implementation must pass the same suite, so the
// tests here assert on the storage interface contract — typed results, typed
// errors, ordering, and state transitions — never on an implementation's SQL
// text or dialect-specific behavior.
//
// An implementation package wires itself in by implementing Harness against
// its own test fixture (container, schema bootstrap, table clearing) and
// calling Run from its integration tests. Per-family Test functions are also
// exported so a family can be exercised in isolation while it is being
// brought up on a new implementation.
package storagetest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Harness supplies dialect-specific fixtures to the parity suite. Each
// storage implementation provides one, backed by its own integration-test
// infrastructure.
//
// The suite runs subtests sequentially (no t.Parallel), so a harness may
// assume only one Storage from NewStorage is live at a time.
type Harness interface {
	// NewStorage returns a Storage backed by an empty, fully bootstrapped
	// schema. The suite calls it at the start of every subtest; state
	// written by earlier subtests must not be visible through the returned
	// Storage. Per-subtest truncation, fresh schemas, and transaction
	// rollback are all valid strategies.
	NewStorage(t *testing.T) storage.Storage
	// NewUnreachableStorage returns a Storage whose backing database
	// connection is unusable (for example, already closed), so the suite can
	// verify that storage methods surface connection failures as errors
	// instead of swallowing them.
	NewUnreachableStorage(t *testing.T) storage.Storage
}

// Run executes every parity family against the harness. Implementations call
// Run from a single integration test so a storage backend cannot silently
// opt out of part of the contract — each new family is added here as it
// lands.
func Run(t *testing.T, h Harness) {
	t.Run("Settings", func(t *testing.T) { TestSettings(t, h) })
	t.Run("ApplyLogs", func(t *testing.T) { TestApplyLogs(t, h) })
}

// Fixture helpers. These build the canonical Lock/Apply rows used by the
// parity families, and implementation packages delegate their own test
// fixtures here so both suites always validate the same row shape.

// CreateLock acquires a lock for the given target and returns the stored row.
func CreateLock(t *testing.T, store storage.Storage, dbName, dbType string) *storage.Lock {
	t.Helper()
	return CreateLockWithPR(t, store, dbName, dbType, "org/repo", 123)
}

// CreateLockWithPR acquires a lock for the given target owned by the given
// repository and pull request, and returns the stored row.
func CreateLockWithPR(t *testing.T, store storage.Storage, dbName, dbType, repo string, pr int) *storage.Lock {
	t.Helper()
	ctx := t.Context()

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

// CreateApply creates a pending apply in the staging environment under the
// given lock and returns it with its stored ID.
func CreateApply(t *testing.T, store storage.Storage, lock *storage.Lock, applyID string, planID int64) *storage.Apply {
	t.Helper()
	return CreateApplyWithEnv(t, store, lock, applyID, planID, "staging")
}

// CreateApplyWithEnv creates a pending apply in the given environment under
// the given lock and returns it with its stored ID.
func CreateApplyWithEnv(t *testing.T, store storage.Storage, lock *storage.Lock, applyID string, planID int64, env string) *storage.Apply {
	t.Helper()
	return CreateApplyWithStateAndEnv(t, store, lock, applyID, planID, state.Apply.Pending, env)
}

// CreateApplyWithStateAndEnv creates an apply in the given state and
// environment under the given lock and returns it with its stored ID.
func CreateApplyWithStateAndEnv(t *testing.T, store storage.Storage, lock *storage.Lock, applyID string, planID int64, applyState, env string) *storage.Apply {
	t.Helper()
	return CreateApplyWithStateEnvDeployment(t, store, lock, applyID, planID, applyState, env, "")
}

// CreateApplyWithStateEnvDeployment creates an apply in the given state,
// environment, and deployment under the given lock and returns it with its
// stored ID.
func CreateApplyWithStateEnvDeployment(t *testing.T, store storage.Storage, lock *storage.Lock, applyID string, planID int64, applyState, env, deployment string) *storage.Apply {
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
		Deployment:      deployment,
		Engine:          storage.EngineForType(lock.DatabaseType),
		State:           applyState,
	}

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = id
	return apply
}
