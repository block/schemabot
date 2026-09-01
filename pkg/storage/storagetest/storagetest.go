// Package storagetest provides a behavioral parity suite for storage.Storage
// implementations. Every implementation must pass the same suite, so the
// tests here assert on the storage interface contract — typed results, typed
// errors, ordering, and state transitions — never on an implementation's SQL
// text or dialect-specific behavior.
//
// Test placement follows one rule: a behavior reachable through the public
// storage interface belongs in this package, where every dialect runs it.
// An implementation's own test files (for example pkg/storage/internal/
// sqlstore) cover only behaviors that require raw SQL or database-specific
// conditions to set up or observe.
//
// An implementation package wires itself in by implementing Harness against
// its own test fixture (container, schema bootstrap, table clearing) and
// calling Run from its integration tests. Per-family Test functions are also
// exported so a family can be exercised in isolation while it is being
// brought up on a new implementation.
//
// Timestamp assertions: stored timestamp precision is dialect- and
// column-specific. Plain MySQL datetime columns are whole-second and round
// fractional seconds, so a stored time can come back up to half a second
// later than the written one; MySQL datetime(6) columns (for example
// webhook_events.lease_expires_at) and PostgreSQL timestamp columns are
// microsecond, so a nanosecond-carrying time.Time round-trips
// microsecond-truncated. Parity families must never assert exact equality
// between a written time.Time and its stored round-trip. Compare stored
// times by ordering, by require.WithinDuration with at least one second of
// tolerance, or by truncating the written time to whole seconds before
// storing it — truncating only at assertion time still flakes against
// MySQL's rounding. Size a lease, claim, or cutoff margin to the column's
// precision: sub-second margins are safe only on microsecond columns.
package storagetest

import (
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

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

type parityFamily struct {
	storageMethod string
	test          func(*testing.T, Harness)
}

var parityFamilies = []parityFamily{
	{storageMethod: "Plans", test: TestPlans},
	{storageMethod: "PlanComments", test: TestPlanComments},
	{storageMethod: "Settings", test: TestSettings},
	{storageMethod: "ApplyLogs", test: TestApplyLogs},
	{storageMethod: "Locks", test: TestLocks},
	{storageMethod: "Applies", test: TestApplies},
	{storageMethod: "ApplyOperations", test: TestApplyOperations},
	{storageMethod: "ApplyComments", test: TestApplyComments},
	{storageMethod: "ControlRequests", test: TestControlRequests},
	{storageMethod: "Tasks", test: TestTasks},
	{storageMethod: "WebhookEvents", test: TestWebhookEvents},
	{storageMethod: "Checks", test: TestChecks},
}

// Run executes every parity family against the harness. Implementations call
// Run from a single integration test so a storage backend cannot silently
// opt out of part of the contract — each new family is added here as it
// lands.
func Run(t *testing.T, h Harness) {
	for _, family := range parityFamilies {
		t.Run(family.storageMethod, func(t *testing.T) { family.test(t, h) })
	}
}

func assertParityFamilyCoverage(t *testing.T) {
	t.Helper()

	storageType := reflect.TypeFor[storage.Storage]()
	allowedMethods := map[string]bool{
		"Close": false,
		"Ping":  false,
	}
	storageMethods := make(map[string]bool, storageType.NumMethod())
	for method := range storageType.Methods() {
		if _, allowed := allowedMethods[method.Name]; allowed {
			allowedMethods[method.Name] = true
			continue
		}
		storageMethods[method.Name] = false
	}

	for _, family := range parityFamilies {
		covered, exists := storageMethods[family.storageMethod]
		require.True(t, exists, "parity family %q does not match a storage.Storage method", family.storageMethod)
		require.False(t, covered, "storage.Storage method %q has more than one parity family", family.storageMethod)
		storageMethods[family.storageMethod] = true

		testFunc := runtime.FuncForPC(reflect.ValueOf(family.test).Pointer())
		require.NotNil(t, testFunc, "resolve parity family test for storage.Storage method %q", family.storageMethod)
		require.True(t, strings.HasSuffix(testFunc.Name(), ".Test"+family.storageMethod),
			"parity family for storage.Storage method %q uses %q; want Test%s", family.storageMethod, testFunc.Name(), family.storageMethod)
	}

	var uncovered []string
	for method, covered := range storageMethods {
		if !covered {
			uncovered = append(uncovered, method)
		}
	}
	sort.Strings(uncovered)
	require.Empty(t, uncovered, "storage.Storage methods missing parity families")

	var staleAllowlist []string
	for method, exists := range allowedMethods {
		if !exists {
			staleAllowlist = append(staleAllowlist, method)
		}
	}
	sort.Strings(staleAllowlist)
	require.Empty(t, staleAllowlist, "allowlisted methods missing from storage.Storage")
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

// CreateClaimedApply creates a pending apply under the given lock, persists
// one task for it so it is ready for driver dispatch, and claims it for the
// given driver. The returned apply carries the live lease
// (LeaseOwner / LeaseToken), so lease-guard scenarios can build owned and
// stale lease contexts entirely through the storage interface.
func CreateClaimedApply(t *testing.T, store storage.Storage, lock *storage.Lock, applyID string, planID int64, owner string) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	apply := CreateApplyWithTask(t, store, lock, applyID, planID)
	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, owner)
	require.NoError(t, err)
	require.NotNil(t, claimed, "an apply with a persisted task must be claimable")
	return claimed
}

// CreateApplyWithTask creates a pending apply and its canonical task fixture.
func CreateApplyWithTask(t *testing.T, store storage.Storage, lock *storage.Lock, applyID string, planID int64) *storage.Apply {
	t.Helper()
	apply := CreateApply(t, store, lock, applyID, planID)
	now := time.Now().UTC().Truncate(time.Second)
	task := newTask(apply, "task_"+applyID, "users", now)
	task.DDL = "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY)"
	task.DDLAction = "create"
	_, err := store.Tasks().Create(t.Context(), task)
	require.NoError(t, err)
	return apply
}
