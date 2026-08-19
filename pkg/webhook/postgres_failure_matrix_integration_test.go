//go:build integration

package webhook

import (
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// A durably queued PostgreSQL apply survives a server restart: the first
// service instance accepts the apply and stores it without driving it, and a
// fresh instance — new operator, new engine with empty in-memory progress —
// claims the queued work and drives the native change to completion on the
// target. This is the crash-window guarantee operators rely on: an apply
// acknowledged on the PR is never lost to a pod restart.
func TestPostgresConfigFixtureQueuedApplySurvivesRestart(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres")
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	createFixtureUsersTable(t, db)

	// First instance: accept and durably queue the apply, but never drive it —
	// the process "crashes" before its operator claims the work.
	svcA := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType: string(fixture.config.Type),
		targetDSN:    dsn,
		skipOperator: true,
	})
	plan, err := svcA.ExecutePlan(t.Context(), fixture.planRequest())
	require.NoError(t, err)
	require.False(t, plan.HasBlockedChanges())
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	assert.Empty(t, plan.Changes[0].TableChanges[0].ExecutionMode,
		"the fixture change must plan as native-safe, never routed to a synchronous execution path")

	_, _, err = svcA.ExecuteApply(t.Context(), api.ApplyRequest{
		PlanID:      plan.PlanID,
		Environment: "staging",
		Caller:      "integration-test",
	})
	require.NoError(t, err)

	queued, err := findFixtureApply(t, svcA, fixture.config.Database)
	require.NoError(t, err)
	require.NotNil(t, queued, "accepted apply must be durably stored before any driving")
	assert.True(t, state.IsState(queued.State, state.Apply.Pending),
		"apply must still be pending when the first instance dies, state=%s", queued.State)
	assert.False(t, postgresColumnExists(t, db, "users", "email"),
		"no DDL may reach the target before an operator drives the apply")

	require.NoError(t, svcA.Close())

	// Second instance over the same storage: its operator must find the
	// queued apply and drive it to completion with no in-memory state from
	// the first instance.
	svcB := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType:         string(fixture.config.Type),
		targetDSN:            dsn,
		preserveDurableState: true,
	})
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		apply, err := findFixtureApply(t, svcB, fixture.config.Database)
		if !assert.NoError(c, err) || !assert.NotNil(c, apply, "no apply stored for the fixture PR") {
			return
		}
		assert.True(c, state.IsState(apply.State, state.Apply.Completed),
			"recovered apply not completed yet, state=%s", apply.State)
	}, postgresConfigFixtureDeadline, 100*time.Millisecond)

	assert.True(t, postgresColumnExists(t, db, "users", "email"),
		"the recovered apply must land the planned column on the target")

	require.NoError(t, svcB.Close())
}

// A stop accepted while a PostgreSQL apply is running resolves as a terminal
// decline once the driver consumes it: the engine's reason is stored for the
// operator, the request does not remain pending for another claim, and the
// unsupported stop is never mistaken for a successful stop. The apply remains
// authoritative and completes its native DDL on the target once its lock clears.
func TestPostgresConfigFixtureStopDeclineIsTerminalAndApplyCompletes(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres")
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	createFixtureUsersTable(t, db)

	svc := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType: string(fixture.config.Type),
		targetDSN:    dsn,
	})
	plan, err := svc.ExecutePlan(t.Context(), fixture.planRequest())
	require.NoError(t, err)
	require.False(t, plan.HasBlockedChanges())

	// Hold the target table so the native ALTER remains observably running
	// while the durable stop traverses the public control path.
	lockTx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lockTx.Rollback() })
	_, err = lockTx.ExecContext(t.Context(), "LOCK TABLE public.users IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	_, _, err = svc.ExecuteApply(t.Context(), api.ApplyRequest{
		PlanID:      plan.PlanID,
		Environment: "staging",
		Caller:      "integration-test",
	})
	require.NoError(t, err)
	var running *storage.Apply
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		apply, getErr := findFixtureApply(t, svc, fixture.config.Database)
		if !assert.NoError(c, getErr) || !assert.NotNil(c, apply, "no apply stored for the fixture PR") {
			return
		}
		if assert.True(c, state.IsState(apply.State, state.Apply.Running),
			"apply not running yet, state=%s", apply.State) {
			running = apply
		}
	}, postgresConfigFixtureDeadline, 100*time.Millisecond)

	stopResp, err := svc.ExecuteStop(t.Context(), apitypes.ControlRequest{
		ApplyID:     running.ApplyIdentifier,
		Environment: running.Environment,
		Caller:      "integration-test",
	})
	require.NoError(t, err)
	require.True(t, stopResp.Accepted)
	pending, err := svc.Storage().ControlRequests().GetPending(t.Context(), running.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, pending, "accepted stop must be durable before the driver consumes it")

	var declined *storage.ApplyControlRequest
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		controlReq, getErr := svc.Storage().ControlRequests().GetByOperation(t.Context(), running.ID, storage.ControlOperationStop)
		if !assert.NoError(c, getErr) || !assert.NotNil(c, controlReq, "durable stop request disappeared") {
			return
		}
		if !assert.Equal(c, storage.ControlRequestFailed, controlReq.Status,
			"unsupported stop must resolve terminally, status=%s", controlReq.Status) {
			return
		}
		declined = controlReq

		apply, getErr := findFixtureApply(t, svc, fixture.config.Database)
		if !assert.NoError(c, getErr) || !assert.NotNil(c, apply, "no apply stored for the fixture PR") {
			return
		}
		assert.False(c, state.IsState(apply.State, state.Apply.Stopped),
			"a declined stop must not be interpreted as a successful operator stop")
	}, postgresConfigFixtureDeadline, 100*time.Millisecond)
	assert.Contains(t, declined.ErrorMessage, "stop is not supported for PostgreSQL schema changes",
		"the terminal request must carry the engine's typed decline reason")
	assert.NotNil(t, declined.CompletedAt, "a terminally declined request must be settled")
	require.NoError(t, lockTx.Rollback())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		apply, getErr := findFixtureApply(t, svc, fixture.config.Database)
		if !assert.NoError(c, getErr) || !assert.NotNil(c, apply, "no apply stored for the fixture PR") {
			return
		}
		assert.True(c, state.IsState(apply.State, state.Apply.Completed),
			"apply did not complete after the stop decline, state=%s", apply.State)
	}, postgresConfigFixtureDeadline, 100*time.Millisecond)
	assert.True(t, postgresColumnExists(t, db, "users", "email"),
		"the apply must land its planned column after the stop is declined")
}

// A native-safe plan whose apply is refused at execution time — here a target
// role without ALTER rights — lands as a permanent failure, not a retryable
// one: the task is Failed (never FailedRetryable), the stored error carries
// the exact provisioning GRANT, and no DDL reaches the target. Retrying
// cannot succeed until the role is provisioned, so the drive must not retry.
func TestPostgresConfigFixtureApplyPrivilegeRefusalIsPermanent(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres")
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	createFixtureUsersTable(t, db)
	// The limited role can plan (CONNECT, USAGE, and CREATE for the diff
	// planner's scratch schema) but does not own public.users, so the apply's
	// privilege preflight must refuse the ALTER.
	_, err := db.ExecContext(t.Context(), fmt.Sprintf(`
		CREATE ROLE limited LOGIN PASSWORD 'limited';
		GRANT CONNECT, CREATE ON DATABASE %s TO limited;
		GRANT USAGE ON SCHEMA public TO limited`, fixture.config.Database))
	require.NoError(t, err)

	limitedDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	limitedDSN.User = url.UserPassword("limited", "limited")

	svc := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType: string(fixture.config.Type),
		targetDSN:    limitedDSN.String(),
	})
	plan, err := svc.ExecutePlan(t.Context(), fixture.planRequest())
	require.NoError(t, err)
	require.False(t, plan.HasBlockedChanges(),
		"the statement shape is native-safe; only the role's privileges are insufficient")

	_, _, err = svc.ExecuteApply(t.Context(), api.ApplyRequest{
		PlanID:      plan.PlanID,
		Environment: "staging",
		Caller:      "integration-test",
	})
	require.NoError(t, err)

	var failed *storage.Apply
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		apply, err := findFixtureApply(t, svc, fixture.config.Database)
		if !assert.NoError(c, err) || !assert.NotNil(c, apply, "no apply stored for the fixture PR") {
			return
		}
		if !assert.True(c, applyIsSettled(apply.State),
			"apply not settled yet, state=%s", apply.State) {
			return
		}
		failed = apply
	}, postgresConfigFixtureDeadline, 100*time.Millisecond)
	assert.True(t, state.IsState(failed.State, state.Apply.Failed),
		"a privilege refusal must fail the apply, state=%s", failed.State)

	tasks, err := svc.Storage().Tasks().GetByApplyID(t.Context(), failed.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	task := tasks[0]
	assert.True(t, state.IsState(task.State, state.Task.Failed),
		"a refusal is permanent and must not be marked retryable, state=%s", task.State)
	assert.Contains(t, task.ErrorMessage, "insufficient privileges; provision with:",
		"the stored error must be the classified refusal detail built from typed error fields, never wrapped server output")
	assert.Contains(t, task.ErrorMessage, "GRANT",
		"the stored error must carry the exact provisioning statement")
	assert.NotNil(t, task.CompletedAt, "a permanently failed task is settled, not awaiting retry")

	assert.False(t, postgresColumnExists(t, db, "users", "email"),
		"a refused apply must not execute any DDL on the target")
}

// createFixtureUsersTable creates the fixture's starting users table on the
// target: the declared schema minus the email column the plan will add.
func createFixtureUsersTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)
}

// postgresColumnExists reports whether the named column exists on a public
// table of the connected PostgreSQL database.
func postgresColumnExists(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	var exists bool
	err := db.QueryRowContext(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)`,
		table, column).Scan(&exists)
	require.NoError(t, err)
	return exists
}

// applyIsSettled reports whether the drive has finished deciding the apply's
// outcome: any terminal state, or failed_retryable, which parks the apply
// awaiting operator recovery. Waits that gate on this instead of terminality
// let the assertions that follow name a wrongly-retryable outcome directly,
// rather than reporting it as a timeout.
func applyIsSettled(s string) bool {
	return state.IsTerminalApplyState(s) || state.IsState(s, state.Apply.FailedRetryable)
}

// findFixtureApply returns the fixture database's apply on the fixture PR, or
// nil when none exists yet.
func findFixtureApply(t *testing.T, svc *api.Service, database string) (*storage.Apply, error) {
	t.Helper()
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	if err != nil {
		return nil, fmt.Errorf("listing applies for fixture PR: %w", err)
	}
	for _, apply := range applies {
		if apply.Database == database {
			return apply, nil
		}
	}
	return nil, nil
}
