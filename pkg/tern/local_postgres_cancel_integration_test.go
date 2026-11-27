//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// postgresCancelDeadline bounds each wait in the PostgreSQL cancel scenario.
const postgresCancelDeadline = 30 * time.Second

// An operator cancels a PostgreSQL apply while its CREATE INDEX CONCURRENTLY
// is still building. The cancel reaches the build the drive started, so the
// build ends, the apply and its task record cancelled, and the target is left
// without the index. Recording the apply cancelled while the build runs on
// would leave a valid index the operator was told was never built.
//
// An open write transaction on the table holds the build in its wait for
// older writers, so the build is still running when the cancel lands however
// fast the container is.
func TestLocalClientPostgresCancelEndsTheRunningIndexBuild(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const database = "cancel_build"
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	targetDSN, targetDB := testutil.StartPostgres(t, database)
	_, err := targetDB.ExecContext(t.Context(), `
		CREATE TABLE public.orders (id bigint PRIMARY KEY, note text);
		INSERT INTO public.orders SELECT g, 'note ' || g FROM pg_catalog.generate_series(1, 1000) g`)
	require.NoError(t, err)

	stor := createStorage(t, storageDSN)
	t.Cleanup(func() { utils.CloseAndLog(stor) })
	client, err := NewLocalClient(LocalConfig{Database: database, Type: storage.DatabaseTypePostgres, TargetDSN: targetDSN,
		SchemaOverrides: map[string]string{"logical": "public"}}, stor, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres,
		SchemaFiles: map[string]*ternv1.SchemaFiles{"logical": {Files: map[string]string{
			"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, note text);\nCREATE INDEX orders_note_idx ON orders (note);",
		}}}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)

	blocker := holdOlderWriter(t, targetDB)

	result, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: localClientTestEnvironment,
		DdlChanges: plan.Changes[0].TableChanges})
	require.NoError(t, err)
	require.True(t, result.Accepted, result.ErrorMessage)
	startTestOperator(t, stor, client, result.ApplyId)

	require.Eventually(t, func() bool { return indexBuildsRunning(t, targetDB) == 1 },
		postgresCancelDeadline, 50*time.Millisecond, "the index build never started")

	cancelResp, err := client.Cancel(t.Context(), &ternv1.CancelRequest{ApplyId: result.ApplyId, Environment: localClientTestEnvironment})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted, cancelResp.ErrorMessage)

	require.Eventually(t, func() bool { return indexBuildsRunning(t, targetDB) == 0 },
		postgresCancelDeadline, 50*time.Millisecond, "the cancel must end the build while it still waits on the older writer")
	require.NoError(t, blocker.Rollback())

	apply := resolveDispatchedApply(t, stor, result.ApplyId)
	require.Eventually(t, func() bool {
		apply = resolveDispatchedApply(t, stor, result.ApplyId)
		return state.IsTerminalApplyState(apply.State)
	}, postgresCancelDeadline, 50*time.Millisecond, "the cancelled apply never settled")
	assert.Equal(t, state.Apply.Cancelled, apply.State, apply.ErrorMessage)
	tasks, err := stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Cancelled, tasks[0].State, tasks[0].ErrorMessage)

	var indexes int
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_catalog.pg_indexes WHERE schemaname = 'public' AND indexname = 'orders_note_idx'`).Scan(&indexes))
	assert.Zero(t, indexes, "a cancelled build leaves no index behind, valid or invalid")
}

// holdOlderWriter opens a transaction that has written to public.orders and
// leaves it open, so a CREATE INDEX CONCURRENTLY on the table waits for it
// before building. The caller rolls it back; cleanup rolls back one left open.
func holdOlderWriter(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(t.Context(), `UPDATE public.orders SET note = note WHERE id = 1`)
	require.NoError(t, err)
	return tx
}

// indexBuildsRunning counts the index builds the server reports in progress.
func indexBuildsRunning(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_catalog.pg_stat_progress_create_index`).Scan(&n))
	return n
}

// An operator cancels a PostgreSQL apply whose plain ALTER TABLE is waiting
// for a lock behind an open reader. Plain DDL commits or fails on its own, so
// the engine declines the cancel: the request settles as a command that did
// not take effect, and the apply is never recorded cancelled while its
// statement can still commit. Once the reader ends, the change completes.
func TestLocalClientPostgresCancelOfPlainDDLIsDeclined(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const database = "cancel_plain"
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	targetDSN, targetDB := testutil.StartPostgres(t, database)
	_, err := targetDB.ExecContext(t.Context(), `CREATE TABLE public.orders (id bigint PRIMARY KEY, note text)`)
	require.NoError(t, err)

	stor := createStorage(t, storageDSN)
	t.Cleanup(func() { utils.CloseAndLog(stor) })
	client, err := NewLocalClient(LocalConfig{Database: database, Type: storage.DatabaseTypePostgres, TargetDSN: targetDSN,
		SchemaOverrides: map[string]string{"logical": "public"}}, stor, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres,
		SchemaFiles: map[string]*ternv1.SchemaFiles{"logical": {Files: map[string]string{
			"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, note text, coupon_code text);",
		}}}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)

	reader, err := targetDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	_, err = reader.ExecContext(t.Context(), `SELECT count(*) FROM public.orders`)
	require.NoError(t, err)
	var readerPID int
	require.NoError(t, reader.QueryRowContext(t.Context(), `SELECT pg_backend_pid()`).Scan(&readerPID))

	result, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: localClientTestEnvironment,
		DdlChanges: plan.Changes[0].TableChanges})
	require.NoError(t, err)
	require.True(t, result.Accepted, result.ErrorMessage)
	startTestOperator(t, stor, client, result.ApplyId)

	// The ALTER waits for the reader outside the lock queue before it asks for
	// its lock, and the engine names the reader while it waits.
	require.Eventually(t, func() bool {
		progress, err := client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: result.ApplyId, Environment: localClientTestEnvironment})
		require.NoError(t, err)
		return progress.GetMetadata()["lock_holder_pid"] == strconv.Itoa(readerPID)
	}, postgresCancelDeadline, 50*time.Millisecond, "the ALTER never reported waiting on the open reader")

	cancelResp, err := client.Cancel(t.Context(), &ternv1.CancelRequest{ApplyId: result.ApplyId, Environment: localClientTestEnvironment})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted, cancelResp.ErrorMessage)

	apply := resolveDispatchedApply(t, stor, result.ApplyId)
	require.Eventually(t, func() bool {
		req, err := stor.ControlRequests().GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
		require.NoError(t, err)
		return req != nil && req.Status != storage.ControlRequestPending
	}, postgresCancelDeadline, 50*time.Millisecond, "the cancel request never settled")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestFailed)
	declined, err := stor.ControlRequests().GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, declined)
	// The request settles through the engine's typed decline, so the operator
	// is told why the cancel did nothing rather than shown a generic failure.
	assert.Contains(t, declined.ErrorMessage, "this schema change runs as plain DDL that commits or fails on its own")
	apply = resolveDispatchedApply(t, stor, result.ApplyId)
	assert.NotEqual(t, state.Apply.Cancelled, apply.State, "a declined cancel must not record the apply cancelled")

	require.NoError(t, reader.Rollback())
	require.Eventually(t, func() bool {
		apply = resolveDispatchedApply(t, stor, result.ApplyId)
		return state.IsState(apply.State, state.Apply.Completed)
	}, postgresCancelDeadline, 50*time.Millisecond, "the change never completed after the reader ended")

	var columns int
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		`SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'coupon_code'`).Scan(&columns))
	assert.Equal(t, 1, columns, "the change the cancel declined is applied")
}

// An owner makes a PostgreSQL column NOT NULL while a row still holds NULL.
// The sequence's first step adds a helper CHECK NOT VALID, which rejects
// every NULL write the application makes, and the helper's validation fails
// on the row. A retry cannot pass until someone fixes the row, so the apply
// fails at once, says to fix the rows and apply again, and does not run the
// steps after the validation. The drive drops the helper the apply added, so
// the application's writes behave as they did before the apply, and the apply
// log names the constraint it removed.
func TestLocalClientPostgresFailedValidationRemovesTheSetNotNullHelper(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		database = "fail_not_null"
		helper   = "users_name_not_null_check"
	)
	stor, targetDB, applyID := applySetNotNullOverANullRow(t, database, "", true)

	apply := waitForTerminalApply(t, stor, applyID)
	assert.Equal(t, state.Apply.Failed, apply.State, "a broken check ends the apply instead of retrying it")
	assert.Contains(t, apply.ErrorMessage, `existing rows in "users" break check constraint "users_name_not_null_check"; fix those rows, then apply again`)
	tasks, err := stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	assert.Equal(t, []string{state.Task.Completed, state.Task.Failed, state.Task.Cancelled, state.Task.Cancelled}, taskStates(tasks),
		"the steps after the failed validation never run")

	assert.Zero(t, checkConstraintCount(t, targetDB, helper), "the failure drops the helper the apply added")
	_, err = targetDB.ExecContext(t.Context(), `INSERT INTO public.users VALUES (3, NULL)`)
	require.NoError(t, err, "a NULL write lands again, as it did before the apply")

	logs, err := stor.ApplyLogs().GetByApply(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessagesContaining(logs,
		`Removed constraint users_name_not_null_check from public.users. The failed schema change added it, and it rejects writes that break CHECK ((name IS NOT NULL))`),
		"the apply log names the constraint the failure removed")
}

// A table already carries a SET NOT NULL helper an earlier run left, and a
// row still holds NULL. The plan takes the helper over and starts at its
// validation, which fails on the row and ends the apply. The apply did not
// add the helper, so the drive leaves it, and the apply log says it still
// checks writes and how to drop it.
func TestLocalClientPostgresFailedValidationLeavesAHelperTheApplyDidNotAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		database = "fail_adopted_not_null"
		helper   = "users_name_not_null_check"
	)
	stor, targetDB, applyID := applySetNotNullOverANullRow(t, database,
		`ALTER TABLE public.users ADD CONSTRAINT users_name_not_null_check CHECK (name IS NOT NULL) NOT VALID`, false)

	apply := waitForTerminalApply(t, stor, applyID)
	assert.Equal(t, state.Apply.Failed, apply.State, apply.ErrorMessage)

	assert.Equal(t, 1, checkConstraintCount(t, targetDB, helper), "the helper the apply found stays")
	logs, err := stor.ApplyLogs().GetByApply(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.Zero(t, countLogMessagesContaining(logs, "Removed constraint"), "the drive removed nothing the apply did not add")
	assert.Equal(t, 1, countLogMessagesContaining(logs,
		`Constraint users_name_not_null_check is still on public.users. No finished step of the failed schema change added it, so SchemaBot left it, and it rejects writes that break CHECK ((name IS NOT NULL)) until it is dropped. Drop it with: ALTER TABLE "public"."users" DROP CONSTRAINT "users_name_not_null_check"`),
		"the apply log says the helper still checks writes and how to drop it")
}

// applySetNotNullOverANullRow creates public.users with a NULL name, runs
// setup on the target when it is set, plans users.name NOT NULL, and starts
// the apply. addsHelper says whether the plan's first step adds the helper or
// takes over one setup left.
func applySetNotNullOverANullRow(t *testing.T, database, setup string, addsHelper bool) (storage.Storage, *sql.DB, string) {
	t.Helper()
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	targetDSN, targetDB := testutil.StartPostgres(t, database)
	_, err := targetDB.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY, name text);
		INSERT INTO public.users VALUES (1, 'ada'), (2, NULL)`)
	require.NoError(t, err)
	if setup != "" {
		_, err = targetDB.ExecContext(t.Context(), setup)
		require.NoError(t, err)
	}

	stor := createStorage(t, storageDSN)
	t.Cleanup(func() { utils.CloseAndLog(stor) })
	client, err := NewLocalClient(LocalConfig{Database: database, Type: storage.DatabaseTypePostgres, TargetDSN: targetDSN,
		SchemaOverrides: map[string]string{"logical": "public"}}, stor, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres,
		SchemaFiles: map[string]*ternv1.SchemaFiles{"logical": {Files: map[string]string{
			"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, name text NOT NULL);",
		}}}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.NotEmpty(t, plan.Changes[0].TableChanges)
	first := plan.Changes[0].TableChanges[0].Ddl
	if addsHelper {
		require.Contains(t, first, "ADD CONSTRAINT", "the plan's first step adds the helper")
	} else {
		require.Equal(t, `ALTER TABLE "public"."users" VALIDATE CONSTRAINT "users_name_not_null_check"`, first,
			"the plan takes the helper over instead of adding it")
	}

	result, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: localClientTestEnvironment,
		DdlChanges: plan.Changes[0].TableChanges})
	require.NoError(t, err)
	require.True(t, result.Accepted, result.ErrorMessage)
	startTestOperator(t, stor, client, result.ApplyId)
	return stor, targetDB, result.ApplyId
}

// waitForTerminalApply waits for the apply to settle and returns it.
func waitForTerminalApply(t *testing.T, stor storage.Storage, applyID string) *storage.Apply {
	t.Helper()
	apply := resolveDispatchedApply(t, stor, applyID)
	require.Eventually(t, func() bool {
		apply = resolveDispatchedApply(t, stor, applyID)
		return state.IsTerminalApplyState(apply.State)
	}, postgresCancelDeadline, 50*time.Millisecond, "the apply never settled")
	return apply
}

// checkConstraintCount counts the constraints named name on public.users.
func checkConstraintCount(t *testing.T, db *sql.DB, name string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid = 'public.users'::regclass AND conname = $1`, name).Scan(&count))
	return count
}

// An owner adds a foreign key from orders to users while an order still
// points at a user that does not exist. The key is added NOT VALID, which
// checks every write the application makes from then on, and its validation
// fails on the orphan row. A retry cannot pass until someone fixes the row,
// so the apply fails at once. The drive drops the key the apply added, so
// the application's writes behave as they did before the apply, and the
// apply log names the key it removed.
func TestLocalClientPostgresFailedValidationRemovesTheForeignKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		database = "fail_foreign_key"
		key      = "orders_user_id_fkey"
	)
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	targetDSN, targetDB := testutil.StartPostgres(t, database)
	_, err := targetDB.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint);
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.orders VALUES (1, 1), (2, 99)`)
	require.NoError(t, err)

	stor := createStorage(t, storageDSN)
	t.Cleanup(func() { utils.CloseAndLog(stor) })
	client, err := NewLocalClient(LocalConfig{Database: database, Type: storage.DatabaseTypePostgres, TargetDSN: targetDSN,
		SchemaOverrides: map[string]string{"logical": "public"}}, stor, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres,
		SchemaFiles: map[string]*ternv1.SchemaFiles{"logical": {Files: map[string]string{
			"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY);",
			"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint, CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES users (id));",
		}}}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 2)
	require.Contains(t, plan.Changes[0].TableChanges[0].Ddl, "NOT VALID", "the plan adds the key without checking existing rows")

	result, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: localClientTestEnvironment,
		DdlChanges: plan.Changes[0].TableChanges})
	require.NoError(t, err)
	require.True(t, result.Accepted, result.ErrorMessage)
	startTestOperator(t, stor, client, result.ApplyId)

	apply := waitForTerminalApply(t, stor, result.ApplyId)
	assert.Equal(t, state.Apply.Failed, apply.State, "an orphan row ends the apply instead of retrying it")
	assert.Contains(t, apply.ErrorMessage, `"orders"`)
	assert.Contains(t, apply.ErrorMessage, key)
	tasks, err := stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{state.Task.Completed, state.Task.Failed}, taskStates(tasks))

	var keys int
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid = 'public.orders'::regclass AND conname = $1`, key).Scan(&keys))
	assert.Zero(t, keys, "the failure drops the key the apply added")
	_, err = targetDB.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (3, 98)`)
	require.NoError(t, err, "an orphan write lands again, as it did before the apply")

	logs, err := stor.ApplyLogs().GetByApply(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessagesContaining(logs, `Removed constraint orders_user_id_fkey from public.orders. The failed schema change added it`),
		"the apply log names the key the failure removed")
}
