//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
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
