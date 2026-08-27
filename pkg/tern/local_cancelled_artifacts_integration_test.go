//go:build integration

package tern

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	spiritutils "github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/pendingdrops"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// newSpiritControlClient builds a local client that drives the real Spirit
// engine, so a cancel exercises the artifact release the engine actually
// implements rather than a stub's.
func newSpiritControlClient(t *testing.T, dsn string, stor storage.Storage) *LocalClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
		Metadata:  map[string]string{"pending_drops": "true"},
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { spiritutils.CloseAndLog(client) })
	return client
}

// seedAbandonedCopy stands in for what a schema change left on the target when
// it stopped partway through copying rows.
func seedAbandonedCopy(t *testing.T, dsn, table string, rows int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open target to seed the abandoned copy")
	defer spiritutils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	// The quarantine is shared with everything else running against this target,
	// so only this artifact's own entries are cleared from it.
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		cleanupDB, err := sql.Open("mysql", dsn)
		if err != nil {
			return
		}
		defer spiritutils.CloseAndLog(cleanupDB)
		for _, name := range []string{spiritutils.NewTableName(table), spiritutils.CheckpointTableName(table)} {
			_, _ = cleanupDB.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `"+name+"`")
		}
		rows, err := cleanupDB.QueryContext(cleanupCtx,
			"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?",
			pendingdrops.Database, "%"+spiritutils.NewTableName(table))
		if err != nil {
			return
		}
		var quarantined []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				break
			}
			quarantined = append(quarantined, name)
		}
		spiritutils.CloseAndLog(rows)
		for _, name := range quarantined {
			_, _ = cleanupDB.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", pendingdrops.Database, name))
		}
	})

	for _, name := range []string{spiritutils.NewTableName(table), spiritutils.CheckpointTableName(table)} {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY AUTO_INCREMENT)", name))
		require.NoError(t, err, "create abandoned artifact %s", name)
	}
	for range rows {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("INSERT INTO `%s` VALUES ()", spiritutils.NewTableName(table)))
		require.NoError(t, err, "seed the abandoned copy")
	}
}

// targetTableExists reports whether a table is present in the target schema.
func targetTableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer spiritutils.CloseAndLog(db)

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table).Scan(&count))
	return count > 0
}

// quarantinedCopies returns the tables held in the pending drops quarantine for
// one artifact, with their row counts. The quarantine is shared by everything
// running against this target, so entries are matched by the artifact's own name
// rather than read wholesale.
func quarantinedCopies(t *testing.T, dsn, artifact string) map[string]int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer spiritutils.CloseAndLog(db)

	rows, err := db.QueryContext(t.Context(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?",
		pendingdrops.Database, "%"+artifact)
	require.NoError(t, err)
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	spiritutils.CloseAndLog(rows)

	counts := map[string]int{}
	for _, name := range names {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", pendingdrops.Database, name)).Scan(&count))
		counts[name] = count
	}
	return counts
}

// stopApplyAndTasks puts the apply and its tasks in the state a schema change
// reaches when an operator stops it: no runner is left holding the copy, and
// nothing on the target is being written any more.
func stopApplyAndTasks(t *testing.T, stor storage.Storage, apply *storage.Apply) {
	t.Helper()
	ctx := t.Context()

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks, "the dispatched apply must carry its task")
	for _, task := range tasks {
		task.State = state.Task.Stopped
		require.NoError(t, stor.Tasks().Update(ctx, task))
	}

	stored, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	stored.State = state.Apply.Stopped
	require.NoError(t, stor.Applies().Update(ctx, stored))
}

// startContendingApply puts a second, running apply on the same target as the
// given one. Storage refuses to create a second active apply for a target, so
// the running state is written directly: this stands in for the apply that wins
// the race the drive re-checks under the target lock, without which a cancel
// could reclaim a copy that is still being written.
func startContendingApply(t *testing.T, stor storage.Storage, dsn string, target *storage.Apply) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	now := time.Now()
	contender := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-contending-%d", now.UnixNano()),
		Database:        target.Database,
		DatabaseType:    target.DatabaseType,
		Deployment:      target.Deployment,
		Environment:     target.Environment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{}),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	id, err := stor.Applies().Create(ctx, contender)
	require.NoError(t, err)
	contender.ID = id

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer spiritutils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, "UPDATE applies SET state = ? WHERE id = ?", state.Apply.Running, id)
	require.NoError(t, err, "start the contending apply on the target")

	contender.State = state.Apply.Running
	return contender
}

// driveCancelForStoppedApply claims the stopped apply the way an operator
// driver does and consumes its pending cancel request.
func driveCancelForStoppedApply(t *testing.T, stor storage.Storage, client *LocalClient, applyID int64) {
	t.Helper()
	ctx := t.Context()

	claimed, err := stor.Applies().ClaimApplyByID(ctx, applyID, "test-operator-"+t.Name())
	require.NoError(t, err)
	require.NotNil(t, claimed, "a stopped apply with a pending cancel must be claimable")

	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())
	handled, err := client.processPendingCancelControlRequest(driveCtx, claimed)
	require.NoError(t, err)
	require.True(t, handled, "the drive must consume the pending cancel")
}

// Cancelling a schema change that stopped partway through its row copy reclaims
// what it left on the target. No runner is alive to ask by then, so the release
// works from the target and the apply's tables alone: the copied rows move to
// the pending drops quarantine where they stay recoverable, and the checkpoint
// describing them is dropped.
func TestLocalClient_CancelReclaimsStoppedApplyArtifacts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer spiritutils.CloseAndLog(stor)
	client := newSpiritControlClient(t, dsn, stor)

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb",
		Table:     "abandoned_users",
		DDL:       "ALTER TABLE `abandoned_users` ADD COLUMN abandoned_note VARCHAR(255)",
		Operation: "alter",
	}})
	stopApplyAndTasks(t, stor, apply)
	seedAbandonedCopy(t, dsn, "abandoned_users", 6)

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)

	driveCancelForStoppedApply(t, stor, client, apply.ID)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State)

	assert.False(t, targetTableExists(t, dsn, spiritutils.NewTableName("abandoned_users")),
		"the abandoned copy must leave the target")
	assert.False(t, targetTableExists(t, dsn, spiritutils.CheckpointTableName("abandoned_users")),
		"the checkpoint describing the copy must be dropped")

	quarantined := quarantinedCopies(t, dsn, spiritutils.NewTableName("abandoned_users"))
	require.Len(t, quarantined, 1, "the copy must be preserved, not dropped")
	for name, rowCount := range quarantined {
		assert.Contains(t, name, spiritutils.NewTableName("abandoned_users"))
		assert.Equal(t, 6, rowCount, "the copied rows must survive the cancel")
	}

	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessagesContaining(logs, "is recoverable at"),
		"the schema change's log must name where the copy was kept")
}

// A cancel cannot tell an abandoned copy from one another apply is actively
// writing: both are named after the same target table. When a live apply owns
// the target, the cancel still completes — an undeliverable cancel wedges the
// pull request, while leftover artifacts are inert — but it leaves the copy
// alone rather than destroying the running apply's work.
func TestLocalClient_CancelLeavesArtifactsWhileAnotherApplyOwnsTheTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer spiritutils.CloseAndLog(stor)
	client := newSpiritControlClient(t, dsn, stor)

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb",
		Table:     "contested_users",
		DDL:       "ALTER TABLE `contested_users` ADD COLUMN contested_note VARCHAR(255)",
		Operation: "alter",
	}})
	stopApplyAndTasks(t, stor, apply)
	seedAbandonedCopy(t, dsn, "contested_users", 4)

	// A second apply owns the same target while the first is stopped.
	startContendingApply(t, stor, dsn, apply)

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)

	driveCancelForStoppedApply(t, stor, client, apply.ID)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State,
		"a refused release must never block the cancel")

	assert.True(t, targetTableExists(t, dsn, spiritutils.NewTableName("contested_users")),
		"the copy the live apply may own must be left alone")
	assert.Empty(t, quarantinedCopies(t, dsn, spiritutils.NewTableName("contested_users")),
		"a refused release must move nothing")

	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessagesContaining(logs, "another schema change is running against the same target"),
		"the operator must find why the copy outlived the cancel")
}
