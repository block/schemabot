//go:build integration

package sqlstore

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// Retention deletes only the logs of applies whose outcome can no longer
// change. The logs of an old completed, failed, cancelled, or reverted apply go.
// The logs of an old stopped apply, which an operator can resume, and of an old
// running apply stay however old they are, and so do the logs of an apply that
// settled recently.
func TestApplyLogStore_DeleteForSettledAppliesOlderThan(t *testing.T) {
	clearTables(t)
	assertDeleteForSettledAppliesOlderThan(t, NewMySQL(testDB), testDB, MySQLDialect{}, func(query string) string { return query })
}

// assertDeleteForSettledAppliesOlderThan ages applies on the database clock and
// checks which apply logs survive a purge. The aging statement is rendered SQL,
// so each dialect suite runs it; rebind adapts that raw statement's
// placeholders.
func assertDeleteForSettledAppliesOlderThan(t *testing.T, store storage.Storage, db *sql.DB, dialect Dialect, rebind func(string) string) {
	t.Helper()
	ctx := t.Context()
	retention := 30 * 24 * time.Hour
	old := retention + time.Hour
	young := retention - time.Hour
	ago := func(d time.Duration) string {
		return dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
			LiteralIntervalAmount(uint64(d.Microseconds())), IntervalMicrosecond)
	}

	planID := int64(9500)
	// seed stores an apply in applyState with logCount log entries, and stamps
	// its completed_at completedAgo before the database's current time. A nil
	// completedAgo leaves completed_at NULL.
	seed := func(name, applyState string, completedAgo *time.Duration, logCount int) *storage.Apply {
		t.Helper()
		planID++
		lock := storagetest.CreateLock(t, store, name+"_db", storage.DatabaseTypeMySQL)
		apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, name, planID, applyState, "staging")
		for i := 1; i <= logCount; i++ {
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				Level:     storage.LogLevelInfo,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   fmt.Sprintf("%s entry %d", name, i),
			}), name)
		}
		completedAt := "NULL"
		if completedAgo != nil {
			completedAt = ago(*completedAgo)
		}
		_, err := db.ExecContext(ctx, rebind(`UPDATE applies SET completed_at = `+completedAt+` WHERE id = ?`), apply.ID)
		require.NoError(t, err, name)
		return apply
	}
	logCountOf := func(apply *storage.Apply) int {
		t.Helper()
		logs, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
		require.NoError(t, err, apply.ApplyIdentifier)
		return len(logs)
	}

	// assertCount checks what a dry run reports: the count equals want, and
	// counting twice gives the same answer because it deletes nothing.
	assertCount := func(want int64, msg string) {
		t.Helper()
		for range 2 {
			counted, err := store.ApplyLogs().CountForSettledAppliesOlderThan(ctx, retention)
			require.NoError(t, err, msg)
			assert.Equal(t, want, counted, msg)
		}
	}

	invalid := seed("logs_invalid_args", state.Apply.Completed, &old, 2)
	for _, age := range []time.Duration{0, -time.Hour} {
		counted, err := store.ApplyLogs().CountForSettledAppliesOlderThan(ctx, age)
		require.Error(t, err, "a non-positive age %s must not be counted", age)
		assert.Equal(t, int64(0), counted)
	}
	assertCount(2, "a dry run counts the entries of the old completed apply")
	for _, tc := range []struct {
		name  string
		age   time.Duration
		limit int
	}{
		{"zero age", 0, 10},
		{"negative age", -time.Hour, 10},
		{"zero limit", retention, 0},
		{"negative limit", retention, -1},
		{"limit above the largest batch", retention, storage.MaxApplyLogPurgeBatch + 1},
	} {
		deleted, err := store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, tc.age, tc.limit)
		require.Error(t, err, tc.name)
		assert.Equal(t, int64(0), deleted, tc.name)
	}
	require.Equal(t, 2, logCountOf(invalid), "an invalid call must delete nothing")
	assertCount(2, "an invalid delete leaves the count unchanged")
	deleted, err := store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	assert.Equal(t, 0, logCountOf(invalid))
	assertCount(0, "nothing is left to count after the purge")

	const logsPerApply = 2
	// Every settled state, so a state added to the list is purged here too.
	oldSettled := state.SettledApplyStates
	oldKept := []string{
		state.Apply.Stopped, state.Apply.Running, state.Apply.FailedRetryable,
	}
	oldSettledApplies := map[string]*storage.Apply{}
	for _, applyState := range oldSettled {
		oldSettledApplies[applyState] = seed("logs_old_"+applyState, applyState, &old, logsPerApply)
	}
	oldKeptApplies := map[string]*storage.Apply{}
	for _, applyState := range oldKept {
		oldKeptApplies[applyState] = seed("logs_old_"+applyState, applyState, &old, logsPerApply)
	}
	youngSettledApplies := map[string]*storage.Apply{}
	for _, applyState := range oldSettled {
		youngSettledApplies[applyState] = seed("logs_young_"+applyState, applyState, &young, logsPerApply)
	}
	noCompletedAt := seed("logs_completed_without_completed_at", state.Apply.Completed, nil, logsPerApply)

	assertCount(int64(len(oldSettled)*logsPerApply), "only the old settled applies count")
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(len(oldSettled)*logsPerApply), deleted, "every log of one old apply per settled state")
	for _, applyState := range oldSettled {
		assert.Equal(t, 0, logCountOf(oldSettledApplies[applyState]), "logs of an old %s apply must be deleted", applyState)
		assert.Equal(t, logsPerApply, logCountOf(youngSettledApplies[applyState]), "logs of a recent %s apply must be kept", applyState)
	}
	for _, applyState := range oldKept {
		assert.Equal(t, logsPerApply, logCountOf(oldKeptApplies[applyState]), "logs of an old %s apply must be kept", applyState)
	}
	assert.Equal(t, logsPerApply, logCountOf(noCompletedAt), "logs of a settled apply with no completed_at must be kept")
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "a second purge finds nothing left")
	assertCount(0, "stopped, in-flight, recent, and no completed_at applies are not counted")

	// The limit caps one call, the apply that settled longest ago goes first, and
	// a second call takes the rest. The count has no limit.
	older := old + 2*time.Hour
	olderBatch := seed("logs_batch_older", state.Apply.Completed, &older, 3)
	newerBatch := seed("logs_batch_newer", state.Apply.Completed, &old, 3)
	assertCount(6, "the count covers every eligible entry, not one batch")
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), deleted)
	assert.Equal(t, 0, logCountOf(olderBatch), "the apply that settled longest ago is taken first")
	assert.Equal(t, 2, logCountOf(newerBatch), "the limit leaves the rest of the next apply for the next call")
	assertCount(2, "the count shrinks by what the batch deleted")
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 4)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted, "the next call takes the rest")
	assert.Equal(t, 0, logCountOf(newerBatch))
	assertCount(0, "nothing is left to count after the last batch")
	for _, applyState := range oldKept {
		assert.Equal(t, logsPerApply, logCountOf(oldKeptApplies[applyState]), "batching leaves the logs of an old %s apply alone", applyState)
	}

	// An apply settled long ago keeps its logs while a driver still holds a
	// fresh lease on it: on a sibling operation, as under a rollout where one
	// deployment's failure settled the apply, or on the apply itself.
	liveSibling := seed("logs_live_sibling", state.Apply.Failed, &old, logsPerApply)
	_, err = db.ExecContext(ctx, rebind(`
		INSERT INTO apply_operations (apply_id, deployment, operation_key, state, lease_owner, lease_token)
		VALUES (?, 'staging-b', 'op-live', ?, 'driver-b', 'op-token')`), liveSibling.ID, state.ApplyOperation.Running)
	require.NoError(t, err)
	liveParent := seed("logs_live_parent", state.Apply.Failed, &old, logsPerApply)
	_, err = db.ExecContext(ctx, rebind(`UPDATE applies SET lease_owner = 'driver-a', lease_token = 'apply-token', updated_at = `+
		dialect.CurrentTimestamp(TimestampPrecisionDefault)+` WHERE id = ?`), liveParent.ID)
	require.NoError(t, err)
	assertCount(0, "an apply a driver still leases is not counted")
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "an apply a driver still leases keeps its logs")
	assert.Equal(t, logsPerApply, logCountOf(liveSibling), "a live sibling operation's logs must survive")
	assert.Equal(t, logsPerApply, logCountOf(liveParent), "a freshly leased apply's logs must survive")

	// An apply claimed after the purge selected its entries keeps them: the
	// delete checks the lease again.
	claimedMidPurge := seed("logs_claimed_mid_purge", state.Apply.Completed, &old, logsPerApply)
	purgedMidPurge := seed("logs_purged_mid_purge", state.Apply.Completed, &old, logsPerApply)
	assertCount(int64(2*logsPerApply), "both unclaimed applies count before the claim")
	logStore := store.ApplyLogs().(*applyLogStore)
	t.Cleanup(func() { logStore.afterPurgeSelect = nil })
	logStore.afterPurgeSelect = func() {
		logStore.afterPurgeSelect = nil
		_, err := db.ExecContext(ctx, rebind(`UPDATE applies SET lease_owner = 'driver-c', lease_token = 'claim-token', updated_at = `+
			dialect.CurrentTimestamp(TimestampPrecisionDefault)+` WHERE id = ?`), claimedMidPurge.ID)
		require.NoError(t, err)
	}
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(logsPerApply), deleted, "only the apply nobody claimed loses its logs")
	assert.Equal(t, 0, logCountOf(purgedMidPurge))
	assert.Equal(t, logsPerApply, logCountOf(claimedMidPurge), "an apply claimed between the select and the delete keeps its logs")
}
