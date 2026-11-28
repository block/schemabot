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

	invalid := seed("logs_invalid_args", state.Apply.Completed, &old, 2)
	for _, tc := range []struct {
		name  string
		age   time.Duration
		limit int
	}{
		{"zero age", 0, 10},
		{"negative age", -time.Hour, 10},
		{"zero limit", retention, 0},
		{"negative limit", retention, -1},
	} {
		deleted, err := store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, tc.age, tc.limit)
		require.Error(t, err, tc.name)
		assert.Equal(t, int64(0), deleted, tc.name)
	}
	require.Equal(t, 2, logCountOf(invalid), "an invalid call must delete nothing")
	deleted, err := store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	assert.Equal(t, 0, logCountOf(invalid))

	const logsPerApply = 2
	oldSettled := []string{
		state.Apply.Completed, state.Apply.Failed, state.Apply.Cancelled, state.Apply.Reverted,
	}
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

	// The limit caps one call, the apply that settled longest ago goes first, and
	// a second call takes the rest.
	older := old + 2*time.Hour
	olderBatch := seed("logs_batch_older", state.Apply.Completed, &older, 3)
	newerBatch := seed("logs_batch_newer", state.Apply.Completed, &old, 3)
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), deleted)
	assert.Equal(t, 0, logCountOf(olderBatch), "the apply that settled longest ago is taken first")
	assert.Equal(t, 2, logCountOf(newerBatch), "the limit leaves the rest of the next apply for the next call")
	deleted, err = store.ApplyLogs().DeleteForSettledAppliesOlderThan(ctx, retention, 4)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted, "the next call takes the rest")
	assert.Equal(t, 0, logCountOf(newerBatch))
	for _, applyState := range oldKept {
		assert.Equal(t, logsPerApply, logCountOf(oldKeptApplies[applyState]), "batching leaves the logs of an old %s apply alone", applyState)
	}
}
