package storagetest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestTasks runs the task-store parity family.
func TestTasks(t *testing.T, h Harness) {
	t.Run("ProgressHighWaterNeverRegresses", func(t *testing.T) { testTaskProgressHighWater(t, h) })
}

// testTaskProgressHighWater proves the task progress contract: the live
// fields (RowsCopied / ProgressPercent) follow every stored sample, while the
// high-water fields (BestRowsCopied / BestProgressPercent) track the furthest
// progress ever stored and never regress. An operator retry relaunches the
// engine's row copy, so a later attempt legitimately writes lower live
// progress; failure surfaces render the high-water so they report how far the
// run actually got.
func testTaskProgressHighWater(t *testing.T, h Harness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := CreateLock(t, store, "highwater_db", storage.DatabaseTypeMySQL)
	apply := CreateApply(t, store, lock, "apply_highwater", 900)

	now := time.Now()
	task := &storage.Task{
		TaskIdentifier: "task_highwater_users",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Repository:     apply.Repository,
		PullRequest:    apply.PullRequest,
		Environment:    apply.Environment,
		State:          state.Task.Running,
		Namespace:      "highwater_db",
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	id, err := store.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = id

	writeProgress := func(pct int, rows int64) *storage.Task {
		t.Helper()
		task.ProgressPercent = pct
		task.RowsCopied = rows
		require.NoError(t, store.Tasks().Update(ctx, task))
		got, err := store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, got)
		return got
	}

	first := writeProgress(47, 470000)
	assert.Equal(t, 47, first.ProgressPercent)
	assert.Equal(t, 47, first.BestProgressPercent)
	assert.Equal(t, int64(470000), first.BestRowsCopied)

	// A retry relaunched the copy: the live sample regresses, the high-water holds.
	relaunched := writeProgress(12, 120000)
	assert.Equal(t, 12, relaunched.ProgressPercent)
	assert.Equal(t, int64(120000), relaunched.RowsCopied)
	assert.Equal(t, 47, relaunched.BestProgressPercent, "high-water percent must survive a lower later sample")
	assert.Equal(t, int64(470000), relaunched.BestRowsCopied, "high-water rows must survive a lower later sample")

	// A later attempt that overtakes the previous best advances the high-water.
	overtaken := writeProgress(60, 600000)
	assert.Equal(t, 60, overtaken.BestProgressPercent)
	assert.Equal(t, int64(600000), overtaken.BestRowsCopied)
}
