package spirit

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// pollProgress returns the engine's progress for tests that assert on the
// answer an idle or drained engine gives.
func pollProgress(t *testing.T, eng *Engine) *engine.ProgressResult {
	t.Helper()
	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")
	return result
}

// A schema change that ran to completion can be drained before its owner's
// next progress poll arrives. The poll must still observe the completed
// outcome — with the table identity and DDL the change carried — because
// reporting pending would tell the caller the work never started even though
// the change landed on the target.
func TestDrainRetainsCompletedOutcome(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateCompleted
	rm.tables = []string{"users"}
	rm.ddls = []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL"}
	rm.tableNamespace = map[string]string{"users": "testdb"}
	eng.mu.Unlock()

	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StateCompleted, result.State)
	assert.Empty(t, result.ErrorMessage)
	assert.False(t, result.Retryable)
	require.Len(t, result.Tables, 1)
	assert.Equal(t, "users", result.Tables[0].Table)
	assert.Equal(t, "testdb", result.Tables[0].Namespace)
	assert.Contains(t, result.Tables[0].DDL, "ADD COLUMN")
	assert.Contains(t, result.Tables[0].DDL, "email")
	assert.Equal(t, string(engine.StateCompleted), result.Tables[0].State)
	assert.Equal(t, 100, result.Tables[0].Progress)
}

// A failed schema change's error message is the operator's only handle on why
// the target was left unchanged, so a drain must carry it through to progress
// polls along with the failed state and its retryable flag.
func TestDrainRetainsFailedOutcomeWithError(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateFailed
	rm.errorMessage = "schema change failed: Unknown column 'missing_column'"
	rm.tables = []string{"orders"}
	rm.ddls = []string{"ALTER TABLE `orders` DROP COLUMN `missing_column`"}
	rm.tableNamespace = map[string]string{"orders": "testdb"}
	eng.mu.Unlock()

	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StateFailed, result.State)
	assert.Equal(t, "schema change failed: Unknown column 'missing_column'", result.ErrorMessage)
	assert.True(t, result.Retryable)
	require.Len(t, result.Tables, 1)
	assert.Equal(t, "orders", result.Tables[0].Table)
	assert.Equal(t, string(engine.StateFailed), result.Tables[0].State)
}

// A failed schema change's last-known copy position is the operator's only
// remaining measure of how far it got before failing — the runners are gone by
// the time the drained outcome is read. The outcome must carry the counters
// from the last live poll so a sync after the drain records that position
// instead of overwriting it with zeroes, while live-pacing fields (ETA,
// throttle) that describe nothing terminal are cleared.
func TestDrainedFailureKeepsLastObservedCopyPosition(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateFailed
	rm.errorMessage = "schema change failed: checksum mismatch"
	rm.tables = []string{"orders"}
	rm.ddls = []string{"ALTER TABLE `orders` ADD COLUMN `note` varchar(255) NULL"}
	rm.tableNamespace = map[string]string{"orders": "testdb"}
	rm.lastLiveTables = []engine.TableProgress{{
		Namespace:      "testdb",
		Table:          "orders",
		DDL:            "ALTER TABLE `orders` ADD COLUMN `note` varchar(255) NULL",
		State:          "copyRows",
		RowsCopied:     42000,
		RowsTotal:      50000,
		Progress:       84,
		ETASeconds:     12,
		Throttled:      true,
		ThrottleReason: "replica lag",
	}}
	eng.mu.Unlock()

	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StateFailed, result.State)
	require.Len(t, result.Tables, 1)
	tp := result.Tables[0]
	assert.Equal(t, "orders", tp.Table)
	assert.Equal(t, string(engine.StateFailed), tp.State)
	assert.Equal(t, int64(42000), tp.RowsCopied)
	assert.Equal(t, int64(50000), tp.RowsTotal)
	assert.Equal(t, 84, tp.Progress)
	assert.Zero(t, tp.ETASeconds)
	assert.False(t, tp.Throttled)
	assert.Empty(t, tp.ThrottleReason)
}

// A completed schema change copied everything, so the drained outcome reports
// a full bar with the row total reconciled to the copied count — the count is
// ground truth once the copy finished, and a full bar contradicted by its own
// rows line would misreport a change that landed.
func TestDrainedCompletionReportsFullProgressWithLastCounters(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateCompleted
	rm.tables = []string{"users"}
	rm.ddls = []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL"}
	rm.tableNamespace = map[string]string{"users": "testdb"}
	rm.lastLiveTables = []engine.TableProgress{{
		Namespace:  "testdb",
		Table:      "users",
		DDL:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL",
		State:      "copyRows",
		RowsCopied: 49000,
		RowsTotal:  50000,
		Progress:   98,
	}}
	eng.mu.Unlock()

	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StateCompleted, result.State)
	require.Len(t, result.Tables, 1)
	tp := result.Tables[0]
	assert.Equal(t, "users", tp.Table)
	assert.Equal(t, string(engine.StateCompleted), tp.State)
	assert.Equal(t, 100, tp.Progress)
	assert.Equal(t, int64(49000), tp.RowsCopied)
	assert.Equal(t, int64(49000), tp.RowsTotal,
		"the estimated total reconciles to the copied count once the copy is complete")
}

// Draining an engine that is not tracking any schema change changes nothing:
// there is no outcome to retain, so progress keeps reporting that nothing is
// active.
func TestDrainIdleEngineReportsPending(t *testing.T) {
	eng := New(Config{})

	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StatePending, result.State)
	assert.Equal(t, "No active schema change", result.Message)
	assert.Empty(t, result.Tables)
}

// A drain can lose a race to newer work: while it waits for the drained
// change's goroutine to exit, the engine can accept a new schema change. The
// losing drain must leave the engine's state alone — releasing the tracked
// state or retaining the old change's outcome here would clobber the newer
// change's tracked progress or shadow its eventual result.
func TestDrainLosingRaceToNewerChangeLeavesItUntouched(t *testing.T) {
	eng := New(Config{})
	first := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	first.state = engine.StateCompleted
	first.tables = []string{"users"}
	eng.mu.Unlock()

	var second *runningSchemaChange
	eng.mu.Lock()
	eng.drainRaceWindow = func() {
		second = registerRunningSchemaChange(eng)
	}
	eng.mu.Unlock()

	eng.Drain()

	eng.mu.Lock()
	tracked := eng.runningSchemaChange
	outcome := eng.drainedOutcome
	eng.mu.Unlock()
	assert.Same(t, second, tracked)
	assert.Nil(t, outcome)
}

// A stopped schema change is not an outcome: it resumes from its stored
// checkpoint through a fresh Apply, and the drive layer owns that stopped
// state durably. Draining one therefore retains nothing, and progress reports
// the engine as having no active schema change.
func TestDrainDoesNotRetainStoppedChange(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateStopped
	rm.tables = []string{"users"}
	eng.mu.Unlock()

	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StatePending, result.State)
	assert.Equal(t, "No active schema change", result.Message)
}

// A cancelled schema change resolves through the cancel call itself — the
// caller already holds the outcome — so draining one retains nothing, and
// progress reports the engine as having no active schema change.
func TestDrainDoesNotRetainCancelledChange(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateCancelled
	rm.tables = []string{"users"}
	eng.mu.Unlock()

	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StatePending, result.State)
	assert.Equal(t, "No active schema change", result.Message)
}

// Accepting new work is the release point for a retained outcome: once the
// engine tracks a new schema change, the previous change's result must be
// gone for good, because Drain never clears an outcome it did not retain. If
// the new change ends in a state Drain does not retain — an operator stop —
// the engine must report idle, never resurface the earlier change's completed
// outcome for work that was stopped half-copied.
func TestAcceptingNewWorkReleasesDrainedOutcome(t *testing.T) {
	eng := New(Config{})
	first := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	first.state = engine.StateCompleted
	first.tables = []string{"users"}
	eng.mu.Unlock()
	eng.Drain()

	second := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	second.state = engine.StateStopped
	eng.mu.Unlock()
	eng.Drain()

	result := pollProgress(t, eng)
	assert.Equal(t, engine.StatePending, result.State)
	assert.Equal(t, "No active schema change", result.Message)
}

// An apply that accepts an empty plan is still accepted work: a poll landing
// after the accept must read an idle engine, not the previous change's
// retained outcome as this request's result.
func TestApplyWithNoChangesReleasesDrainedOutcome(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateCompleted
	rm.tables = []string{"users"}
	eng.mu.Unlock()
	eng.Drain()

	result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Credentials: &engine.Credentials{DSN: "root:pass@tcp(127.0.0.1:1)/testdb"},
	})
	require.NoError(t, err, "Apply() with no changes")
	assert.True(t, result.Accepted)

	progress := pollProgress(t, eng)
	assert.Equal(t, engine.StatePending, progress.State)
	assert.Equal(t, "No active schema change", progress.Message)
}

// A stop that arrives after the completed change was drained must be told the
// change completed — through the same typed rejection a stop racing a tracked
// completion receives — so the caller reconciles to the completed outcome
// instead of concluding no schema change ever ran.
func TestStopAfterDrainedCompletionReportsAlreadyCompleted(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateCompleted
	eng.mu.Unlock()
	eng.Drain()

	result, err := eng.Stop(t.Context(), &engine.ControlRequest{})
	assert.Nil(t, result)
	require.Error(t, err)
	assert.True(t, engine.IsAlreadyCompleted(err),
		"the caller must reconcile to the completed outcome, not treat the stop as permanently impossible")
	assert.Contains(t, err.Error(), "testdb")
}

// A cancel that arrives after the completed change was drained must likewise
// reconcile to the completed outcome: marking it cancelled or reporting that
// nothing ran would misrepresent a target the change already landed on.
func TestCancelAfterDrainedCompletionReportsAlreadyCompleted(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateCompleted
	eng.mu.Unlock()
	eng.Drain()

	result, err := eng.Cancel(t.Context(), &engine.ControlRequest{})
	assert.Nil(t, result)
	require.Error(t, err)
	assert.True(t, engine.IsAlreadyCompleted(err),
		"the caller must reconcile to the completed outcome, not treat the cancel as permanently impossible")
	assert.Contains(t, err.Error(), "testdb")
}

// A drained failure is not a completion: a stop or cancel for it has nothing
// left to act on, and answering already-completed would misrepresent a target
// the change never landed on. The permanent rejection stands.
func TestStopAndCancelAfterDrainedFailureStayPermanent(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	eng.mu.Lock()
	rm.state = engine.StateFailed
	rm.errorMessage = "schema change failed: checksum mismatch"
	eng.mu.Unlock()
	eng.Drain()

	var permanent *engine.PermanentError

	_, stopErr := eng.Stop(t.Context(), &engine.ControlRequest{})
	require.Error(t, stopErr)
	assert.False(t, engine.IsAlreadyCompleted(stopErr))
	assert.True(t, errors.As(stopErr, &permanent), "a stop with nothing left to act on is permanently impossible")

	_, cancelErr := eng.Cancel(t.Context(), &engine.ControlRequest{})
	require.Error(t, cancelErr)
	assert.False(t, engine.IsAlreadyCompleted(cancelErr))
	assert.True(t, errors.As(cancelErr, &permanent), "a cancel with nothing left to act on is permanently impossible")
}
