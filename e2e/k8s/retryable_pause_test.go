//go:build e2e

package k8s

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestK8s_DataPlaneRetryablePauseHoldsControlPlaneOpenUntilRecovery drives a
// schema change through the full two-tier stack and forces a real engine
// failure mid-run by killing the data plane's connections to the target
// database. The data plane parks the apply between its own recovery attempts,
// and that pause must be survivable end to end:
//
//   - The pause crosses the wire as STATE_FAILED_RETRYABLE, so the control
//     plane can tell "paused, will self-retry" from a settled failure without
//     inspecting per-table statuses.
//   - The control plane's stored apply stays non-terminal for the whole pause.
//     A terminal verdict here would end the drive and orphan a live remote
//     apply that goes on to change the schema with nobody watching.
//   - Once the failure injection stops, the data plane's own recovery claims
//     another attempt and both planes land completed with the DDL applied.
func TestK8s_DataPlaneRetryablePauseHoldsControlPlaneOpenUntilRecovery(t *testing.T) {
	cleanupState(t)

	// Dial a data-plane pod and open the direct database handles before the
	// apply starts: every second between the copy being observed running and
	// the first kill is time for a fast copy to complete, and a completed
	// apply leaves no pause to observe. Progress renders from stored state on
	// every instance, so any pod serves the pause regardless of which one owns
	// the apply.
	pods := podNamesForInstance(t, "data-plane")
	require.NotEmpty(t, pods, "expected k8s e2e data plane pods")
	podClient := dialDataPlanePod(t, pods[0])
	killer := testutil.OpenMySQL(t, testutil.TernStagingDSN(t))
	controlPlaneDB := testutil.OpenMySQL(t, testutil.SchemabotDSN(t))

	// Return from the fixture at dispatch rather than waiting for the control
	// plane to report running: the control plane's view lags the engine by a
	// progress-poll cycle, and a fast copy can finish inside that lag. The
	// data plane stamps its stored tasks running the moment the engine accepts
	// the run, so its own wire is the earliest signal that a kill will land on
	// an active engine run. Even that signal trails the engine by a drive
	// poll tick, so the table is seeded well past the suite's usual row count:
	// the copy must still be running when the first kill lands, or the apply
	// completes with no pause to observe.
	fixture := startIndexAddApplyWithOptions(t, "k8s_retry_pause", false, nil, 2000000)
	waitForPodApplyState(t, podClient, fixture.DataPlaneApplyID, ternv1.State_STATE_RUNNING, testutil.PollDeadline)

	// Kill the data plane's target connections on every poll tick until the
	// engine run fails and the data plane parks the apply for its own retry.
	// Spirit may absorb a single kill mid-chunk, so the injection repeats until
	// the pause is actually observed on the wire. Throughout, the control
	// plane's stored apply must stay non-terminal: the data plane will retry,
	// so any terminal state here is the split-brain this stack prevents.
	var lastWireState ternv1.State
	testutil.Poll(t, 3*time.Minute, 250*time.Millisecond,
		func() bool {
			killDataPlaneTargetConnections(t, killer)

			cpState := storedControlPlaneApplyState(t, controlPlaneDB, fixture.ApplyID)
			require.False(t, state.IsTerminalApplyState(cpState),
				"control plane terminalized the apply (%s) while the data plane was still retrying", cpState)

			lastWireState = podProgress(t, podClient, fixture.DataPlaneApplyID).State
			require.NotEqual(t, ternv1.State_STATE_FAILED, lastWireState,
				"data plane settled failed: the retry budget was exhausted before the pause was observed")
			return lastWireState == ternv1.State_STATE_FAILED_RETRYABLE
		},
		func() string {
			return fmt.Sprintf("timeout waiting for the data-plane pause to cross the wire as STATE_FAILED_RETRYABLE, last wire state: %s", lastWireState)
		})

	// The pause is on the wire and the control plane is still holding. Stop
	// injecting failures: the data plane's next recovery attempt must finish
	// the schema change and reconcile both planes to completed.
	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)

	// The data plane stamps its task rows terminal before its apply row, and
	// the wire derives completion from the task rows, so the control plane
	// can terminalize while the data-plane apply row is still converging.
	// Poll the stored rows until both land completed.
	var dataPlaneApplyState, dataPlaneTaskState string
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			dataPlaneApplyState, dataPlaneTaskState = storedK8sApplyAndTaskStates(t, storageDSNs(t)[0], fixture.DataPlaneApplyID)
			return state.IsState(dataPlaneApplyState, state.Apply.Completed) &&
				state.IsState(dataPlaneTaskState, state.Task.Completed)
		},
		func() string {
			return fmt.Sprintf("data-plane apply and task should complete after recovery, got apply=%s task=%s",
				dataPlaneApplyState, dataPlaneTaskState)
		})
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}

// storedControlPlaneApplyState reads the control plane's stored apply state.
func storedControlPlaneApplyState(t *testing.T, db *sql.DB, applyID string) string {
	t.Helper()
	var applyState string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT state FROM applies WHERE apply_identifier = ?", applyID).Scan(&applyState))
	return applyState
}

// killDataPlaneTargetConnections kills every connection the data plane holds
// to the target database, failing whatever engine work those connections were
// doing. The killer's own connection is excluded.
func killDataPlaneTargetConnections(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		"SELECT id FROM information_schema.processlist WHERE db = 'testapp' AND id <> CONNECTION_ID()")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var connectionIDs []int64
	for rows.Next() {
		var connectionID int64
		require.NoError(t, rows.Scan(&connectionID))
		connectionIDs = append(connectionIDs, connectionID)
	}
	require.NoError(t, rows.Err())
	for _, connectionID := range connectionIDs {
		// The listed connection can finish and vanish before KILL reaches it;
		// that raced disappearance is the injected failure itself, not a
		// problem for the test.
		if _, err := db.ExecContext(t.Context(), fmt.Sprintf("KILL %d", connectionID)); err != nil {
			t.Logf("kill data-plane target connection %d: %v", connectionID, err)
		}
	}
}
