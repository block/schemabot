//go:build integration

package webhook

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This scenario covers the operator UX when a cancel is accepted but the
// schema change completes on the engine before it can take effect: the
// terminal summary comment must disclose that the cancel did not take effect
// and the change is live on the target — otherwise the accepted cancel
// followed by a completed summary reads as if the cancel worked. A completed
// apply without an accepted cancel, or whose cancel was rejected outright
// (failed request, which posted its own rejection reply), keeps a clean
// summary.
func TestE2ECompletedApplySummaryDisclosesMootedCancel(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := mysqlstore.New(schemabotDB)
	repo := "org/mooted-cancel"

	_, err = schemabotDB.ExecContext(ctx, "DELETE cr FROM apply_control_requests cr JOIN applies a ON cr.apply_id = a.id WHERE a.repository = ?", repo)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE ac FROM apply_comments ac JOIN applies a ON ac.apply_id = a.id WHERE a.repository = ?", repo)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM tasks WHERE repository = ?", repo)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM applies WHERE repository = ?", repo)
	require.NoError(t, err)

	seedApply := func(suffix string) *storage.Apply {
		now := time.Now()
		apply := &storage.Apply{
			ApplyIdentifier: fmt.Sprintf("apply_mootedcancel_%s_%d", suffix, now.UnixNano()),
			PlanID:          1,
			Database:        "e2e_mooted_cancel_db_" + suffix,
			DatabaseType:    storage.DatabaseTypeMySQL,
			Repository:      repo,
			PullRequest:     47,
			Environment:     "staging",
			Caller:          repo + "#47",
			InstallationID:  12345,
			Engine:          storage.EngineSpirit,
			State:           state.Apply.Running,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		applyID, err := st.Applies().Create(ctx, apply)
		require.NoError(t, err)
		apply.ID = applyID
		_, err = schemabotDB.ExecContext(ctx, `
			UPDATE applies
			SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ?
		`, "mooted-cancel-driver", "mooted-cancel-token-"+suffix, applyID)
		require.NoError(t, err)
		return apply
	}

	completedTask := func(apply *storage.Apply) *storage.Task {
		now := time.Now()
		task := &storage.Task{
			TaskIdentifier: fmt.Sprintf("task_%s", apply.ApplyIdentifier),
			ApplyID:        apply.ID,
			PlanID:         apply.PlanID,
			Database:       apply.Database,
			DatabaseType:   apply.DatabaseType,
			Engine:         storage.EngineSpirit,
			Repository:     apply.Repository,
			PullRequest:    apply.PullRequest,
			Environment:    apply.Environment,
			State:          state.Task.Completed,
			TableName:      "users",
			DDL:            "ALTER TABLE `users` ADD COLUMN `mooted_cancel_note` varchar(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		taskID, err := st.Tasks().Create(ctx, task)
		require.NoError(t, err)
		task.ID = taskID
		return task
	}

	terminalSummary := func(apply *storage.Apply, suffix string) string {
		installClient, capture := setupFakeGitHubForComments(t)
		observer := NewCommentObserver(CommentObserverConfig{
			GHClient:       &fakeClientFactory{client: installClient},
			Storage:        st,
			Repo:           repo,
			PR:             47,
			InstallationID: 12345,
			ApplyID:        apply.ID,
			ApplyLease: storage.ApplyLease{
				ApplyID: apply.ID,
				Owner:   "mooted-cancel-driver",
				Token:   "mooted-cancel-token-" + suffix,
			},
			Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		})
		task := completedTask(apply)
		now := time.Now()
		terminal := *apply
		terminal.State = state.Apply.Completed
		terminal.CompletedAt = &now
		observer.OnTerminal(&terminal, []*storage.Task{task})
		return waitForSummaryCreate(t, capture)
	}

	// Cancel accepted, then swept completed when the engine finished first:
	// the summary must disclose the mooted cancel and name the requester.
	mootedApply := seedApply("mooted")
	_, alreadyPending, err := st.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     mootedApply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "armand",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	require.NoError(t, st.ControlRequests().CompletePending(ctx, mootedApply.ID, storage.ControlOperationCancel))

	mootedSummary := terminalSummary(mootedApply, "mooted")
	assert.Contains(t, mootedSummary, "**Cancel did not take effect**")
	assert.Contains(t, mootedSummary, "completed on the engine before the cancel requested by @armand could act")
	assert.Contains(t, mootedSummary, "The change is live on the target.")

	// No cancel was ever requested: the completed summary stays clean.
	cleanApply := seedApply("clean")
	cleanSummary := terminalSummary(cleanApply, "clean")
	assert.NotContains(t, cleanSummary, "Cancel did not take effect")

	// A rejected cancel (failed request) already posted its own rejection
	// reply, so the completed summary carries no mooted-cancel note.
	rejectedApply := seedApply("rejected")
	_, alreadyPending, err = st.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     rejectedApply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "armand",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	require.NoError(t, st.ControlRequests().FailPending(ctx, rejectedApply.ID, storage.ControlOperationCancel, "apply is already terminal"))

	rejectedSummary := terminalSummary(rejectedApply, "rejected")
	assert.NotContains(t, rejectedSummary, "Cancel did not take effect")
}
