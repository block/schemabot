//go:build integration

package webhook

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/webhook/templates"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This scenario covers the failure-triage UX on the PR timeline: when an apply
// fails, its terminal summary comment must carry the apply's recent log entries
// folded into a details block — the same entries the CLI logs command shows —
// so an operator can triage without leaving GitHub. A successful apply's
// summary must stay clean: no logs section.
func TestE2EFailedApplySummaryCarriesRecentLogs(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := mysqlstore.New(schemabotDB)
	repo := "org/failure-logs"

	_, err = schemabotDB.ExecContext(ctx, "DELETE al FROM apply_logs al JOIN applies a ON al.apply_id = a.id WHERE a.repository = ?", repo)
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
			ApplyIdentifier: fmt.Sprintf("apply_faillogs_%s_%d", suffix, now.UnixNano()),
			PlanID:          1,
			Database:        "e2e_failure_logs_db_" + suffix,
			DatabaseType:    storage.DatabaseTypeMySQL,
			Repository:      repo,
			PullRequest:     46,
			Environment:     "staging",
			Caller:          repo + "#46",
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
		`, "failure-logs-driver", "failure-logs-token-"+suffix, applyID)
		require.NoError(t, err)
		return apply
	}

	task := func(apply *storage.Apply, taskState string) *storage.Task {
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
			State:          taskState,
			TableName:      "users",
			DDL:            "ALTER TABLE `users` ADD COLUMN `failure_logs_note` varchar(255)",
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

	// Failed apply: the summary must fold the recent log entries.
	failedApply := seedApply("failed")
	failedTask := task(failedApply, state.Task.Failed)
	require.NoError(t, st.ApplyLogs().Append(ctx, &storage.ApplyLog{
		ApplyID: failedApply.ID, Level: "info", EventType: "state_transition",
		Message: "Apply claimed by driver", OldState: "queued", NewState: "running",
	}))
	require.NoError(t, st.ApplyLogs().Append(ctx, &storage.ApplyLog{
		ApplyID: failedApply.ID, Level: "error", EventType: "state_transition",
		Message: "Apply failed: lost MySQL connection during copy", OldState: "running", NewState: "failed",
	}))

	installClient, capture := setupFakeGitHubForComments(t)
	observer := NewCommentObserver(CommentObserverConfig{
		GHClient:       &fakeClientFactory{client: installClient},
		Storage:        st,
		Repo:           repo,
		PR:             46,
		InstallationID: 12345,
		ApplyID:        failedApply.ID,
		ApplyLease: storage.ApplyLease{
			ApplyID: failedApply.ID,
			Owner:   "failure-logs-driver",
			Token:   "failure-logs-token-failed",
		},
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
	})

	terminalApply := *failedApply
	terminalApply.State = state.Apply.Failed
	terminalApply.ErrorMessage = "lost MySQL connection during copy"
	now := time.Now()
	terminalApply.CompletedAt = &now
	observer.OnTerminal(&terminalApply, []*storage.Task{failedTask})

	summary := waitForSummaryCreate(t, capture)
	assert.Contains(t, summary, "<summary>Show logs (2 entries)</summary>")
	assert.Contains(t, summary, "[INF] Apply claimed by driver [queued -> running]")
	assert.Contains(t, summary, "[ERR] Apply failed: lost MySQL connection during copy [running -> failed]")

	// A summary body that already fills GitHub's comment budget leaves no room
	// for the section — it must be dropped so the summary itself still posts.
	hugeBase := strings.Repeat("x", templates.GitHubIssueCommentMaxChars)
	noRoom := failureLogsSection(ctx, st,
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		&terminalApply, hugeBase)
	assert.Empty(t, noRoom)

	// Completed apply: the summary stays clean even though log entries exist.
	completedApply := seedApply("done")
	completedTask := task(completedApply, state.Task.Completed)
	require.NoError(t, st.ApplyLogs().Append(ctx, &storage.ApplyLog{
		ApplyID: completedApply.ID, Level: "info", EventType: "state_transition",
		Message: "Apply claimed by driver", OldState: "queued", NewState: "running",
	}))

	installClient2, capture2 := setupFakeGitHubForComments(t)
	observer2 := NewCommentObserver(CommentObserverConfig{
		GHClient:       &fakeClientFactory{client: installClient2},
		Storage:        st,
		Repo:           repo,
		PR:             46,
		InstallationID: 12345,
		ApplyID:        completedApply.ID,
		ApplyLease: storage.ApplyLease{
			ApplyID: completedApply.ID,
			Owner:   "failure-logs-driver",
			Token:   "failure-logs-token-done",
		},
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
	})

	terminalDone := *completedApply
	terminalDone.State = state.Apply.Completed
	terminalDone.CompletedAt = &now
	observer2.OnTerminal(&terminalDone, []*storage.Task{completedTask})

	doneSummary := waitForSummaryCreate(t, capture2)
	assert.NotContains(t, doneSummary, "<summary>Show logs (")
	assert.NotContains(t, doneSummary, "Show recent logs")
}

// waitForSummaryCreate reads created comments until the terminal summary
// appears (OnTerminal also freezes the progress comment via edit; only creates
// arrive on this channel, and the summary is the only create in this flow).
func waitForSummaryCreate(t *testing.T, capture *commentCapture) string {
	t.Helper()
	select {
	case created := <-capture.creates:
		return created.Body
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for terminal summary comment")
		return ""
	}
}
