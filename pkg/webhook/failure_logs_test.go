package webhook

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubApplyLogStore serves the control-plane log read the failed-summary folds
// make, from whatever the test seeded.
type stubApplyLogStore struct {
	storage.ApplyLogStore
	logs []*storage.ApplyLog
	err  error
}

func (s *stubApplyLogStore) GetRecentByApply(context.Context, int64, int) ([]*storage.ApplyLog, error) {
	return s.logs, s.err
}

func failureLogsTestApply() *storage.Apply {
	return &storage.Apply{
		ID: 9, ApplyIdentifier: "apply-control", Database: "commerce",
		DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging",
		State: state.Apply.Failed,
	}
}

func failureLogsTestStorage(logs ...*storage.ApplyLog) storage.Storage {
	return &stubStorage{applyLogs: &stubApplyLogStore{logs: logs}}
}

func failureLogsTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func engineLogSource(deployment string, messages ...string) []api.EngineLogSource {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	entries := make([]*apitypes.LogEntry, len(messages))
	for i, message := range messages {
		entries[i] = &apitypes.LogEntry{CreatedAt: at.Add(time.Duration(i) * time.Second), Level: "warn", Message: message}
	}
	return []api.EngineLogSource{{Deployment: deployment, Entries: entries}}
}

// A failed apply that ran on a data plane carries both accounts of it: the
// control plane's timeline, and the engine's own lines — which for a remote
// drive live in the data plane's storage and would otherwise reach an
// operator only through the CLI. The control-plane fold comes first, because
// it is what sets the scene for the engine's lines.
func TestFailureLogsSectionsRendersBothFolds(t *testing.T) {
	apply := failureLogsTestApply()
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return engineLogSource("region-a", "[orders] unsafe warning 1265: Data truncated"), nil
	}

	rendered := failureLogsSections(t.Context(), stor, engineLogs, failureLogsTestLogger(), apply, "summary body")

	assert.Contains(t, rendered, "<summary>Show apply logs (1 entry)</summary>")
	assert.Contains(t, rendered, "Apply failed [running -> failed]")
	assert.Contains(t, rendered, "<summary>Show engine logs (1 entry)</summary>")
	assert.Contains(t, rendered, "[orders] unsafe warning 1265: Data truncated")
	assert.Less(t, strings.Index(rendered, "Show apply logs ("), strings.Index(rendered, "Show engine logs ("),
		"the apply's own timeline reads before the engine's account of the failure")
}

// An apply with no data plane renders exactly what it rendered before the
// engine fold existed: one fold, spending the whole budget. An empty second
// fold would be worse than none — it reads as an engine that said nothing,
// when the engine's lines are in the fold above it.
func TestFailureLogsSectionsOmitsTheEngineFoldWithoutADataPlane(t *testing.T) {
	apply := failureLogsTestApply()
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	noSources := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return nil, nil
	}

	withReader := failureLogsSections(t.Context(), stor, noSources, failureLogsTestLogger(), apply, "summary body")
	withoutReader := failureLogsSections(t.Context(), stor, nil, failureLogsTestLogger(), apply, "summary body")

	assert.NotContains(t, withReader, "engine logs")
	assert.NotContains(t, withReader, "<details>\n<summary>Show engine")
	assert.Equal(t, withoutReader, withReader,
		"a data plane that reported no engine lines renders the same summary as an apply that never had one")
}

// A data plane that cannot be read costs the summary its engine fold and
// nothing else. The terminal comment is how the PR learns the apply ended, so
// an unreachable deployment must never be the reason it does not post.
func TestFailureLogsSectionsKeepsTheSummaryWhenTheDataPlaneCannotBeRead(t *testing.T) {
	apply := failureLogsTestApply()
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	unreadable := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return nil, fmt.Errorf("storage unavailable")
	}

	rendered := failureLogsSections(t.Context(), stor, unreadable, failureLogsTestLogger(), apply, "summary body")

	assert.Contains(t, rendered, "<summary>Show logs (1 entry)</summary>")
	assert.NotContains(t, rendered, "engine logs")
}

// A long control-plane timeline must not be able to spend the room the engine
// fold needs: the engine's account of the failure is the reason an operator
// opens the comment. Both folds render, and together they stay inside the
// comment's budget.
func TestFailureLogsSectionsReservesRoomForTheEngineFold(t *testing.T) {
	apply := failureLogsTestApply()
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	var logs []*storage.ApplyLog
	for i := range 2000 {
		logs = append(logs, &storage.ApplyLog{ApplyID: apply.ID, Level: "info", Message: strings.Repeat("c", 200), CreatedAt: at.Add(time.Duration(i) * time.Second)})
	}
	stor := failureLogsTestStorage(logs...)
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return engineLogSource("region-a", "[orders] unsafe warning 1265: Data truncated"), nil
	}

	base := "summary body"
	rendered := failureLogsSections(t.Context(), stor, engineLogs, failureLogsTestLogger(), apply, base)

	assert.Contains(t, rendered, "Show recent apply logs (")
	assert.Contains(t, rendered, "[orders] unsafe warning 1265: Data truncated")
	assert.LessOrEqual(t, len(base)+len(rendered), templates.GitHubIssueCommentMaxChars-commentChromeHeadroom)
}

// Only a failed apply carries logs. A completed, stopped, or cancelled
// apply's summary stays clean, and the data plane is not read for one.
func TestFailureLogsSectionsSkipsAnApplyThatDidNotFail(t *testing.T) {
	apply := failureLogsTestApply()
	apply.State = state.Apply.Completed
	read := false
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		read = true
		return engineLogSource("region-a", "[orders] copy complete"), nil
	}

	rendered := failureLogsSections(t.Context(), failureLogsTestStorage(), engineLogs, failureLogsTestLogger(), apply, "summary body")

	assert.Empty(t, rendered)
	assert.False(t, read)
}

// A summary body that already fills GitHub's comment budget leaves room for
// neither fold. Both are dropped so the summary itself still posts, and
// neither load is attempted.
func TestFailureLogsSectionsDropsBothFoldsWhenTheCommentIsFull(t *testing.T) {
	apply := failureLogsTestApply()
	read := false
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		read = true
		return engineLogSource("region-a", "[orders] copy complete"), nil
	}
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed"})

	rendered := failureLogsSections(t.Context(), stor, engineLogs, failureLogsTestLogger(), apply,
		strings.Repeat("x", templates.GitHubIssueCommentMaxChars))

	require.Empty(t, rendered)
	assert.False(t, read)
}
