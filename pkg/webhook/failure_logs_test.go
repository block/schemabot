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
	"github.com/block/schemabot/pkg/mysqlerr"
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

// renderFailureLogsSection renders the fold summaryWithFailureLogs appends
// under a summary body, without the body itself, so a test can assert on the
// fold alone while still driving the production entry point.
func renderFailureLogsSection(t *testing.T, stor storage.Storage, engineLogs EngineLogReader, apply *storage.Apply, base string) string {
	t.Helper()
	rendered := summaryWithFailureLogs(t.Context(), stor, engineLogs, failureLogsTestLogger(), apply,
		func(*storage.Apply) string { return base })
	return strings.TrimPrefix(rendered, base)
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
// operator only through the CLI. One fold holds both, each under its own
// heading, and the control plane's comes first because it sets the scene for
// the engine's lines.
func TestFailureLogsSectionRendersEveryAccountInOneFold(t *testing.T) {
	apply := failureLogsTestApply()
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return engineLogSource("region-a", "[orders] unsafe warning 1265: Data truncated"), nil
	}

	rendered := renderFailureLogsSection(t, stor, engineLogs, apply, "summary body")

	assert.Contains(t, rendered, "<summary>Show logs (2 entries)</summary>")
	assert.Equal(t, 1, strings.Count(rendered, "<details>"), "one fold carries every account of the apply")
	assert.Contains(t, rendered, "== apply logs ==")
	assert.Contains(t, rendered, "Apply failed [running -> failed]")
	assert.Contains(t, rendered, "== engine logs: region-a ==")
	assert.Contains(t, rendered, "[orders] unsafe warning 1265: Data truncated")
	assert.Less(t, strings.Index(rendered, "== apply logs =="), strings.Index(rendered, "== engine logs: region-a =="),
		"the apply's own timeline reads before the engine's account of the failure")
}

// An apply with no data plane renders exactly what it rendered before the
// engine's account reached the PR: one unnamed group spending the whole
// budget. An empty engine group would be worse than none — it reads as an
// engine that said nothing, when its lines are in the group above it.
func TestFailureLogsSectionOmitsTheEngineGroupWithoutADataPlane(t *testing.T) {
	apply := failureLogsTestApply()
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	noSources := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return nil, nil
	}

	withReader := renderFailureLogsSection(t, stor, noSources, apply, "summary body")
	withoutReader := renderFailureLogsSection(t, stor, nil, apply, "summary body")

	assert.NotContains(t, withReader, "engine logs")
	assert.NotContains(t, withReader, "==", "one account needs no heading")
	assert.Equal(t, withoutReader, withReader,
		"a data plane that reported no engine lines renders the same summary as an apply that never had one")
}

// A data plane that cannot be read costs the summary the engine's group and
// nothing else. The terminal comment is how the PR learns the apply ended, so
// an unreachable deployment must never be the reason it does not post.
func TestFailureLogsSectionKeepsTheSummaryWhenTheDataPlaneCannotBeRead(t *testing.T) {
	apply := failureLogsTestApply()
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	unreadable := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return nil, fmt.Errorf("storage unavailable")
	}

	rendered := renderFailureLogsSection(t, stor, unreadable, apply, "summary body")

	assert.Contains(t, rendered, "<summary>Show logs (1 entry)</summary>")
	assert.NotContains(t, rendered, "engine logs")
	assert.NotContains(t, rendered, "==", "the apply's own account is unnamed when it is the only one")
}

// A long control-plane timeline must not be able to spend the room the
// engine's lines need: the engine's account of the failure is the reason an
// operator opens the comment. Each account gets its own share, both render,
// and the fold stays inside the comment's budget.
func TestFailureLogsSectionSharesTheRoomWithTheEngineGroup(t *testing.T) {
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
	rendered := renderFailureLogsSection(t, stor, engineLogs, apply, base)

	assert.Contains(t, rendered, "Show recent logs (")
	assert.Contains(t, rendered, "[orders] unsafe warning 1265: Data truncated")
	assert.LessOrEqual(t, len(base)+len(rendered), templates.GitHubIssueCommentMaxChars-commentChromeHeadroom)
}

// Only a failed apply carries logs. A completed, stopped, or cancelled
// apply's summary stays clean, and the data plane is not read for one.
func TestFailureLogsSectionSkipsAnApplyThatDidNotFail(t *testing.T) {
	apply := failureLogsTestApply()
	apply.State = state.Apply.Completed
	read := false
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		read = true
		return engineLogSource("region-a", "[orders] copy complete"), nil
	}

	rendered := renderFailureLogsSection(t, failureLogsTestStorage(), engineLogs, apply, "summary body")

	assert.Empty(t, rendered)
	assert.False(t, read)
}

// A summary body that already fills GitHub's comment budget leaves no room
// for the fold at all. It is dropped so the summary itself still posts, and
// neither load is attempted.
func TestFailureLogsSectionDropsTheFoldWhenTheCommentIsFull(t *testing.T) {
	apply := failureLogsTestApply()
	read := false
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		read = true
		return engineLogSource("region-a", "[orders] copy complete"), nil
	}
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed"})

	rendered := renderFailureLogsSection(t, stor, engineLogs, apply,
		strings.Repeat("x", templates.GitHubIssueCommentMaxChars))

	require.Empty(t, rendered)
	assert.False(t, read)
}

// A failure SchemaBot has no account of reports the generic sentence, which
// tells an operator to read a server log. When the fold below it carries the
// engine's own lines, that sentence sends them to a server they may not be
// able to reach while the line explaining the failure is one click away — so
// the summary points at the fold instead. The stored error is untouched: it is
// the record of what the target reported, and this is one surface's rendering.
func TestSummaryWithFailureLogsPointsTheGenericErrorAtTheFold(t *testing.T) {
	apply := failureLogsTestApply()
	apply.ErrorMessage = mysqlerr.Generic + " (error 1265)"
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return engineLogSource("region-a", "[orders] unsafe warning 1265: Data truncated"), nil
	}

	rendered := summaryWithFailureLogs(t.Context(), stor, engineLogs, failureLogsTestLogger(), apply,
		func(apply *storage.Apply) string { return "Error: " + apply.ErrorMessage + "\n" })

	assert.Contains(t, rendered, "Error: "+mysqlerr.GenericRenderedLogs+" (error 1265)")
	assert.NotContains(t, rendered, "see the server logs for the reason")
	assert.Equal(t, mysqlerr.Generic+" (error 1265)", apply.ErrorMessage, "the stored error is the record of what the target reported")
}

// Without the engine's account the fold holds only what the server already
// logged, so "the logs below" would be a promise the comment cannot keep. The
// summary keeps pointing at the server logs, where the reason actually is.
func TestSummaryWithFailureLogsKeepsTheServerLogsPointerWithoutAnEngineGroup(t *testing.T) {
	apply := failureLogsTestApply()
	apply.ErrorMessage = mysqlerr.Generic + " (error 1265)"
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})

	rendered := summaryWithFailureLogs(t.Context(), stor, nil, failureLogsTestLogger(), apply,
		func(apply *storage.Apply) string { return "Error: " + apply.ErrorMessage + "\n" })

	assert.Contains(t, rendered, "Error: "+mysqlerr.Generic+" (error 1265)")
	assert.NotContains(t, rendered, "in the logs below")
}

// A reason SchemaBot chose from the target's error code already says what an
// operator should do about it. The fold below adds detail; it does not replace
// the instruction, so the sentence stands whatever the fold turns out to hold.
func TestSummaryWithFailureLogsLeavesAnAuthoredReasonAlone(t *testing.T) {
	apply := failureLogsTestApply()
	authored := "Existing rows hold duplicate values for a unique key. Resolve the duplicates before applying a change that adds or narrows that key. (error 1062)"
	apply.ErrorMessage = authored
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return engineLogSource("region-a", "[orders] duplicate entry"), nil
	}

	rendered := summaryWithFailureLogs(t.Context(), stor, engineLogs, failureLogsTestLogger(), apply,
		func(apply *storage.Apply) string { return "Error: " + apply.ErrorMessage + "\n" })

	assert.Contains(t, rendered, "Error: "+authored)
	assert.Contains(t, rendered, "== engine logs: region-a ==")
}

// One deployment can drive several targets whose names differ only at the
// tail. The heading names both, and each name is clamped on its own, so a
// long deployment cannot cost the target beside it the characters that tell
// two groups apart — and the whole heading still fits its budget.
func TestEngineLogGroupLabelKeepsLongTargetsDistinct(t *testing.T) {
	const deployment = "payments-production-us-west-2"
	third := engineLogGroupLabel(deployment, "payments-production-shard-003")
	fourth := engineLogGroupLabel(deployment, "payments-production-shard-004")

	assert.NotEqual(t, third, fourth)
	assert.LessOrEqual(t, len(third), templates.MaxGroupLabelChars)
	assert.LessOrEqual(t, len(fourth), templates.MaxGroupLabelChars)
	assert.True(t, strings.HasPrefix(third, engineLogGroupLabelPrefix))
	assert.Equal(t, "engine logs: region-a, target: cluster-a", engineLogGroupLabel("region-a", "cluster-a"),
		"names that fit are left alone")
	assert.Equal(t, "engine logs: region-a", engineLogGroupLabel("region-a", ""))
}

// The sentence pointing at the fold is longer than the one naming the server
// logs, so a summary that only just had room for the fold can lose it to the
// rewrite. Promising an account in the logs below and then posting no fold is
// worse than the generic sentence, so the original stands in that case.
func TestSummaryWithFailureLogsKeepsTheServerLogsPointerWhenTheFoldWouldNotFit(t *testing.T) {
	apply := failureLogsTestApply()
	apply.ErrorMessage = mysqlerr.Generic + " (error 1265)"
	stor := failureLogsTestStorage(&storage.ApplyLog{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"})
	engineLogs := func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
		return engineLogSource("region-a", "[orders] unsafe warning 1265: Data truncated"), nil
	}
	// A body that clears the room check by a margin narrower than the rewrite
	// grows it: the unpointed body leaves exactly the minimum, the pointed one
	// leaves less.
	room := templates.GitHubIssueCommentMaxChars - commentChromeHeadroom - templates.MinFailureLogsSectionChars
	pad := strings.Repeat("x", room-len(apply.ErrorMessage))
	renderBody := func(apply *storage.Apply) string {
		return pad + apply.ErrorMessage
	}

	rendered := summaryWithFailureLogs(t.Context(), stor, engineLogs, failureLogsTestLogger(), apply, renderBody)

	assert.Contains(t, rendered, mysqlerr.Generic+" (error 1265)")
	assert.NotContains(t, rendered, "in the logs below", "the summary never promises a fold it does not carry")
	assert.Contains(t, rendered, "<details>", "the fold the original body had room for still renders")
}

// Rendering the summary body twice must not read storage twice. A failed
// apply's summary can be rendered again once the fold's contents are known, so
// every section under the body is loaded before the first render and rendered
// from the loaded value. A best-effort read that succeeded the first time and
// failed the second would otherwise drop a section from the body posted.
func TestSummaryCommentFromOpsReadsEachSectionOnce(t *testing.T) {
	apply := failureLogsTestApply()
	apply.ErrorMessage = mysqlerr.Generic + " (error 1265)"
	reads := 0
	observer := &CommentObserver{
		stor: &stubStorage{
			ops:          &stubApplyOperationStore{},
			applyLogs:    &stubApplyLogStore{logs: []*storage.ApplyLog{{ApplyID: apply.ID, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"}}},
			settledReads: &reads,
			settled: []*storage.ApplyControlRequest{{
				Operation: storage.ControlOperationStop, Status: storage.ControlRequestFailed,
				ErrorMessage: "the apply had already finished", RequestedBy: "example-operator",
			}},
		},
		logger: failureLogsTestLogger(),
		engineLogs: func(context.Context, *storage.Apply, int) ([]api.EngineLogSource, error) {
			return engineLogSource("region-a", "[orders] unsafe warning 1265: Data truncated"), nil
		},
	}

	body := observer.summaryCommentFromOps(t.Context(), apply, nil, nil, nil, nil)

	assert.Equal(t, 1, reads, "the settled control requests are read once however often the body renders")
	assert.Contains(t, body, "the apply had already finished", "the notice survives the second render")
	// The summary escapes the reason for markdown, so the rendered sentence is
	// matched by the part of it that carries no apostrophe.
	assert.Contains(t, body, "account of it is in the logs below.", "the second render is what points at the fold")
	assert.NotContains(t, body, "see the server logs for the reason")
	assert.Contains(t, body, "== engine logs: region-a ==")
}
