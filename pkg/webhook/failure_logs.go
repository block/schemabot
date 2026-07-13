package webhook

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// failureSummaryLogLimit bounds the log load for a failed apply's summary
// comment. It is derived, not a product choice: the fold's byte budget is the
// real limit, and this is the most entries that could ever render within it.
// Loading more rows could never add a rendered line, so the query stays
// bounded (engine log lines flow into apply_logs, so a long apply accumulates
// far more rows than any comment can carry) without the load bound ever being
// the reason a line is dropped. The newest entries are kept — the tail leading
// up to the failure is what an operator triaging from the PR needs.
const failureSummaryLogLimit = templates.MaxFailureLogsSectionChars / templates.MinRenderedLogLineChars

// failureLogsLoadTimeout bounds the log load so a slow storage read degrades
// to a summary without logs rather than delaying the terminal comment.
const failureLogsLoadTimeout = 2 * time.Second

// gitHubIssueCommentMaxChars is GitHub's hard cap on an issue comment body — a
// larger body is rejected outright, so anything appended to a summary must fit
// within what the summary itself leaves free.
const gitHubIssueCommentMaxChars = 65536

// commentChromeHeadroom reserves room under the GitHub cap for markup added to
// the body after the section is appended (the support-channel footer) plus
// margin, so the assembled comment never lands exactly at the limit.
const commentChromeHeadroom = 1024

// failureLogsSection renders the collapsed recent-logs section for a terminal
// summary comment whose already-rendered body is baseBody. Only a failed apply
// carries logs — a completed, stopped, or cancelled apply's summary stays
// clean, so this returns "" for those states. The section spends only the room
// baseBody leaves under GitHub's comment size cap: a large summary shrinks the
// fold, and one that leaves no meaningful room drops it, so appending never
// pushes the comment over the limit and blocks the summary from posting. The
// load runs under its own short deadline, detached from the caller's
// cancellation, so the section is decided by storage health alone. Best-effort:
// a log-load failure is logged and returns "" so the summary comment still
// posts; the full history remains available from the CLI and the server logs.
func failureLogsSection(ctx context.Context, stor storage.Storage, logger interface {
	Error(msg string, args ...any)
}, apply *storage.Apply, baseBody string) string {
	if !state.IsState(apply.State, state.Apply.Failed) {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failureLogsLoadTimeout)
	defer cancel()
	// Load one entry beyond the limit so the fold can label itself honestly:
	// "Logs" when it carries the complete history, "Recent logs" when older
	// entries exist beyond the tail.
	logs, err := stor.ApplyLogs().GetRecentByApply(ctx, apply.ID, failureSummaryLogLimit+1)
	if err != nil {
		logger.Error("failed to load apply logs for failure summary; posting summary without recent logs",
			append(apply.LogAttrs(), "error", err)...)
		return ""
	}
	hasOlder := len(logs) > failureSummaryLogLimit
	if hasOlder {
		logs = logs[len(logs)-failureSummaryLogLimit:]
	}
	entries := make([]templates.LogEntryData, len(logs))
	for i, entry := range logs {
		entries[i] = templates.LogEntryData{
			CreatedAt: entry.CreatedAt,
			Level:     entry.Level,
			Message:   entry.Message,
			OldState:  entry.OldState,
			NewState:  entry.NewState,
		}
	}
	available := gitHubIssueCommentMaxChars - commentChromeHeadroom - len(baseBody)
	section := templates.RenderRecentFailureLogs(entries, available, hasOlder)
	if section == "" && len(entries) > 0 {
		logger.Error("summary body leaves no room for the recent-logs section under the GitHub comment size limit; posting summary without recent logs",
			append(apply.LogAttrs(), "summary_chars", len(baseBody), "log_entries", len(entries))...)
	}
	return section
}
