package webhook

import (
	"context"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// failureSummaryLogLimit is how many log entries a failed apply's summary
// comment carries. The newest entries are kept — the tail leading up to the
// failure is what an operator triaging from the PR needs.
const failureSummaryLogLimit = 50

// failureLogsSection renders the collapsed recent-logs section for a terminal
// summary comment. Only a failed apply carries logs — a completed, stopped, or
// cancelled apply's summary stays clean, so this returns "" for those states.
// Best-effort: a log-load failure is logged and returns "" so the summary
// comment still posts; the full logs remain available from the CLI.
func failureLogsSection(ctx context.Context, stor storage.Storage, logger interface {
	Error(msg string, args ...any)
}, apply *storage.Apply) string {
	if !state.IsState(apply.State, state.Apply.Failed) {
		return ""
	}
	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	if err != nil {
		logger.Error("failed to load apply logs for failure summary; posting summary without recent logs",
			append(apply.LogAttrs(), "error", err)...)
		return ""
	}
	if len(logs) > failureSummaryLogLimit {
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
	return templates.RenderRecentFailureLogs(entries)
}
