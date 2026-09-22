package webhook

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// commentChromeHeadroom reserves room under GitHub's comment size cap for
// markup added to the body after the section is appended (the support-channel
// footer) plus margin, so the assembled comment never lands exactly at the
// limit. Config validation caps the support-channel name and URL lengths so
// the rendered footer always fits inside this reservation.
const commentChromeHeadroom = 1024

// failureSummaryLogLimit bounds the log load for a failed apply's summary
// comment. It is derived, not a product choice: the comment's byte budget is
// the real limit, and this is the most entries that could ever render within
// it. Loading more rows could never add a rendered line, so the query stays
// bounded (engine log lines flow into apply_logs, so a long apply accumulates
// far more rows than any comment can carry) without the load bound ever being
// the reason a line is dropped. The newest entries are kept — the tail leading
// up to the failure is what an operator triaging from the PR needs.
const failureSummaryLogLimit = (templates.GitHubIssueCommentMaxChars - commentChromeHeadroom) / templates.MinRenderedLogLineChars

// failureLogsLoadTimeout bounds the log load so a slow storage read degrades
// to a summary without logs rather than delaying the terminal comment.
const failureLogsLoadTimeout = 2 * time.Second

// engineLogsLoadTimeout bounds the data-plane read behind the engine-logs
// fold. It is longer than the control-plane load because it crosses a network
// to another deployment's endpoint, and still short enough that an
// unreachable data plane costs the terminal comment a pause rather than a
// delivery.
const engineLogsLoadTimeout = 3 * time.Second

// failureLogsLogger is what the failure-logs sections need from a logger:
// Error for the loads that failed and Debug for the ones that were never
// wired or had nothing to read.
type failureLogsLogger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
}

// EngineLogReader reads the engine's own account of an apply from the data
// planes that ran it, as api.Service.EngineApplyLogs does. It is injected
// rather than reached through the service so the comment observer, which
// holds storage and a GitHub client but no service, can render the fold, and
// so a test can drive the rendering without a data plane. A nil reader means
// no data-plane reader was wired: the engine fold is skipped and the summary
// renders as it does for an apply with no data plane.
type EngineLogReader func(ctx context.Context, apply *storage.Apply, limit int) ([]api.EngineLogSource, error)

// engineLogReader returns the reader that backs the engine-logs fold, or nil
// when the handler has no service to read a data plane through. Every comment
// surface that can render a failed apply's summary builds its reader here, so
// the fold appears on the same terms whichever surface posted the comment.
func (h *Handler) engineLogReader() EngineLogReader {
	if h.service == nil {
		return nil
	}
	return h.service.EngineApplyLogs
}

// failureLogsSections renders the collapsed log folds for a terminal summary
// comment whose already-rendered body is baseBody: SchemaBot's own account of
// the apply, and — when the change ran on a data plane — the engine's account
// of the failure, which otherwise reaches an operator only through the CLI.
// Only a failed apply carries logs — a completed, stopped, or cancelled
// apply's summary stays clean, so this returns "" for those states.
//
// The folds spend only the room baseBody leaves under GitHub's comment size
// cap: a large summary shrinks them, and one that leaves no meaningful room
// drops both, so appending never pushes the comment over the limit and blocks
// the summary from posting. Each load runs under its own short deadline,
// detached from the caller's cancellation, so the folds are decided by
// storage and data-plane health alone. Best-effort throughout: a load failure
// is logged and drops its fold so the summary comment still posts, and the
// full history remains available from the CLI and the server logs.
func failureLogsSections(ctx context.Context, stor storage.Storage, engineLogs EngineLogReader, logger failureLogsLogger, apply *storage.Apply, baseBody string) string {
	if !state.IsState(apply.State, state.Apply.Failed) {
		return ""
	}
	// Check the room before touching storage: a summary body that leaves no
	// renderable space makes the load pure waste.
	available := templates.GitHubIssueCommentMaxChars - commentChromeHeadroom - len(baseBody)
	if available < templates.MinFailureLogsSectionChars {
		logger.Error("summary body leaves no room for the recent-logs section under the GitHub comment size limit; posting summary without recent logs",
			append(apply.LogAttrs(), "summary_chars", len(baseBody))...)
		return ""
	}
	// The engine fold is rendered first so it can claim its half of the room
	// before the apply fold spends it: the engine's lines are the reason an
	// operator opens a failed apply's comment, and a long control-plane
	// timeline must not be able to push them out. It is appended second,
	// because the apply's own timeline is what sets the scene for them.
	engineSection := engineFailureLogsSection(ctx, engineLogs, logger, apply, available/2)
	companion := templates.LogFoldAlone
	if engineSection != "" {
		companion = templates.LogFoldBesideEngineLogs
	}
	applySection := applyFailureLogsSection(ctx, stor, logger, apply, available-len(engineSection), companion)
	return applySection + engineSection
}

// applyFailureLogsSection renders the fold carrying SchemaBot's own log
// stream for the apply: the state transitions it recorded and, for an apply
// driven in this process, the engine lines that landed in the same storage.
// companion names the fold for what the summary carries beside it.
func applyFailureLogsSection(ctx context.Context, stor storage.Storage, logger failureLogsLogger, apply *storage.Apply, available int, companion templates.LogFoldCompanion) string {
	if available < templates.MinFailureLogsSectionChars {
		logger.Debug("no room left for the apply-logs fold after the engine-logs fold; posting summary with engine logs only",
			append(apply.LogAttrs(), "available_chars", available)...)
		return ""
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failureLogsLoadTimeout)
	defer cancel()
	// Load one entry beyond the limit so the fold can label itself honestly:
	// "Show logs" when it carries the complete history, "Show recent logs"
	// when older entries exist beyond the tail.
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
	return templates.RenderRecentFailureLogs(entries, available, hasOlder, companion)
}

// engineFailureLogsSection renders the fold carrying the engine's own lines,
// read back from the data planes that ran the apply. An apply with no data
// plane produces no sources and no fold.
func engineFailureLogsSection(ctx context.Context, engineLogs EngineLogReader, logger failureLogsLogger, apply *storage.Apply, available int) string {
	if engineLogs == nil {
		logger.Debug("no data-plane log reader wired for this apply's summary; posting summary without the engine-logs fold",
			apply.LogAttrs()...)
		return ""
	}
	if available < templates.MinFailureLogsSectionChars {
		logger.Error("summary body leaves no room for the engine-logs section under the GitHub comment size limit; posting summary without engine logs",
			append(apply.LogAttrs(), "available_chars", available)...)
		return ""
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), engineLogsLoadTimeout)
	defer cancel()
	sources, err := engineLogs(ctx, apply, failureSummaryLogLimit)
	if err != nil {
		logger.Error("failed to read data-plane engine logs for failure summary; posting summary without the engine-logs fold",
			append(apply.LogAttrs(), "error", err)...)
		return ""
	}
	if len(sources) == 0 {
		logger.Debug("no data-plane engine logs for this apply; posting summary without the engine-logs fold",
			apply.LogAttrs()...)
		return ""
	}
	rendered := make([]templates.EngineLogSourceData, 0, len(sources))
	for _, source := range sources {
		entries := make([]templates.LogEntryData, len(source.Entries))
		for i, entry := range source.Entries {
			entries[i] = templates.LogEntryData{
				CreatedAt: entry.CreatedAt,
				Level:     entry.Level,
				Message:   entry.Message,
				OldState:  entry.OldState,
				NewState:  entry.NewState,
			}
		}
		rendered = append(rendered, templates.EngineLogSourceData{
			Deployment: source.Deployment,
			Entries:    entries,
			HasOlder:   source.HasOlder,
		})
	}
	return templates.RenderEngineFailureLogs(rendered, available)
}
