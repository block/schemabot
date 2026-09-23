package webhook

import (
	"context"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/mysqlerr"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// failureSummaryLogLimit bounds the log load for a failed apply's summary
// comment. It is derived, not a product choice: the comment's byte budget is
// the real limit, and this is the most entries that could ever render within
// it. Loading more rows could never add a rendered line, so the query stays
// bounded (engine log lines flow into apply_logs, so a long apply accumulates
// far more rows than any comment can carry) without the load bound ever being
// the reason a line is dropped. The newest entries are kept — the tail leading
// up to the failure is what an operator triaging from the PR needs.
const failureSummaryLogLimit = (templates.GitHubIssueCommentMaxChars - templates.CommentChromeHeadroom) / templates.MinRenderedLogLineChars

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

// applyLogGroupLabel names SchemaBot's own account of an apply inside the
// logs fold, beside the engine's. It renders only when the fold carries both.
const applyLogGroupLabel = "apply logs"

// summaryBodyRenderer renders a failed apply's summary body. It is a function
// rather than a string so the body can be rendered again once the logs are
// loaded: what the fold turns out to carry decides where the body's error
// sentence sends the reader.
type summaryBodyRenderer func(apply *storage.Apply) string

// summaryWithFailureLogs renders a terminal summary comment and the collapsed
// logs fold that belongs under it, as one body.
//
// The fold carries every account of the apply: SchemaBot's own, and — when the
// change ran on a data plane — the engine's, which otherwise reaches an
// operator only through the CLI. One fold means an operator triaging a failure
// opens one thing, and the accounts stay separate groups inside it rather than
// one interleaved stream, because each is written against a different clock.
// Only a failed apply carries logs, so a completed, stopped, or cancelled
// apply's summary stays clean.
//
// The body is rendered before the logs are loaded, because the room it leaves
// under GitHub's comment size cap is what the fold may spend: a large summary
// shrinks the fold, and one that leaves no meaningful room drops it, so
// appending never pushes the comment over the limit and blocks the summary
// from posting. When the fold turns out to carry the engine's own account, the
// body is rendered a second time against a reason that points at it: an error
// SchemaBot has no account of otherwise tells the reader to go and read a
// server log, while the line that explains the failure is a click away in the
// same comment.
//
// Each load runs under its own short deadline, detached from the caller's
// cancellation, so the fold is decided by storage and data-plane health alone.
// Best-effort throughout: a load failure is logged and costs the fold the
// group it would have filled, never the summary comment, and the full history
// remains available from the CLI and the server logs.
func summaryWithFailureLogs(ctx context.Context, stor storage.Storage, engineLogs EngineLogReader, logger failureLogsLogger, apply *storage.Apply, renderBody summaryBodyRenderer) string {
	body := renderBody(apply)
	if !state.IsState(apply.State, state.Apply.Failed) {
		return body
	}
	available := templates.GitHubIssueCommentMaxChars - templates.CommentChromeHeadroom - len(body)
	if available < templates.MinFailureLogsSectionChars {
		logger.Error("summary body leaves no room for the recent-logs section under the GitHub comment size limit; posting summary without recent logs",
			append(apply.LogAttrs(), "summary_chars", len(body))...)
		return body
	}
	groups := failureLogGroups(ctx, stor, engineLogs, logger, apply)
	if pointed := applyPointingAtRenderedLogs(apply, groups); pointed != apply {
		pointedBody := renderBody(pointed)
		pointedRoom := templates.GitHubIssueCommentMaxChars - templates.CommentChromeHeadroom - len(pointedBody)
		// The pointed sentence is longer than the one it replaces, so a body
		// that only just cleared the check above can fail it now. Keeping the
		// pointed body then would promise an account in the logs below and
		// post no fold at all, so the original sentence stands instead — it
		// names the server logs, which do have the reason.
		if pointedRoom < templates.MinFailureLogsSectionChars {
			logger.Error("pointing the failure summary at the rendered logs leaves no room for them under the GitHub comment size limit; keeping the reason that names the server logs",
				append(apply.LogAttrs(), "summary_chars", len(pointedBody))...)
		} else {
			body, available = pointedBody, pointedRoom
		}
	}
	return body + templates.RenderFailureLogs(groups, available)
}

// applyPointingAtRenderedLogs returns the apply as the summary should render
// it. When the fold will carry the engine's own account and the apply's error
// is the sentence that sends an operator to the server logs, the summary says
// so about the logs in front of them instead. The apply itself is never
// changed: the stored error is the record of what the target reported, and
// this is one surface's rendering of it.
func applyPointingAtRenderedLogs(apply *storage.Apply, groups []templates.LogGroupData) *storage.Apply {
	if !carriesEngineAccount(groups) {
		return apply
	}
	pointed := mysqlerr.PointToRenderedLogs(apply.ErrorMessage)
	if pointed == apply.ErrorMessage {
		return apply
	}
	rendered := *apply
	rendered.ErrorMessage = pointed
	return &rendered
}

// carriesEngineAccount reports whether any group came from a data plane, which
// is what makes "the logs below" true: an apply driven in this process has one
// group, and it is the same log the server writes.
func carriesEngineAccount(groups []templates.LogGroupData) bool {
	for _, group := range groups {
		if strings.HasPrefix(group.Label, engineLogGroupLabelPrefix) {
			return true
		}
	}
	return false
}

// failureLogGroups loads every account of a failed apply: SchemaBot's own, and
// the engine's from each data plane that ran it.
func failureLogGroups(ctx context.Context, stor storage.Storage, engineLogs EngineLogReader, logger failureLogsLogger, apply *storage.Apply) []templates.LogGroupData {
	groups := applyLogGroup(ctx, stor, logger, apply)
	engineGroups := engineLogGroups(ctx, engineLogs, logger, apply)
	// The apply's own account is named only when the engine's sits beside it:
	// on its own it is simply the log, and naming it would read as though
	// something were missing.
	if len(groups) == 1 && len(engineGroups) > 0 {
		groups[0].Label = applyLogGroupLabel
	}
	return append(groups, engineGroups...)
}

// applyLogGroup builds the group carrying SchemaBot's own log stream for the
// apply: the state transitions it recorded and, for an apply driven in this
// process, the engine lines that landed in the same storage.
func applyLogGroup(ctx context.Context, stor storage.Storage, logger failureLogsLogger, apply *storage.Apply) []templates.LogGroupData {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failureLogsLoadTimeout)
	defer cancel()
	// Load one entry beyond the limit so the fold can label itself honestly:
	// "Show logs" when it carries the complete history, "Show recent logs"
	// when older entries exist beyond the tail.
	logs, err := stor.ApplyLogs().GetRecentByApply(ctx, apply.ID, failureSummaryLogLimit+1)
	if err != nil {
		logger.Error("failed to load apply logs for failure summary; posting summary without recent logs",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	hasOlder := len(logs) > failureSummaryLogLimit
	if hasOlder {
		logs = logs[len(logs)-failureSummaryLogLimit:]
	}
	if len(logs) == 0 {
		logger.Debug("no apply logs recorded for this failed apply; posting summary without its own log group",
			apply.LogAttrs()...)
		return nil
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
	return []templates.LogGroupData{{Entries: entries, HasOlder: hasOlder}}
}

// engineLogGroups builds one group per data plane that recorded engine lines
// for the apply. An apply with no data plane produces none, so its summary
// renders exactly as it did before the engine's account reached the PR.
func engineLogGroups(ctx context.Context, engineLogs EngineLogReader, logger failureLogsLogger, apply *storage.Apply) []templates.LogGroupData {
	if engineLogs == nil {
		logger.Debug("no data-plane log reader wired for this apply's summary; posting summary without the engine's account",
			apply.LogAttrs()...)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), engineLogsLoadTimeout)
	defer cancel()
	sources, err := engineLogs(ctx, apply, failureSummaryLogLimit)
	if err != nil {
		logger.Error("failed to read data-plane engine logs for failure summary; posting summary without the engine's account",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	if len(sources) == 0 {
		logger.Debug("no data-plane engine logs for this apply; posting summary without the engine's account",
			apply.LogAttrs()...)
		return nil
	}
	groups := make([]templates.LogGroupData, 0, len(sources))
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
		groups = append(groups, templates.LogGroupData{
			Label:    engineLogGroupLabel(source.Deployment, source.Target),
			Entries:  entries,
			HasOlder: source.HasOlder,
		})
	}
	return groups
}

// engineLogGroupLabelPrefix opens every heading a data plane's account gets,
// and is what tells the two kinds of group apart after they are assembled.
const engineLogGroupLabelPrefix = "engine logs: "

// engineLogGroupLabel names one data plane's account of the apply. The target
// is part of the name whenever the source carries one: a deployment that
// drives several targets produces a group per target, and without the target
// an operator cannot tell which one raised the warning they are reading.
//
// Each name is clamped on its own rather than the joined label being cut to
// length, so a deployment long enough to fill the heading cannot cost the
// target beside it the characters that tell two targets apart.
func engineLogGroupLabel(deployment, target string) string {
	label := engineLogGroupLabelPrefix + templates.ElideMiddle(deployment, templates.MaxGroupLabelPartChars)
	if target != "" {
		label += ", target: " + templates.ElideMiddle(target, templates.MaxGroupLabelPartChars)
	}
	return label
}
