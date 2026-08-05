package webhook

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// mootedCancelLoadTimeout bounds the cancel-request load so a slow storage
// read degrades to a summary without the note rather than delaying the
// terminal comment.
const mootedCancelLoadTimeout = 2 * time.Second

// mootedCancelSection renders the note disclosing that a cancel was accepted
// but the schema change completed on the engine before it could take effect.
// Only a completed apply carrying a non-failed cancel request qualifies: an
// effective cancel settles the apply cancelled, so a completed apply whose
// cancel request was accepted (pending or completed) means the engine finished
// first and the change is live on the target. A failed request was rejected
// with its own operator-visible reason, so it renders nothing. The load runs
// under its own short deadline, detached from the caller's cancellation.
// Best-effort: a storage failure is logged and returns "" so the summary
// comment still posts; the disclosure remains in the apply event history.
func mootedCancelSection(ctx context.Context, stor storage.Storage, logger interface {
	Error(msg string, args ...any)
}, apply *storage.Apply, baseBody string) string {
	if !state.IsState(apply.State, state.Apply.Completed) {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mootedCancelLoadTimeout)
	defer cancel()
	req, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationCancel)
	if err != nil {
		logger.Error("failed to load cancel control request for terminal summary; posting summary without mooted-cancel note",
			append(apply.LogAttrs(), "error", err)...)
		return ""
	}
	if req == nil || req.Status == storage.ControlRequestFailed {
		return ""
	}
	note := templates.RenderMootedCancelNote(req.RequestedBy)
	if len(baseBody)+len(note) > templates.GitHubIssueCommentMaxChars-commentChromeHeadroom {
		logger.Error("summary body leaves no room for the mooted-cancel note under the GitHub comment size limit; posting summary without it",
			append(apply.LogAttrs(), "summary_chars", len(baseBody))...)
		return ""
	}
	return note
}
