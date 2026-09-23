package webhook

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// controlRejectionLoadTimeout bounds the control-request load so a slow storage
// read degrades to a comment without the notice rather than delaying the update.
const controlRejectionLoadTimeout = 2 * time.Second

// controlRejectionLogger is what the notice needs from a logger: Error for the
// load that failed and for a body with no room left for the notice.
type controlRejectionLogger interface {
	Error(msg string, args ...any)
}

// loadControlRejections reads the control commands this apply rejected.
// Accepting a control command only means it was queued: the driver — local, or
// remote and mirrored back — can still fail it, and without the notice the
// operator sees an acknowledgement and no effect. The set is rebuilt from
// storage on every comment render, so it stays accurate as commands settle and
// never double-posts. The load runs under its own short deadline, detached
// from the caller's cancellation. Best-effort: a load failure is logged and
// returns nothing so the comment still posts; the rejection remains in the
// apply logs and the server logs.
//
// Loading is separate from rendering so a caller that renders the same body
// more than once reads storage once. Two renders of one comment must differ
// only in what the caller changed between them: a second read that failed
// where the first succeeded would silently drop the notice from the body
// actually posted.
func loadControlRejections(ctx context.Context, stor storage.Storage, logger controlRejectionLogger, apply *storage.Apply) []templates.ControlRejectionData {
	requests := stor.ControlRequests()
	if requests == nil {
		logger.Error("comment will omit rejected control commands: control request store is not available",
			apply.LogAttrs()...)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controlRejectionLoadTimeout)
	defer cancel()
	settled, err := requests.ListSettled(ctx, apply.ID)
	if err != nil {
		logger.Error("comment will omit rejected control commands: failed to load control requests",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	rejections := make([]templates.ControlRejectionData, 0, len(settled))
	for _, req := range settled {
		if req.Status != storage.ControlRequestFailed {
			continue
		}
		if req.Operation.Retired() {
			// A rejection recorded by a previous release for an operation this
			// release removed: the notice would tell the operator to re-issue a
			// command that no longer exists, and nothing can ever clear it. The
			// row stays in storage; it just no longer renders.
			continue
		}
		rejections = append(rejections, templates.ControlRejectionData{
			Operation:   string(req.Operation),
			Message:     req.ErrorMessage,
			RequestedBy: req.RequestedBy,
		})
	}
	return rejections
}

// renderControlRejections renders the "command not applied" notice under a
// comment body that has already been rendered. It spends only the room
// baseBody leaves under GitHub's comment size cap, so appending it can never
// push the comment over the limit and block the update.
func renderControlRejections(rejections []templates.ControlRejectionData, logger controlRejectionLogger, apply *storage.Apply, baseBody string) string {
	section := templates.RenderControlRejections(rejections)
	if section == "" {
		return ""
	}
	if len(baseBody)+len(section) > templates.GitHubIssueCommentMaxChars-templates.CommentChromeHeadroom {
		logger.Error("comment body leaves no room for the rejected-command notice under the GitHub comment size limit; posting without it",
			append(apply.LogAttrs(), "comment_chars", len(baseBody))...)
		return ""
	}
	return section
}

// controlRejectionSection loads and renders the notice in one step, for a
// caller that renders its comment body exactly once.
func controlRejectionSection(ctx context.Context, stor storage.Storage, logger controlRejectionLogger, apply *storage.Apply, baseBody string) string {
	return renderControlRejections(loadControlRejections(ctx, stor, logger, apply), logger, apply, baseBody)
}
