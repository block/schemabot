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

// controlRejectionSection renders the "command not applied" notice for an apply
// whose already-rendered comment body is baseBody. Accepting a control command
// only means it was queued: the driver — local, or remote and mirrored back —
// can still fail it, and without this the operator sees an acknowledgement and
// no effect. The notice is rebuilt from storage on every render, so it stays
// accurate as commands settle and never double-posts. It spends only the room
// baseBody leaves under GitHub's comment size cap, so appending it can never
// push the comment over the limit and block the update. The load runs under its
// own short deadline, detached from the caller's cancellation. Best-effort: a
// load failure is logged and returns "" so the comment still posts; the
// rejection remains in the apply logs and the server logs.
func controlRejectionSection(ctx context.Context, stor storage.Storage, logger interface {
	Error(msg string, args ...any)
}, apply *storage.Apply, baseBody string) string {
	requests := stor.ControlRequests()
	if requests == nil {
		logger.Error("comment will omit rejected control commands: control request store is not available",
			apply.LogAttrs()...)
		return ""
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controlRejectionLoadTimeout)
	defer cancel()
	settled, err := requests.ListSettled(ctx, apply.ID)
	if err != nil {
		logger.Error("comment will omit rejected control commands: failed to load control requests",
			append(apply.LogAttrs(), "error", err)...)
		return ""
	}
	rejections := make([]templates.ControlRejectionData, 0, len(settled))
	for _, req := range settled {
		if req.Status != storage.ControlRequestFailed {
			continue
		}
		rejections = append(rejections, templates.ControlRejectionData{
			Operation:   string(req.Operation),
			Message:     req.ErrorMessage,
			RequestedBy: req.RequestedBy,
		})
	}
	section := templates.RenderControlRejections(rejections)
	if section == "" {
		return ""
	}
	if len(baseBody)+len(section) > templates.GitHubIssueCommentMaxChars-commentChromeHeadroom {
		logger.Error("comment body leaves no room for the rejected-command notice under the GitHub comment size limit; posting without it",
			append(apply.LogAttrs(), "comment_chars", len(baseBody))...)
		return ""
	}
	return section
}
