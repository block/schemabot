package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// ControlRejectionData is one operator control command that SchemaBot accepted
// and then could not carry out.
type ControlRejectionData struct {
	// Operation is the control operation as the operator issued it (stop,
	// cutover, revert, …).
	Operation string
	// Message is the engine or driver explanation for the rejection. It is
	// sanitized before rendering.
	Message string
	// RequestedBy identifies who issued the command, when known. Requests that
	// only ever carry SchemaBot's internal forwarding caller name no operator,
	// and the notice omits the attribution rather than crediting the command to
	// an internal path.
	RequestedBy string
}

// RenderControlRejections renders the notice appended to an apply's PR comment
// for control commands that were accepted but never took effect. Acknowledging a
// control command only means it was queued, so an operator who sees the
// acknowledgement and no effect otherwise has nothing to act on. Returns "" when
// nothing was rejected, so the comment renders unchanged.
func RenderControlRejections(rejections []ControlRejectionData) string {
	if len(rejections) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("\n> [!WARNING]\n")
	sb.WriteString("> **Command not applied**\n>\n")
	for _, r := range rejections {
		fmt.Fprintf(&sb, "> - `%s` was accepted but did not take effect", sanitizeCommentError(r.Operation))
		if storage.ControlRequestNamesAnOperator(r.RequestedBy) {
			fmt.Fprintf(&sb, " (requested by `%s`)", sanitizeCommentError(r.RequestedBy))
		}
		if msg := sanitizeCommentError(r.Message); msg != "" {
			fmt.Fprintf(&sb, ": %s", quoteBlockLines(msg))
		}
		sb.WriteString("\n")
	}
	sb.WriteString(">\n> The schema change is otherwise unaffected. Re-issue the command or reconcile the target environment.\n")
	return sb.String()
}
