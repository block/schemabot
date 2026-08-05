package templates

import (
	"fmt"
	"strings"
)

// RenderMootedCancelNote renders the warning appended to a completed apply's
// terminal summary when an accepted cancel never took effect: the engine
// completed the schema change before the cancel could act. The operator asked
// for the change not to land, so the summary must say plainly that it landed
// anyway and is live on the target — otherwise the accepted cancel followed
// by a completed summary reads as if the cancel worked.
func RenderMootedCancelNote(requestedBy string) string {
	var sb strings.Builder
	sb.WriteString("\n> ⚠️ **Cancel did not take effect** — the schema change completed on the engine before the cancel")
	if requestedBy != "" {
		fmt.Fprintf(&sb, " requested by @%s", requestedBy)
	}
	sb.WriteString(" could act. The change is live on the target.\n")
	return sb.String()
}
