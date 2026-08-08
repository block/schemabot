package templates

import (
	"fmt"
	"strings"
)

// MergeQueueBlockedTarget names one database whose stored check state blocked
// merge-queue admission for this PR.
type MergeQueueBlockedTarget struct {
	Database    string
	Environment string
}

// MergeQueueEjectedData describes a merge-queue admission block: the PR
// entered the queue, its stored check state turned out to be blocking, and the
// blocking admission check removed it from the queue.
type MergeQueueEjectedData struct {
	// Blocking lists the databases whose stored check state blocked admission,
	// deduplicated. May be empty when the blocking rows carry no database
	// identifiers; the guidance stands on its own.
	Blocking []MergeQueueBlockedTarget
}

// RenderMergeQueueEjected renders the PR comment posted when the merge-queue
// admission check blocks a queued pull request. The blocking Check Run lives
// on the synthetic merge-group commit, not the PR head, so without this
// comment the author only sees the pull request silently leave the queue.
// It explains why, what clears on its own, and the one step that does not
// happen automatically: the queue never re-adds a pull request by itself.
func RenderMergeQueueEjected(data MergeQueueEjectedData) string {
	var sb strings.Builder
	sb.WriteString("## 🚦 Removed From Merge Queue\n\n")
	sb.WriteString("This pull request's SchemaBot check state turned blocking after it entered the merge queue — ")
	sb.WriteString("most often because another change's apply is in flight on a database this pull request also changes, ")
	sb.WriteString("which invalidates the verdict it queued with. SchemaBot posted a blocking admission check on the ")
	sb.WriteString("merge group, so the queue removed this pull request instead of merging it on a stale verdict.\n\n")
	if len(data.Blocking) > 0 {
		sb.WriteString("Blocking right now:\n\n")
		for _, target := range data.Blocking {
			fmt.Fprintf(&sb, "- `%s` in `%s`\n",
				sanitizeInlineCode(target.Database), sanitizeInlineCode(target.Environment))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("**What happens next**\n\n")
	sb.WriteString("- Check this pull request's SchemaBot check for the reason. A held check clears on its own: when the in-flight apply settles, SchemaBot re-plans this pull request and refreshes the check.\n")
	sb.WriteString("- The merge queue does not re-add pull requests on its own — once this pull request's checks are green again, add it to the merge queue again.\n")
	return sb.String()
}
