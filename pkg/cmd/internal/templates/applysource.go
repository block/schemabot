package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/caller"
)

// applySource renders an apply's provenance for list surfaces: the full
// clickable PR URL when the caller carries PR provenance, or the short
// caller such as "cli:jdoe" for everything else, so CLI-driven applies still
// show where they came from.
func applySource(c string) string {
	if url := applyPullRequestURL(c, ""); url != "" {
		return url
	}
	return caller.Short(c)
}

// callerAndSourceBoxRows renders an apply's attribution for detail boxes.
// When the caller's own trailing location is a repo#pr, the two are
// separated: a short Caller row (who drove it) and a clickable Source row
// (the PR it came from). Any other caller keeps its trailing location — a
// CLI caller's host says which machine drove the apply even when a
// server-provided PR URL adds a Source row alongside it. A server-substituted
// bare repo#pr location names no driver at all, so only the Source row
// renders for it.
func callerAndSourceBoxRows(c, serverURL string) []BoxRow {
	sourceURL := applyPullRequestURL(c, serverURL)
	var rows []BoxRow
	if c != "" {
		if _, _, ok := caller.PullRequest(c); !ok {
			rows = append(rows, BoxRow{"Caller", c})
		} else if short := caller.Short(c); short != c {
			rows = append(rows, BoxRow{"Caller", short})
		}
	}
	if sourceURL != "" {
		rows = append(rows, BoxRow{"Source", sourceURL})
	}
	return rows
}

// applyPullRequestURL resolves the PR an apply came from as a clickable URL:
// the server-provided URL when present, otherwise the one embedded in the
// caller attribution. Empty when the apply has no PR provenance.
func applyPullRequestURL(c, serverURL string) string {
	if serverURL != "" {
		return serverURL
	}
	if repo, pr, ok := caller.PullRequest(c); ok {
		return fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr)
	}
	return ""
}
