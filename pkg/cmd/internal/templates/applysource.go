package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/caller"
)

// applySource renders an apply's provenance for list surfaces: the full
// clickable PR URL when the caller records a webhook-driven apply, or the
// short caller such as "cli:jdoe" for everything else, so CLI-driven applies
// still show where they came from.
func applySource(c string) string {
	if url := applyPullRequestURL(c, ""); url != "" {
		return url
	}
	return caller.Short(c)
}

// callerAndSourceBoxRows renders an apply's attribution for detail boxes.
// When the apply has PR provenance the two are separated: a short Caller row
// (who drove it) and a clickable Source row (the PR it came from). Without
// one, the raw caller is the whole story, so it stays a single Caller row
// keeping its trailing location, such as the CLI host.
func callerAndSourceBoxRows(c, serverURL string) []BoxRow {
	sourceURL := applyPullRequestURL(c, serverURL)
	var rows []BoxRow
	if c != "" {
		if sourceURL != "" {
			rows = append(rows, BoxRow{"Caller", caller.Short(c)})
		} else {
			rows = append(rows, BoxRow{"Caller", c})
		}
	}
	if sourceURL != "" {
		rows = append(rows, BoxRow{"Source", sourceURL})
	}
	return rows
}

// applyPullRequestURL resolves the PR an apply came from as a clickable URL:
// the server-provided URL when present, otherwise the one embedded in a
// webhook-shaped caller. Empty when the apply has no PR provenance.
func applyPullRequestURL(c, serverURL string) string {
	if serverURL != "" {
		return serverURL
	}
	if repo, pr, ok := caller.PullRequest(c); ok {
		return fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr)
	}
	return ""
}
