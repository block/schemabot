package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/ui"
)

// applySource renders an apply's provenance for list surfaces: a link to the
// PR when the caller carries PR provenance — the short "owner/repo#pr" as an
// OSC 8 hyperlink on an interactive terminal, the full URL otherwise — or the
// short caller such as "cli:jdoe" for everything else, so CLI-driven applies
// still show where they came from.
func applySource(c string) string {
	if text, url, ok := sourceRef(c, ""); ok {
		return ui.Link(text, url)
	}
	return caller.Short(c)
}

// callerAndSourceBoxRows renders an apply's attribution for detail boxes.
// When the caller's own trailing location is a repo#pr, the two are
// separated: a short Caller row (who drove it) and a linked Source row (the
// PR it came from). Any other caller keeps its trailing location — a CLI
// caller's host says which machine drove the apply even when a
// server-provided PR URL adds a Source row alongside it. A server-substituted
// bare repo#pr location names no driver at all, so only the Source row
// renders for it.
func callerAndSourceBoxRows(c, serverURL string) []BoxRow {
	text, url, hasSource := sourceRef(c, serverURL)
	var rows []BoxRow
	if c != "" {
		if _, _, ok := caller.PullRequest(c); !ok {
			rows = append(rows, BoxRow{"Caller", c})
		} else if short := caller.Short(c); short != c {
			rows = append(rows, BoxRow{"Caller", short})
		}
	}
	if hasSource {
		rows = append(rows, BoxRow{"Source", ui.Link(text, url)})
	}
	return rows
}

// sourceRef resolves the PR an apply came from as a display-text / URL pair:
// the short "owner/repo#pr" when the caller's own provenance names the PR,
// otherwise the server-provided URL as its own display text — when the two
// disagree the server is authoritative, and a short name derived from the
// caller would mislabel the link. ok is false when the apply has no PR
// provenance at all.
func sourceRef(c, serverURL string) (text, url string, ok bool) {
	if repo, pr, found := caller.PullRequest(c); found {
		derived := fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr)
		if serverURL == "" || serverURL == derived {
			return fmt.Sprintf("%s#%d", repo, pr), derived, true
		}
	}
	if serverURL != "" {
		return serverURL, serverURL, true
	}
	return "", "", false
}
