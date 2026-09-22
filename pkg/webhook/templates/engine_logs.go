package templates

import (
	"fmt"
	"strings"
)

// EngineLogSourceData is one data plane's engine lines, rendered into the
// engine-logs fold of a failed apply's summary comment. An apply that fanned
// out keeps one source per data plane read: their clocks and log ids are
// independent, so interleaving them by timestamp would assert an ordering
// across machines that nothing establishes.
type EngineLogSourceData struct {
	// Deployment names the data plane the lines came from. It labels the
	// group only when more than one source contributed, so the common
	// single-data-plane fold reads as a plain log.
	Deployment string
	// Entries are the engine's lines, oldest first.
	Entries []LogEntryData
	// HasOlder reports that the source has engine lines older than Entries,
	// so the fold can say it is showing a tail rather than the whole account.
	HasOlder bool
}

// RenderEngineFailureLogs renders the collapsed engine-logs section of a
// failed apply's summary comment: the engine's own lines for the change,
// which for a remotely driven apply live in the data plane's storage and are
// otherwise reachable only from the CLI. It is a sibling of the
// recent-logs fold rather than a replacement — the two carry different
// streams, so a reader opening the comment sees SchemaBot's account of the
// apply and the engine's account of the failure side by side.
//
// The section spends at most available characters, the room the rest of the
// comment leaves under GitHub's size limit, and returns "" when no source
// carried an engine line or there is no meaningful room. An apply with no
// data-plane deployment has no sources at all, so its summary renders exactly
// as it did before this fold existed rather than gaining an empty one.
func RenderEngineFailureLogs(sources []EngineLogSourceData, available int) string {
	sources = withEntries(sources)
	if len(sources) == 0 {
		return ""
	}
	if available < MinFailureLogsSectionChars {
		return ""
	}
	labelled := len(sources) > 1
	// Every source gets an equal share of the budget, so the fold shows each
	// data plane's tail rather than letting the first one read spend the room
	// the others needed. Trimming the joined block instead would drop whole
	// sources from the front, and with them the group labels that say which
	// data plane the surviving lines came from.
	budget := available - sectionChromeChars
	if labelled {
		// The label and the newline joining it to the lines below come out of
		// the budget before it is shared, so a labelled fold cannot overrun
		// the room a bare one would have fitted in.
		budget -= len(sources) * (len(sourceLabel("")) + maxDeploymentLabelChars + 1)
	}
	share := budget / len(sources)

	var blocks []string
	total := 0
	omitted := 0
	hasOlder := false
	for _, source := range sources {
		lines := make([]string, len(source.Entries))
		for i, entry := range source.Entries {
			lines[i] = formatLogEntryLine(entry)
		}
		lines, dropped := trimLogLinesToBudget(lines, share)
		omitted += dropped
		hasOlder = hasOlder || source.HasOlder
		total += len(lines)
		if labelled {
			lines = append([]string{sourceLabel(source.Deployment)}, lines...)
		}
		blocks = append(blocks, strings.Join(lines, "\n"))
	}

	label := "Show engine logs"
	if hasOlder || omitted > 0 {
		label = "Show recent engine logs"
	}
	noun := "entries"
	if total == 1 {
		noun = "entry"
	}
	section := fmt.Sprintf("\n<details>\n<summary>%s (%d %s)</summary>\n\n", label, total, noun)
	if omitted > 0 {
		note := fmt.Sprintf("_%d earlier entries omitted to fit the comment size limit", omitted)
		if hasOlder {
			note += " (older entries also exist)"
		}
		section += note + "._\n\n"
	}
	section += "```text\n" + strings.Join(blocks, "\n") + "\n```\n\n</details>\n"
	return section
}

// maxDeploymentLabelChars bounds what a group label costs the budget. A
// deployment name longer than this is clamped rather than allowed to eat the
// lines the label exists to introduce.
const maxDeploymentLabelChars = 64

// sourceLabel introduces one data plane's lines inside the fenced block. The
// deployment name is sanitized like any other text in the fence: it is server
// configuration rather than a log line, but it shares the fence with text the
// engine wrote and nothing about a name should be able to close it.
func sourceLabel(deployment string) string {
	return "== deployment: " + truncateToBytes(sanitizeLogText(deployment), maxDeploymentLabelChars) + " =="
}

// withEntries drops the sources that carried no engine line, so a data plane
// that answered with nothing does not turn into an empty labelled group.
func withEntries(sources []EngineLogSourceData) []EngineLogSourceData {
	kept := make([]EngineLogSourceData, 0, len(sources))
	for _, source := range sources {
		if len(source.Entries) > 0 {
			kept = append(kept, source)
		}
	}
	return kept
}
