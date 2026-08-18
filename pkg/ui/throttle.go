package ui

import "strings"

// ThrottleDocURL points at the throttle reference doc, which explains each
// throttle signal and how to remediate it. Rendered next to a throttle tip so
// an operator can jump from the one-line tip to the full prose. The URL is
// deliberately the project's canonical public home, not derived from the
// configured GitHub host: that host serves users' schema repos, which do not
// carry this project's docs, so a host-derived link would always be broken.
const ThrottleDocURL = "https://github.com/block/schemabot/blob/main/docs/throttle.md"

// ThrottleTip translates an engine throttle reason into a short operator-facing
// tip. Reasons follow the grammar "<signal> <observed> <op> <threshold>", with
// several concurrently-throttling signals joined by "; ", so the tip is keyed
// on each part's leading signal token. Signals that read the same to a user
// (the two active-thread variants) share a tip, duplicate tips collapse, and
// an unrecognized signal contributes nothing, so a new engine signal degrades
// to the raw reason rather than a wrong explanation.
func ThrottleTip(reason string) string {
	seen := map[string]bool{}
	var tips []string
	for part := range strings.SplitSeq(reason, ";") {
		tip := throttleSignalTip(strings.TrimSpace(part))
		if tip == "" || seen[tip] {
			continue
		}
		seen[tip] = true
		tips = append(tips, tip)
	}
	return strings.Join(tips, "; ")
}

// throttleSignalTip maps one reason part to its tip by the leading signal
// token. The wording states what the pause protects, so a user reads a slowed
// bar as deliberate backpressure rather than a hang.
func throttleSignalTip(part string) string {
	signal, _, _ := strings.Cut(part, " ")
	switch signal {
	case "replica-lag":
		if strings.Contains(part, "unobservable") {
			return "replication lag cannot be measured, pausing to protect the replica"
		}
		return "waiting for the read replica to catch up"
	case "redo-aware", "threads-running":
		return "yielding to application query load on the database"
	case "commit-latency":
		return "backing off while database writes commit slowly"
	}
	return ""
}
