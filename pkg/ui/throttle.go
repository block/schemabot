package ui

import "strings"

// ThrottleTip translates an engine throttle reason into a short operator-facing
// tip. Reasons follow the grammar "<signal> <observed> <op> <threshold>", with
// several concurrently-throttling signals joined by "; ", so the tip is keyed
// on each part's leading signal token. Signals that read the same to a user
// (the two active-thread variants) share a tip, and duplicate tips collapse.
// A reason containing any unrecognized signal yields no tip at all — a partial
// explanation would silently bind to signals it does not cover, so an
// unrecognized signal always degrades the whole reason to its raw text rather
// than a wrong explanation.
func ThrottleTip(reason string) string {
	seen := map[string]bool{}
	var tips []string
	for part := range strings.SplitSeq(reason, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		tip := throttleSignalTip(part)
		if tip == "" {
			return ""
		}
		if seen[tip] {
			continue
		}
		seen[tip] = true
		tips = append(tips, tip)
	}
	return strings.Join(tips, "; ")
}

// throttleSignalTip maps one reason part to its tip by the leading signal
// token. The wording states what the pause protects, so a user reads a slowed
// bar as deliberate backpressure rather than a hang. The thread-budget tip
// stays neutral about whose load crossed the threshold: the engine counts its
// own copy threads toward the budget, so the pause is not evidence of
// application overload.
func throttleSignalTip(part string) string {
	signal, _, _ := strings.Cut(part, " ")
	switch signal {
	case "redo-aware", "threads-running":
		return "backing off while the database's active threads exceed its budget"
	case "commit-latency":
		return "backing off while database writes commit slowly"
	}
	return ""
}
