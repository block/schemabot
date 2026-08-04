package storage

import "strings"

// ApplyLogCaller returns the caller identity to embed in an apply-log
// message. Apply-log messages render into PR-facing markdown (a failed
// apply's summary comment folds in the recent log tail), so a CLI caller's
// hostname — internal machine detail — is stripped down to its channel and
// user ("cli:jdoe"). The full caller, hostname included, stays on the apply
// record and the durable control-request row, where the CLI renders it for
// triage. An empty caller reads as "unknown" so the message never shows a
// blank attribution.
func ApplyLogCaller(caller string) string {
	if caller == "" {
		return "unknown"
	}
	rest, ok := strings.CutPrefix(caller, "cli:")
	if !ok {
		return caller
	}
	if user, _, found := strings.Cut(rest, "@"); found && user != "" {
		return "cli:" + user
	}
	return caller
}
