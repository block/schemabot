package storage

import "github.com/block/schemabot/pkg/caller"

// MaxCallerChars is the widest caller attribution the storage schema holds
// (the applies and apply_control_requests caller columns). Writers composing
// a caller from client-supplied parts must keep the result within this bound
// or fall back to a shorter attribution — an oversized caller fails the row
// insert under strict SQL mode.
const MaxCallerChars = 255

// ApplyLogCaller returns the caller identity to embed in an apply-log
// message. Apply-log messages render into PR-facing markdown (a failed
// apply's summary comment folds in the recent log tail), so a CLI caller's
// hostname — internal machine detail — is stripped down to its channel and
// user ("cli:jdoe"). The full caller, hostname included, stays on the apply
// record and the durable control-request row, where the CLI renders it for
// triage. An empty caller or user reads as "unknown" so the message never
// shows a blank attribution and a malformed caller never leaks its hostname.
func ApplyLogCaller(c string) string {
	if c == "" {
		return "unknown"
	}
	user, _, ok := caller.SplitCLI(c)
	if !ok {
		return c
	}
	if user == "" {
		return caller.CLIPrefix + "unknown"
	}
	return caller.CLIPrefix + user
}
