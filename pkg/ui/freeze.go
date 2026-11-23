package ui

import "time"

// FreezeUntil says how long a freeze holds: "until" its end, in RFC3339 UTC,
// the form `schemabot freeze create --end` takes, or "until lifted" when it
// has none. The API refusal, the CLI and the PR comment all name a freeze's
// end with this phrase, so the three agree.
func FreezeUntil(endsAt *time.Time) string {
	if endsAt == nil {
		return "until lifted"
	}
	return "until " + endsAt.UTC().Format(time.RFC3339)
}
