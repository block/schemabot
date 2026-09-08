package engine

import (
	"strings"
	"unicode/utf8"
)

// SanitizeThrottleReason bounds an engine-produced throttle reason for the
// operator surfaces that render it (PR comment tables, CLI rows): newlines and
// table separators are neutralized and the text is clamped, so a reason can
// never break markdown layout no matter what a throttler reports. Every
// boundary that ingests a throttle reason — an engine stamping its own
// throttler's reason, or a client mirroring one reported by a remote data
// plane — must pass it through here before persisting it.
func SanitizeThrottleReason(reason string) string {
	reason = strings.Join(strings.Fields(reason), " ")
	reason = strings.ReplaceAll(reason, "|", "/")
	const maxLen = 200
	if len(reason) > maxLen {
		cut := maxLen - len("…")
		for cut > 0 && !utf8.RuneStart(reason[cut]) {
			cut--
		}
		reason = reason[:cut] + "…"
	}
	return reason
}
