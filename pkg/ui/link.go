package ui

import (
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Hyperlinks reports whether OSC 8 hyperlink escapes may be emitted on
// stdout. It is enabled only when stdout is an interactive terminal: in
// pipes, redirects, and CI logs an escape sequence is unreadable bytes, so
// Link falls back to printing the bare URL there and the address is never
// lost. FORCE_HYPERLINK overrides the detection either way — "0" or empty
// disables (for terminals that silently drop the escape, such as tmux
// without hyperlink features enabled), any other value enables.
var Hyperlinks = stdoutSupportsHyperlinks()

func stdoutSupportsHyperlinks() bool {
	if force, ok := os.LookupEnv("FORCE_HYPERLINK"); ok {
		return force != "" && force != "0"
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	// Terminal multiplexers swallow the hyperlink escape unless the user has
	// opted into passing it through, leaving the link text with no URL at
	// all. Operators frequently run inside tmux, so print the full URL there
	// and let FORCE_HYPERLINK=1 re-enable links for multiplexers configured
	// to pass them.
	if os.Getenv("TMUX") != "" || strings.HasPrefix(os.Getenv("TERM"), "screen") {
		return false
	}
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// Link renders text as an OSC 8 hyperlink to url when stdout is an
// interactive terminal. Terminals without hyperlink support ignore the
// escape and show the bare text, so the text must identify the target on its
// own. Everywhere else the full URL is printed instead, so it can still be
// read, copied, and matched by line-oriented tools.
//
// The URL may come from a server response, so both parts are validated
// before being embedded in the escape: a control byte inside either would
// terminate the sequence early and leak the remainder as live terminal
// input. Unsafe values render as the plain URL.
func Link(text, url string) string {
	if !Hyperlinks || !osc8SafeURL(url) || containsControl(text) {
		return url
	}
	return "\x1b]8;;" + url + "\x1b\\" + text + "\x1b]8;;\x1b\\"
}

// osc8SafeURL reports whether url can be embedded as an OSC 8 URI. The spec
// allows only printable ASCII (bytes 32-126) there; control bytes would end
// the escape early and anything non-ASCII is undefined, so both fall back to
// plain output.
func osc8SafeURL(url string) bool {
	for i := 0; i < len(url); i++ {
		if url[i] < 32 || url[i] > 126 {
			return false
		}
	}
	return true
}

// containsControl reports whether s contains a control rune. Display text may
// be any printable Unicode, but a control rune inside the hyperlink escape
// would corrupt the terminal's parse of it.
func containsControl(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool {
		return r < 32 || r == 127
	})
}

// terminalEscapePattern matches the zero-width escape sequences this CLI
// emits: OSC sequences (hyperlinks) terminated by BEL or ST, and SGR color
// codes.
var terminalEscapePattern = regexp.MustCompile("\x1b\\][^\x07\x1b]*(?:\x07|\x1b\\\\)|\x1b\\[[0-9;]*m")

// VisibleWidth counts the runes a string displays after stripping zero-width
// terminal escapes, so padded layouts stay aligned around colored or
// hyperlinked values. It treats every rune as one column, which holds for
// the identifiers and URLs this CLI renders but undercounts double-width
// runes such as CJK characters.
func VisibleWidth(s string) int {
	return utf8.RuneCountInString(terminalEscapePattern.ReplaceAllString(s, ""))
}
