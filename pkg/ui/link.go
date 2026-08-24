package ui

import (
	"os"
	"regexp"
	"strings"

	"golang.org/x/text/width"
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
	term := os.Getenv("TERM")
	if term == "dumb" {
		return false
	}
	// Terminal multiplexers swallow the hyperlink escape unless the user has
	// opted into passing it through, leaving the link text with no URL at
	// all. Operators frequently run inside tmux, so print the full URL there
	// and let FORCE_HYPERLINK=1 re-enable links for multiplexers configured
	// to pass them. TERM is checked alongside TMUX because environments like
	// ssh or sudo can strip the TMUX variable while TERM still names the
	// multiplexer.
	if os.Getenv("TMUX") != "" || strings.HasPrefix(term, "screen") || strings.HasPrefix(term, "tmux") {
		return false
	}
	return IsTerminal(os.Stdout)
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
// input. Unsafe values render as the plain URL with control runes stripped,
// so they cannot inject escapes on the plain path either.
func Link(text, url string) string {
	if !Hyperlinks || !osc8SafeURL(url) || containsControl(text) {
		return stripControl(url)
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
	return strings.ContainsFunc(s, isControl)
}

// stripControl removes control runes so a value from a server response is
// safe to print as plain terminal output.
func stripControl(s string) string {
	return strings.Map(func(r rune) rune {
		if isControl(r) {
			return -1
		}
		return r
	}, s)
}

func isControl(r rune) bool {
	return r < 32 || r == 127
}

// terminalEscapePattern matches the zero-width escape sequences this CLI
// emits: OSC sequences (hyperlinks) terminated by BEL or ST, and SGR color
// codes.
var terminalEscapePattern = regexp.MustCompile("\x1b\\][^\x07\x1b]*(?:\x07|\x1b\\\\)|\x1b\\[[0-9;]*m")

// VisibleWidth counts the terminal cells a string occupies after stripping
// zero-width terminal escapes, so padded layouts stay aligned around
// colored, hyperlinked, or emoji-bearing values. East Asian wide and
// fullwidth runes occupy two cells, and an emoji variation selector widens
// its narrow base rune to two, matching how terminals render
// emoji-presentation sequences such as "⚠️".
func VisibleWidth(s string) int {
	cells := 0
	prevCells := 0
	for _, r := range terminalEscapePattern.ReplaceAllString(s, "") {
		switch {
		case r == emojiVariationSelector:
			if prevCells == 1 {
				cells++
				prevCells = 2
			}
		case isWideRune(r):
			cells += 2
			prevCells = 2
		default:
			cells++
			prevCells = 1
		}
	}
	return cells
}

// PadVisible right-pads s with spaces until it occupies width terminal cells,
// so a column of colored, hyperlinked, or emoji-bearing values lines up where
// fmt's byte-counting %-*s would not. A value already at or past width is
// returned unchanged, so a caller that sized the column from a subset of its
// rows still renders every one of them.
func PadVisible(s string, width int) string {
	if pad := width - VisibleWidth(s); pad > 0 {
		return s + strings.Repeat(" ", pad)
	}
	return s
}

// emojiVariationSelector (U+FE0F) is zero width itself but requests emoji
// presentation for the rune it follows, which terminals render two cells
// wide.
const emojiVariationSelector = '\uFE0F'

// isWideRune reports whether a rune occupies two terminal cells on its own,
// such as CJK characters and default-emoji-presentation symbols like "⛔".
func isWideRune(r rune) bool {
	switch width.LookupRune(r).Kind() {
	case width.EastAsianWide, width.EastAsianFullwidth:
		return true
	default:
		return false
	}
}
