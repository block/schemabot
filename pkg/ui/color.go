package ui

import "os"

// Colors reports whether ANSI color escapes may be emitted on stdout. It is
// enabled only when stdout is an interactive terminal: in pipes, redirects,
// and CI logs an escape sequence is unreadable bytes, so styled output must
// degrade to the plain text there byte-for-byte. FORCE_COLOR overrides the
// detection either way — "0" or empty disables, any other value enables —
// and the conventional NO_COLOR disables when set non-empty.
var Colors = SupportsColors(os.Stdout)

// SupportsColors answers the same question as Colors for a stream that is not
// stdout. A command that deliberately splits its output — a result a caller
// parses on stdout, progress for a person on stderr — has two streams that can
// be redirected independently, and the one being written to is the only one
// whose terminal-ness says anything about what may be written there.
//
// The environment overrides are the stream's as much as stdout's: NO_COLOR is
// a statement about the command's output, not about one of its file
// descriptors.
func SupportsColors(f *os.File) bool {
	if force, ok := os.LookupEnv("FORCE_COLOR"); ok {
		return force != "" && force != "0"
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	return IsTerminal(f)
}
