package ui

import "os"

// Colors reports whether ANSI color escapes may be emitted on stdout. It is
// enabled only when stdout is an interactive terminal: in pipes, redirects,
// and CI logs an escape sequence is unreadable bytes, so styled output must
// degrade to the plain text there byte-for-byte. FORCE_COLOR overrides the
// detection either way — "0" or empty disables, any other value enables —
// and the conventional NO_COLOR disables when set non-empty.
var Colors = stdoutSupportsColors()

func stdoutSupportsColors() bool {
	if force, ok := os.LookupEnv("FORCE_COLOR"); ok {
		return force != "" && force != "0"
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	return IsTerminal(os.Stdout)
}
