package ui

import "os"

// IsTerminal reports whether f is a character device, i.e. an interactive
// terminal rather than a pipe, a redirect, or a CI log. Every escape-emitting
// decision in the CLI starts here — colors and hyperlinks on stdout, the
// loading spinner on stderr — because an escape sequence written anywhere
// else is unreadable bytes in the captured output.
func IsTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
