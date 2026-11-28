package ui

import "os"

// IsTerminal reports whether f is an interactive terminal rather than a pipe,
// a redirect, a CI log, or the null device. Every escape-emitting decision in
// the CLI starts here — colors and hyperlinks on stdout, the loading spinner on
// stderr — because an escape sequence written anywhere else is unreadable bytes
// in the captured output. So does every decision to ask a question on stdin,
// because a prompt on input nobody types into reads an empty answer.
func IsTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	// The null device is a character device too, and it is the input a
	// container started without a terminal, a systemd service, or a redirect
	// from it gives a command.
	// Nobody types into it or reads from it. If it cannot be found, f is
	// treated as the terminal its mode says it is.
	null, err := os.Stat(os.DevNull)
	if err != nil {
		return true
	}
	return !os.SameFile(info, null)
}
