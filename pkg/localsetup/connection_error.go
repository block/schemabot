package localsetup

// Keep the friendly surface separate from driver errors, which may contain
// connection material. Diagnostic callers can unwrap the original cause.
type setupConnectionError struct {
	message string
	cause   error
}

func (e *setupConnectionError) Error() string { return e.message }
func (e *setupConnectionError) Unwrap() error { return e.cause }
