package commands

import (
	"errors"
	"fmt"
)

// Process exit statuses beyond the CLI's usual 0 for success and 1 for
// failure. A status is a distinct number only when a script would branch on it:
// an operator reading the output can tell a converged database from an
// outstanding one, but a pre-deploy job or a monitor cannot, and telling them
// apart is the whole reason to run the command unattended.
const (
	// ExitStorageSchemaOutstanding says a storage schema diff succeeded and
	// found work: statements are outstanding, refused, or waiting on manual
	// remediation. The read itself worked, so it is not a failure — a caller
	// that treated it as one could not distinguish it from an unreachable
	// database, which is the distinction that matters when deciding whether to
	// proceed with a deploy.
	ExitStorageSchemaOutstanding = 2
)

// ExitCodeError carries the process exit status a command wants, alongside its
// error. main honors the status and prints the error unless it is silent, so a
// command chooses its status without reaching for os.Exit and skipping the
// signal teardown around the run.
type ExitCodeError struct {
	Code int
	Err  error
}

func (e *ExitCodeError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("exit status %d", e.Code)
	}
	return e.Err.Error()
}

func (e *ExitCodeError) Unwrap() error { return e.Err }

// ExitCodeFor returns the process exit status an error asks for, defaulting to
// 1 for an error that asks for nothing in particular. A nil error is status 0.
func ExitCodeFor(err error) int {
	if err == nil {
		return 0
	}
	var coded *ExitCodeError
	if errors.As(err, &coded) && coded.Code != 0 {
		return coded.Code
	}
	return 1
}

// exitStorageSchemaOutstanding asks for that status without printing anything
// further: the diff has already rendered everything the operator needs to see,
// and an "Error:" line under a report that is not an error would misread it.
func exitStorageSchemaOutstanding() error {
	return &ExitCodeError{Code: ExitStorageSchemaOutstanding, Err: ErrSilent}
}
