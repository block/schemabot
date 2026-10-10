package commands

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The exit status is the machine-readable half of a plan's answer: a
// pre-deploy gate has to tell "converged" from "statements outstanding" from
// "the read failed", and two of those three are not failures of the command.
func TestExitCodeFor(t *testing.T) {
	assert.Equal(t, 0, ExitCodeFor(nil))
	assert.Equal(t, 1, ExitCodeFor(errors.New("storage unreachable")))
	assert.Equal(t, ExitStorageSchemaOutstanding, ExitCodeFor(exitStorageSchemaOutstanding()))
	assert.Equal(t, ExitStorageSchemaOutstanding,
		ExitCodeFor(fmt.Errorf("wrapped: %w", exitStorageSchemaOutstanding())),
		"a wrapped request for a status is still honored")
	assert.Equal(t, 1, ExitCodeFor(&ExitCodeError{Err: errors.New("no status asked for")}))
}

// A status request prints nothing further: the report is already on the screen,
// and an "Error:" line under it would read as a failure of the read.
func TestSilentExitIsSilent(t *testing.T) {
	err := exitStorageSchemaOutstanding()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSilent)
}
