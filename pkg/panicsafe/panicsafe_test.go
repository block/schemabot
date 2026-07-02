package panicsafe

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCall_ReturnsNilOnSuccess(t *testing.T) {
	require.NoError(t, Call(func() error { return nil }))
}

func TestCall_PassesThroughError(t *testing.T) {
	want := errors.New("engine failure")
	err := Call(func() error { return want })
	require.ErrorIs(t, err, want)

	var contained *Error
	assert.False(t, errors.As(err, &contained), "an ordinary error must not be reported as a contained panic")
}

func TestCall_ConvertsPanicToError(t *testing.T) {
	var err error
	require.NotPanics(t, func() {
		err = Call(func() error { panic("poisoned metadata") })
	})
	require.Error(t, err)

	var contained *Error
	require.ErrorAs(t, err, &contained)
	assert.Equal(t, "poisoned metadata", contained.Value)
	assert.Contains(t, err.Error(), "poisoned metadata")
	assert.NotEmpty(t, contained.Stack, "the stack must be captured for triage logging")
}

func TestCall_ConvertsNonStringPanicToError(t *testing.T) {
	var err error
	require.NotPanics(t, func() {
		err = Call(func() error { panic(fmt.Errorf("nil dereference")) })
	})

	var contained *Error
	require.ErrorAs(t, err, &contained)
	assert.Contains(t, err.Error(), "nil dereference")
}
