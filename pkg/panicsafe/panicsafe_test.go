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

func TestCatch_SeparatesPanicFromFnError(t *testing.T) {
	var fnErr error
	var recovered *Error
	require.NotPanics(t, func() {
		recovered, fnErr = Catch(func() error { panic("poisoned metadata") })
	})
	require.NotNil(t, recovered)
	assert.NoError(t, fnErr, "a panicking fn never returned, so it has no error of its own")
	assert.Equal(t, "poisoned metadata", recovered.Value)
	assert.NotEmpty(t, recovered.Stack, "the stack must be captured for triage logging")
}

func TestCatch_PassesThroughFnErrorWithoutRecovering(t *testing.T) {
	want := errors.New("engine failure")
	recovered, fnErr := Catch(func() error { return want })
	require.ErrorIs(t, fnErr, want)
	assert.Nil(t, recovered, "an ordinary fn error must not be reported as a contained panic")
}

// A fn error that wraps an *Error is still fn's own error: Catch reports a
// contained panic only through its recovered result, so error-shape inspection
// can never misroute it.
func TestCatch_FnErrorWrappingErrorTypeIsNotARecoveredPanic(t *testing.T) {
	want := fmt.Errorf("apply already terminal: %w", &Error{Value: "poisoned row"})
	recovered, fnErr := Catch(func() error { return want })
	require.ErrorIs(t, fnErr, want)
	assert.Nil(t, recovered)
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
