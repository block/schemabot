package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestState_IsTerminal(t *testing.T) {
	tests := []struct {
		state    State
		terminal bool
	}{
		{StatePending, false},
		{StateRunning, false},
		{StateWaitingForCutover, false},
		{StateCuttingOver, false},
		{StateRevertWindow, false},
		{StateCompleted, true},
		{StateFailed, true},
		{StateStopped, true},
		{StateCancelled, true},
		{StateReverted, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			got := tt.state.IsTerminal()
			assert.Equal(t, tt.terminal, got)
		})
	}
}

func TestEncodeDecodeResumeState(t *testing.T) {
	rs := &ResumeState{
		MigrationContext: "schemabot:task-abc",
		Metadata:         `{"branch":"tern-mydb-abc12345","deploy_request_id":42}`,
	}

	encoded, err := EncodeResumeState(rs)
	require.NoError(t, err)

	decoded := DecodeResumeState(encoded)
	require.NotNil(t, decoded)
	assert.Equal(t, rs.MigrationContext, decoded.MigrationContext)
	assert.Equal(t, rs.Metadata, decoded.Metadata)
}

func TestDecodeResumeState_Empty(t *testing.T) {
	assert.Nil(t, DecodeResumeState(""))
}

func TestDecodeResumeState_SpiritUUID(t *testing.T) {
	// Spirit stores a plain UUID string as EngineMigrationID, not JSON.
	// DecodeResumeState should return nil for non-JSON strings.
	assert.Nil(t, DecodeResumeState("abc12345-6789-0def-1234-567890abcdef"))
}

func TestDecodeResumeState_EmptyFields(t *testing.T) {
	// JSON with zero-value fields should return nil (not a useful ResumeState).
	assert.Nil(t, DecodeResumeState(`{"MigrationContext":"","Metadata":""}`))
}

func TestPlanResult_HasErrors(t *testing.T) {
	tests := []struct {
		name     string
		warnings []LintViolation
		want     bool
	}{
		{"nil warnings", nil, false},
		{"empty warnings", []LintViolation{}, false},
		{"warning only", []LintViolation{{Severity: "warning"}}, false},
		{"info only", []LintViolation{{Severity: "info"}}, false},
		{"error only", []LintViolation{{Severity: "error"}}, true},
		{"mixed with error", []LintViolation{{Severity: "warning"}, {Severity: "error"}}, true},
		{"mixed without error", []LintViolation{{Severity: "warning"}, {Severity: "info"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &PlanResult{LintViolations: tt.warnings}
			assert.Equal(t, tt.want, r.HasErrors())
		})
	}
}

func TestPlanResult_Errors(t *testing.T) {
	r := &PlanResult{LintViolations: []LintViolation{
		{Message: "warn1", Severity: "warning"},
		{Message: "err1", Severity: "error"},
		{Message: "info1", Severity: "info"},
		{Message: "err2", Severity: "error"},
	}}
	errors := r.Errors()
	assert.Len(t, errors, 2)
	assert.Equal(t, "err1", errors[0].Message)
	assert.Equal(t, "err2", errors[1].Message)
}

func TestPlanResult_Warnings(t *testing.T) {
	r := &PlanResult{LintViolations: []LintViolation{
		{Message: "warn1", Severity: "warning"},
		{Message: "err1", Severity: "error"},
		{Message: "info1", Severity: "info"},
	}}
	warnings := r.Warnings()
	assert.Len(t, warnings, 2)
	assert.Equal(t, "warn1", warnings[0].Message)
	assert.Equal(t, "info1", warnings[1].Message)
}

func TestEncodeResumeState_Nil(t *testing.T) {
	encoded, err := EncodeResumeState(nil)
	require.NoError(t, err)
	assert.Equal(t, "", encoded)
}

func TestIsRetryable(t *testing.T) {
	t.Run("plain error is retryable by default", func(t *testing.T) {
		err := fmt.Errorf("connection refused")
		assert.True(t, IsRetryable(err))
	})

	t.Run("wrapped error is retryable by default", func(t *testing.T) {
		err := fmt.Errorf("apply failed: %w", fmt.Errorf("network timeout"))
		assert.True(t, IsRetryable(err))
	})

	t.Run("PermanentError is not retryable", func(t *testing.T) {
		err := NewPermanentError("DDL syntax error")
		assert.False(t, IsRetryable(err))
	})

	t.Run("wrapped PermanentError is not retryable", func(t *testing.T) {
		err := fmt.Errorf("apply failed: %w", NewPermanentError("auth failure"))
		assert.False(t, IsRetryable(err))
	})

	t.Run("nil is not retryable", func(t *testing.T) {
		assert.False(t, IsRetryable(nil))
	})

}

func TestIsNotReady(t *testing.T) {
	t.Run("plain error is not a not-ready condition", func(t *testing.T) {
		assert.False(t, IsNotReady(fmt.Errorf("connection refused")))
	})

	t.Run("NotReadyError is a not-ready condition", func(t *testing.T) {
		err := NewNotReadyError("deploy request has not staged its changes yet")
		assert.True(t, IsNotReady(err))
	})

	t.Run("wrapped NotReadyError is a not-ready condition", func(t *testing.T) {
		err := fmt.Errorf("cutover deploy request #7: %w", NewNotReadyError("not staged"))
		assert.True(t, IsNotReady(err))
	})

	t.Run("NotReadyError stays retryable", func(t *testing.T) {
		assert.True(t, IsRetryable(NewNotReadyError("not staged")))
	})

	t.Run("nil is not a not-ready condition", func(t *testing.T) {
		assert.False(t, IsNotReady(nil))
	})
}

func TestIsUnsupportedOperation(t *testing.T) {
	t.Run("plain error is not an unsupported-operation decline", func(t *testing.T) {
		assert.False(t, IsUnsupportedOperation(fmt.Errorf("connection refused")))
	})

	t.Run("UnsupportedOperationError is an unsupported-operation decline", func(t *testing.T) {
		assert.True(t, IsUnsupportedOperation(NewUnsupportedOperationError("stop is not supported")))
	})

	t.Run("wrapped UnsupportedOperationError is an unsupported-operation decline", func(t *testing.T) {
		err := fmt.Errorf("stop local engine for task t-1: %w", NewUnsupportedOperationError("stop is not supported"))
		assert.True(t, IsUnsupportedOperation(err))
	})

	t.Run("UnsupportedOperationError stays retryable", func(t *testing.T) {
		// The schema change itself is healthy — only the control operation is
		// undeliverable — so the generic failure path must not record it as a
		// permanent failure. Paths that can receive the decline resolve the
		// control request terminally via IsUnsupportedOperation instead.
		assert.True(t, IsRetryable(NewUnsupportedOperationError("stop is not supported")))
	})

	t.Run("nil is not an unsupported-operation decline", func(t *testing.T) {
		assert.False(t, IsUnsupportedOperation(nil))
	})
}

func TestIsTransientTransportError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "connection refused", err: fmt.Errorf("dial tcp: connection refused"), want: true},
		{name: "connection reset", err: fmt.Errorf("read tcp: connection reset by peer"), want: true},
		{name: "timeout", err: fmt.Errorf("proxy query: i/o timeout"), want: true},
		{name: "deadline", err: fmt.Errorf("context deadline exceeded"), want: true},
		{name: "rate limit", err: fmt.Errorf("Too many requests"), want: true},
		{name: "syntax error", err: fmt.Errorf("DDL syntax error"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsTransientTransportError(tt.err))
		})
	}
}

// remoteWorkEngine models an engine whose schema change runs outside this
// process: it declares no shutdown-halt capability because nothing it started
// is affected by this process exiting.
type remoteWorkEngine struct{ Engine }

// haltableEngine models an engine that runs its schema change in this process.
type haltableEngine struct {
	Engine
	haltErr error
	halts   int
}

func (e *haltableEngine) HaltForShutdown(context.Context) error {
	e.halts++
	return e.haltErr
}

// Halting an engine on shutdown exists to release resources this process holds
// on a target. An engine whose work runs elsewhere holds none, so shutdown must
// report there is nothing to halt rather than treating the missing capability as
// an error and blocking the process from exiting.
func TestHaltEngineForShutdownSkipsEnginesWithNoInProcessWork(t *testing.T) {
	supported, err := HaltEngineForShutdown(t.Context(), &remoteWorkEngine{})

	require.NoError(t, err)
	assert.False(t, supported, "an engine whose work runs elsewhere declares nothing to halt")
}

// An engine that runs its work in this process is halted, and a halt that does
// not complete is reported to the caller rather than swallowed: the target may
// still be held while the process stops renewing the apply's lease.
func TestHaltEngineForShutdownReportsTheHaltResult(t *testing.T) {
	t.Run("halted", func(t *testing.T) {
		eng := &haltableEngine{}

		supported, err := HaltEngineForShutdown(t.Context(), eng)

		require.NoError(t, err)
		assert.True(t, supported)
		assert.Equal(t, 1, eng.halts)
	})

	t.Run("halt failed", func(t *testing.T) {
		eng := &haltableEngine{haltErr: fmt.Errorf("runner still copying")}

		supported, err := HaltEngineForShutdown(t.Context(), eng)

		require.Error(t, err)
		assert.True(t, supported)
		assert.Contains(t, err.Error(), "runner still copying")
	})
}
