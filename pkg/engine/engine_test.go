package engine

import (
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
