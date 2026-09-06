package api

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestIsInternalControlError verifies that control operation errors are
// classified so callers log internal failures at error severity and
// operator-actionable rejections at warning severity.
func TestIsInternalControlError(t *testing.T) {
	t.Run("conflict rejection is not internal", func(t *testing.T) {
		assert.False(t, IsInternalControlError(controlConflictf("schema change is already terminal (current state: %s)", "completed")))
	})

	t.Run("client-facing status is not internal", func(t *testing.T) {
		assert.False(t, IsInternalControlError(controlHTTPErrorf(http.StatusNotFound, "apply not found: %s", "apply-123")))
		assert.False(t, IsInternalControlError(controlHTTPErrorf(http.StatusBadRequest, "environment is required")))
	})

	t.Run("wrapped rejection keeps its classification", func(t *testing.T) {
		wrapped := fmt.Errorf("execute start: %w", controlConflictf("schema change is still running; stop it before starting it again"))
		assert.False(t, IsInternalControlError(wrapped))
	})

	t.Run("unclassified error is internal", func(t *testing.T) {
		assert.True(t, IsInternalControlError(errors.New("control request store is not available")))
		assert.True(t, IsInternalControlError(fmt.Errorf("record start control request for apply %s: %w", "apply-123", errors.New("storage unavailable"))))
	})

	t.Run("explicit server status is internal", func(t *testing.T) {
		assert.True(t, IsInternalControlError(controlHTTPErrorf(http.StatusBadGateway, "tern unavailable")))
	})
}

// TestIsTerminalControlError verifies the retry disposition durable command
// processing hinges on: typed terminal errors and operator-actionable
// rejections must never be re-driven, while untyped internal failures and
// remote unavailability stay retryable.
func TestIsTerminalControlError(t *testing.T) {
	t.Run("typed terminal error is terminal", func(t *testing.T) {
		assert.True(t, IsTerminalControlError(terminalControlf("plan not found for apply %s", "apply-123")))
	})

	t.Run("wrapped terminal error stays terminal", func(t *testing.T) {
		wrapped := fmt.Errorf("rollback command plan: %w", terminalControlf("rollback source plan is invalid"))
		assert.True(t, IsTerminalControlError(wrapped))
	})

	t.Run("guardrail conflict is terminal", func(t *testing.T) {
		assert.True(t, IsTerminalControlError(controlConflictf("apply is in state %q; only completed applies can be rolled back", "running")))
	})

	t.Run("client-facing status is terminal", func(t *testing.T) {
		assert.True(t, IsTerminalControlError(controlHTTPErrorf(http.StatusBadRequest, "apply is required")))
		assert.True(t, IsTerminalControlError(controlHTTPErrorf(http.StatusNotFound, "apply not found: %s", "apply-123")))
	})

	t.Run("untyped error is not terminal", func(t *testing.T) {
		assert.False(t, IsTerminalControlError(errors.New("storage unavailable")))
		assert.False(t, IsTerminalControlError(fmt.Errorf("get rollback source plan: %w", errors.New("connection reset"))))
	})

	t.Run("remote unavailability is not terminal", func(t *testing.T) {
		assert.False(t, IsTerminalControlError(&RemoteDeploymentUnavailableError{
			Deployment: "tenant-a",
			Target:     "orders-staging",
			Err:        errors.New("connection refused"),
		}))
	})

	t.Run("explicit server status without terminal marker is not terminal", func(t *testing.T) {
		assert.False(t, IsTerminalControlError(controlHTTPErrorf(http.StatusBadGateway, "tern unavailable")))
	})
}

// A failed control operation is triaged from logs alone, so the failure line
// must carry the apply's full triage attributes — including external_id, the
// join key to the data plane's logs — alongside the error itself.
func TestWriteControlError_LogsCarryFullApplyAttrs(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc := New(nil, testServerConfig(), nil, logger)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		DatabaseType:    "mysql",
		Deployment:      "east",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     123,
		State:           state.Apply.Running,
		ExternalID:      "remote-apply-7",
	}

	svc.writeControlError(httptest.NewRecorder(), "stop", apply, errors.New("storage unavailable"))

	lines := decodeLogLines(t, logBuf.Bytes())
	line := requireLogLine(t, lines, "stop failed")
	assert.Equal(t, "apply-42", line["apply_id"])
	assert.Equal(t, "appdb", line["database"])
	assert.Equal(t, "mysql", line["database_type"])
	assert.Equal(t, "staging", line["environment"])
	assert.Equal(t, "org/repo", line["repo"])
	assert.Equal(t, float64(123), line["pr"])
	assert.Equal(t, "east", line["deployment"])
	assert.Equal(t, state.Apply.Running, line["state"])
	assert.Equal(t, "remote-apply-7", line["external_id"])
	assert.Contains(t, line, "error")
}

// The engine's own settle counts and the command's task-selection counts are
// different numbers with the same names, and an operator reads them a line apart
// during a cancel. The engine's are reported under engine_-prefixed keys so a
// zero here reads as "the engine deferred to the apply owner" rather than "the
// cancel selected nothing".
func TestTryImmediateCancel_LogsEnginePrefixedCounts(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-7",
		Database:        "appdb",
		DatabaseType:    "mysql",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	svc := New(&mockStorageWithApplyStores{applies: &staticApplyStore{apply: apply}}, testServerConfig(), nil, logger)
	client := &mockTernClient{cancelResp: &ternv1.CancelResponse{Accepted: true, CancelledCount: 0, SkippedCount: 2}}

	svc.tryImmediateCancel(t.Context(), client, apply, "github:alice")

	lines := decodeLogLines(t, logBuf.Bytes())
	line := requireLogLine(t, lines, "immediate cancel accepted; durable apply owner will reconcile final cancel state")
	assert.Equal(t, float64(0), line["engine_cancelled_count"])
	assert.Equal(t, float64(2), line["engine_skipped_count"])
	assert.NotContains(t, line, "cancelled_count", "the engine's counts must not share the command's key names")
	assert.NotContains(t, line, "skipped_count")
}

// The stop path carries the same collision as cancel: the engine's settle counts
// and the command's task-selection counts share names one hop apart. A stop the
// engine declines is where the two are most likely to be read together, so its
// counts are reported under engine_-prefixed keys.
func TestTryImmediateStopAfterQueue_LogsEnginePrefixedCounts(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	apply := &storage.Apply{
		ID:              8,
		ApplyIdentifier: "apply-8",
		Database:        "appdb",
		DatabaseType:    "mysql",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	svc := New(&mockStorageWithApplyStores{applies: &staticApplyStore{apply: apply}}, testServerConfig(), nil, logger)
	client := &mockTernClient{stopResp: &ternv1.StopResponse{Accepted: false, StoppedCount: 0, SkippedCount: 3}}

	svc.tryImmediateStopAfterQueue(t.Context(), client, apply, apply.ApplyIdentifier, apply.Environment, "github:alice")

	lines := decodeLogLines(t, logBuf.Bytes())
	line := requireLogLine(t, lines, "immediate stop was not accepted; durable stop request remains pending for apply owner retry")
	assert.Equal(t, float64(0), line["engine_stopped_count"])
	assert.Equal(t, float64(3), line["engine_skipped_count"])
	assert.NotContains(t, line, "stopped_count", "the engine's counts must not share the command's key names")
	assert.NotContains(t, line, "skipped_count")
}
