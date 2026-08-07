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
