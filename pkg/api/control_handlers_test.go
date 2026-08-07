package api

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestCompleteResolvedStopBeforeStart verifies the stop-request normalization
// that runs before a start of a remote apply: when the data plane reports the
// apply stopped, the stored row is written to stopped only if it still holds
// the state the handler read before the remote check. If a driver advanced the
// row while the check was in flight, the stale write is skipped, the pending
// stop request stays with the current owner, and the handler proceeds from the
// reloaded state instead of overwriting a newer verdict.
func TestCompleteResolvedStopBeforeStart(t *testing.T) {
	newSnapshot := func() *storage.Apply {
		return &storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply_stop_sync",
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
			ExternalID:      "remote-apply-7",
			State:           state.Apply.Running,
		}
	}
	newService := func(t *testing.T, applies *staticApplyStore) (*Service, *memoryControlRequestStore) {
		t.Helper()
		controls := &memoryControlRequestStore{}
		_, _, err := controls.RequestPending(t.Context(), &storage.ApplyControlRequest{
			ApplyID:     7,
			Operation:   storage.ControlOperationStop,
			RequestedBy: "alice",
			Status:      storage.ControlRequestPending,
		})
		require.NoError(t, err)
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		return New(&mockStorageWithApplyStores{
			applies:   applies,
			controls:  controls,
			applyLogs: &noopApplyLogStore{},
		}, testServerConfig(), nil, logger), controls
	}
	remoteStopped := &mockTernClient{
		isRemote:     true,
		progressResp: &ternv1.ProgressResponse{State: ternv1.State_STATE_STOPPED},
	}

	t.Run("stored row still matches the handler's read", func(t *testing.T) {
		snapshot := newSnapshot()
		stored := *snapshot
		applies := &staticApplyStore{apply: &stored}
		svc, controls := newService(t, applies)

		require.NoError(t, svc.completeResolvedStopBeforeStart(t.Context(), remoteStopped, snapshot, "bob"))

		assert.Equal(t, state.Apply.Stopped, stored.State)
		assert.Equal(t, state.Apply.Stopped, snapshot.State)
		assert.NotNil(t, snapshot.CompletedAt)
		pending, err := controls.GetPending(t.Context(), 7, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.Nil(t, pending, "resolved stop request must be completed")
	})

	t.Run("stored row advanced while the remote check was in flight", func(t *testing.T) {
		snapshot := newSnapshot()
		completedAt := time.Now().Add(-time.Minute)
		stored := *snapshot
		stored.State = state.Apply.Completed
		stored.CompletedAt = &completedAt
		applies := &staticApplyStore{apply: &stored}
		svc, controls := newService(t, applies)

		require.NoError(t, svc.completeResolvedStopBeforeStart(t.Context(), remoteStopped, snapshot, "bob"))

		assert.Equal(t, state.Apply.Completed, stored.State, "a stale stop write must not overwrite the newer verdict")
		assert.Equal(t, state.Apply.Completed, snapshot.State, "handler must proceed from the reloaded state")
		pending, err := controls.GetPending(t.Context(), 7, storage.ControlOperationStop)
		require.NoError(t, err)
		require.NotNil(t, pending, "pending stop request must stay with the current owner")
		assert.Equal(t, "alice", pending.RequestedBy)
	})
}
