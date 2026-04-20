package commands

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

func TestFetchProgress_ServerReturns500_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "internal server error")
	}))
	t.Cleanup(srv.Close)

	m := NewWatchModel(srv.URL, "testdb", "staging", false)
	cmd := m.fetchProgress()
	msg := cmd()

	pmsg, ok := msg.(progressMsg)
	require.True(t, ok, "expected progressMsg, got %T", msg)
	assert.Empty(t, pmsg.state, "fetchProgress should not set state on error")
	assert.True(t, pmsg.failed, "should be a fetch error")
	assert.False(t, pmsg.retryable, "5xx without error code should not be retryable")
	assert.Contains(t, pmsg.errorMsg, "500")
}

func TestFetchProgress_ServerReturnsNoActiveChange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"state":"no_active_change","tables":[]}`)
	}))
	t.Cleanup(srv.Close)

	m := NewWatchModel(srv.URL, "testdb", "staging", false)
	cmd := m.fetchProgress()
	msg := cmd()

	pmsg, ok := msg.(progressMsg)
	require.True(t, ok, "expected progressMsg, got %T", msg)
	assert.Equal(t, state.NoActiveChange, pmsg.state)
	assert.False(t, pmsg.retryable, "successful response should not be marked retryable")
	assert.Empty(t, pmsg.errorMsg)
}

func TestWatchModel_FirstPollRetryableError_ShowsLoadingWithError(t *testing.T) {
	// First poll fails with a retryable error (engine unavailable).
	// TUI should stay in loading state with the error visible.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, `{"error":"progress failed: rpc timeout","error_code":"engine_unavailable"}`)
	}))
	t.Cleanup(srv.Close)

	m := NewWatchModel(srv.URL, "testdb", "staging", false)
	cmd := m.fetchProgress()
	msg := cmd()

	updated, retCmd := m.Update(msg)
	model := updated.(WatchModel)

	assert.False(t, model.initialized,
		"should not be initialized — we haven't gotten a real response yet")
	assert.Contains(t, model.errorMsg, "rpc timeout")

	view := model.View()
	assert.NotContains(t, view, "No active schema change")
	assert.Contains(t, view, "Loading",
		"should still show loading spinner")
	assert.Contains(t, view, "rpc timeout",
		"should show the error")

	assert.Nil(t, retCmd, "retryable error should return nil cmd (tick loop handles retry)")
}

func TestWatchModel_FirstPollPermanentError_QuitsWithError(t *testing.T) {
	// First poll fails with a permanent error (not found).
	// TUI should show the error and quit.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"error":"apply not found: abc123","error_code":"not_found"}`)
	}))
	t.Cleanup(srv.Close)

	m := NewWatchModel(srv.URL, "testdb", "staging", false)
	cmd := m.fetchProgress()
	msg := cmd()

	updated, retCmd := m.Update(msg)
	model := updated.(WatchModel)

	assert.True(t, model.initialized, "permanent error should mark initialized for view rendering")
	assert.Contains(t, model.errorMsg, "apply not found")
	assert.NotNil(t, retCmd, "permanent error should return tea.Quit")
}

func TestWatchModel_MidFlightError_PreservesLastState(t *testing.T) {
	// Apply is running, then server crashes (returns 500).
	// TUI should preserve the running state and show the error, not quit.
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)

	// First: a successful poll with running state.
	successMsg := progressMsg{
		state: state.Apply.Running,
		tables: []tableProgress{
			{Name: "users", Status: state.Apply.Running, RowsCopied: 500, RowsTotal: 1000, Percent: 50},
		},
	}
	updated, _ := m.Update(successMsg)
	m = updated.(WatchModel)
	assert.True(t, m.initialized)
	assert.Equal(t, state.Apply.Running, m.state)

	// Second: server crashes — API call fails (retryable flag distinguishes this
	// from a server response that happens to have an empty state).
	errorMsg := progressMsg{
		errorMsg:  "500: connection refused",
		failed:    true,
		retryable: true,
	}
	updated, cmd := m.Update(errorMsg)
	m = updated.(WatchModel)

	// State should be preserved from last successful poll.
	assert.Equal(t, state.Apply.Running, m.state,
		"mid-flight error should preserve last known state")
	assert.Contains(t, m.errorMsg, "500")
	assert.Len(t, m.tables, 1, "tables should be preserved from last successful poll")

	// TUI should not quit — keep polling.
	assert.Nil(t, cmd, "should return nil cmd to continue polling")
}

func TestWatchModel_NoActiveChange_WithoutError(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)

	msg := progressMsg{
		state: state.NoActiveChange,
	}

	updated, _ := m.Update(msg)
	model := updated.(WatchModel)

	view := model.View()
	assert.Contains(t, view, "No active schema change")
}

func TestWatchModel_ConnectionError_CanEscape(t *testing.T) {
	// User should be able to ESC out of the loading+error state.
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)

	// Simulate transient fetch error (API call failed).
	updated, _ := m.Update(progressMsg{errorMsg: "connection refused", failed: true, retryable: true})
	m = updated.(WatchModel)
	assert.False(t, m.initialized)

	// ESC should quit.
	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = updated.(WatchModel)
	assert.True(t, m.detached)
	assert.NotNil(t, cmd, "ESC should return tea.Quit")
}

func TestWatchModel_ServerErrorWithState_TreatedAsRealResponse(t *testing.T) {
	// Server returns a real response with state=failed and an error message.
	// This is NOT a fetch error — it's a valid API response indicating the
	// apply failed. The TUI should update state normally (and quit on terminal state).
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)

	msg := progressMsg{
		state:    state.Apply.Failed,
		errorMsg: "engine error: checksum mismatch",
	}
	updated, cmd := m.Update(msg)
	model := updated.(WatchModel)

	assert.True(t, model.initialized, "should be initialized from real response")
	assert.Equal(t, state.Apply.Failed, model.state)
	assert.Contains(t, model.errorMsg, "checksum mismatch")
	assert.NotNil(t, cmd, "terminal state should return tea.Quit")
}

func TestFetchProgress_ServerReturns404_PermanentError(t *testing.T) {
	// 404 without error_code — falls back to status code classification.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"error":"apply not found"}`)
	}))
	t.Cleanup(srv.Close)

	m := NewWatchModel(srv.URL, "testdb", "staging", false)
	cmd := m.fetchProgress()
	msg := cmd()

	pmsg, ok := msg.(progressMsg)
	require.True(t, ok, "expected progressMsg, got %T", msg)
	assert.False(t, pmsg.retryable, "4xx should not be retryable")
	assert.Contains(t, pmsg.errorMsg, "apply not found")
}

func TestFetchProgress_ErrorCodeClassification(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		retryable bool
	}{
		{
			name:      "engine_unavailable is retryable",
			status:    http.StatusInternalServerError,
			body:      `{"error":"progress failed: rpc timeout","error_code":"engine_unavailable"}`,
			retryable: true,
		},
		{
			name:      "storage_error is retryable",
			status:    http.StatusInternalServerError,
			body:      `{"error":"failed to get apply: connection lost","error_code":"storage_error"}`,
			retryable: true,
		},
		{
			name:      "not_found is permanent",
			status:    http.StatusNotFound,
			body:      `{"error":"apply not found: abc123","error_code":"not_found"}`,
			retryable: false,
		},
		{
			name:      "deployment_not_found is permanent",
			status:    http.StatusNotFound,
			body:      `{"error":"no deployment configured","error_code":"deployment_not_found"}`,
			retryable: false,
		},
		{
			name:      "invalid_request is permanent",
			status:    http.StatusBadRequest,
			body:      `{"error":"database is required","error_code":"invalid_request"}`,
			retryable: false,
		},
		{
			name:      "no error_code treated as permanent",
			status:    http.StatusInternalServerError,
			body:      `{"error":"internal server error"}`,
			retryable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = fmt.Fprint(w, tt.body)
			}))
			t.Cleanup(srv.Close)

			m := NewWatchModel(srv.URL, "testdb", "staging", false)
			cmd := m.fetchProgress()
			msg := cmd()

			pmsg, ok := msg.(progressMsg)
			require.True(t, ok, "expected progressMsg, got %T", msg)
			assert.Equal(t, tt.retryable, pmsg.retryable)
		})
	}
}

func TestWatchModel_PermanentError_QuitsImmediately(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)

	msg := progressMsg{
		errorMsg: "apply not found",
		failed:   true,
	}
	updated, cmd := m.Update(msg)
	model := updated.(WatchModel)

	assert.True(t, model.initialized, "permanent error should mark as initialized so view renders")
	assert.Contains(t, model.errorMsg, "apply not found")
	assert.NotNil(t, cmd, "permanent error should return tea.Quit")

	view := model.View()
	assert.Contains(t, view, "apply not found")
	assert.NotContains(t, view, "Loading")
}

func TestWatchModel_ConsecutiveErrors_IncrementCounter(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)

	// Three consecutive transient errors.
	for i := 1; i <= 3; i++ {
		updated, cmd := m.Update(progressMsg{
			errorMsg:  "connection refused",
			failed:    true,
			retryable: true,
		})
		m = updated.(WatchModel)
		assert.Equal(t, i, m.consecutiveErrors)
		assert.Nil(t, cmd)
	}

	// Successful poll resets the counter.
	updated, _ := m.Update(progressMsg{state: state.Apply.Running})
	m = updated.(WatchModel)
	assert.Equal(t, 0, m.consecutiveErrors)
	assert.Empty(t, m.errorMsg, "error should be cleared on success")
}

func TestFetchProgress_ConnectionRefused_RetryableConnectionError(t *testing.T) {
	// Server is not listening — connection refused.
	m := NewWatchModel("http://127.0.0.1:1", "testdb", "staging", false)
	cmd := m.fetchProgress()
	msg := cmd()

	pmsg, ok := msg.(progressMsg)
	require.True(t, ok, "expected progressMsg, got %T", msg)
	assert.True(t, pmsg.retryable, "connection errors should be retryable")
	assert.Contains(t, pmsg.errorMsg, "cannot connect")
}

func TestGetProgress_ServerReturns500_CLIReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "internal server error")
	}))
	t.Cleanup(srv.Close)

	_, err := client.GetProgress(srv.URL, "test-apply-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}
