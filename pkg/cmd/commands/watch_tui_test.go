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
	assert.True(t, pmsg.fetchErr, "fetchProgress should set fetchErr on HTTP error")
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
	assert.False(t, pmsg.fetchErr, "successful response should not set fetchErr")
	assert.Empty(t, pmsg.errorMsg)
}

func TestWatchModel_FirstPollError_ShowsLoadingWithError(t *testing.T) {
	// First poll fails (server 500 before any successful poll).
	// TUI should stay in loading state with the error visible, not show
	// "No active schema change" or assume the apply failed.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "progress endpoint not implemented")
	}))
	t.Cleanup(srv.Close)

	m := NewWatchModel(srv.URL, "testdb", "staging", false)
	cmd := m.fetchProgress()
	msg := cmd()

	updated, retCmd := m.Update(msg)
	model := updated.(WatchModel)

	assert.False(t, model.initialized,
		"should not be initialized — we haven't gotten a real response yet")
	assert.Contains(t, model.errorMsg, "500")

	view := model.View()
	assert.NotContains(t, view, "No active schema change")
	assert.Contains(t, view, "Loading",
		"should still show loading spinner")
	assert.Contains(t, view, "500",
		"should show the connection error")

	// Should keep polling — the server may recover.
	assert.Nil(t, retCmd, "should return nil cmd to continue polling")
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

	// Second: server crashes — API call fails (fetchErr distinguishes this
	// from a server response that happens to have an empty state).
	errorMsg := progressMsg{
		errorMsg: "500: connection refused",
		fetchErr: true,
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

	// Simulate fetch error (API call failed).
	updated, _ := m.Update(progressMsg{errorMsg: "connection refused", fetchErr: true})
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

func TestGetProgress_ServerReturns500_CLIReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "internal server error")
	}))
	t.Cleanup(srv.Close)

	_, err := client.GetProgress(srv.URL, "testdb", "staging")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}
