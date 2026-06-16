package commands

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
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

func TestWatchModel_CompletedViewShowsCompactSummary(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)
	m.applyID = "apply-abc123"
	m.state = state.Apply.Completed
	m.initialized = true
	m.tables = []tableProgress{
		{Name: "users", ChangeType: "CREATE"},
		{Name: "orders", ChangeType: "CREATE"},
		{Name: "products", ChangeType: "CREATE"},
		{Name: "audit_events", ChangeType: "CREATE"},
		{Name: "legacy_orders", ChangeType: "DROP"},
		{Name: "VSchema: commerce", ChangeType: "vschema_update"},
	}

	view := m.View()
	assert.Contains(t, view, "✓ Apply complete!")
	assert.Contains(t, view, "Changes: 4 created, 1 dropped, 1 VSchema update. Apply ID: apply-abc123")
	assert.NotContains(t, view, "Resume:")
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

func TestWatchModel_StoppedWithErrorShowsReason(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", false)
	m.applyID = "apply-stopped-start-timeout"

	updated, _ := m.Update(progressMsg{
		state:    state.Apply.Stopped,
		errorMsg: "remote apply remote-123 remained stopped after start grace period 30s",
		tables: []tableProgress{{
			Name:    "users",
			Status:  state.Task.Stopped,
			Percent: 40,
		}},
	})
	model := updated.(WatchModel)

	view := model.View()
	assert.Contains(t, view, "remote apply remote-123 remained stopped after start grace period 30s")
	assert.Contains(t, view, "Apply stopped")
	assert.Contains(t, view, "schemabot start")
}

func TestWatchModel_RecoveringShowsBlockedCutoverMessage(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", true)
	m.applyID = "apply-recovering-cutover"

	updated, _ := m.Update(progressMsg{
		state: state.Apply.Recovering,
		tables: []tableProgress{{
			Name:    "users",
			Status:  state.Task.Recovering,
			Percent: 100,
		}},
	})
	model := updated.(WatchModel)

	view := model.View()
	assert.Contains(t, view, "Recovering state")
	assert.Contains(t, view, "Cutover will be available once recovery completes")
	assert.NotContains(t, view, "Press Enter to proceed with cutover")
}

func TestWatchModel_RecoveringShowsCopyingRows(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", true)
	m.applyID = "apply-recovering-cutover"

	updated, _ := m.Update(progressMsg{
		state: state.Apply.Recovering,
		tables: []tableProgress{{
			Name:       "users",
			Status:     state.Task.Recovering,
			RowsCopied: 420,
			RowsTotal:  1000,
			Percent:    42,
		}},
	})
	model := updated.(WatchModel)

	view := model.View()
	assert.Contains(t, view, "Row copy in progress (42%)")
	assert.Contains(t, view, "Rows: 420 / 1,000")
	assert.Contains(t, view, "Row copy is in progress (42%)")
	assert.Contains(t, view, "progress returns to the normal row-copy view")
	assert.Contains(t, view, "SchemaBot is recovering after restart")
	assert.NotContains(t, view, "Cutover will be available once recovery completes")
	assert.NotContains(t, view, "Press Enter to proceed with cutover")
}

func TestWatchModel_EstimateExceededUsesActivityLabel(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", true)
	m.initialized = true
	m.state = state.Apply.Running
	m.activityLabelFrame = 2
	m.tables = []tableProgress{{
		Name:       "users",
		Status:     state.Task.Running,
		RowsCopied: 145000,
		RowsTotal:  100000,
		Percent:    145,
	}}

	view := m.View()

	assert.Contains(t, view, ui.ProgressBarActivity()+" Active ⠹")
	assert.Contains(t, view, "Rows copied: 145,000 so far")
	assert.Contains(t, view, "More rows than initially estimated, copying is still active and will continue")
	assert.NotContains(t, view, "145%")
}

func TestWatchModel_MultiDeploymentView(t *testing.T) {
	progress := multiDeploymentTUITestProgress()
	msg := parseProgressResult(&progress)

	m := NewWatchModel("http://localhost:8080", msg.database, msg.environment, false)
	m.applyID = msg.applyID
	m.state = msg.state
	m.tables = msg.tables
	m.operations = msg.operations
	m.errorMsg = msg.errorMsg
	m.initialized = true
	m.activityLabelFrame = 2

	view := m.View()

	assert.Contains(t, view, "1 completed · 1 halted · 1 failed")
	assert.Contains(t, view, "⚠ First failure: eu-west — duplicate key name 'idx_orders_source'")
	assert.Contains(t, view, "Apply ID: apply-multi-test")
	assert.Contains(t, view, "Environment: production")
	assertContainsInOrder(t, view,
		"✅ us-east — completed (orders-us-east)",
		"❌ eu-west — failed (orders-eu-west)",
		"⏸ ap-south — halted — eu-west failed (orders-ap-south)",
	)
	assert.Contains(t, view, "duplicate key name 'idx_orders_source'")
	assert.Contains(t, view, "orders")
	assert.Contains(t, view, "✓ Complete")
	assert.Contains(t, view, "Failed")
}

func TestWatchModel_SingleDeploymentOutputDoesNotUseMultiView(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "orders", "production", false)
	m.applyID = "apply-single-test"
	m.state = state.Apply.Running
	m.initialized = true
	m.tables = []tableProgress{{
		Name:       "orders",
		Status:     state.Task.Running,
		RowsCopied: 420,
		RowsTotal:  1000,
		Percent:    42,
	}}

	withoutOperations := m.View()
	m.operations = []templates.ProgressOperation{{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Running}}
	withSingleOperation := m.View()

	assert.Equal(t, withoutOperations, withSingleOperation)
	assert.NotContains(t, withSingleOperation, "1 running")
	assert.NotContains(t, withSingleOperation, "us-east — running table copy")
}

func multiDeploymentTUITestProgress() apitypes.ProgressResponse {
	return apitypes.ProgressResponse{
		State:       state.Apply.Failed,
		ApplyID:     "apply-multi-test",
		Database:    "orders",
		Environment: "production",
		Operations: []*apitypes.ProgressOperationResponse{
			{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
			{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt, ErrorMessage: "duplicate key name 'idx_orders_source'"},
			{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		},
		Tables: []*apitypes.TableProgressResponse{
			{Deployment: "us-east", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
			{Deployment: "eu-west", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_orders_source` (`source`)", Status: state.Task.Failed},
			{Deployment: "ap-south", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Pending},
		},
	}
}

func assertContainsInOrder(t *testing.T, text string, values ...string) {
	t.Helper()
	start := 0
	for _, value := range values {
		idx := strings.Index(text[start:], value)
		require.NotEqual(t, -1, idx, "expected %q after offset %d", value, start)
		start += idx + len(value)
	}
}

func TestWatchModel_SpinnerTickAdvancesActivityLabel(t *testing.T) {
	m := NewWatchModel("http://localhost:8080", "testdb", "staging", true)

	updated, cmd := m.Update(m.spinner.Tick())
	m = updated.(WatchModel)

	assert.Equal(t, 1, m.activityLabelFrame)
	assert.NotNil(t, cmd)
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
			body:      `{"error":"apply_id is required","error_code":"invalid_request"}`,
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

func TestWatchModel_EnterTriggersDeferredDeploy(t *testing.T) {
	tests := []struct {
		name string
		key  tea.KeyMsg
	}{
		{name: "carriage return", key: tea.KeyMsg{Type: tea.KeyEnter}},
		{name: "line feed", key: tea.KeyMsg{Type: tea.KeyCtrlJ}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalCallStartAPI := callStartAPI
			t.Cleanup(func() { callStartAPI = originalCallStartAPI })

			var gotEndpoint string
			var gotEnvironment string
			var gotApplyID string
			callStartAPI = func(endpoint, environment, applyID string) (*apitypes.StartResponse, error) {
				gotEndpoint = endpoint
				gotEnvironment = environment
				gotApplyID = applyID
				return &apitypes.StartResponse{Accepted: true, StartedCount: 1}, nil
			}

			m := NewWatchModel("https://schemabot.example", "inventory2", "staging", true)
			m.applyID = "apply-abc123"
			m.state = state.Apply.WaitingForDeploy
			m.initialized = true

			updated, cmd := m.Update(tt.key)
			model := updated.(WatchModel)
			require.NotNil(t, cmd)
			assert.True(t, model.deployTriggered)

			msg := cmd()
			result, ok := msg.(deployResultMsg)
			require.True(t, ok, "expected deployResultMsg, got %T", msg)
			require.NoError(t, result.err)
			assert.True(t, result.success)

			assert.Equal(t, "https://schemabot.example", gotEndpoint)
			assert.Equal(t, "apply-abc123", gotApplyID)
			assert.Equal(t, "staging", gotEnvironment)
		})
	}
}

func TestWatchModel_EnterTriggersCutover(t *testing.T) {
	tests := []struct {
		name string
		key  tea.KeyMsg
	}{
		{name: "carriage return", key: tea.KeyMsg{Type: tea.KeyEnter}},
		{name: "line feed", key: tea.KeyMsg{Type: tea.KeyCtrlJ}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewWatchModel("https://schemabot.example", "inventory2", "staging", true)
			m.applyID = "apply-abc123"
			m.state = state.Apply.WaitingForCutover
			m.initialized = true

			updated, cmd := m.Update(tt.key)
			model := updated.(WatchModel)
			require.NotNil(t, cmd)
			assert.True(t, model.cutoverTriggered)
		})
	}
}

func TestWatchModel_EnterTriggersSkipRevert(t *testing.T) {
	tests := []struct {
		name string
		key  tea.KeyMsg
	}{
		{name: "carriage return", key: tea.KeyMsg{Type: tea.KeyEnter}},
		{name: "line feed", key: tea.KeyMsg{Type: tea.KeyCtrlJ}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewWatchModel("https://schemabot.example", "inventory2", "staging", true)
			m.applyID = "apply-abc123"
			m.state = state.Apply.RevertWindow
			m.initialized = true

			updated, cmd := m.Update(tt.key)
			model := updated.(WatchModel)
			require.NotNil(t, cmd)
			assert.True(t, model.skipRevertTriggered)
			assert.False(t, model.skipRevertAt.IsZero())
		})
	}
}

func TestFormatExitContext(t *testing.T) {
	t.Run("includes apply ID and resume command", func(t *testing.T) {
		result := formatExitContext("apply-abc123", "", "mydb", "production")
		assert.Contains(t, result, "apply-abc123")
		assert.Contains(t, result, "schemabot progress apply-abc123 -e production")
	})

	t.Run("includes deploy request URL when present", func(t *testing.T) {
		result := formatExitContext("apply-abc123", "https://app.planetscale.com/org/db/deploy-requests/42", "mydb", "production")
		assert.Contains(t, result, "apply-abc123")
		assert.Contains(t, result, "https://app.planetscale.com/org/db/deploy-requests/42")
		assert.Contains(t, result, "schemabot progress apply-abc123 -e production")
	})

	t.Run("omits deploy URL when empty", func(t *testing.T) {
		result := formatExitContext("apply-abc123", "", "mydb", "staging")
		assert.NotContains(t, result, "Deploy Request:")
	})

	t.Run("empty apply ID returns empty string", func(t *testing.T) {
		result := formatExitContext("", "https://example.com", "mydb", "staging")
		assert.Empty(t, result)
	})

	t.Run("omits environment flag when empty", func(t *testing.T) {
		result := formatExitContext("apply-abc123", "", "mydb", "")
		assert.Contains(t, result, "schemabot progress apply-abc123")
		assert.NotContains(t, result, "-e ")
	})
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
