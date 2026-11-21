package commands

import (
	"errors"
	"net/http"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func freezeRefusedAPIError() *client.APIError {
	return &client.APIError{
		Status:    http.StatusConflict,
		ErrorCode: apitypes.ErrCodeChangeGateRefused,
		Message:   "staging is frozen",
		Blockers:  []apitypes.ChangeGateBlocker{{Gate: "freeze", ID: "freeze-abc", Reason: "SEV-1234 payments latency"}},
	}
}

// waitingWatchModel is a watcher with control keys on, parked in s.
func waitingWatchModel(s string) WatchModel {
	m := NewWatchModel("https://schemabot.example", "inventory2", "staging", true)
	m.applyID = "apply-abc123"
	m.state = s
	m.initialized = true
	return m
}

// pressEnter presses Enter and runs the command it returns through Update, as
// the program would.
func pressEnter(t *testing.T, m WatchModel) WatchModel {
	t.Helper()
	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.NotNil(t, cmd, "Enter sent nothing")
	updated, _ = updated.(WatchModel).Update(cmd())
	return updated.(WatchModel)
}

// A freeze refusal of the start key names the exact start command that asks
// for an override, keeps it on screen across polls, and lets Enter try again
// once an approver has approved the request.
func TestWatchModel_FreezeRefusedStartNamesTheOverrideCommand(t *testing.T) {
	original := callStartAPI
	t.Cleanup(func() { callStartAPI = original })
	calls := 0
	callStartAPI = func(_, _, _ string) (*apitypes.StartResponse, error) {
		calls++
		if calls == 1 {
			return nil, freezeRefusedAPIError()
		}
		return &apitypes.StartResponse{Accepted: true, StartedCount: 1}, nil
	}

	m := pressEnter(t, waitingWatchModel(state.Apply.WaitingForDeploy))

	assert.False(t, m.deployTriggered, "a refused start must let Enter try again")
	view := m.View()
	assert.Contains(t, view, "staging is frozen. This change did not start.")
	assert.Contains(t, view, "freeze-abc  SEV-1234 payments latency")
	assert.Contains(t, view, `start apply-abc123 -e staging --override "<reason>"`)
	assert.Contains(t, view, changeGateRetryHint)

	// A poll in the same state clears errorMsg but keeps the refusal.
	updated, _ := m.Update(progressMsg{state: state.Apply.WaitingForDeploy})
	m = updated.(WatchModel)
	assert.Contains(t, m.View(), `--override "<reason>"`)

	// Enter tries again and clears the refusal while the request is in flight.
	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(WatchModel)
	require.NotNil(t, cmd)
	assert.True(t, m.deployTriggered)
	assert.Empty(t, m.changeGateAnswer)
	result, ok := cmd().(deployResultMsg)
	require.True(t, ok)
	require.NoError(t, result.err)
	assert.Equal(t, 2, calls)
}

// A freeze refusal of the cutover key leaves the watcher out of its cutover
// lock, so the footer shows the refusal rather than a cutover in progress and
// Enter can ask for the cutover again.
func TestWatchModel_FreezeRefusedCutoverCanBeRetried(t *testing.T) {
	original := callCutoverAPI
	t.Cleanup(func() { callCutoverAPI = original })
	calls := 0
	callCutoverAPI = func(_, _, _ string) (*apitypes.ControlResponse, error) {
		calls++
		return nil, freezeRefusedAPIError()
	}

	m := pressEnter(t, waitingWatchModel(state.Apply.WaitingForCutover))

	assert.False(t, m.cutoverTriggered, "a refused cutover did not begin")
	assert.False(t, m.cuttingOver())
	view := m.View()
	assert.NotContains(t, view, cutoverInProgressFooter)
	assert.Contains(t, view, "staging is frozen. This cutover did not start.")
	assert.Contains(t, view, `cutover apply-abc123 -e staging --override "<reason>"`)

	m = pressEnter(t, m)
	assert.Equal(t, 2, calls)
}

// The freezes could not be checked, so nothing started: Enter can try again.
func TestWatchModel_ChangeGateUnavailableCutoverCanBeRetried(t *testing.T) {
	original := callCutoverAPI
	t.Cleanup(func() { callCutoverAPI = original })
	callCutoverAPI = func(_, _, _ string) (*apitypes.ControlResponse, error) {
		return nil, &client.APIError{Status: http.StatusServiceUnavailable, ErrorCode: apitypes.ErrCodeChangeGateUnavailable}
	}

	m := pressEnter(t, waitingWatchModel(state.Apply.WaitingForCutover))

	assert.False(t, m.cutoverTriggered)
	assert.Contains(t, m.View(), "could not check the freezes, so nothing started")
}

// Any other cutover error may mean the cutover is underway, so the watcher
// keeps its cutover lock as before.
func TestWatchModel_OtherCutoverErrorKeepsTheCutoverLock(t *testing.T) {
	original := callCutoverAPI
	t.Cleanup(func() { callCutoverAPI = original })
	callCutoverAPI = func(_, _, _ string) (*apitypes.ControlResponse, error) {
		return nil, errors.New("connection reset")
	}

	m := pressEnter(t, waitingWatchModel(state.Apply.WaitingForCutover))

	assert.True(t, m.cutoverTriggered)
	assert.Empty(t, m.changeGateAnswer)
	assert.Equal(t, "connection reset", m.errorMsg)
}

// Once the apply leaves the state the refusal was given in, the refusal no
// longer applies and leaves the screen.
func TestWatchModel_FreezeRefusalClearsWhenTheApplyMovesOn(t *testing.T) {
	m := waitingWatchModel(state.Apply.WaitingForCutover)
	updated, _ := m.Update(cutoverResultMsg{err: freezeRefusedAPIError()})
	m = updated.(WatchModel)
	require.NotEmpty(t, m.changeGateAnswer)

	updated, _ = m.Update(progressMsg{state: state.Apply.CuttingOver})
	m = updated.(WatchModel)
	assert.Empty(t, m.changeGateAnswer)
	assert.NotContains(t, m.View(), "is frozen")
}

// A rollout watched in the multi-deployment view shows the refusal too.
func TestWatchModel_MultiDeploymentViewShowsTheFreezeRefusal(t *testing.T) {
	progress := apitypes.ProgressResponse{
		State:       state.Apply.WaitingForCutover,
		ApplyID:     "apply-abc123",
		Database:    "orders",
		Environment: "staging",
		Options:     map[string]string{"defer_cutover": "true"},
		Operations: []*apitypes.ProgressOperationResponse{
			{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
			{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		},
	}
	updated, _ := NewWatchModel("http://localhost:8080", "", "", true).Update(parseProgressResult(&progress))
	updated, _ = updated.(WatchModel).Update(cutoverResultMsg{err: freezeRefusedAPIError()})
	view := updated.(WatchModel).View()

	assert.NotContains(t, view, cutoverInProgressFooter)
	assert.Contains(t, view, "staging is frozen. This cutover did not start.")
	assert.Contains(t, view, `cutover apply-abc123 -e staging --override "<reason>"`)
}
