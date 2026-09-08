package commands

import (
	"fmt"
	"sort"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// Commands (async operations)

// pollInterval is the base polling interval between progress fetches.
const pollInterval = 2 * time.Second

// maxPollInterval is the ceiling for exponential backoff on consecutive errors.
const maxPollInterval = 30 * time.Second

var callStartAPI = client.CallStartAPI

func (m WatchModel) tick() tea.Cmd {
	d := pollInterval
	if m.consecutiveErrors > 0 {
		d = min(pollInterval<<min(m.consecutiveErrors, 4), maxPollInterval)
	}
	return tea.Tick(d, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m WatchModel) fetchProgress() tea.Cmd {
	return func() tea.Msg {
		result, err := client.GetProgress(m.endpoint, m.applyID)
		if err != nil {
			return progressMsg{
				errorMsg:  err.Error(),
				failed:    true,
				retryable: isRetryableFetchError(err),
			}
		}

		return parseProgressResult(result)
	}
}

func (m WatchModel) triggerDeploy() tea.Cmd {
	return func() tea.Msg {
		result, err := callStartAPI(m.endpoint, m.environment, m.applyID)
		if err != nil {
			return deployResultMsg{success: false, err: err}
		}

		if !result.Accepted {
			errMsg := result.ErrorMessage
			if errMsg == "" {
				errMsg = "deploy not accepted"
			}
			return deployResultMsg{success: false, err: fmt.Errorf("%s", errMsg)}
		}

		return deployResultMsg{success: true}
	}
}

func (m WatchModel) triggerCutover() tea.Cmd {
	return func() tea.Msg {
		result, err := client.CallCutoverAPI(m.endpoint, m.environment, m.applyID)
		if err != nil {
			return cutoverResultMsg{success: false, err: err}
		}

		if !result.Accepted {
			errMsg := result.ErrorMessage
			if errMsg == "" {
				errMsg = "cutover not accepted"
			}
			return cutoverResultMsg{success: false, err: fmt.Errorf("%s", errMsg)}
		}

		return cutoverResultMsg{success: true}
	}
}

func (m WatchModel) triggerStop() tea.Cmd {
	return func() tea.Msg {
		result, err := client.CallStopAPI(m.endpoint, m.environment, m.applyID)
		if err != nil {
			return stopResultMsg{success: false, err: err}
		}

		if !result.Accepted {
			errMsg := result.ErrorMessage
			if errMsg == "" {
				errMsg = "stop not accepted"
			}
			return stopResultMsg{success: false, err: fmt.Errorf("%s", errMsg)}
		}

		// Pass through informational message (e.g. "Schema change already completed")
		return stopResultMsg{success: true, message: result.ErrorMessage}
	}
}

func (m WatchModel) triggerSkipRevert() tea.Cmd {
	return func() tea.Msg {
		result, err := client.CallSkipRevertAPI(m.endpoint, m.environment, m.applyID)
		if err != nil {
			return stopResultMsg{success: false, err: err}
		}

		if !result.Accepted {
			errMsg := result.ErrorMessage
			if errMsg == "" {
				errMsg = "skip-revert not accepted"
			}
			return stopResultMsg{success: false, err: fmt.Errorf("%s", errMsg)}
		}

		return stopResultMsg{success: true, message: "Revert window closed"}
	}
}

// Helper functions

func parseProgressResult(result *apitypes.ProgressResponse) progressMsg {
	data := templates.ParseProgressResponse(result)

	return progressMsg{
		state:       data.State,
		tables:      data.Tables,
		operations:  data.Operations,
		released:    data.Released,
		errorMsg:    data.ErrorMessage,
		applyID:     result.ApplyID,
		database:    result.Database,
		environment: result.Environment,
		engine:      result.Engine,
		metadata:    result.Metadata,
	}
}

func sortTablesByProgress(tables []templates.TableProgress) {
	sort.SliceStable(tables, func(i, j int) bool {
		return ui.TableStatePriority(state.NormalizeTaskStatus(tables[i].Status)) <
			ui.TableStatePriority(state.NormalizeTaskStatus(tables[j].Status))
	})
}

// sortStoppedByProgress sorts stopped tables so the one with progress shows first.
func sortStoppedByProgress(tables []templates.TableProgress) {
	sort.SliceStable(tables, func(i, j int) bool {
		// Tables with progress (were actively running) come first
		if tables[i].PercentComplete != tables[j].PercentComplete {
			return tables[i].PercentComplete > tables[j].PercentComplete
		}
		return false
	})
}

func isTableStopped(s string) bool {
	return state.IsState(s, state.Apply.Stopped)
}

// isEffectivelyStopped returns true if the apply is effectively stopped.
// This is true if:
// - The overall state is stopped
// - OR any table has stopped status (in atomic mode, if one stops, all stop)
// Note: stopTriggered alone does NOT count — we wait for the backend to confirm
// the stop so the progress data reflects the true final state of each table.
func (m WatchModel) isEffectivelyStopped() bool {
	if state.IsState(m.state, state.Apply.Stopped) {
		return true
	}
	// Check if any table is stopped (backend may not have updated apply state yet)
	for _, t := range m.tables {
		if isTableStopped(t.Status) {
			return true
		}
	}
	return false
}
