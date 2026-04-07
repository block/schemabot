package commands

import (
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// Commands (async operations)

func (m WatchModel) tick() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m WatchModel) fetchProgress() tea.Cmd {
	return func() tea.Msg {
		var result *apitypes.ProgressResponse
		var err error
		if m.applyID != "" {
			result, err = client.GetProgressByApplyID(m.endpoint, m.applyID)
		} else {
			result, err = client.GetProgress(m.endpoint, m.database, m.environment)
		}
		if err != nil {
			return progressMsg{errorMsg: err.Error()}
		}

		return parseProgressResult(result)
	}
}

func (m WatchModel) triggerCutover() tea.Cmd {
	return func() tea.Msg {
		result, err := client.CallCutoverAPI(m.endpoint, m.database, m.environment, m.applyID)
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
		result, err := client.CallStopAPI(m.endpoint, m.database, m.environment, m.applyID)
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

// handleVolumeKeys handles keyboard input when in volume mode.
func (m WatchModel) handleVolumeKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	switch key {
	case "esc", "q":
		// Exit volume mode without changing
		m.volumeMode = false
		return m, nil

	case "up", "right":
		// Increase volume by 1 (max 11)
		newVol := min(m.currentVolume+1, 11)
		if newVol != m.currentVolume && m.volumePending == 0 {
			m.volumePending = newVol
			m.volumeChanging = true
			m.volumeMode = false
			return m, m.triggerVolumeChange(newVol)
		}
		m.volumeMode = false
		return m, nil

	case "down", "left":
		// Decrease volume by 1 (min 1)
		newVol := max(m.currentVolume-1, 1)
		if newVol != m.currentVolume && m.volumePending == 0 {
			m.volumePending = newVol
			m.volumeChanging = true
			m.volumeMode = false
			return m, m.triggerVolumeChange(newVol)
		}
		m.volumeMode = false
		return m, nil

	case "1", "2", "3", "4", "5", "6", "7", "8", "9":
		// Direct set volume (1-9)
		newVol := int(key[0] - '0')
		if newVol != m.currentVolume && m.volumePending == 0 {
			m.volumePending = newVol
			m.volumeChanging = true
			m.volumeMode = false
			return m, m.triggerVolumeChange(newVol)
		}
		m.volumeMode = false
		return m, nil

	case "0":
		// 0 sets to 10
		if m.currentVolume != 10 && m.volumePending == 0 {
			m.volumePending = 10
			m.volumeChanging = true
			m.volumeMode = false
			return m, m.triggerVolumeChange(10)
		}
		m.volumeMode = false
		return m, nil

	case "-":
		// - sets to 11 (max)
		if m.currentVolume != 11 && m.volumePending == 0 {
			m.volumePending = 11
			m.volumeChanging = true
			m.volumeMode = false
			return m, m.triggerVolumeChange(11)
		}
		m.volumeMode = false
		return m, nil
	}

	return m, nil
}

func (m WatchModel) triggerVolumeChange(volume int) tea.Cmd {
	return func() tea.Msg {
		result, err := client.CallVolumeAPI(m.endpoint, m.database, m.environment, m.applyID, volume)
		if err != nil {
			return volumeResultMsg{success: false, err: err}
		}

		if !result.Accepted {
			errMsg := result.ErrorMessage
			if errMsg == "" {
				errMsg = "volume change not accepted"
			}
			return volumeResultMsg{success: false, err: fmt.Errorf("%s", errMsg)}
		}

		return volumeResultMsg{success: true, newVolume: int(result.NewVolume)}
	}
}

// Helper functions

func parseProgressResult(result *apitypes.ProgressResponse) progressMsg {
	data := templates.ParseProgressResponse(result)

	msg := progressMsg{
		state:       data.State,
		errorMsg:    data.ErrorMessage,
		volume:      int(result.Volume),
		applyID:     result.ApplyID,
		database:    result.Database,
		environment: result.Environment,
	}

	// Convert tables with internal table filtering and Spirit progress parsing
	filtered := ddl.FilterInternalTablesTyped(result.Tables)
	for _, tbl := range filtered {
		tp := tableProgress{
			Name:           tbl.TableName,
			DDL:            tbl.DDL,
			Status:         tbl.Status,
			RowsCopied:     tbl.RowsCopied,
			RowsTotal:      tbl.RowsTotal,
			Percent:        int(tbl.PercentComplete),
			ProgressDetail: tbl.ProgressDetail,
		}
		if tp.ProgressDetail != "" {
			if info := templates.ParseSpiritProgress(tp.ProgressDetail); info != nil {
				tp.Percent = info.Percent
				tp.RowsCopied = info.RowsCopied
				tp.RowsTotal = info.RowsTotal
				tp.ETA = info.ETA
			}
		}
		msg.tables = append(msg.tables, tp)
	}

	return msg
}

func sortTablesByProgress(tables []tableProgress) {
	sort.SliceStable(tables, func(i, j int) bool {
		return ui.TableStatePriority(state.NormalizeTaskStatus(tables[i].Status)) <
			ui.TableStatePriority(state.NormalizeTaskStatus(tables[j].Status))
	})
}

// sortStoppedByProgress sorts stopped tables so the one with progress shows first.
func sortStoppedByProgress(tables []tableProgress) {
	sort.SliceStable(tables, func(i, j int) bool {
		// Tables with progress (were actively running) come first
		if tables[i].Percent != tables[j].Percent {
			return tables[i].Percent > tables[j].Percent
		}
		return false
	})
}

func isTableComplete(status string) bool {
	upper := strings.ToUpper(status)
	return upper == "COMPLETED" || upper == "STATE_COMPLETED"
}

func isTableStopped(status string) bool {
	upper := strings.ToUpper(status)
	return upper == "STOPPED" || upper == "STATE_STOPPED"
}

// isEffectivelyStopped returns true if the apply is effectively stopped.
// This is true if:
// - The overall state is stopped
// - OR any table has stopped status (in atomic mode, if one stops, all stop)
// Note: stopTriggered alone does NOT count — we wait for the backend to confirm
// the stop so the progress data reflects the true final state of each table.
func (m WatchModel) isEffectivelyStopped() bool {
	if state.IsState(m.state, StateStopped) {
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
