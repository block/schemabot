package commands

import (
	"errors"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// WatchModel is the Bubbletea model for watching apply progress.
type WatchModel struct {
	// Config
	endpoint            string
	database            string
	environment         string
	applyID             string // When set, fetches progress by apply ID instead of database/environment
	allowCutover        bool
	maxTableNameLen     int
	deployTriggered     bool
	cutoverTriggered    bool
	skipRevertTriggered bool
	skipRevertAt        time.Time // When skip-revert was triggered (for timeout)
	stopTriggered       bool

	// State from API
	state         string
	tables        []tableProgress
	errorMsg      string
	currentVolume int // Current volume (1-11)

	// Engine metadata
	engine           string // "Spirit", "PlanetScale", etc.
	deployRequestURL string
	metadata         map[string]string // Full metadata from progress response

	// UI state
	detached          bool
	quitting          bool
	spinner           spinner.Model
	startedAt         time.Time
	initialized       bool
	volumeMode        bool // True when in volume adjustment mode
	volumePending     int  // Pending volume change (0 = none)
	volumeChanging    bool // True while volume change is in progress
	consecutiveErrors int  // Consecutive fetch failures (drives backoff)
}

// tableProgress represents progress for a single table.
type tableProgress struct {
	Name           string
	Keyspace       string
	DDL            string
	Status         string
	RowsCopied     int64
	RowsTotal      int64
	Percent        int
	ETA            string
	ProgressDetail string
	Shards         []shardProgress
}

type shardProgress struct {
	Shard           string
	Status          string
	RowsCopied      int64
	RowsTotal       int64
	Percent         int
	ETASeconds      int64
	CutoverAttempts int
}

// Messages
type tickMsg time.Time

// isRetryableFetchError reports whether a fetch error is retryable.
//
//   - ConnectionError (server unreachable): always retryable.
//   - APIError with error code: classified by apitypes.IsRetryableErrorCode.
//   - APIError without error code, or unknown error types: permanent.
func isRetryableFetchError(err error) bool {
	var connErr *client.ConnectionError
	if errors.As(err, &connErr) {
		return true
	}
	var apiErr *client.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode != "" {
		return apitypes.IsRetryableErrorCode(apiErr.ErrorCode)
	}
	return false
}

type progressMsg struct {
	state       string
	tables      []tableProgress
	errorMsg    string // Human-readable error message
	failed      bool   // true when the API call didn't return usable progress data
	retryable   bool   // when failed, whether the TUI should keep polling
	volume      int
	applyID     string            // Populated from progress responses
	database    string            // Populated from apply-id progress responses
	environment string            // Populated from apply-id progress responses
	engine      string            // Engine name (e.g., "Spirit", "PlanetScale")
	metadata    map[string]string // Engine metadata (e.g., deploy_request_url)
}

type cutoverResultMsg struct {
	success bool
	err     error
}

type deployResultMsg struct {
	success bool
	err     error
}

type stopResultMsg struct {
	success bool
	err     error
	message string // Informational message from backend (e.g. "Schema change already completed")
}

type volumeResultMsg struct {
	success   bool
	newVolume int
	err       error
}

// NewWatchModel creates a new WatchModel.
func NewWatchModel(endpoint, database, environment string, allowCutover bool) WatchModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	return WatchModel{
		endpoint:      endpoint,
		database:      database,
		environment:   environment,
		allowCutover:  allowCutover,
		spinner:       s,
		startedAt:     time.Now(),
		currentVolume: 4, // Default Spirit volume
	}
}

// Init implements tea.Model.
func (m WatchModel) Init() tea.Cmd {
	return tea.Batch(
		m.fetchProgress(),
		m.tick(),
		m.spinner.Tick,
	)
}

// Update implements tea.Model.
func (m WatchModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		// During cutover, ignore all keyboard input except q to force quit
		isCuttingOver := state.IsState(m.state, state.Apply.CuttingOver) || m.cutoverTriggered

		// Handle volume mode inputs
		if m.volumeMode {
			return m.handleVolumeKeys(msg)
		}

		switch msg.String() {
		case "esc", "ctrl+c":
			// Don't allow detach during cutover
			if isCuttingOver {
				return m, nil
			}
			m.detached = true
			return m, tea.Quit
		case "q":
			m.quitting = true
			return m, tea.Quit
		case "s", "c":
			// Don't allow stop/cancel during cutover
			if isCuttingOver {
				return m, nil
			}
			// Stop (Spirit) or cancel (PlanetScale) the schema change
			if state.IsState(m.state, state.Apply.Running, state.Apply.WaitingForDeploy, state.Apply.WaitingForCutover) && !m.stopTriggered {
				m.stopTriggered = true
				return m, m.triggerStop()
			}
		case "v":
			// Enter volume mode (only when running)
			if state.IsState(m.state, state.Apply.Running) && !isCuttingOver {
				m.volumeMode = true
				return m, nil
			}
		case "enter":
			// Trigger deploy if waiting for deploy and not already triggered
			if state.IsState(m.state, state.Apply.WaitingForDeploy) && m.allowCutover && !m.deployTriggered {
				m.deployTriggered = true
				return m, m.triggerDeploy()
			}
			// Trigger cutover if waiting and not already triggered
			if state.IsState(m.state, state.Apply.WaitingForCutover) && m.allowCutover && !m.cutoverTriggered {
				m.cutoverTriggered = true
				return m, m.triggerCutover()
			}
			// Trigger skip-revert if in revert window
			if state.IsState(m.state, state.Apply.RevertWindow) && !m.skipRevertTriggered {
				m.skipRevertTriggered = true
				m.skipRevertAt = time.Now()
				return m, m.triggerSkipRevert()
			}
		}

	case tickMsg:
		return m, tea.Batch(m.fetchProgress(), m.tick())

	case progressMsg:
		if msg.failed && msg.retryable {
			// Transient error (connection refused, timeout, engine_unavailable).
			// Preserve last known state and tables, keep polling with backoff.
			m.consecutiveErrors++
			m.errorMsg = msg.errorMsg
			return m, nil
		}
		if msg.failed && !msg.retryable {
			// Permanent error (not_found, invalid_request, deployment_not_found).
			m.errorMsg = msg.errorMsg
			m.initialized = true
			return m, tea.Quit
		}

		m.consecutiveErrors = 0
		m.errorMsg = ""
		m.state = msg.state

		// Preserve last known tables during volume change to avoid visual reset
		if !m.volumeChanging || len(m.tables) == 0 {
			m.tables = msg.tables
		}
		m.errorMsg = msg.errorMsg

		// Timeout skip-revert if state hasn't transitioned after 10s.
		if m.skipRevertTriggered && !m.skipRevertAt.IsZero() &&
			state.IsState(m.state, state.Apply.RevertWindow) &&
			time.Since(m.skipRevertAt) > 10*time.Second {
			m.skipRevertTriggered = false
			m.errorMsg = "skip-revert timed out — press Enter to retry"
		}
		m.initialized = true
		// Update volume from API if not pending a change
		if msg.volume > 0 && m.volumePending == 0 {
			m.currentVolume = msg.volume
		}
		// Populate applyID/database/environment from response
		if m.applyID == "" && msg.applyID != "" {
			m.applyID = msg.applyID
		}
		if m.database == "" && msg.database != "" {
			m.database = msg.database
		}
		if m.environment == "" && msg.environment != "" {
			m.environment = msg.environment
		}
		if m.engine == "" && msg.engine != "" {
			m.engine = msg.engine
		}
		if msg.metadata != nil {
			m.metadata = msg.metadata
			if m.deployRequestURL == "" {
				if url := msg.metadata["deploy_request_url"]; url != "" {
					m.deployRequestURL = url
				}
			}
		}

		// Calculate max table name length for alignment
		for _, t := range m.tables {
			if len(t.Name) > m.maxTableNameLen {
				m.maxTableNameLen = len(t.Name)
			}
		}

		// Check for terminal states
		if state.IsState(m.state, state.Apply.Completed, state.Apply.Failed) {
			return m, tea.Quit
		}
		// Also quit on stopped/cancelled state
		if state.IsState(m.state, state.Apply.Stopped, state.Apply.Cancelled) {
			return m, tea.Quit
		}
		// Quit if no active schema change
		if state.IsState(m.state, state.NoActiveChange) {
			return m, tea.Quit
		}

	case cutoverResultMsg:
		if msg.err != nil {
			m.errorMsg = msg.err.Error()
		}
		// Continue polling - next tick will fetch updated state

	case deployResultMsg:
		if msg.err != nil {
			m.errorMsg = msg.err.Error()
			m.deployTriggered = false // Allow retry
		}
		// Continue polling - next tick will fetch updated state

	case stopResultMsg:
		if msg.err != nil {
			m.errorMsg = msg.err.Error()
			m.stopTriggered = false       // Allow retry
			m.skipRevertTriggered = false // Allow retry
		} else if msg.message != "" {
			// Backend returned an informational message (e.g. apply completed before stop)
			// Clear stop state so the TUI transitions cleanly to the completion view
			m.stopTriggered = false
		}
		// Continue polling - next tick will fetch updated state

	case volumeResultMsg:
		m.volumePending = 0      // Clear pending state
		m.volumeChanging = false // Clear changing state
		if msg.err != nil {
			m.errorMsg = msg.err.Error()
		} else if msg.success {
			m.currentVolume = msg.newVolume
			m.errorMsg = "" // Clear any previous error
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	return m, nil
}

// WatchApplyProgressTUI uses Bubbletea to display progress.
func WatchApplyProgressTUI(endpoint, database, environment string, allowCutover bool) error {
	model := NewWatchModel(endpoint, database, environment, allowCutover)
	return runWatchModel(model)
}

// WatchApplyProgressByApplyID watches progress using an apply ID instead of database/environment.
func WatchApplyProgressByApplyID(endpoint, applyID string, allowCutover bool) error {
	model := NewWatchModel(endpoint, "", "", allowCutover)
	model.applyID = applyID
	return runWatchModel(model)
}

// drainStdin discards any pending bytes on stdin so that leftover input
// from a prior prompt (e.g., confirmation "yes" + key repeat) doesn't
// leak into the Bubbletea event loop.
// runWatchModel runs a WatchModel and returns the result.
func runWatchModel(model WatchModel) error {
	// Don't use alt-screen - render inline for seamless experience
	p := tea.NewProgram(model)
	finalModel, err := p.Run()
	if err != nil {
		return err
	}

	m := finalModel.(WatchModel)

	// The TUI view already displays errors inline, so return ErrSilent
	// to exit with code 1 without printing the error again.
	if m.errorMsg != "" && !state.IsState(m.state, state.Apply.Completed) {
		return ErrSilent
	}
	if state.IsState(m.state, state.Apply.Failed) {
		return ErrSilent
	}

	return nil
}
