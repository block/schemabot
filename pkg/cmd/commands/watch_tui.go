package commands

import (
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/state"
)

// WatchModel is the Bubbletea model for watching apply progress.
type WatchModel struct {
	// Config
	endpoint         string
	database         string
	environment      string
	applyID          string // When set, fetches progress by apply ID instead of database/environment
	allowCutover     bool
	maxTableNameLen  int
	cutoverTriggered bool
	stopTriggered    bool

	// State from API
	state         string
	tables        []tableProgress
	errorMsg      string
	currentVolume int // Current volume (1-11)

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
	DDL            string
	Status         string
	RowsCopied     int64
	RowsTotal      int64
	Percent        int
	ETA            string
	ProgressDetail string
}

// Messages
type tickMsg time.Time

// Error codes for progressMsg.errorCode.
const (
	errRetryable = "retryable" // 5xx, connection refused, timeout — retry with backoff
	errPermanent = "permanent" // 4xx: bad request, not found — don't retry
)

type progressMsg struct {
	state       string
	tables      []tableProgress
	errorMsg    string // Human-readable error message
	errorCode   string // "" on success, errRetryable or errPermanent on fetch failure
	volume      int
	applyID     string // Populated from progress responses
	database    string // Populated from apply-id progress responses
	environment string // Populated from apply-id progress responses
}

type cutoverResultMsg struct {
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
		case "s":
			// Don't allow stop during cutover
			if isCuttingOver {
				return m, nil
			}
			// Stop the schema change if running and not already stopped/stopping
			if state.IsState(m.state, state.Apply.Running, state.Apply.WaitingForCutover) && !m.stopTriggered {
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
			// Trigger cutover if waiting and not already triggered
			if state.IsState(m.state, state.Apply.WaitingForCutover) && m.allowCutover && !m.cutoverTriggered {
				m.cutoverTriggered = true
				return m, m.triggerCutover()
			}
		}

	case tickMsg:
		return m, tea.Batch(m.fetchProgress(), m.tick())

	case progressMsg:
		switch msg.errorCode {
		case errRetryable:
			// 5xx, connection refused, timeout, DNS failure.
			// Preserve last known state and tables, keep polling with backoff.
			m.consecutiveErrors++
			m.errorMsg = msg.errorMsg
			return m, nil
		case errPermanent:
			// 4xx: bad request, not found, etc. Retrying won't help.
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
		// Also quit on stopped state
		if state.IsState(m.state, state.Apply.Stopped) {
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

	case stopResultMsg:
		if msg.err != nil {
			m.errorMsg = msg.err.Error()
			m.stopTriggered = false // Allow retry
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
