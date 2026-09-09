package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type initStageMsg string
type initFinishedMsg struct {
	result *initResult
	err    error
}
type initProgress struct {
	spinner  spinner.Model
	stages   []string
	run      tea.Cmd
	cancel   context.CancelFunc
	stopping bool
	finished *initFinishedMsg
	width    int
}

func (m *initProgress) Init() tea.Cmd { return tea.Batch(m.spinner.Tick, m.run) }
func (m *initProgress) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = max(20, min(72, msg.Width-4))
	case initStageMsg:
		m.stages = append(m.stages, string(msg))
	case initFinishedMsg:
		m.finished = &msg
		return m, tea.Quit
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" || msg.String() == "esc" {
			m.stopping = true
			m.cancel()
		}
	}
	var c tea.Cmd
	m.spinner, c = m.spinner.Update(msg)
	return m, c
}
func (m *initProgress) View() string {
	if m.finished != nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("SchemaBot  /  connect and verify\n\n")
	for i, s := range m.stages {
		marker := "✓"
		if i == len(m.stages)-1 {
			marker = m.spinner.View()
		}
		b.WriteString(marker + " " + s + "\n")
	}
	if m.stopping {
		b.WriteString("\nFinishing cleanup…\n")
	} else {
		b.WriteString("\nYour application’s schema stays as it is.\nesc cancel\n")
	}
	return "\n" + lipgloss.NewStyle().Width(m.width).PaddingLeft(2).Render(b.String())
}
func (cmd *InitCmd) initializeWithUI(ctx context.Context, g *Globals) (*initResult, error) {
	if !cmd.interactive {
		return cmd.initialize(ctx, g)
	}
	return runInitProgress(ctx, func(runCtx context.Context, report func(string)) (*initResult, error) {
		cmd.progress = report
		defer func() { cmd.progress = nil }()
		return cmd.initialize(runCtx, g)
	}, tea.WithInput(os.Stdin), tea.WithOutput(os.Stdout))
}

type initProgressProgram interface {
	Run() (tea.Model, error)
	Send(tea.Msg)
	Kill()
}

func runInitProgress(ctx context.Context, initialize func(context.Context, func(string)) (*initResult, error), options ...tea.ProgramOption) (*initResult, error) {
	options = append(options, tea.WithoutSignalHandler())
	return runInitProgressProgram(ctx, initialize, func(model tea.Model) initProgressProgram {
		return tea.NewProgram(model, options...)
	})
}

func runInitProgressProgram(ctx context.Context, initialize func(context.Context, func(string)) (*initResult, error), newProgram func(tea.Model) initProgressProgram) (*initResult, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	m := &initProgress{spinner: s, cancel: cancel, width: 72}
	p := newProgram(m)
	started := make(chan struct{})
	finished := make(chan struct{})
	var outcome initFinishedMsg
	initialized := false
	// Own cleanup outside Bubble Tea, but begin work only after its Init
	// command runs. A terminal startup failure must not register a runtime.
	go func() {
		defer close(finished)
		select {
		case <-runCtx.Done():
			outcome.err = runCtx.Err()
			return
		case <-started:
		}
		if err := runCtx.Err(); err != nil {
			outcome.err = err
			return
		}
		initialized = true
		outcome.result, outcome.err = initialize(runCtx, func(stage string) { p.Send(initStageMsg(stage)) })
	}()
	m.run = func() tea.Msg { close(started); <-finished; return outcome }
	_, runErr := p.Run()
	cancel()
	p.Kill()
	<-finished
	if runErr != nil {
		if !initialized && ctx.Err() == nil {
			return nil, fmt.Errorf("run setup display: %w", runErr)
		}
		return outcome.result, errors.Join(fmt.Errorf("run setup display: %w", runErr), outcome.err)
	}
	if m.finished == nil {
		return nil, fmt.Errorf("setup interrupted")
	}
	return m.finished.result, m.finished.err
}
func initCompletion(result *initResult, environment string) string {
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	next := fmt.Sprintf("%s plan -s %s -e %s --profile %s", cliname.Name(), quote(result.SchemaDir), quote(environment), quote(result.Profile))
	noun := "tables"
	if result.Tables == 1 {
		noun = "table"
	}
	return fmt.Sprintf("\n  ✓ Your schema is ready\n\n  %d %s · %s\n  Baseline plan: no changes.\n\n  Make your first edit, then review the plan:\n\n    %s\n\n", result.Tables, noun, result.SchemaDir, next)
}
