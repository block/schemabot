package commands

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

type initStageMsg string

// initDetailMsg replaces the line shown under the current step, such as how
// far a download has come. A new step clears it.
type initDetailMsg string
type initFinishedMsg struct {
	result *initResult
	err    error
}
type initProgress struct {
	heading  string
	spinner  spinner.Model
	stages   []string
	detail   string
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
		m.detail = ""
	case initDetailMsg:
		m.detail = string(msg)
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
	b.WriteString("SchemaBot  /  " + m.heading + "\n\n")
	for i, s := range m.stages {
		marker := "✓"
		if i == len(m.stages)-1 {
			marker = m.spinner.View()
		}
		b.WriteString(marker + " " + s + "\n")
	}
	if m.detail != "" && len(m.stages) > 0 {
		b.WriteString("  " + initDetailStyle.Render(m.detail) + "\n")
	}
	if m.stopping {
		b.WriteString("\nFinishing cleanup…\n")
	} else {
		b.WriteString("\nYour application’s schema stays as it is.\nesc cancel\n")
	}
	return "\n" + lipgloss.NewStyle().Width(m.width).PaddingLeft(2).Render(b.String())
}

var initDetailStyle = lipgloss.NewStyle().Faint(true)

func (cmd *InitCmd) initializeWithUI(ctx context.Context, g *Globals) (*initResult, error) {
	if !cmd.interactive {
		return cmd.initialize(ctx, g)
	}
	return runInitProgress(ctx, "connect and verify", func(runCtx context.Context, report, _ func(string)) (*initResult, error) {
		cmd.progress = report
		defer func() { cmd.progress = nil }()
		return cmd.initialize(runCtx, g)
	}, tea.WithInput(os.Stdin), tea.WithOutput(os.Stdout))
}

// initProgressSetup is a setup run under the progress display: step starts a
// new step and detail replaces the line under the current one.
type initProgressSetup func(ctx context.Context, step, detail func(string)) (*initResult, error)

type initProgressProgram interface {
	Run() (tea.Model, error)
	Send(tea.Msg)
	Kill()
}

// runInitProgress shows a setup while it runs. The setup reports each step as
// it starts, and may report a detail line under the current step.
func runInitProgress(ctx context.Context, heading string, initialize initProgressSetup, options ...tea.ProgramOption) (*initResult, error) {
	options = append(options, tea.WithoutSignalHandler())
	return runInitProgressProgram(ctx, initialize, func(model tea.Model) initProgressProgram {
		model.(*initProgress).heading = heading
		return tea.NewProgram(model, options...)
	})
}

// errInitDisplayClosed is why the setup's context was cancelled when the setup
// display ended while the setup was still running: the display stops a setup
// it can no longer show. It is recorded as the cancellation's cause so the
// setup can tell this from the operator stopping it (initStoppedByOperator).
var errInitDisplayClosed = errors.New("setup display closed")

func runInitProgressProgram(ctx context.Context, initialize initProgressSetup, newProgram func(tea.Model) initProgressProgram) (*initResult, error) {
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(errInitDisplayClosed)
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	// The display's cancel key is the operator stopping the setup, so it
	// cancels with no cause of its own.
	m := &initProgress{spinner: s, cancel: func() { cancel(nil) }, width: 72}
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
		outcome.result, outcome.err = initialize(runCtx,
			func(stage string) { p.Send(initStageMsg(stage)) },
			func(detail string) { p.Send(initDetailMsg(detail)) })
	}()
	m.run = func() tea.Msg { close(started); <-finished; return outcome }
	_, runErr := p.Run()
	cancel(errInitDisplayClosed)
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

// initCompletion prints the next command with only the flags it needs: the
// profile is named when the CLI would not resolve to it on its own.
func initCompletion(result *initResult, environment, defaultProfile, invocation string) string {
	next := fmt.Sprintf("%s plan -s %s -e %s", invocation, initShellArg(result.SchemaDir), initShellArg(environment))
	if result.Profile != defaultProfile {
		next += " --profile " + initShellArg(result.Profile)
	}
	noun := "tables"
	if result.Tables == 1 {
		noun = "table"
	}
	heading := lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#1A7F37", Dark: "#3FB950"}).Render("✓ Your schema is ready")
	return fmt.Sprintf("\n  %s\n\n  %d %s · Baseline plan: no changes.\n\n%s\n%s  Make your first edit, then review the plan:\n\n    %s\n\n", heading, result.Tables, noun, initSchemaTree(result.SchemaDir), initSkippedSummary(result.SkippedTables), next)
}

// initSkippedSummary names the tables the import left out, one per line, so
// the operator learns at setup that SchemaBot will not manage them.
func initSkippedSummary(skipped []initSkippedTable) string {
	if len(skipped) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  %s  Not managed: %d %s no schema file, so schemabot.yaml lists %s in ignore_tables:\n",
		glyph.Attention, len(skipped), ui.PluralizeLabel("table has", "tables have", len(skipped)), ui.PluralizeLabel("it", "them", len(skipped)))
	for _, table := range skipped {
		fmt.Fprintf(&b, "    %s.%s: %s\n", table.Namespace, table.Table, table.Reason)
	}
	b.WriteString("\n")
	return b.String()
}

// Preview a bounded number of SQL files, without following directory symlinks.
func initSchemaTree(root string) string {
	type node struct {
		name     string
		children []*node
	}
	tree := &node{}
	count := 0
	more := false
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !entry.Type().IsRegular() || filepath.Ext(path) != ".sql" {
			return nil
		}
		if count == 8 {
			more = true
			return fs.SkipAll
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		parent := tree
		for part := range strings.SplitSeq(relative, string(filepath.Separator)) {
			var child *node
			for _, candidate := range parent.children {
				if candidate.name == part {
					child = candidate
					break
				}
			}
			if child == nil {
				child = &node{name: part}
				parent.children = append(parent.children, child)
			}
			parent = child
		}
		count++
		return nil
	})
	absolute, err := filepath.Abs(root)
	if err != nil {
		absolute = root
	}
	var b strings.Builder
	b.WriteString("  " + initTerminalText(absolute) + string(filepath.Separator) + "\n")
	var render func(*node, string)
	render = func(parent *node, prefix string) {
		for i, child := range parent.children {
			branch, continuation := "├── ", "│   "
			if i == len(parent.children)-1 {
				branch, continuation = "└── ", "    "
			}
			suffix := ""
			if len(child.children) > 0 {
				suffix = "/"
			}
			b.WriteString("  " + prefix + branch + initTerminalText(child.name) + suffix + "\n")
			render(child, prefix+continuation)
		}
	}
	render(tree, "")
	if more {
		b.WriteString("  … more schema files in this folder\n")
	}
	return b.String()
}

func initCommandName(name, executable string) string {
	if name == "schemabot" && strings.ContainsAny(executable, `/\`) {
		if installed, err := exec.LookPath(name); err == nil {
			currentInfo, currentErr := os.Stat(executable)
			installedInfo, installedErr := os.Stat(installed)
			if currentErr == nil && installedErr == nil && os.SameFile(currentInfo, installedInfo) {
				return name
			}
		}
		return initShellArg(executable)
	}
	return name
}

func initShellArg(value string) string {
	if value != "" && strings.IndexFunc(value, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune("_./:-", r)
	}) == -1 {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
