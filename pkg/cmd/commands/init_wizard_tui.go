package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var initName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
var initVariable = regexp.MustCompile(`^env:[A-Za-z_][A-Za-z0-9_]*$`)

type initField struct{ label, hint, value string }

// The wizard edits a private draft. Only explicit confirmation copies it back;
// initialize remains the sole registration and verification path (AZ-7, AZ-8, AZ-9).
type initWizard struct {
	connectionSummary                        string
	hasExistingSchema                        bool
	originalNamespaces                       []string
	checkingConnection, connectionChecked    bool
	check                                    func(context.Context, string, string) error
	fields                                   []initField
	step, width                              int
	height, scroll                           int
	spinner                                  spinner.Model
	input                                    textinput.Model
	renderer                                 *lipgloss.Renderer
	err                                      string
	confirmed, cancelled                     bool
	ctx                                      context.Context
	cancelDiscovery                          context.CancelFunc
	discover                                 func(context.Context, string, string) ([]string, error)
	skip                                     []bool
	explicitNamespaces, editing, discovering bool
	generation, cursor                       int
	names                                    []string
	selected                                 map[string]bool
	notice                                   string
}

func newInitWizard(cmd *InitCmd, profile string, output io.Writer) *initWizard {
	value := func(s, fallback string) string {
		if s == "" {
			return fallback
		}
		return s
	}
	m := &initWizard{width: 72, height: 24, renderer: lipgloss.NewRenderer(output), fields: []initField{
		{"Database engine", "Which database are you working with?", value(cmd.Type, "mysql")},
		{"Database name", "Give your database a name, like shop or analytics.", cmd.Database},
		{"Environment", "Where are you working? Start with development if you’re trying things out.", value(cmd.Environment, "development")},
		{"Connect your database", "Use a connection variable you’ve already set. Confirm it below, or edit the variable name.", value(cmd.DSN, "env:DATABASE_URL")},
		{"Connect SchemaBot’s state database", "Plans and progress live in a separate database. It can share your application’s server.", value(cmd.StorageDSN, "env:SCHEMABOT_STORAGE_DSN")},
		{"Namespaces", "Which namespaces would you like to bring in? You can list several, separated by commas.", strings.Join(cmd.Namespaces, ", ")},
		{"Schema directory", "Choose a home for your schema files. This is where you’ll make changes.", value(cmd.SchemaDir, "schema")},
		{"Connection profile", "Give this connection a profile name so you can use it again.", profile},
	}}
	m.originalNamespaces = slices.Clone(cmd.Namespaces)
	m.spinner = spinner.New()
	m.spinner.Spinner = spinner.Dot
	m.spinner.Style = m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	m.input = textinput.New()
	m.input.Prompt = "› "
	m.input.CharLimit = 1024
	configureInitWizard(m, cmd)
	m.loadField()
	return m
}
func (m *initWizard) Init() tea.Cmd {
	return textinput.Blink
}
func (m *initWizard) loadField() {
	m.scroll = 0
	m.connectionChecked = false
	if m.step >= len(m.fields) {
		reuse, err := initSchemaReuse(m.fields[6].value)
		if err != nil {
			m.step = 6
			m.loadField()
			m.err = err.Error()
			return
		}
		m.hasExistingSchema = reuse
		m.input.Blur()
		return
	}
	m.input.Placeholder = ""
	m.input.SetValue(m.fields[m.step].value)
	if m.step == 3 || m.step == 4 {
		m.connectionSummary = initConnectionSummary(m.fields[0].value, m.input.Value())
	}
	if m.step == 5 && !m.explicitNamespaces {
		m.input.SetValue("")
		m.input.Placeholder = "Search namespaces"
	}
	m.input.CursorEnd()
	m.input.Focus()
}
func (m *initWizard) validate() string {
	v := strings.TrimSpace(m.input.Value())
	if m.step == 0 {
		return ""
	}
	if v == "" {
		return "Fill this in and we can keep going."
	}
	switch m.step {
	case 1:
		if !initName.MatchString(v) {
			return "Use lowercase letters, numbers, hyphens, or underscores."
		}
	case 3, 4:
		if !initVariable.MatchString(v) {
			return "Use env:VARIABLE_NAME here, with the connection string saved in that variable."
		}
		if os.Getenv(strings.TrimPrefix(v, "env:")) == "" {
			return "This variable is empty. Choose one you’ve already set, or restart setup after setting it."
		}
	case 5:
		if _, err := onboardPullNamespaces(initNamespaceInput(v, m.originalNamespaces)); err != nil {
			return err.Error()
		}
	case 6:
		if _, err := initSchemaReuse(v); err != nil {
			return err.Error()
		}
		if info, err := os.Stat(v); err == nil && !info.IsDir() {
			return "There’s a file at that path. Choose a folder for your schema files."
		} else if err != nil && !os.IsNotExist(err) {
			return "We couldn’t read that path: " + err.Error()
		}
	}
	return ""
}
func (m *initWizard) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case initConnectionMsg:
		if msg.generation != m.generation || !m.checkingConnection {
			return m, nil
		}
		m.checkingConnection = false
		if m.cancelDiscovery != nil {
			m.cancelDiscovery()
		}
		if msg.err != nil {
			m.err = msg.err.Error()
		} else {
			m.connectionChecked = true
		}
		return m, nil
	case initNamespacesMsg:
		return m, m.acceptNamespaces(msg)
	case tea.WindowSizeMsg:
		m.height = max(8, msg.Height)
		m.width = max(20, min(72, msg.Width-4))
		m.input.Width = max(10, m.width-4)
	case tea.KeyMsg:
		if m.checkingConnection && msg.String() != "esc" && msg.String() != "ctrl+c" && msg.String() != "shift+tab" {
			return m, nil
		}
		if m.step == 5 && !m.explicitNamespaces && msg.String() != "shift+tab" && msg.String() != "esc" && msg.String() != "ctrl+c" {
			return m.namespaceKey(msg)
		}
		switch msg.String() {
		case "pgup", "pgdown":
			if msg.String() == "pgup" {
				m.scroll = max(0, m.scroll-max(1, m.height-4))
			} else {
				m.scroll += max(1, m.height-4)
			}
			return m, nil
		case "ctrl+c", "esc":
			m.cancelled = true
			if m.cancelDiscovery != nil {
				m.cancelDiscovery()
			}
			return m, tea.Quit
		case "shift+tab":
			m.editing = true
			if m.cancelDiscovery != nil {
				m.cancelDiscovery()
			}
			m.generation++
			m.discovering = false
			m.checkingConnection = false
			if m.step > 0 {
				if m.step < len(m.fields) && m.step != 0 && (m.step != 5 || m.explicitNamespaces) {
					m.fields[m.step].value = m.input.Value()
				}
				m.step--
				m.err = ""
				m.loadField()
			}
			return m, textinput.Blink
		case "up", "down", "left", "right":
			if m.step == len(m.fields) {
				if msg.String() == "up" {
					m.scroll = max(0, m.scroll-1)
				}
				if msg.String() == "down" {
					m.scroll = min(m.scroll+1, max(0, len(strings.Split(m.contentView(), "\n"))-(m.height-4)))
				}
				return m, nil
			}
			if m.step == 0 {
				if m.fields[0].value == "mysql" {
					m.fields[0].value = "postgres"
				} else {
					m.fields[0].value = "mysql"
				}
				return m, nil
			}
		case "enter":
			if m.step == len(m.fields) {
				if _, err := initSchemaReuse(m.fields[6].value); err != nil {
					m.step = 6
					m.loadField()
					m.err = err.Error()
					return m, nil
				}
				m.confirmed = true
				return m, tea.Quit
			}
			if m.err = m.validate(); m.err != "" {
				return m, nil
			}
			if (m.step == 3 || m.step == 4) && !m.connectionChecked {
				return m, tea.Batch(m.checkConnection(), m.spinner.Tick)
			}
			if m.step != 0 {
				m.fields[m.step].value = strings.TrimSpace(m.input.Value())
			}
			return m, tea.Batch(m.advance(), textinput.Blink)
		}
	}
	if m.discovering || m.checkingConnection {
		var c tea.Cmd
		m.spinner, c = m.spinner.Update(msg)
		return m, c
	}
	if m.step > 0 && m.step < len(m.fields) {
		var c tea.Cmd
		before := m.input.Value()
		m.input, c = m.input.Update(msg)
		if m.input.Value() != before {
			m.connectionChecked = false
			if m.step == 3 || m.step == 4 {
				m.connectionSummary = initConnectionSummary(m.fields[0].value, m.input.Value())
			}
			m.err = ""
		}
		return m, c
	}
	return m, nil
}
func (m *initWizard) contentView() string {
	if m.cancelled {
		return "\n  Setup cancelled. Come back whenever you’re ready.\n"
	}
	blue := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	bold := m.renderer.NewStyle().Bold(true)
	muted := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	wrap := m.renderer.NewStyle().Width(m.width)
	var b strings.Builder
	stage := "connect"
	if m.step == 5 {
		stage = "choose schemas"
	}
	if m.step > 5 {
		stage = "review setup"
	}
	b.WriteString(bold.Render("SchemaBot") + muted.Render("  /  "+stage) + "\n\n")
	if m.confirmed {
		return ""
	}
	if m.step < len(m.fields) {
		f := m.fields[m.step]
		b.WriteString(muted.Render("Let’s get your schema ready.") + "\n\n")
		b.WriteString(bold.Render(f.label) + "\n" + wrap.Render(muted.Render(f.hint)) + "\n\n")
		switch {
		case m.step == 0:
			for _, engine := range []struct{ key, label, detail string }{{"mysql", "MySQL", "Online schema changes with Spirit"}, {"postgres", "PostgreSQL", "Schema changes with pg-sprite"}} {
				line := "  " + engine.label
				if f.value == engine.key {
					line = blue.Render("› " + engine.label)
				}
				b.WriteString(line + "\n  " + muted.Render(engine.detail) + "\n\n")
			}
		case m.step == 5 && !m.explicitNamespaces:
			b.WriteString(m.namespaceView())
		default:
			b.WriteString(m.input.View() + "\n\n")
			if m.step == 3 || m.step == 4 {
				b.WriteString(wrap.Render(m.connectionSummary) + "\n\n")
				b.WriteString(muted.Render("Credentials stay in your environment.") + "\n\n")
				if m.checkingConnection {
					b.WriteString(m.spinner.View() + " Checking connection…\n\n")
				}
				if m.connectionChecked {
					b.WriteString(blue.Render("✓ Connected") + "\n\n")
				}
			}
		}
		if m.err != "" {
			b.WriteString(wrap.Render(m.renderer.NewStyle().Foreground(lipgloss.Color("1")).Render(m.err)) + "\n\n")
			if m.step == 3 || m.step == 4 {
				b.WriteString(wrap.Render("Private network? Connect your VPN or tunnel, then retry here.") + "\n\n")
			}
		}
		help := "enter continue · shift+tab back · esc cancel"
		if (m.step == 3 || m.step == 4) && !m.connectionChecked {
			help = "enter check connection · shift+tab back · esc cancel"
		}
		if m.step == 0 {
			help = "↑/↓ choose · enter continue · esc cancel"
		}
		if m.step == 5 && !m.explicitNamespaces {
			help = "shift+tab edit connection · esc cancel"
		}
		b.WriteString(muted.Render(help))
	} else {
		b.WriteString(bold.Render("Ready when you are") + "\n\n")
		if m.notice != "" {
			b.WriteString(wrap.Render(m.notice) + "\n\n")
		}
		b.WriteString(bold.Render("Your database") + "\n")
		b.WriteString(wrap.Render(m.fields[1].value+" · "+m.fields[0].value+" · "+m.fields[2].value) + "\n")
		b.WriteString(wrap.Render("Namespaces: "+initTerminalText(m.fields[5].value)) + "\n\n")
		b.WriteString(bold.Render("Your schema files") + "\n")
		b.WriteString(wrap.Render(m.fields[6].value+" · profile "+m.fields[7].value) + "\n\n")
		b.WriteString(bold.Render("Connections") + "\n")
		b.WriteString(wrap.Render("Application: "+m.fields[3].value) + "\n")
		b.WriteString(wrap.Render("SchemaBot state: "+m.fields[4].value) + "\n")
		if m.hasExistingSchema {
			b.WriteString("\nYou already have schema files here. We’ll verify them and keep your edits.\n")
		}
		b.WriteString("\n" + wrap.Render("We’ll prepare SchemaBot’s state and verify your schema files. We won’t change your application’s schema.") + "\n\n")
		b.WriteString(blue.Render("enter connect and verify") + muted.Render(" · shift+tab edit · esc cancel"))
	}
	return m.renderer.NewStyle().Width(m.width + 2).PaddingLeft(2).Render(b.String())
}

func (m *initWizard) View() string {
	content := m.contentView()
	wrap := m.renderer.NewStyle().Width(m.width)
	lines := strings.Split(content, "\n")
	if len(lines) > m.height-2 {
		available := m.height - 4
		offset := min(m.scroll, len(lines)-available)
		content = strings.Join(lines[offset:offset+available], "\n") + "\n" + wrap.Render("  pgup/pgdown scroll · enter continue · esc cancel")
	}
	return "\n" + content + "\n"
}

func (cmd *InitCmd) promptInputs(ctx context.Context, input io.Reader, output io.Writer, g *Globals) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	m := newInitWizard(cmd, client.ResolveProfileName(cfg, g.Profile), output)
	m.ctx = ctx
	_, err = tea.NewProgram(m, tea.WithInput(input), tea.WithOutput(output), tea.WithContext(ctx)).Run()
	if ctx.Err() != nil {
		return fmt.Errorf("setup cancelled: %w", ctx.Err())
	}
	if err != nil {
		return err
	}
	if !m.confirmed {
		return ErrSilent
	}
	return m.copyToCommand(cmd, g)
}

func (m *initWizard) copyToCommand(cmd *InitCmd, g *Globals) error {
	if !m.confirmed {
		return ErrSilent
	}
	reuse, err := initSchemaReuse(m.fields[6].value)
	if err != nil {
		return err
	}
	cmd.Type = m.fields[0].value
	cmd.Database = m.fields[1].value
	cmd.Environment = m.fields[2].value
	cmd.DSN = m.fields[3].value
	cmd.StorageDSN = m.fields[4].value
	cmd.Namespaces = m.namespaceChoices(cmd.Namespaces)
	cmd.SchemaDir = m.fields[6].value
	g.Profile = m.fields[7].value
	cmd.ReuseSchema = reuse
	return nil
}
