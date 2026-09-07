package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var initName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
var initVariable = regexp.MustCompile(`^env:[A-Za-z_][A-Za-z0-9_]*$`)

type initField struct{ label, hint, value string }

// The wizard edits a private draft. Only explicit confirmation copies it back;
// initialize remains the sole registration and verification path (AZ-6).
type initWizard struct {
	fields               []initField
	step, width          int
	input                textinput.Model
	renderer             *lipgloss.Renderer
	err                  string
	confirmed, cancelled bool
}

func newInitWizard(cmd *InitCmd, profile string, output io.Writer) *initWizard {
	value := func(s, fallback string) string {
		if s == "" {
			return fallback
		}
		return s
	}
	m := &initWizard{width: 72, renderer: lipgloss.NewRenderer(output), fields: []initField{
		{"Database engine", "Choose the engine your database runs.", value(cmd.Type, "mysql")},
		{"Database name", "A name for this connection, such as shop or analytics.", cmd.Database},
		{"Environment", "Where this database runs.", value(cmd.Environment, "development")},
		{"Database connection", "Use an environment variable, never paste a password.", value(cmd.DSN, "env:DATABASE_URL")},
		{"SchemaBot state connection", "A separate, existing database for plans and progress.", value(cmd.StorageDSN, "env:SCHEMABOT_STORAGE_DSN")},
		{"Namespaces", "Which live namespaces should SchemaBot import? Separate with commas.", strings.Join(cmd.Namespaces, ", ")},
		{"Schema directory", "Your editable schema files will live here.", value(cmd.SchemaDir, "schema")},
		{"Connection profile", "A saved connection for your next SchemaBot command.", profile},
	}}
	m.input = textinput.New()
	m.input.Prompt = "› "
	m.input.CharLimit = 1024
	m.loadField()
	return m
}
func (m *initWizard) Init() tea.Cmd { return textinput.Blink }
func (m *initWizard) loadField() {
	if m.step >= len(m.fields) {
		m.input.Blur()
		return
	}
	if m.step == 5 && m.fields[5].value == "" {
		m.fields[5].value = m.fields[1].value
		if m.fields[0].value == "postgres" {
			m.fields[5].value = "public"
		}
	}
	m.input.SetValue(m.fields[m.step].value)
	m.input.CursorEnd()
	m.input.Focus()
}
func (m *initWizard) validate() string {
	v := strings.TrimSpace(m.input.Value())
	if m.step == 0 {
		return ""
	}
	if v == "" {
		return "Enter a value to continue."
	}
	switch m.step {
	case 1:
		if !initName.MatchString(v) {
			return "Use lowercase letters, numbers, hyphens, or underscores."
		}
	case 3, 4:
		if !initVariable.MatchString(v) {
			return "Use env:VARIABLE_NAME. Keep the connection string in your environment."
		}
		if os.Getenv(strings.TrimPrefix(v, "env:")) == "" {
			return "Set this environment variable before continuing, or choose one already set."
		}
	case 5:
		if _, err := onboardPullNamespaces(strings.Split(v, ",")); err != nil {
			return err.Error()
		}
	case 6:
		if info, err := os.Stat(v); err == nil && !info.IsDir() {
			return "Choose a directory, not a file."
		} else if err != nil && !os.IsNotExist(err) {
			return "Cannot read this path: " + err.Error()
		}
	}
	return ""
}
func (m *initWizard) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = max(20, min(72, msg.Width-4))
		m.input.Width = max(10, m.width-4)
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			m.cancelled = true
			return m, tea.Quit
		case "shift+tab":
			if m.step > 0 {
				if m.step < len(m.fields) && m.step != 0 {
					m.fields[m.step].value = m.input.Value()
				}
				m.step--
				m.err = ""
				m.loadField()
			}
			return m, textinput.Blink
		case "up", "down", "left", "right":
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
				m.confirmed = true
				return m, tea.Quit
			}
			if m.err = m.validate(); m.err != "" {
				return m, nil
			}
			if m.step != 0 {
				m.fields[m.step].value = strings.TrimSpace(m.input.Value())
			}
			m.step++
			m.loadField()
			return m, textinput.Blink
		}
	}
	if m.step > 0 && m.step < len(m.fields) {
		var c tea.Cmd
		m.input, c = m.input.Update(msg)
		return m, c
	}
	return m, nil
}
func (m *initWizard) View() string {
	if m.cancelled {
		return "\n  Setup cancelled. Nothing was initialized.\n"
	}
	blue := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	bold := m.renderer.NewStyle().Bold(true)
	muted := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	wrap := m.renderer.NewStyle().Width(m.width)
	var b strings.Builder
	b.WriteString(bold.Render("SchemaBot") + muted.Render("  /  setup") + "\n\n")
	if m.confirmed {
		return ""
	}
	if m.step < len(m.fields) {
		f := m.fields[m.step]
		b.WriteString(muted.Render(fmt.Sprintf("Connect your database   %d / %d", m.step+1, len(m.fields))) + "\n\n")
		b.WriteString(bold.Render(f.label) + "\n" + wrap.Render(muted.Render(f.hint)) + "\n\n")
		if m.step == 0 {
			for _, engine := range []struct{ key, label, detail string }{{"mysql", "MySQL", "Online schema changes with Spirit"}, {"postgres", "PostgreSQL", "Declarative schemas for Postgres"}} {
				line := "  " + engine.label
				if f.value == engine.key {
					line = blue.Render("› " + engine.label)
				}
				b.WriteString(line + "\n  " + muted.Render(engine.detail) + "\n\n")
			}
		} else {
			b.WriteString(m.input.View() + "\n\n")
		}
		if m.err != "" {
			b.WriteString(wrap.Render(m.renderer.NewStyle().Foreground(lipgloss.Color("1")).Render(m.err)) + "\n\n")
		}
		help := "enter continue · shift+tab back · esc cancel"
		if m.step == 0 {
			help = "↑/↓ choose · enter continue · esc cancel"
		}
		b.WriteString(muted.Render(help))
	} else {
		b.WriteString(bold.Render("Ready to connect") + "\n\n")
		for _, f := range m.fields {
			b.WriteString(wrap.Render(muted.Render(f.label+": ")+f.value) + "\n")
		}
		if _, err := os.Stat(m.fields[6].value); err == nil {
			b.WriteString("\nExisting schema files will be verified and preserved.\n")
		}
		b.WriteString("\n" + wrap.Render("SchemaBot will prepare its state database, read your schema, and verify a baseline plan. No application schema changes will be applied.") + "\n\n")
		b.WriteString(blue.Render("enter connect and verify") + muted.Render(" · shift+tab edit · esc cancel"))
	}
	return "\n" + m.renderer.NewStyle().PaddingLeft(2).Render(b.String()) + "\n"
}

func (cmd *InitCmd) promptInputs(ctx context.Context, input io.Reader, output io.Writer, g *Globals) error {
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	m := newInitWizard(cmd, client.ResolveProfileName(cfg, g.Profile), output)
	_, err = tea.NewProgram(m, tea.WithInput(input), tea.WithOutput(output), tea.WithContext(ctx)).Run()
	if ctx.Err() != nil {
		return fmt.Errorf("setup cancelled: %w", ctx.Err())
	}
	if err != nil {
		return err
	}
	if !m.confirmed {
		return fmt.Errorf("setup cancelled; nothing was initialized")
	}
	cmd.Type = m.fields[0].value
	cmd.Database = m.fields[1].value
	cmd.Environment = m.fields[2].value
	cmd.DSN = m.fields[3].value
	cmd.StorageDSN = m.fields[4].value
	cmd.Namespaces = strings.Split(m.fields[5].value, ",")
	for i := range cmd.Namespaces {
		cmd.Namespaces[i] = strings.TrimSpace(cmd.Namespaces[i])
	}
	cmd.SchemaDir = m.fields[6].value
	g.Profile = m.fields[7].value
	if _, err := os.Stat(cmd.SchemaDir); err == nil {
		cmd.ReuseSchema = true
	}
	return nil
}
