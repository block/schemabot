package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/localsetup"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var initName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
var initVariable = regexp.MustCompile(`^env:[A-Za-z_][A-Za-z0-9_]*$`)

// Wizard steps in display order. The PlanetScale steps are shown only for a
// Vitess database; every other engine moves straight from the application
// connection to the state connection.
const (
	stepEngine = iota
	stepName
	stepEnvironment
	stepDSN
	stepOrganization
	stepAPIToken
	stepStorageDSN
	stepNamespaces
	stepSchemaDir
	stepProfile
)

type initEngine struct{ key, label, detail string }

var initEngines = []initEngine{
	{"mysql", "MySQL", "Schema changes with Spirit"},
	{"postgres", "PostgreSQL", "Schema changes with pg-sprite"},
	{"vitess", "Vitess", "Schema changes through PlanetScale deploy requests"},
}

func initEngineKeys() []string {
	keys := make([]string, 0, len(initEngines))
	for _, engine := range initEngines {
		keys = append(keys, engine.key)
	}
	return keys
}

type initField struct{ label, hint, value string }

// The wizard edits a private draft. Only explicit confirmation copies it back;
// initialize remains the sole registration and verification path (AZ-7, AZ-8, AZ-9).
type initWizard struct {
	connectionEditor                         initConnectionEditor
	draftConnections                         map[string]string
	integrated, choosingStorage              bool
	connectionSummary                        string
	existingProject                          string
	originalNamespaces                       []string
	checkingConnection, connectionChecked    bool
	applicationConnected                     bool
	check                                    func(context.Context, string, string) error
	checkAPI                                 func(context.Context, localsetup.Target) error
	apiURL                                   string
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
	discover                                 func(context.Context, localsetup.Target) ([]string, error)
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
		{"PlanetScale organization", "Which PlanetScale organization owns this database?", cmd.Organization},
		{"Connect the PlanetScale API", "Use a variable holding a service token as name:value. SchemaBot opens deploy requests with it.", value(cmd.APIToken, "env:PLANETSCALE_TOKEN")},
		{"Connect SchemaBot’s state database", "Plans and progress live in a separate database. It can share your application’s server.", value(cmd.StorageDSN, "env:SCHEMABOT_STORAGE_DSN")},
		{"Namespaces", "Which namespaces would you like to bring in? You can list several, separated by commas.", strings.Join(cmd.Namespaces, ", ")},
		{"Schema directory", "Choose a home for your schema files. This is where you’ll make changes.", value(cmd.SchemaDir, "schema")},
		{"Connection profile", "Give this connection a profile name so you can use it again.", profile},
	}}
	if cfg, err := LoadCLIConfig(m.fields[stepSchemaDir].value); err == nil {
		if cmd.Type == "" {
			m.fields[stepEngine].value = cfg.Type
		}
		if cmd.Database == "" {
			m.fields[stepName].value = cfg.Database
		}
		m.existingProject = fmt.Sprintf("Found %s\n%s · %s", initTerminalText(filepath.Join(m.fields[stepSchemaDir].value, "schemabot.yaml")), initTerminalText(cfg.Database), initTerminalText(cfg.Type))
	}
	m.integrated = cmd.Integrated || cmd.StorageDSN == ""
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
func (m *initWizard) engine() string { return m.fields[stepEngine].value }
func (m *initWizard) isVitess() bool { return m.engine() == "vitess" }
func (m *initWizard) hidden(step int) bool {
	return (step == stepOrganization || step == stepAPIToken) && !m.isVitess()
}

// Connection steps confirm access before continuing, whatever supplied the value.
func (m *initWizard) connectionStep(step int) bool {
	return step == stepDSN || step == stepStorageDSN || step == stepAPIToken
}

// A Vitess database is addressed by its PlanetScale name and keeps SchemaBot's
// state outside Vitess, so two hints read differently for it.
func (m *initWizard) hint(step int) string {
	if m.isVitess() {
		switch step {
		case stepName:
			return "Use your PlanetScale database name. SchemaBot addresses the database by it."
		case stepStorageDSN:
			return "Use a separate MySQL database for SchemaBot’s plans and progress."
		}
	}
	return m.fields[step].hint
}

func (m *initWizard) planetScaleTarget(token string) localsetup.Target {
	return localsetup.Target{
		Engine:       "vitess",
		Database:     strings.TrimSpace(m.fields[stepName].value),
		Organization: strings.TrimSpace(m.fields[stepOrganization].value),
		Token:        token,
		APIURL:       m.apiURL,
	}
}
func (m *initWizard) summary() string {
	if m.step == stepAPIToken {
		return initTokenSummary(m.input.Value())
	}
	return initConnectionSummary(m.engine(), m.input.Value())
}
func (m *initWizard) loadField() {
	m.scroll = 0
	m.connectionChecked = false
	if m.step >= len(m.fields) {
		_, err := validateInitSchemaReuse(m.fields[stepSchemaDir].value, m.fields[stepName].value, m.fields[stepEngine].value)
		if err != nil {
			m.step = stepSchemaDir
			m.loadField()
			m.err = err.Error()
			return
		}
		m.input.Blur()
		return
	}
	m.choosingStorage = m.step == stepStorageDSN && !m.isVitess()
	if m.isVitess() {
		m.integrated = false
	}
	m.input.EchoMode = textinput.EchoNormal
	m.input.Placeholder = ""
	m.input.SetValue(m.fields[m.step].value)
	if m.step == stepDSN || m.step == stepStorageDSN {
		m.connectionSummary = initConnectionSummary(m.fields[stepEngine].value, m.input.Value())
		m.loadConnectionEditor()
	}
	if m.step == stepNamespaces && !m.explicitNamespaces {
		m.input.SetValue("")
		m.input.Placeholder = "Search namespaces"
	}
	if m.step == stepAPIToken {
		m.connectionSummary = m.summary()
	}
	m.input.CursorEnd()
	m.input.Focus()
}
func (m *initWizard) validate() string {
	v := strings.TrimSpace(m.input.Value())
	if m.step == stepEngine {
		return ""
	}
	if v == "" {
		return "Fill this in and we can keep going."
	}
	switch m.step {
	case stepName:
		if !initName.MatchString(v) {
			return "Use lowercase letters, numbers, hyphens, or underscores."
		}
	case stepOrganization:
		if !initName.MatchString(v) {
			return "Use the organization’s slug: lowercase letters, numbers, hyphens, or underscores."
		}
	case stepDSN, stepStorageDSN, stepAPIToken:
		if _, err := m.resolveConnection(v); err != nil {
			return err.Error()
		}

	case stepNamespaces:
		if _, err := onboardPullNamespaces(initNamespaceInput(v, m.originalNamespaces)); err != nil {
			return err.Error()
		}
	case stepSchemaDir:
		if info, err := os.Stat(v); err == nil && !info.IsDir() {
			return "There’s a file at that path. Choose a folder for your schema files."
		} else if err != nil && !os.IsNotExist(err) {
			return "We couldn’t read that path: " + err.Error()
		}
		if _, err := validateInitSchemaReuse(v, m.fields[stepName].value, m.fields[stepEngine].value); err != nil {
			return err.Error()
		}

	}
	return ""
}
func (m *initWizard) cycleEngine(delta int) {
	index := max(slices.IndexFunc(initEngines, func(e initEngine) bool { return e.key == m.engine() }), 0)
	m.fields[stepEngine].value = initEngines[(index+delta+len(initEngines))%len(initEngines)].key
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
			if m.step == stepDSN {
				m.applicationConnected = true
			}
			generation, step := m.generation, m.step
			return m, tea.Tick(700*time.Millisecond, func(time.Time) tea.Msg {
				return initConnectionAdvanceMsg{generation: generation, step: step}
			})
		}
		return m, nil
	case initConnectionAdvanceMsg:
		if m.cancelled || msg.generation != m.generation || msg.step != m.step || !m.connectionChecked {
			return m, nil
		}
		m.fields[m.step].value = strings.TrimSpace(m.input.Value())
		return m, tea.Batch(m.advance(), textinput.Blink)
	case initNamespacesMsg:
		return m, m.acceptNamespaces(msg)
	case tea.WindowSizeMsg:
		m.height = max(8, msg.Height)
		m.width = max(20, min(72, msg.Width-4))
		m.input.Width = max(10, m.width-4)
	case tea.KeyMsg:
		if (m.step == stepDSN || m.step == stepStorageDSN && !m.choosingStorage) && (!m.checkingConnection || msg.String() == "shift+tab" || msg.String() == "esc" || msg.String() == "ctrl+c") {
			if handled, cmd := m.connectionKey(msg); handled {
				return m, cmd
			}
		}
		if m.step == stepStorageDSN && m.choosingStorage {
			switch msg.String() {
			case "up", "down", "left", "right":
				m.integrated = !m.integrated
				return m, nil
			case "enter":
				if m.integrated {
					return m, m.advance()
				}
				m.choosingStorage = false
				m.loadConnectionEditor()
				return m, textinput.Blink
			}
		}
		if m.checkingConnection && msg.String() != "esc" && msg.String() != "ctrl+c" && msg.String() != "shift+tab" {
			return m, nil
		}
		if m.step == stepNamespaces && !m.explicitNamespaces && msg.String() != "shift+tab" && msg.String() != "esc" && msg.String() != "ctrl+c" {
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
			if m.step > stepEngine {
				if m.step < len(m.fields) && (m.step != stepNamespaces || m.explicitNamespaces) {
					m.fields[m.step].value = m.input.Value()
				}
				m.step--
				for m.step > stepEngine && m.hidden(m.step) {
					m.step--
				}
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
			if m.step == stepEngine {
				if msg.String() == "up" || msg.String() == "left" {
					m.cycleEngine(-1)
				} else {
					m.cycleEngine(1)
				}
				return m, nil
			}
		case "enter":
			if m.step == len(m.fields) {
				if _, err := validateInitSchemaReuse(m.fields[stepSchemaDir].value, m.fields[stepName].value, m.fields[stepEngine].value); err != nil {
					m.step = stepSchemaDir
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
			if m.connectionStep(m.step) && !m.connectionChecked {
				return m, tea.Batch(m.checkConnection(), m.spinner.Tick)
			}
			if m.step != stepEngine {
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
	connectionReadOnly := (m.step == stepDSN || m.step == stepStorageDSN) && m.connectionEditor.mode == "ready"
	if m.step > stepEngine && m.step < len(m.fields) && !connectionReadOnly {
		var c tea.Cmd
		before := m.input.Value()
		m.input, c = m.input.Update(msg)
		if m.input.Value() != before {
			m.connectionChecked = false
			if (m.step == stepDSN || m.step == stepStorageDSN) && m.connectionEditor.mode == "reference" {
				m.connectionSummary = initConnectionSummary(m.fields[stepEngine].value, m.input.Value())
			}
			if m.step == stepAPIToken {
				m.connectionSummary = m.summary()
			}
			m.err = ""
		}
		return m, c
	}
	return m, nil
}
func (m *initWizard) contentView() string {
	if m.cancelled {
		return "\n  Setup cancelled.\n"
	}
	blue := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	bold := m.renderer.NewStyle().Bold(true)
	muted := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	wrap := m.renderer.NewStyle().Width(m.width)
	var b strings.Builder
	stage := "connect"
	if m.step == stepNamespaces {
		stage = "choose schemas"
	}
	if m.step > stepNamespaces {
		stage = "review setup"
	}
	b.WriteString(bold.Render("SchemaBot") + muted.Render("  /  "+stage) + "\n\n")
	if m.confirmed {
		return ""
	}
	if m.step < len(m.fields) {
		f := m.fields[m.step]
		if m.step == stepStorageDSN && m.choosingStorage {
			return m.renderer.NewStyle().Width(m.width + 2).PaddingLeft(2).Render(b.String() + m.storageChoiceView())
		}
		if m.step == stepEngine {
			b.WriteString(bold.Render("Let’s connect your database.") + "\n\n")
			b.WriteString(wrap.Render("Connect your database, bring its schema into your project, and get ready for your first change.") + "\n\n")
		} else if m.step != stepDSN && m.step != stepStorageDSN {
			b.WriteString(muted.Render("Let’s get your schema ready.") + "\n\n")
		}
		if m.step <= stepName && m.existingProject != "" {
			b.WriteString(wrap.Render(muted.Render(m.existingProject)) + "\n\n")
		}
		if m.step == stepDSN || m.step == stepStorageDSN {
			b.WriteString(bold.Render(f.label) + "\n\n")
			if m.isVitess() && m.step == stepStorageDSN {
				b.WriteString(wrap.Render(muted.Render(m.hint(m.step))) + "\n\n")
			}
			b.WriteString(wrap.Render(m.connectionEditorView()))
			return m.renderer.NewStyle().Width(m.width + 2).PaddingLeft(2).Render(b.String())
		}
		b.WriteString(bold.Render(f.label) + "\n" + wrap.Render(muted.Render(m.hint(m.step))) + "\n\n")
		switch {
		case m.step == stepEngine:
			for _, engine := range initEngines {
				line := "  " + engine.label
				if f.value == engine.key {
					line = blue.Render("› " + engine.label)
				}
				b.WriteString(line + "\n  " + muted.Render(engine.detail) + "\n\n")
			}
		case m.step == stepNamespaces && !m.explicitNamespaces:
			b.WriteString(m.namespaceView())
		default:
			b.WriteString(m.input.View() + "\n\n")
			if m.connectionStep(m.step) {
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
			if m.step == stepDSN || m.step == stepStorageDSN {
				b.WriteString(wrap.Render("Private network? Connect your VPN or tunnel, then retry here.") + "\n\n")
			}
		}
		help := "enter continue · shift+tab back · esc cancel"
		if m.connectionStep(m.step) && !m.connectionChecked {
			help = "enter check connection · shift+tab back · esc cancel"
		}
		if m.step == stepEngine {
			help = "↑/↓ choose · enter continue · esc cancel"
		}
		if m.step == stepNamespaces && !m.explicitNamespaces {
			help = "shift+tab back · esc cancel"
		}
		b.WriteString(muted.Render(help))
	} else {
		b.WriteString(bold.Render("Review your setup") + "\n\n")
		row := func(label, value string) {
			b.WriteString(wrap.Render(muted.Render(fmt.Sprintf("%-14s", label))+initTerminalText(value)) + "\n")
		}
		row("Database", m.fields[stepName].value+" ("+m.fields[stepEngine].value+")")
		if m.isVitess() {
			row("Organization", m.fields[stepOrganization].value)
		}
		row("Environment", m.fields[stepEnvironment].value)
		row("Namespaces", m.fields[stepNamespaces].value)
		row("Profile", m.fields[stepProfile].value)
		if m.integrated {
			row("Storage", "schemabot (new database, same server)")
		} else {
			row("Storage", initConnectionLabel(m.fields[stepStorageDSN].value))
		}
		if strings.HasPrefix(m.fields[stepDSN].value, "draft:") || !m.integrated && strings.HasPrefix(m.fields[stepStorageDSN].value, "draft:") {
			b.WriteString("\nCredentials will be saved outside your project in private, unencrypted\nfiles under ~/.schemabot/credentials.\n")
		}
		b.WriteString("\n" + wrap.Render("We’ll set up SchemaBot and verify your schema files.") + "\n\n")
		b.WriteString(blue.Render("enter finish setup") + muted.Render(" · shift+tab back · esc cancel"))
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
		content = strings.Join(lines[offset:offset+available], "\n") + "\n" + wrap.Render("  pgup/pgdown scroll · shift+tab back · esc cancel")
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
	reuse, err := validateInitSchemaReuse(m.fields[stepSchemaDir].value, m.fields[stepName].value, m.fields[stepEngine].value)
	if err != nil {
		return err
	}
	for _, i := range []int{stepDSN, stepStorageDSN} {
		if i == stepStorageDSN && m.integrated {
			continue
		}
		if dsn, ok := m.draftConnections[m.fields[i].value]; ok {
			purpose := "application"
			if i == stepStorageDSN {
				purpose = "storage"
			}
			ref, err := saveInitConnection(cmd.Runtime, m.fields[stepName].value, m.fields[stepEnvironment].value, purpose, dsn)
			if err != nil {
				return err
			}
			m.fields[i].value = ref
		}
	}
	cmd.Type = m.fields[stepEngine].value
	cmd.Database = m.fields[stepName].value
	cmd.Environment = m.fields[stepEnvironment].value
	cmd.DSN = m.fields[stepDSN].value
	if m.isVitess() {
		cmd.Organization = m.fields[stepOrganization].value
		cmd.APIToken = m.fields[stepAPIToken].value
	} else {
		cmd.Organization = ""
		cmd.APIToken = ""
		cmd.APIURL = ""
	}
	cmd.Integrated = m.integrated
	cmd.StorageDSN = m.fields[stepStorageDSN].value
	if cmd.Integrated {
		cmd.StorageDSN = ""
	}
	cmd.Namespaces = m.namespaceChoices(cmd.Namespaces)
	cmd.SchemaDir = m.fields[stepSchemaDir].value
	g.Profile = m.fields[stepProfile].value
	cmd.ReuseSchema = cmd.ReuseSchema || reuse
	return nil
}
