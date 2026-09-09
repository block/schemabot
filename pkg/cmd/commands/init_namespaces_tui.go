package commands

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/block/schemabot/pkg/localsetup"
	tea "github.com/charmbracelet/bubbletea"
)

type initNamespacesMsg struct {
	generation int
	names      []string
	err        error
}

func (m *initWizard) discoverNamespaces() tea.Cmd {
	m.notice = ""
	m.discovering = true
	m.err = ""
	m.generation++
	generation := m.generation
	engine, ref, tokenRef := m.engine(), m.fields[stepDSN].value, m.fields[stepAPIToken].value
	target := m.planetScaleTarget("")
	ctx := m.ctx
	if ctx == nil {
		ctx = context.TODO()
	}
	if m.cancelDiscovery != nil {
		m.cancelDiscovery()
	}
	ctx, cancel := context.WithCancel(ctx)
	m.cancelDiscovery = cancel
	return func() tea.Msg {
		defer cancel()
		dsn := os.Getenv(strings.TrimPrefix(ref, "env:"))
		if !initVariable.MatchString(ref) || strings.TrimSpace(dsn) == "" {
			return initNamespacesMsg{generation: generation, err: fmt.Errorf("choose an env:VARIABLE connection that you’ve already set")}
		}
		if engine != "vitess" {
			target = localsetup.Target{Engine: engine}
		} else {
			target.Token = os.Getenv(strings.TrimPrefix(tokenRef, "env:"))
			if !initVariable.MatchString(tokenRef) || strings.TrimSpace(target.Token) == "" {
				return initNamespacesMsg{generation: generation, err: fmt.Errorf("choose an env:VARIABLE PlanetScale token that you’ve already set")}
			}
		}
		target.DSN = dsn
		names, err := m.discover(ctx, target)
		return initNamespacesMsg{generation, names, err}
	}
}
func (m *initWizard) acceptNamespaces(msg initNamespacesMsg) tea.Cmd {
	if msg.generation != m.generation || m.step != stepNamespaces {
		return nil
	}
	m.discovering = false
	if msg.err != nil {
		m.err = msg.err.Error()
		return nil
	}
	m.names = msg.names
	m.selected = map[string]bool{}
	m.cursor = 0
	if len(m.names) == 0 {
		m.err = "We didn’t find any application namespaces. Check the connection and permissions, then try again."
		return nil
	}
	if len(m.names) == 1 {
		if _, err := onboardPullNamespaces(m.names); err != nil {
			m.err = err.Error()
			m.names = nil
			return nil
		}
		m.fields[stepNamespaces].value = m.names[0]
		m.selected[m.names[0]] = true
		m.notice = fmt.Sprintf("Found %s. We’ll use that.", initTerminalText(m.names[0]))
		return m.advance()
	}
	m.fields[stepNamespaces].value = ""
	return nil
}
func (m *initWizard) filteredNamespaces() []string {
	var names []string
	for _, name := range m.names {
		if strings.Contains(strings.ToLower(name), strings.ToLower(m.input.Value())) {
			names = append(names, name)
		}
	}
	return names
}
func (m *initWizard) namespaceKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.discovering {
		return m, nil
	}
	if len(m.names) == 0 {
		if msg.String() == "m" {
			m.explicitNamespaces = true
			m.fields[stepNamespaces].value = ""
			m.fields[stepNamespaces].hint = "Enter the namespaces you want to manage, separated by commas."
			m.err = ""
			m.loadField()
			return m, nil
		}
		if msg.String() == "enter" {
			return m, tea.Batch(m.discoverNamespaces(), m.spinner.Tick)
		}
		return m, nil
	}
	names := m.filteredNamespaces()
	switch msg.String() {
	case "up":
		m.cursor = max(0, m.cursor-1)
	case "down":
		m.cursor = min(max(0, len(names)-1), m.cursor+1)
	case " ":
		if m.cursor < len(names) {
			name := names[m.cursor]
			m.selected[name] = !m.selected[name]
			m.err = ""
		}
	case "enter":
		var chosen []string
		for _, name := range m.names {
			if m.selected[name] {
				chosen = append(chosen, name)
			}
		}
		if len(chosen) == 0 {
			m.err = "Choose at least one namespace and we can keep going."
			return m, nil
		}
		if _, err := onboardPullNamespaces(chosen); err != nil {
			m.err = err.Error()
			return m, nil
		}
		m.fields[stepNamespaces].value = strings.Join(chosen, ", ")
		return m, m.advance()
	default:
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		m.cursor = 0
		return m, cmd
	}
	return m, nil
}
func (m *initWizard) namespaceView() string {
	if m.discovering {
		return m.spinner.View() + " Connecting and finding your namespaces…\n\nYou can cancel while we connect.\n"
	}
	if len(m.names) == 0 {
		return "enter try again · m enter namespaces manually · shift+tab back\n"
	}
	var b strings.Builder
	b.WriteString(m.input.View() + "\n\n")
	names := m.filteredNamespaces()
	if len(names) == 0 {
		b.WriteString("No matches. Try a different search.\n")
	}
	visible := max(1, m.height-18)
	start := max(0, m.cursor-visible+1)
	for i := start; i < min(len(names), start+visible); i++ {
		cursor := " "
		if i == m.cursor {
			cursor = "›"
		}
		check := " "
		if m.selected[names[i]] {
			check = "✓"
		}
		line := fmt.Sprintf("%s [%s] %s", cursor, check, initTerminalText(names[i]))
		if i == m.cursor {
			line = m.spinner.Style.Bold(true).Render(line)
		}
		b.WriteString(line + "\n")
	}
	if len(names) > visible {
		fmt.Fprintf(&b, "\n%d matches · use ↑/↓ to browse\n", len(names))
	}
	b.WriteString("\nspace select · enter continue · type to search\n")
	return b.String()
}
func (m *initWizard) advance() tea.Cmd {
	m.step++
	for m.step < len(m.fields) && ((!m.editing && m.skip[m.step]) || m.hidden(m.step)) {
		m.step++
	}
	m.loadField()
	if m.step == stepNamespaces && !m.explicitNamespaces {
		m.input.SetValue("")
		m.input.Placeholder = "Search namespaces"
		m.names = nil
		return tea.Batch(m.discoverNamespaces(), m.spinner.Tick)
	}
	return nil
}

// Defaults are visible and editable in the final review. Explicit namespace
// flags bypass discovery; their scope is still verified by the shared baseline.
func configureInitWizard(m *initWizard, cmd *InitCmd) {
	m.discover = localsetup.DiscoverNamespaces
	m.checkAPI = localsetup.CheckPlanetScale
	m.apiURL = cmd.APIURL
	m.skip = make([]bool, len(m.fields))
	for _, i := range []int{stepEnvironment, stepSchemaDir, stepProfile} {
		m.skip[i] = true
	}
	for i, v := range map[int]string{stepEngine: cmd.Type, stepName: cmd.Database, stepOrganization: cmd.Organization} {
		m.skip[i] = v != ""
	}
	m.explicitNamespaces = len(cmd.Namespaces) > 0
	if !m.explicitNamespaces {
		m.fields[stepNamespaces].hint = "We’ll find the namespaces available through your connection."
	}
	m.skip[stepNamespaces] = m.explicitNamespaces
	// Connection steps always remain visible, even when flags or environment
	// variables supplied them. Users confirm the target before discovery.
	m.selected = map[string]bool{}
	for m.step < len(m.fields) && (m.skip[m.step] || m.hidden(m.step)) {
		m.step++
	}
}

func (m *initWizard) namespaceChoices(original []string) []string {
	if m.explicitNamespaces {
		return initNamespaceInput(m.fields[stepNamespaces].value, original)
	}
	var chosen []string
	for _, name := range m.names {
		if m.selected[name] {
			chosen = append(chosen, name)
		}
	}
	return chosen
}
