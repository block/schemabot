package commands

import (
	"context"
	"fmt"
	"os"
	"slices"
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
	engine, ref := m.fields[0].value, m.fields[3].value
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
		names, err := m.discover(ctx, engine, dsn)
		return initNamespacesMsg{generation, names, err}
	}
}
func (m *initWizard) acceptNamespaces(msg initNamespacesMsg) tea.Cmd {
	if msg.generation != m.generation || m.step != 5 {
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
		m.fields[5].value = m.names[0]
		m.selected[m.names[0]] = true
		m.notice = fmt.Sprintf("Found %s. We’ll use that.", m.names[0])
		return m.advance()
	}
	m.fields[5].value = ""
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
		m.fields[5].value = strings.Join(chosen, ", ")
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
		return "enter try again · shift+tab edit connection · esc cancel\n"
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
		line := fmt.Sprintf("%s [%s] %s", cursor, check, names[i])
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
	for m.step < len(m.fields) && !m.editing && m.skip[m.step] {
		m.step++
	}
	m.loadField()
	if m.step == 5 && !m.explicitNamespaces {
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
	m.skip = make([]bool, len(m.fields))
	for _, i := range []int{2, 6, 7} {
		m.skip[i] = true
	}
	for i, v := range map[int]string{0: cmd.Type, 1: cmd.Database, 3: cmd.DSN, 4: cmd.StorageDSN} {
		m.skip[i] = v != ""
	}
	m.explicitNamespaces = len(cmd.Namespaces) > 0
	if !m.explicitNamespaces {
		m.fields[5].hint = "We’ll find the namespaces available through your connection."
	}
	m.skip[5] = m.explicitNamespaces
	// Both conventional connection variables can be accepted without another
	// prompt when already set. Their references remain visible in the review.
	for _, i := range []int{3, 4} {
		if os.Getenv(strings.TrimPrefix(m.fields[i].value, "env:")) != "" {
			m.skip[i] = true
		}
	}
	m.selected = map[string]bool{}
	if slices.Contains(m.skip, false) {
		for m.skip[m.step] {
			m.step++
		}
	}
}

func (m *initWizard) namespaceChoices(original []string) []string {
	if m.explicitNamespaces {
		if m.fields[5].value == strings.Join(original, ", ") {
			return slices.Clone(original)
		}
		names := strings.Split(m.fields[5].value, ",")
		for i := range names {
			names[i] = strings.TrimSpace(names[i])
		}
		return names
	}
	var chosen []string
	for _, name := range m.names {
		if m.selected[name] {
			chosen = append(chosen, name)
		}
	}
	return chosen
}
