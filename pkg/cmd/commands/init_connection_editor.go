package commands

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/block/mysql"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type initConnectionEditor struct {
	mode     string
	choice   int
	detected bool
	detail   int
	values   [5]string
}

func (m *initWizard) resolveConnection(ref string) (string, error) {
	if value, ok := m.draftConnections[ref]; ok {
		return value, nil
	}
	return resolveInitConnection(ref)
}
func (m *initWizard) loadConnectionEditor() {
	_, err := m.resolveConnection(m.fields[m.step].value)
	m.connectionEditor = initConnectionEditor{mode: "menu", detected: err == nil}
	m.input.EchoMode = textinput.EchoNormal
}
func (m *initWizard) connectionOptions() []string {
	options := []string{"Paste a connection string", "Enter connection details", "Use an environment variable or file"}
	if m.connectionEditor.detected {
		options = append([]string{"Use this connection"}, options...)
	}
	return options
}
func (m *initWizard) useDraftConnection(dsn string) tea.Cmd {
	ref := fmt.Sprintf("draft:%d", m.step)
	if m.draftConnections == nil {
		m.draftConnections = map[string]string{}
	}
	m.draftConnections[ref] = dsn
	m.input.SetValue(ref)
	m.input.EchoMode = textinput.EchoNormal
	m.connectionSummary = initConnectionDestination(m.fields[stepEngine].value, dsn)
	m.connectionEditor.mode = "ready"
	return tea.Batch(m.checkConnection(), m.spinner.Tick)
}
func (m *initWizard) loadConnectionDetail() {
	e := &m.connectionEditor
	m.input.EchoMode = textinput.EchoNormal
	if e.detail == 4 {
		m.input.EchoMode = textinput.EchoPassword
	}
	m.input.SetValue(e.values[e.detail])
	m.input.CursorEnd()
}

// Returns handled=false for the existing connection check/continue and global
// cancellation paths. Draft credentials never enter field labels or review text.
func (m *initWizard) connectionKey(msg tea.KeyMsg) (bool, tea.Cmd) {
	e := &m.connectionEditor
	key := msg.String()
	if key == "esc" || key == "ctrl+c" {
		return false, nil
	}
	if key == "shift+tab" && e.mode != "menu" {
		if m.cancelDiscovery != nil {
			m.cancelDiscovery()
		}
		m.generation++
		m.checkingConnection = false
		m.connectionChecked = false
		m.err = ""
		if e.mode == "details" && e.detail > 0 {
			e.values[e.detail] = m.input.Value()
			e.detail--
			m.loadConnectionDetail()
		} else {
			m.loadConnectionEditor()
			m.input.SetValue(m.fields[m.step].value)
		}
		return true, nil
	}
	if e.mode == "menu" {
		switch key {
		case "up", "left":
			e.choice = (e.choice + len(m.connectionOptions()) - 1) % len(m.connectionOptions())
			return true, nil
		case "down", "right":
			e.choice = (e.choice + 1) % len(m.connectionOptions())
			return true, nil
		case "enter":
			choice := e.choice
			if e.detected {
				if choice == 0 {
					dsn, err := m.resolveConnection(m.fields[m.step].value)
					if err != nil {
						m.err = err.Error()
						return true, nil
					}
					m.input.SetValue(m.fields[m.step].value)
					m.connectionSummary = initConnectionDestination(m.fields[stepEngine].value, dsn)
					e.mode = "ready"
					return true, tea.Batch(m.checkConnection(), m.spinner.Tick)
				}
				choice--
			}
			m.err = ""
			m.input.EchoMode = textinput.EchoNormal
			switch choice {
			case 0:
				e.mode = "paste"
				m.input.SetValue("")
				m.input.EchoMode = textinput.EchoPassword
			case 1:
				e.mode = "details"
				e.detail = 0
				e.values = [5]string{"localhost", "3306", "", "", ""}
				if m.fields[stepEngine].value == "postgres" {
					e.values[1] = "5432"
				}
				m.loadConnectionDetail()
			case 2:
				e.mode = "reference"
				m.input.SetValue(m.fields[m.step].value)
				if strings.HasPrefix(m.input.Value(), "draft:") {
					m.input.SetValue("env:DATABASE_URL")
				}
				m.input.CursorEnd()
			}
			return true, textinput.Blink
		}
		return key != "shift+tab" && key != "pgup" && key != "pgdown", nil
	}
	if key != "enter" || e.mode == "reference" || e.mode == "ready" {
		return false, nil
	}
	if e.mode == "paste" {
		value, err := normalizeInitConnection(m.fields[stepEngine].value, strings.TrimSpace(m.input.Value()))
		if err != nil {
			m.err = err.Error()
			return true, nil
		}
		if value == "" {
			m.err = "Paste your connection string to continue."
			return true, nil
		}
		return true, m.useDraftConnection(value)
	}
	if e.mode == "details" {
		value := m.input.Value()
		if e.detail != 4 {
			value = strings.TrimSpace(value)
			if value == "" {
				m.err = "Fill this in and we can keep going."
				return true, nil
			}
		}
		e.values[e.detail] = value
		m.err = ""
		if e.detail < 4 {
			e.detail++
			m.loadConnectionDetail()
			return true, textinput.Blink
		}
		var dsn string
		if m.fields[stepEngine].value != "postgres" {
			cfg := mysql.NewConfig()
			cfg.Net = "tcp"
			cfg.Addr = net.JoinHostPort(e.values[0], e.values[1])
			cfg.DBName = e.values[2]
			cfg.User = e.values[3]
			cfg.Passwd = e.values[4]
			dsn = cfg.FormatDSN()
		} else {
			u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(e.values[0], e.values[1]), Path: "/" + e.values[2], User: url.UserPassword(e.values[3], e.values[4])}
			dsn = u.String()
		}
		return true, m.useDraftConnection(dsn)
	}
	return false, nil
}

func (m *initWizard) connectionEditorView() string {
	e := m.connectionEditor
	muted := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	accent := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"}).Bold(true)
	success := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#1A7F37", Dark: "#7EE787"})
	failure := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#CF222E", Dark: "#FF7B72"})
	var b strings.Builder
	switch e.mode {
	case "menu":
		if e.detected {
			ref := m.fields[m.step].value
			if strings.HasPrefix(ref, "draft:") {
				b.WriteString("Your entered connection\n")
			} else {
				b.WriteString("Found a connection in " + initTerminalText(ref) + ".\n")
			}
			dsn, err := m.resolveConnection(ref)
			if err == nil {
				b.WriteString(initConnectionDestination(m.fields[stepEngine].value, dsn) + "\n")
			}
			b.WriteString("\n")
		}
		for i, option := range m.connectionOptions() {
			prefix := "  "
			if i == e.choice {
				prefix = "› "
			}
			line := prefix + option
			if i == e.choice {
				line = accent.Render(line)
			}
			b.WriteString(line + "\n")
		}
		b.WriteString("\n" + muted.Render("↑/↓ choose · enter continue · shift+tab back · esc cancel"))
	case "paste":
		b.WriteString("Paste your connection string. Input is hidden.\n\n" + m.input.View())
	case "details":
		labels := []string{"Host", "Port", "Database", "Username", "Password (hidden; Enter for none)"}
		b.WriteString(labels[e.detail] + "\n\n" + m.input.View())
	case "reference":
		b.WriteString("Use env:VARIABLE or file:/absolute/path.\nThe file should contain only the connection string.\n\n" + m.input.View())
	case "ready":
		b.WriteString(m.connectionSummary)
	}
	if e.mode == "menu" {
		return b.String()
	}
	help := "enter continue · shift+tab back · esc cancel"
	if e.mode == "ready" || e.mode == "reference" {
		switch {
		case m.checkingConnection:
			b.WriteString("\n\n" + m.spinner.View() + " Checking connection…")
			help = "shift+tab back · esc cancel"
		case m.err != "":
			b.WriteString("\n\n" + failure.Render(initConnectionFailure(m.err)))
			help = "enter retry · shift+tab back · esc cancel"
		case m.connectionChecked:
			b.WriteString("\n\n" + success.Render("✓ Connected"))
			help = "shift+tab back · esc cancel"
		default:
			help = "enter check connection · shift+tab back · esc cancel"
		}
	} else if m.err != "" {
		b.WriteString("\n\n" + failure.Render(m.err))
	}
	b.WriteString("\n\n" + muted.Render(help))
	return b.String()
}

func initConnectionLabel(ref string) string {
	if strings.HasPrefix(ref, "draft:") {
		return "Local credential file"
	}
	return initTerminalText(ref)
}

// Keep the immediate recovery action visible without repeating driver details.
func initConnectionFailure(message string) string {
	switch {
	case strings.Contains(message, "connection refused"):
		return "Couldn’t connect. Check that the database is running and the host and port are correct."
	case strings.Contains(message, "hostname could not be resolved"):
		return "Couldn’t find that host. Check the address and your network connection."
	case strings.Contains(message, "timed out"):
		return "The connection timed out. Check the address and network access, then retry."
	default:
		return message
	}
}
