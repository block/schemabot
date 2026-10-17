package commands

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type initPScaleWizard struct {
	client         initPScale
	phase          string
	cursor         int
	choices        []string
	org, database  string
	busy, complete bool
}
type initPScaleMsg struct {
	phase       string
	names       []string
	credentials initPScaleCredentials
	err         error
}

func (m *initWizard) pscaleContext() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
func (m *initWizard) pscaleList(phase string) tea.Cmd {
	m.pscale.busy = true
	m.pscale.phase = phase
	m.err = ""
	client, ctx, org := m.pscale.client, m.pscaleContext(), m.pscale.org
	return tea.Batch(m.spinner.Tick, func() tea.Msg {
		var names []string
		var err error
		if phase == "organizations" {
			names, err = client.organizations(ctx)
		} else {
			names, err = client.databases(ctx, org)
		}
		return initPScaleMsg{phase: phase, names: names, err: err}
	})
}
func (m *initWizard) updatePScale(msg tea.Msg) (bool, tea.Cmd) {
	if m.step != stepPlanetScale {
		return false, nil
	}
	p := &m.pscale
	switch msg := msg.(type) {
	case initPScaleMsg:
		p.busy = false
		if msg.err != nil {
			m.err = msg.err.Error()
			return true, nil
		}
		if msg.phase == "login" {
			return true, m.pscaleList("organizations")
		}
		if msg.phase == "create" {
			p.complete = true
			m.fields[stepName].value = p.database
			m.fields[stepOrganization].value = p.org
			m.fields[stepDSN].value = msg.credentials.dsn
			m.fields[stepAPIToken].value = msg.credentials.token
			m.applicationConnected = false
			return true, m.advance()
		}
		p.choices = msg.names
		p.cursor = 0
		return true, nil
	case tea.KeyMsg:
		key := msg.String()
		if p.busy {
			if p.phase != "create" && (key == "esc" || key == "ctrl+c") {
				return false, nil
			}
			// Finish provisioning or its bounded cleanup before leaving the screen.
			// Killing a create halfway through would leave an unknown remote outcome.
			return true, nil
		}
		if key == "esc" || key == "ctrl+c" {
			return false, nil
		}
		if key == "shift+tab" {
			m.err = ""
			switch p.phase {
			case "":
				return false, nil
			case "organizations", "login":
				p.phase = ""
				p.cursor = 0
			case "databases":
				return true, m.pscaleList("organizations")
			case "review":
				return true, m.pscaleList("databases")
			case "create":
				p.phase = "review"
			}
			return true, nil
		}
		if key == "up" || key == "down" {
			count := len(p.choices)
			if p.phase == "" {
				count = 2
			}
			if count > 0 {
				delta := 1
				if key == "up" {
					delta = -1
				}
				p.cursor = (p.cursor + delta + count) % count
			}
			return true, nil
		}
		if key != "enter" {
			return true, nil
		}
		if m.err != "" {
			if p.phase == "organizations" || p.phase == "databases" {
				return true, m.pscaleList(p.phase)
			}
			if p.phase == "create" {
				return true, nil
			} // Do not blindly repeat a possibly successful create.
		}
		switch p.phase {
		case "":
			if p.cursor == 1 {
				p.complete = false
				return true, m.advance()
			}
			if m.apiURL != "" {
				m.err = "Use manual setup for a custom PlanetScale API address."
				return true, nil
			}
			if _, err := exec.LookPath("pscale"); err != nil {
				m.err = "Install the PlanetScale CLI, then retry, or choose manual setup.\nhttps://planetscale.com/docs/cli/planetscale-environment-setup"
				return true, nil
			}
			p.phase = "login"
			p.busy = true
			ctx, cancel := context.WithTimeout(m.pscaleContext(), 5*time.Minute)
			cmd := initPScaleCommand(ctx, "auth", "login")
			return true, tea.ExecProcess(cmd, func(err error) tea.Msg {
				cancel()
				if err != nil {
					err = fmt.Errorf("PlanetScale sign-in did not finish; press Enter to try again or Shift+Tab for manual setup")
				}
				return initPScaleMsg{phase: "login", err: err}
			})
		case "login":
			p.phase = ""
			return m.updatePScale(msg)
		case "organizations":
			if len(p.choices) == 0 {
				return true, m.pscaleList("organizations")
			}
			p.org = p.choices[p.cursor]
			return true, m.pscaleList("databases")
		case "databases":
			if len(p.choices) == 0 {
				return true, m.pscaleList("databases")
			}
			p.database = p.choices[p.cursor]
			p.phase = "review"
			p.cursor = 0
		case "review":
			p.phase = "create"
			p.busy = true
			m.err = ""
			client, ctx, org, database := p.client, m.pscaleContext(), p.org, p.database
			return true, tea.Batch(m.spinner.Tick, func() tea.Msg {
				credentials, err := client.provision(ctx, org, database)
				return initPScaleMsg{phase: "create", credentials: credentials, err: err}
			})
		}
		return true, nil
	}
	if _, ok := msg.(tea.WindowSizeMsg); ok {
		return false, nil
	}
	if p.busy {
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return true, cmd
	}
	return false, nil
}

func (m *initWizard) pscaleView() string {
	p := &m.pscale
	bold := m.renderer.NewStyle().Bold(true)
	blue := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	muted := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	var b strings.Builder
	title := "Connect to PlanetScale"
	switch p.phase {
	case "organizations":
		title = "Choose your organization"
	case "databases":
		title = "Choose your Vitess database"
	case "review", "create":
		title = "Set up access for SchemaBot"
	}
	b.WriteString(bold.Render(title) + "\n\n")
	if p.busy {
		status := "Loading your PlanetScale account…"
		if p.phase == "create" {
			status = "Creating credentials and saving them privately…"
		}
		b.WriteString(blue.Render(m.spinner.View()+" "+status) + "\n")
		return b.String()
	}
	switch p.phase {
	case "":
		options := []string{"Set up with PlanetScale CLI", "Enter credentials manually"}
		details := []string{"Sign in, choose your database, and let us set up access.", "Use an existing database password and service token."}
		for i, label := range options {
			prefix := "  "
			if i == p.cursor {
				prefix = "› "
				label = blue.Render(label)
			}
			b.WriteString(prefix + label + "\n  " + muted.Render(details[i]) + "\n\n")
		}
	case "organizations", "databases":
		start := max(0, p.cursor-4)
		end := min(len(p.choices), start+8)
		for i := start; i < end; i++ {
			prefix := "  "
			label := p.choices[i]
			if i == p.cursor {
				prefix = "› "
				label = blue.Render(label)
			}
			b.WriteString(prefix + label + "\n")
		}
	case "review", "create":
		b.WriteString(blue.Render(p.org+" / "+p.database) + "\n\n")
		b.WriteString("Create a read-only password for main and a service token\nscoped to this database:\n\n")
		b.WriteString(muted.Render("  read_branch · create_branch · connect_branch · delete_branch\n  read_deploy_request · create_deploy_request\n  write_branch_vschema") + "\n\n")
		b.WriteString("Save credentials in private, unencrypted files under\n~/.schemabot/credentials. They remain if you cancel later.\n")
	}
	if m.err != "" {
		b.WriteString("\n" + m.err + "\n")
	}
	controls := "enter continue · shift+tab back · esc cancel"
	if p.phase == "review" {
		controls = "enter create credentials · shift+tab back · esc cancel"
	}
	if p.phase == "create" {
		controls = "shift+tab review · esc cancel"
	}
	b.WriteString("\n" + muted.Render(controls) + "\n")
	return b.String()
}
