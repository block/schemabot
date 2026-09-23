package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/localdemo"
	"github.com/block/schemabot/pkg/ui"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// A sample is provisioned only after an explicit choice. The ordinary init
// workflow then imports and verifies it exactly like a user-owned database.
func (cmd *InitCmd) prepareSample(ctx context.Context, g *Globals) error {
	if !cmd.Sample && !cmd.NonInteractive && !cmd.JSON && ui.IsTerminal(os.Stdin) && ui.IsTerminal(os.Stdout) && cmd.DSN == "" && cmd.Database == "" && cmd.Type == "" {
		if _, err := os.Stat(cmd.SchemaDir); !os.IsNotExist(err) {
			return nil
		}
		m := &initStartChoice{}
		result, err := tea.NewProgram(m, tea.WithContext(ctx)).Run()
		if err != nil {
			return err
		}
		selected := result.(*initStartChoice)
		if !selected.confirmed {
			return ErrSilent
		}
		if selected.choice == 0 {
			return nil
		}
		cmd.Sample = true
		cmd.Type = selected.engine()
	}
	if !cmd.Sample {
		return nil
	}
	if cmd.DSN != "" || cmd.StorageDSN != "" || cmd.Integrated || cmd.Database != "" || len(cmd.Namespaces) > 0 {
		return fmt.Errorf("--sample creates its own database and connections; do not combine it with database, connection, storage, or namespace flags")
	}
	if g.Endpoint != "" || g.Token != "" || os.Getenv("SCHEMABOT_ENDPOINT") != "" || os.Getenv("SCHEMABOT_TOKEN") != "" {
		return fmt.Errorf("sample setup uses a local runtime; remove endpoint and authentication overrides")
	}
	if cmd.Type == "" {
		cmd.Type = "mysql"
	}
	if cmd.Type != "mysql" && cmd.Type != "postgres" {
		return fmt.Errorf("choose --type mysql or --type postgres for a sample database")
	}
	if cmd.Environment != "" && cmd.Environment != "development" {
		return fmt.Errorf("sample databases use the development environment")
	}
	project, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err = validateInitSchemaReuse(cmd.SchemaDir, "shop", cmd.Type); err != nil {
		return err
	}
	if !cmd.JSON {
		fmt.Println("\n  Starting your sample database. The first run may download a Docker image.")
	}
	setupCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	sample, err := localdemo.Ensure(setupCtx, project, cmd.Type)
	if err != nil {
		return err
	}
	// Each project gets its own execution authority, independent of the default runtime.
	cmd.Runtime = sample.Name
	if g.Profile == "" {
		g.Profile = sample.Name
	}
	cmd.Database = "shop"
	cmd.Environment = "development"
	cmd.Namespaces = []string{sample.Namespace}
	cmd.DSN, err = saveInitConnection(cmd.Runtime, cmd.Database, cmd.Environment, "application", sample.DSN)
	if err != nil {
		return err
	}
	cmd.StorageDSN, err = saveInitConnection(cmd.Runtime, cmd.Database, cmd.Environment, "storage", sample.StorageDSN)
	if err != nil {
		return err
	}
	if !cmd.JSON {
		heading := lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#1A7F37", Dark: "#3FB950"}).Render("✓ Sample database connected")
		fmt.Printf("\n  %s\n  %s · Docker container %s\n  Your schema files will be saved in %s.\n\n", heading, cmd.Type, sample.Name, filepath.Join(project, cmd.SchemaDir))
	}
	return nil
}

type initStartChoice struct {
	choice    int
	confirmed bool
}

func (m *initStartChoice) engine() string {
	if m.choice == 2 {
		return "postgres"
	}
	return "mysql"
}
func (m *initStartChoice) Init() tea.Cmd { return nil }
func (m *initStartChoice) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "up", "k":
			m.choice = (m.choice + 2) % 3
		case "down", "j":
			m.choice = (m.choice + 1) % 3
		case "enter":
			m.confirmed = true
			return m, tea.Quit
		case "esc", "ctrl+c":
			return m, tea.Quit
		}
	}
	return m, nil
}
func (m *initStartChoice) View() string {
	if m.confirmed {
		return ""
	}
	blue := lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	muted := lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	var b strings.Builder
	b.WriteString("\n  " + lipgloss.NewStyle().Bold(true).Render("What would you like to try?") + "\n\n")
	for i, choice := range []struct{ label, hint string }{{"Connect my database", "Bring an existing database into your project."}, {"Try a sample MySQL database", "A local Docker database with a few tables to explore."}, {"Try a sample PostgreSQL database", "The same quick start, with PostgreSQL."}} {
		label := "  " + choice.label
		if i == m.choice {
			label = blue.Render("› " + choice.label)
		}
		b.WriteString("  " + label + "\n    " + muted.Render(choice.hint) + "\n\n")
	}
	b.WriteString("  " + muted.Render("↑/↓ choose · enter continue · esc cancel") + "\n")
	return b.String()
}
