package commands

import (
	"context"
	"fmt"
	"os"
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
		selected, err := chooseInitStart(ctx)
		if err != nil {
			return err
		}
		if !selected.sample {
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
	if cmd.Type != "mysql" && cmd.Type != "postgres" && cmd.Type != "vitess" {
		return fmt.Errorf("choose --type mysql, --type postgres, or --type vitess for a sample database")
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
	// A Vitess sample brings up an embedded cluster rather than one database
	// server, so it is given longer to come up.
	setupTimeout := 5 * time.Minute
	if cmd.Type == "vitess" {
		setupTimeout = 10 * time.Minute
	}
	setupCtx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	var sample localdemo.Database
	provision := func(runCtx context.Context, report func(string)) (*initResult, error) {
		var provisionErr error
		sample, provisionErr = localdemo.Ensure(runCtx, project, cmd.Type, g.Version, report)
		return nil, provisionErr
	}
	if !cmd.NonInteractive && !cmd.JSON && ui.IsTerminal(os.Stdin) && ui.IsTerminal(os.Stdout) {
		_, err = runInitProgress(setupCtx, "sample database", provision, tea.WithInput(os.Stdin), tea.WithOutput(os.Stdout))
	} else {
		_, err = provision(setupCtx, func(stage string) {
			if !cmd.JSON {
				fmt.Println("  " + stage)
			}
		})
	}
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
	// A Vitess target is named by its PlanetScale-compatible endpoint; its DSN
	// is the keyspace's vtgate endpoint, saved separately below.
	target := sample.DSN
	if cmd.Type == "vitess" {
		target = sample.APIURL
	}
	cmd.DSN, err = saveInitConnection(cmd.Runtime, cmd.Database, cmd.Environment, "application", target)
	if err != nil {
		return err
	}
	cmd.StorageDSN, err = saveInitConnection(cmd.Runtime, cmd.Database, cmd.Environment, "storage", sample.StorageDSN)
	if err != nil {
		return err
	}
	if cmd.Type == "vitess" {
		cmd.organization = sample.Organization
		cmd.tokenRef, err = saveInitConnection(cmd.Runtime, cmd.Database, cmd.Environment, "token", sample.Token)
		if err != nil {
			return err
		}
		cmd.vtgateDSN, err = saveInitConnection(cmd.Runtime, cmd.Database, cmd.Environment, "vtgate", sample.DSN)
		if err != nil {
			return err
		}
	}
	if !cmd.JSON {
		heading := lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#1A7F37", Dark: "#3FB950"}).Render("✓ Sample database connected")
		detail := "Docker container: " + sample.Name
		if cmd.Type == "vitess" {
			detail = fmt.Sprintf("Docker container: %s\n  Keyspace %s is split across %d shards", sample.Name, sample.Namespace, localdemo.SampleShards)
		}
		fmt.Printf("\n  %s\n  %s\n\n", heading, detail)
	}
	return nil
}

// chooseInitStart asks whether to connect a database or try a sample, and
// returns the confirmed choice. A choice the operator left without confirming,
// by their signal or by the screen's cancel key, is a stopped init: nothing
// has been acted on yet.
func chooseInitStart(ctx context.Context, options ...tea.ProgramOption) (*initStartChoice, error) {
	m := &initStartChoice{}
	_, err := tea.NewProgram(m, append(options, tea.WithContext(ctx))...).Run()
	// The program reports how it ended in its own terms, so the context's own
	// error is the one that says whether the operator's signal ended it.
	if endedByInterrupt(ctx, ctx.Err()) {
		return nil, initInterrupted(ctx, initCancelledNothingChanged)
	}
	if err != nil {
		return nil, err
	}
	if !m.confirmed {
		return nil, initCancelled(initCancelledNothingChanged)
	}
	return m, nil
}

type initStartChoice struct {
	choice         int
	sample         bool
	choosingEngine bool
	confirmed      bool
}

// sampleEngines are the engines the second screen offers, in the order it
// lists them. engine() reads the confirmed choice back out by the same index.
var sampleEngines = []struct{ name, label, hint string }{
	{"mysql", "MySQL", "Schema changes powered by Spirit."},
	{"postgres", "PostgreSQL", "Schema changes powered by pg-sprite."},
	{"vitess", "Vitess", "A sharded cluster, powered by PlanetScale deploy requests."},
}

func (m *initStartChoice) engine() string {
	if m.choice < 0 || m.choice >= len(sampleEngines) {
		return sampleEngines[0].name
	}
	return sampleEngines[m.choice].name
}
func (m *initStartChoice) Init() tea.Cmd { return nil }

// choices is how many options the current screen offers, so the keys wrap
// around the list in front of the operator rather than a fixed count.
func (m *initStartChoice) choices() int {
	if m.choosingEngine {
		return len(sampleEngines)
	}
	return 2
}
func (m *initStartChoice) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "up", "k":
			m.choice = (m.choice + m.choices() - 1) % m.choices()
		case "down", "j":
			m.choice = (m.choice + 1) % m.choices()
		case "shift+tab":
			if m.choosingEngine {
				m.choosingEngine = false
				m.choice = 1
			}
		case "enter":
			if !m.choosingEngine && m.choice == 1 {
				m.sample = true
				m.choosingEngine = true
				m.choice = 0
				return m, nil
			}
			m.sample = m.choosingEngine
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
	title := "What would you like to try?"
	choices := []struct{ label, hint string }{{"Connect my database", "Bring an existing database into your project."}, {"Try a sample database", "A local Docker database with a few tables to explore."}}
	if m.choosingEngine {
		title = "Choose your sample database"
		choices = nil
		for _, e := range sampleEngines {
			choices = append(choices, struct{ label, hint string }{e.label, e.hint})
		}
	}
	b.WriteString("\n  " + lipgloss.NewStyle().Bold(true).Render(title) + "\n\n")
	for i, choice := range choices {
		label := "  " + choice.label
		if i == m.choice {
			label = blue.Render("› " + choice.label)
		}
		b.WriteString("  " + label + "\n    " + muted.Render(choice.hint) + "\n\n")
	}
	controls := "↑/↓ choose · enter continue"
	if m.choosingEngine {
		controls += " · shift+tab back"
	}
	b.WriteString("  " + muted.Render(controls+" · esc cancel") + "\n")
	return b.String()
}
