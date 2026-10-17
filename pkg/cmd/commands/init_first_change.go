package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/schema"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const initExampleDDL = "CREATE TABLE `schemabot_example` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `message` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"

// A first change is a separate, opt-in apply after initialization has succeeded.
// Restrict the starter to one empty, unsharded keyspace: choosing routing for a
// sharded database belongs to its owner, not an onboarding example.
func (cmd *InitCmd) offerFirstChange(ctx context.Context, g *Globals, result *initResult) error {
	if !cmd.interactive || cmd.JSON || cmd.NonInteractive || cmd.Type != "vitess" || !result.Verified || result.Tables != 0 || cmd.ReuseSchema {
		return nil
	}
	snapshot, err := initSchemaSnapshot(result.SchemaDir)
	if err != nil {
		return err
	}
	namespace := initExampleNamespace(snapshot)
	if namespace == "" {
		return nil
	}
	model, err := tea.NewProgram(&initFirstChangeMenu{}, tea.WithContext(ctx)).Run()
	if err != nil {
		return err
	}
	if !model.(*initFirstChangeMenu).accepted {
		return nil
	}
	// Pin the newly registered profile even when another one is the default.
	route := *g
	route.Profile = result.Profile
	route.Endpoint = ""
	ep, err := route.Resolve()
	if err != nil {
		return err
	}
	path, err := prepareInitFirstChange(ctx, ep, result, namespace, snapshot)
	if err != nil {
		return fmt.Errorf("setup is complete, but the example was not prepared: %w", err)
	}
	fmt.Printf("\n  Saved %s\n\n  Review the plan below. Approving it creates the example table\n  through a PlanetScale deploy request. The file stays here if you cancel.\n\n", path)
	apply := ApplyCmd{
		SchemaDir: result.SchemaDir, Environment: result.Environment,
		Watch: true, Yield: true, Output: OutputFormatInteractive, LogHeartbeat: 10 * time.Second,
	}
	if err := apply.Run(ctx, &route); err != nil {
		return fmt.Errorf("setup is complete; the sample change did not finish: %w", err)
	}
	return nil
}

func prepareInitFirstChange(ctx context.Context, ep string, result *initResult, namespace string, snapshot map[string]string) (string, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pulled, err := client.CallPullSchemaAPIWithContext(checkCtx, ep, result.Database, "vitess", result.Environment, client.PullSchemaOptions{})
	if err != nil {
		return "", fmt.Errorf("check the example's target: %w", err)
	}
	if pulled == nil || pulled.TableCount != 0 || len(pulled.Namespaces) != 1 || len(pulled.Targets) > 1 || pulled.Namespaces[namespace] == nil || len(pulled.Namespaces[namespace].Tables) != 0 || !initExampleUnsharded(pulled.Namespaces[namespace].Artifacts["vschema.json"]) {
		return "", fmt.Errorf("the database changed; review a plan before making your first change")
	}
	for _, target := range pulled.Targets {
		if target == nil || target.TableCount != 0 || len(target.DivergedTables) != 0 {
			return "", fmt.Errorf("the database changed; review a plan before making your first change")
		}
	}
	return writeInitExample(result.SchemaDir, namespace, snapshot)
}

func initExampleUnsharded(raw string) bool {
	if raw == "" {
		return false
	}
	var v *struct {
		Sharded bool `json:"sharded"`
	}
	return json.Unmarshal([]byte(raw), &v) == nil && v != nil && !v.Sharded
}

func initExampleNamespace(files map[string]string) string {
	var namespace string
	for path, content := range files {
		switch filepath.Ext(path) {
		case ".sql":
			if namespace != "" || strings.TrimSpace(content) != strings.TrimSpace(schema.EmptyNamespaceDeclaration) || filepath.Base(path) != "schema.sql" {
				return ""
			}
			namespace = filepath.Dir(path)
			if namespace == "." || strings.ContainsAny(namespace, `/\{}`) {
				return ""
			}
		case ".json":
			if !initExampleUnsharded(content) {
				return ""
			}
		}
	}
	if !initExampleUnsharded(files[filepath.Join(namespace, "vschema.json")]) {
		return ""
	}
	return namespace
}

func writeInitExample(root, namespace string, before map[string]string) (string, error) {
	after, err := initSchemaSnapshot(root)
	if err != nil {
		return "", err
	}
	if !reflect.DeepEqual(before, after) || initExampleNamespace(after) != namespace || namespace == "" {
		return "", fmt.Errorf("schema files changed; review your edits before adding an example")
	}
	path := filepath.Join(root, namespace, "schemabot_example.sql")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", err
	}
	_, writeErr := f.WriteString(initExampleDDL)
	closeErr := f.Close()
	if writeErr != nil {
		return "", writeErr
	}
	return path, closeErr
}

type initFirstChangeMenu struct {
	selected int
	accepted bool
	done     bool
}

func (m *initFirstChangeMenu) Init() tea.Cmd { return nil }
func (m *initFirstChangeMenu) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "up", "down", "tab", "shift+tab":
			m.selected = 1 - m.selected
		case "enter":
			m.accepted = m.selected == 1
			m.done = true
			return m, tea.Quit
		case "esc", "ctrl+c":
			m.done = true
			return m, tea.Quit
		}
	}
	return m, nil
}
func (m *initFirstChangeMenu) View() string {
	if m.done {
		return ""
	}
	var b strings.Builder
	blue := lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	muted := lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	b.WriteString(lipgloss.NewStyle().Bold(true).Render("No tables yet. Want to try your first change?") + "\n\n")
	for i, label := range []string{"Finish setup", "Try a sample schema change"} {
		if i == m.selected {
			b.WriteString(blue.Render("› "+label) + "\n")
		} else {
			b.WriteString("  " + label + "\n")
		}
	}
	b.WriteString("\nWe’ll add a small example file, then show you the plan.\nNothing runs until you approve it.\n\n" + muted.Render("↑/↓ choose · enter continue · esc finish") + "\n")
	return "\n" + lipgloss.NewStyle().Width(68).PaddingLeft(2).Render(b.String())
}
