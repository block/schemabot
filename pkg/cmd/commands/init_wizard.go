package commands

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/ui"
)

func (cmd *InitCmd) missingInputs() []string {
	var missing []string
	for _, field := range []struct{ name, value string }{
		{"database", cmd.Database}, {"environment", cmd.Environment}, {"type", cmd.Type}, {"dsn", cmd.DSN}, {"storage-dsn", cmd.StorageDSN},
	} {
		if strings.TrimSpace(field.value) == "" {
			missing = append(missing, field.name)
		}
	}
	if len(cmd.Namespaces) == 0 {
		missing = append(missing, "namespace")
	}
	return missing
}

func (cmd *InitCmd) collectInputs(ctx context.Context, g *Globals) error {
	if cmd.Type != "" && cmd.Type != "mysql" && cmd.Type != "postgres" {
		return fmt.Errorf("database engine must be mysql or postgres")
	}
	missing := cmd.missingInputs()
	if len(missing) == 0 {
		return nil
	}
	if cmd.NonInteractive || cmd.JSON || !ui.IsTerminal(os.Stdin) {
		if cmd.JSON {
			if err := json.NewEncoder(os.Stdout).Encode(struct {
				Error   string   `json:"error"`
				Missing []string `json:"missing"`
			}{"missing_inputs", missing}); err != nil {
				return err
			}
			return ErrSilent
		}
		return fmt.Errorf("initialization needs --%s; provide the missing flags or run init in a terminal", strings.Join(missing, ", --"))
	}
	if err := cmd.promptInputs(ctx, os.Stdin, os.Stdout, g); err != nil {
		return err
	}
	cmd.progress = func(message string) { fmt.Fprintln(os.Stderr, message) }
	return nil
}

// Prompt only collects decisions. Registration, verification, file publication,
// and conflict handling remain in initialize for both people and agents.
func (cmd *InitCmd) promptInputs(ctx context.Context, input io.Reader, output io.Writer, g *Globals) error {
	reader := bufio.NewReader(input)
	ask := func(label, fallback string) (string, error) {
		if fallback != "" {
			if _, err := fmt.Fprintf(output, "%s [%s]: ", label, fallback); err != nil {
				return "", err
			}
		} else {
			if _, err := fmt.Fprintf(output, "%s: ", label); err != nil {
				return "", err
			}
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		type answer struct {
			value string
			err   error
		}
		ready := make(chan answer, 1)
		go func() { value, err := reader.ReadString('\n'); ready <- answer{value, err} }()
		var value string
		var err error
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("setup cancelled: %w", ctx.Err())
		case result := <-ready:
			value, err = result.value, result.err
		}
		if err != nil {
			return "", fmt.Errorf("setup cancelled before completion: %w", err)
		}
		value = strings.TrimSpace(value)
		if value == "" {
			value = fallback
		}
		return value, nil
	}
	if _, err := fmt.Fprint(output, "SchemaBot setup\nConnect your database. Start with a verified schema.\n\n"); err != nil {
		return err
	}
	var err error
	if cmd.Type == "" {
		for {
			cmd.Type, err = ask("Database engine (mysql or postgres)", "")
			if err != nil {
				return err
			}
			if cmd.Type == "mysql" || cmd.Type == "postgres" {
				break
			}
			if _, err := fmt.Fprintln(output, "Choose mysql or postgres."); err != nil {
				return err
			}
		}
	}
	fields := []struct {
		label, fallback string
		value           *string
	}{
		{"Database name", "", &cmd.Database},
		{"Environment", "development", &cmd.Environment},
		{"Database connection variable", "env:DATABASE_URL", &cmd.DSN},
		{"Separate state database variable", "env:SCHEMABOT_STORAGE_DSN", &cmd.StorageDSN},
	}
	for _, field := range fields {
		if *field.value != "" {
			continue
		}
		for *field.value == "" {
			*field.value, err = ask(field.label, field.fallback)
			if err != nil {
				return err
			}
		}
	}
	if len(cmd.Namespaces) == 0 {
		fallback := cmd.Database
		if cmd.Type == "postgres" {
			fallback = "public"
		}
		selected, err := ask("Namespaces (comma-separated)", fallback)
		if err != nil {
			return err
		}
		for namespace := range strings.SplitSeq(selected, ",") {
			cmd.Namespaces = append(cmd.Namespaces, strings.TrimSpace(namespace))
		}
	}
	cmd.SchemaDir, err = ask("Schema directory", cmd.SchemaDir)
	if err != nil {
		return err
	}
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	g.Profile, err = ask("Connection profile", client.ResolveProfileName(cfg, g.Profile))
	if err != nil {
		return err
	}
	if _, err := os.Stat(cmd.SchemaDir); err == nil && !cmd.ReuseSchema {
		answer, err := ask("Verify and reuse the existing schema files? (y/N)", "n")
		if err != nil {
			return err
		}
		if !strings.EqualFold(answer, "y") && !strings.EqualFold(answer, "yes") {
			return fmt.Errorf("setup cancelled; choose a new schema directory to import into")
		}
		cmd.ReuseSchema = true
	}
	if _, err := fmt.Fprintf(output, "\nDatabase: %s (%s) / %s\nScope: %s\nSchema: %s\nProfile: %s\n", cmd.Database, cmd.Type, cmd.Environment, strings.Join(cmd.Namespaces, ", "), cmd.SchemaDir, g.Profile); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(output, "\nSchemaBot will initialize metadata in the separate state database and verify a plan.\nNo application schema changes will be applied."); err != nil {
		return err
	}
	answer, err := ask("Continue? (y/N)", "n")
	if err != nil {
		return err
	}
	if !strings.EqualFold(answer, "y") && !strings.EqualFold(answer, "yes") {
		return fmt.Errorf("setup cancelled; nothing was initialized")
	}
	if _, err := fmt.Fprintln(output); err != nil {
		return err
	}
	return nil
}
