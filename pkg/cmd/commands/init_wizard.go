package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/ui"
)

type initInputError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (cmd *InitCmd) missingInputs() []string {
	var missing []string
	for _, field := range []struct{ name, value string }{
		{"database", cmd.Database}, {"environment", cmd.Environment}, {"type", cmd.Type}, {"dsn", cmd.DSN}, {"storage-dsn", cmd.StorageDSN},
	} {
		if strings.TrimSpace(field.value) == "" {
			missing = append(missing, field.name)
		}
	}
	if cmd.Type == "vitess" {
		for _, field := range []struct{ name, value string }{{"organization", cmd.Organization}, {"api-token", cmd.APIToken}} {
			if strings.TrimSpace(field.value) == "" {
				missing = append(missing, field.name)
			}
		}
	}
	if len(cmd.Namespaces) == 0 {
		missing = append(missing, "namespace")
	}
	return missing
}

func (cmd *InitCmd) collectInputs(ctx context.Context, g *Globals) error {
	return cmd.collectInputsWithTerminalState(ctx, g, ui.IsTerminal(os.Stdin), ui.IsTerminal(os.Stdout))
}

func (cmd *InitCmd) collectInputsWithTerminalState(ctx context.Context, g *Globals, stdinTerminal, stdoutTerminal bool) error {
	cmd.interactive = !cmd.NonInteractive && !cmd.JSON && stdinTerminal && stdoutTerminal
	if cmd.Type != "" && !slices.Contains(initEngineKeys(), cmd.Type) {
		return fmt.Errorf("database engine must be %s", strings.Join(initEngineKeys(), ", "))
	}
	missing := cmd.missingInputs()
	if len(missing) == 0 {
		return nil
	}
	if !cmd.interactive {
		if cmd.JSON {
			if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
				"error":   initInputError{Code: "missing_inputs", Message: "Provide the missing flags or run init interactively."},
				"missing": missing,
			}); err != nil {
				return err
			}
			return ErrSilent
		}
		return fmt.Errorf("initialization needs --%s; provide the missing flags or run init with terminal input and output", strings.Join(missing, ", --"))
	}
	if err := cmd.promptInputs(ctx, os.Stdin, os.Stdout, g); err != nil {
		return err
	}
	return nil
}
