package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

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
	return cmd.collectInputsWithTerminalState(ctx, g, ui.IsTerminal(os.Stdin), ui.IsTerminal(os.Stdout))
}

func (cmd *InitCmd) collectInputsWithTerminalState(ctx context.Context, g *Globals, stdinTerminal, stdoutTerminal bool) error {
	cmd.interactive = !cmd.NonInteractive && !cmd.JSON && stdinTerminal && stdoutTerminal
	if cmd.Type != "" && cmd.Type != "mysql" && cmd.Type != "postgres" {
		return fmt.Errorf("database engine must be mysql or postgres")
	}
	missing := cmd.missingInputs()
	if len(missing) == 0 {
		return nil
	}
	if !cmd.interactive {
		if cmd.JSON {
			if err := json.NewEncoder(os.Stdout).Encode(struct {
				Error   string   `json:"error"`
				Missing []string `json:"missing"`
			}{"missing_inputs", missing}); err != nil {
				return err
			}
			return ErrSilent
		}
		return fmt.Errorf("initialization needs --%s; provide the missing flags or run init with terminal input and output", strings.Join(missing, ", --"))
	}
	if err := cmd.promptInputs(ctx, os.Stdin, os.Stdout, g); err != nil {
		return err
	}
	cmd.interactive = true
	return nil
}
