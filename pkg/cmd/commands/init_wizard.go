package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
		{"database", cmd.Database}, {"environment", cmd.Environment}, {"type", cmd.Type}, {"dsn", cmd.DSN},
	} {
		if strings.TrimSpace(field.value) == "" {
			missing = append(missing, field.name)
		}
	}
	if cmd.StorageDSN == "" && !cmd.Integrated {
		missing = append(missing, "storage-dsn")
	}
	if len(cmd.Namespaces) == 0 {
		missing = append(missing, "namespace")
	}
	return missing
}

// engineSupported reports whether init can register this target. A Vitess
// target is reached through a PlanetScale-compatible API, and the organization
// and service token that go with it come from sample setup, so it is offered
// as a sample rather than as connections to name by hand.
func (cmd *InitCmd) engineSupported() bool {
	switch cmd.Type {
	case "mysql", "postgres":
		return true
	case "vitess":
		return cmd.Sample
	}
	return false
}

func (cmd *InitCmd) collectInputs(ctx context.Context, g *Globals) error {
	return cmd.collectInputsWithTerminalState(ctx, g, ui.IsTerminal(os.Stdin), ui.IsTerminal(os.Stdout))
}

func (cmd *InitCmd) collectInputsWithTerminalState(ctx context.Context, g *Globals, stdinTerminal, stdoutTerminal bool) error {
	cmd.interactive = !cmd.NonInteractive && !cmd.JSON && stdinTerminal && stdoutTerminal
	if cmd.Type != "" && !cmd.engineSupported() {
		if cmd.Type == "vitess" {
			return fmt.Errorf("a Vitess target is set up as a sample; run init --sample --type vitess")
		}
		return fmt.Errorf("database engine must be mysql or postgres")
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
