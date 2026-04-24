// Command schemabot provides CLI commands for managing database schema changes
// and running the SchemaBot server. Run 'schemabot help' for usage.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/alecthomas/kong"

	"github.com/block/schemabot/pkg/cmd/commands"
)

// Set by ldflags at build time.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

type CLI struct {
	commands.Globals

	VersionFlag kong.VersionFlag `name:"version" help:"Show version information"`

	Plan       commands.PlanCmd       `cmd:"" help:"Create a schema change plan"`
	Apply      commands.ApplyCmd      `cmd:"" help:"Apply schema changes"`
	Progress   commands.ProgressCmd   `cmd:"" help:"Get schema change progress"`
	Cutover    commands.CutoverCmd    `cmd:"" help:"Trigger cutover for a deferred schema change"`
	Stop       commands.StopCmd       `cmd:"" help:"Stop an in-progress schema change"`
	Start      commands.StartCmd      `cmd:"" help:"Resume a stopped schema change"`
	Volume     commands.VolumeCmd     `cmd:"" help:"Adjust schema change speed (1-11)"`
	Revert     commands.RevertCmd     `cmd:"" help:"Revert a completed schema change during the revert window"`
	SkipRevert commands.SkipRevertCmd `cmd:"" name:"skip-revert" help:"Skip the revert window, finalizing the schema change"`
	Rollback   commands.RollbackCmd   `cmd:"" help:"Rollback to the previous schema state"`
	Unlock     commands.UnlockCmd     `cmd:"" help:"Release a database lock"`
	Locks      commands.LocksCmd      `cmd:"" help:"List all active database locks"`
	Logs       commands.LogsCmd       `cmd:"" help:"View apply logs"`
	Status     commands.StatusCmd     `cmd:"" help:"Show schema change status"`
	Preview    commands.PreviewCmd    `cmd:"" help:"Preview CLI output templates (for development)"`
	FixLint    commands.FixLintCmd    `cmd:"" name:"fix-lint" help:"Auto-fix lint issues in schema files"`
	Configure  commands.ConfigureCmd  `cmd:"" help:"Configure CLI settings (endpoint, profiles)"`
	Settings   commands.SettingsCmd   `cmd:"" help:"View or update schema change settings"`
	Serve      commands.ServeCmd      `cmd:"" help:"Start the SchemaBot HTTP API server"`
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("schemabot"),
		kong.Description("Declarative schema GitOps orchestrator"),
		kong.UsageOnError(),
		kong.Vars{"version": fmt.Sprintf("%s (commit: %s)", version, commit)},
	)

	cli.Version = version
	cli.Commit = commit
	cli.Date = date

	err := ctx.Run(&cli.Globals)
	if err != nil {
		// ErrSilent means the error was already displayed - just exit with code 1
		if !errors.Is(err, commands.ErrSilent) {
			fmt.Fprintf(os.Stderr, "\033[31mError: %v\033[0m\n", err)
		}
		os.Exit(1)
	}
}
