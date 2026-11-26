// Command schemabot provides CLI commands for managing database schema changes
// and running the SchemaBot server. Run 'schemabot help' for usage.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/cmd/commands"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/ui"
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

	Init      commands.InitCmd      `cmd:"" help:"Initialize a local connection and verify imported schema" group:"setup"`
	Configure commands.ConfigureCmd `cmd:"" help:"Configure CLI settings (endpoint, profiles)" group:"setup"`
	Login     commands.LoginCmd     `cmd:"" help:"Log in via OIDC and cache a token for the active profile" group:"setup"`
	Local     commands.LocalCmd     `cmd:"" help:"Inspect or stop the local runtime for the active profile" group:"setup"`

	Databases commands.DatabasesCmd `cmd:"" help:"List configured databases" group:"explore"`
	Pull      commands.PullCmd      `cmd:"" help:"Return live schema from a source environment" group:"explore"`
	Onboard   commands.OnboardCmd   `cmd:"" help:"Pull live schema into a new declarative schema directory" group:"explore"`
	Status    commands.StatusCmd    `cmd:"" help:"Show schema change status" group:"explore"`
	Plans     commands.PlansCmd     `cmd:"" name:"list-plans" help:"List recently generated schema change plans" group:"explore"`
	Logs      commands.LogsCmd      `cmd:"" help:"View apply logs" group:"explore"`

	Plan  commands.PlanCmd  `cmd:"" help:"Create a schema change plan" group:"change"`
	Apply commands.ApplyCmd `cmd:"" help:"Apply schema changes" group:"change"`

	Progress   commands.ProgressCmd   `cmd:"" help:"Get schema change progress" group:"control"`
	Stop       commands.StopCmd       `cmd:"" help:"Stop an in-progress schema change" group:"control"`
	Start      commands.StartCmd      `cmd:"" help:"Resume a stopped schema change" group:"control"`
	Cutover    commands.CutoverCmd    `cmd:"" help:"Trigger cutover for a deferred schema change" group:"control"`
	Cancel     commands.CancelCmd     `cmd:"" help:"Cancel a schema change permanently" group:"control"`
	Release    commands.ReleaseCmd    `cmd:"" help:"Release a rollout paused after an on_failure=pause failure" group:"control"`
	Revert     commands.RevertCmd     `cmd:"" help:"Revert a completed schema change during the revert window" group:"control"`
	SkipRevert commands.SkipRevertCmd `cmd:"" name:"skip-revert" help:"Skip the revert window, finalizing the schema change" group:"control"`
	Rollback   commands.RollbackCmd   `cmd:"" help:"Rollback to the previous schema state" group:"control"`

	Freeze   commands.FreezeCmd   `cmd:"" help:"Freeze an environment, or list, show and approve overrides of freezes" group:"environment"`
	Unfreeze commands.UnfreezeCmd `cmd:"" help:"Lift an environment freeze" group:"environment"`
	Locks    commands.LocksCmd    `cmd:"" help:"List all active database locks" group:"environment"`
	Unlock   commands.UnlockCmd   `cmd:"" help:"Release a database lock" group:"environment"`
	Settings commands.SettingsCmd `cmd:"" help:"View or update schema change settings" group:"environment"`

	Serve   commands.ServeCmd   `cmd:"" help:"Start the SchemaBot HTTP API server" group:"server"`
	Checks  commands.ChecksCmd  `cmd:"" help:"Manage SchemaBot Check Runs on PRs" group:"server"`
	Storage commands.StorageCmd `cmd:"" help:"Inspect and maintain SchemaBot's own storage database" group:"server"`

	// Hidden: not part of the user-facing surface. preview is a development
	// tool; fix-lint works but is not ready to be advertised.
	Preview commands.PreviewCmd `cmd:"" hidden:"" help:"Preview CLI output templates (for development)"`
	FixLint commands.FixLintCmd `cmd:"" name:"fix-lint" hidden:"" help:"Auto-fix lint issues in schema files"`
}

// helpOptions is how every help page renders: one line per command, with
// subcommands left to the parent's own --help. The top-level listing is the
// first thing a newcomer reads; it should fit on a screen and name what
// SchemaBot does, not enumerate every operation.
func helpOptions() kong.HelpOptions {
	return kong.HelpOptions{Compact: true, NoExpandSubcommands: true}
}

func main() {
	// Render command hints — and kong's own usage text — with the tool name a
	// wrapper passes via --cli-name, so wrapper-invoked runs print commands
	// that work as pasted. Scanned from the raw args because kong's usage name
	// must be fixed before kong parses; an absent flag keeps the default.
	cliname.Set(cliname.FromArgs(os.Args[1:]))

	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name(cliname.Name()),
		kong.Description("Declarative schema GitOps orchestrator"),
		kong.UsageOnError(),
		kong.ConfigureHelp(helpOptions()),
		// Section order follows the CLI struct, which follows docs/cli.md.
		kong.Groups{
			"setup":       "Set up the CLI:",
			"explore":     "Explore your databases:",
			"change":      "Plan and apply a change:",
			"control":     "Follow and control a change:",
			"environment": "Manage an environment:",
			"server":      "Run SchemaBot:",
		},
		kong.Vars{
			"version":  fmt.Sprintf("%s (commit: %s)", version, commit),
			"cli_name": cliname.Name(),
		},
	)

	cli.Version = version
	cli.Commit = commit
	cli.Date = date

	// Record the routing flags as typed, before a local runtime replaces the
	// endpoint below, so hints that send the operator back to this server
	// reach it again when pasted.
	cliname.SetRoute(cli.RouteFlags()...)

	// Authenticate every API request with the resolved Bearer token, renewing an
	// expired cached token first. Empty when no token is configured, which is
	// correct against a server with auth off.
	// Bound the resolution so a slow or unreachable issuer during a token refresh
	// can't hang the CLI at startup. Cancel as soon as it returns rather than via
	// defer, since the os.Exit below would skip a deferred cancel.
	// Hosting a local runtime does not use a remote profile or its credentials.
	var localEndpoint string
	if !strings.HasPrefix(ctx.Command(), "local ") && ctx.Command() != "init" {
		if usesLocalRuntime(ctx.Command(), &cli) {
			localCtx, cancelLocal := context.WithTimeout(context.Background(), 30*time.Second)
			connection, err := client.ResolveLocalConnection(localCtx, cli.Endpoint, cli.Profile, cli.Token, version)
			cancelLocal()
			if err != nil {
				fmt.Fprint(os.Stderr, errorLine("", "Error", err))
				os.Exit(1)
			}
			if connection != nil {
				localEndpoint = connection.Endpoint
				cli.Endpoint = connection.Endpoint
				cli.Token = connection.Token
			}
		}
		authCtx, cancelAuth := context.WithTimeout(context.Background(), 30*time.Second)
		token, err := client.ResolveBearerToken(authCtx, cli.Token, cli.Endpoint, cli.Profile)
		cancelAuth()
		if err != nil {
			// Per ResolveBearerToken's contract: an empty token with an error is a hard
			// failure; a non-empty token with an error is a non-fatal warning (the
			// returned token is still usable and re-login can fix it).
			if token == "" {
				fmt.Fprint(os.Stderr, errorLine(templates.ANSIRed, "Error", err))
				os.Exit(1)
			}
			fmt.Fprint(os.Stderr, errorLine(templates.ANSIYellow, "Warning", err))
		}
		client.SetAuthToken(token)
		if localEndpoint != "" {
			client.SetLocalAuth(token, localEndpoint)
		}
	}

	// Cancel long-running commands on Ctrl+C / SIGTERM. The first signal
	// cancels the context so in-flight requests stop and the command can print
	// a clean summary; a second signal exits immediately, so a command that
	// does not unwind promptly can always be force-stopped. Bind the context
	// as the context.Context interface so command Run methods can declare a
	// ctx parameter (kong binds concrete values by dynamic type, so an
	// interface parameter needs BindTo).
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	runCtx, cancelRun := context.WithCancel(context.Background())
	// runDone signals the watcher that the command finished — kept separate
	// from runCtx so the second select waits for a genuine second signal
	// rather than the just-cancelled runCtx.
	runDone := make(chan struct{})
	go func() {
		select {
		case <-sigCh:
			cancelRun()
		case <-runDone:
			return
		}
		select {
		case <-sigCh:
			os.Exit(commands.ExitInterrupted)
		case <-runDone:
		}
	}()
	ctx.BindTo(runCtx, (*context.Context)(nil))

	err := ctx.Run(&cli.Globals)
	// Release the watcher and stop intercepting signals now that the command
	// is done; explicit rather than deferred, since the os.Exit below would
	// skip a deferred call.
	close(runDone)
	signal.Stop(sigCh)
	cancelRun()
	if err != nil {
		// ErrSilent means the error was already displayed, so only the status
		// is left to report. A command may ask for its own status when a caller
		// scripting it needs to tell two successful-but-different outcomes
		// apart (see commands.ExitCodeFor); anything else exits 1.
		if !errors.Is(err, commands.ErrSilent) {
			fmt.Fprint(os.Stderr, errorLine(templates.ANSIRed, "Error", err))
		}
		os.Exit(commands.ExitCodeFor(err))
	}
}

// errorLine is the line main writes to the operator's terminal to report err:
// the label ("Error" or "Warning"), then the text of err, wrapped in color
// when one is given. An error a command returns can quote the server, or a
// proxy that answered in its place, so control runes are removed here, where
// every such error is printed, rather than by each command that builds one.
// The color is main's own and is added around the cleaned text. main writes to
// stderr only through this function.
func errorLine(color, label string, err error) string {
	line := label + ": " + ui.TerminalSafe(err.Error())
	if color != "" {
		line = color + line + templates.ANSIReset
	}
	return line + "\n"
}

// Only commands that use the schema API may implicitly start a selected runtime.
func usesLocalRuntime(command string, cli *CLI) bool {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return false
	}
	switch parts[0] {
	case "plan", "onboard", "pull", "apply", "progress", "cutover", "stop", "cancel", "start", "release", "revert", "skip-revert", "rollback", "databases", "unlock", "locks", "freeze", "unfreeze", "logs", "status", "list-plans":
		return true
	case "storage":
		// A storage subcommand reaches its database either through the API or
		// by opening it directly, and only the first needs an endpoint. The
		// direct path exists for when no server is up, so starting a runtime
		// for it would be work the operator asked this command to avoid.
		return len(parts) > 1 && cli.Storage.UsesAPI(parts[1])
	default:
		return false
	}
}
