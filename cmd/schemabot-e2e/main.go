// Binary schemabot-e2e runs a live end-to-end test against a deployed
// SchemaBot: it opens a test pull request, drives a schema change through it
// with PR commands, and checks the test database after every step. A deploy
// gate runs it after each SchemaBot release.
//
// It exits 0 when the run passed, 1 when a step failed, and 2 when the run was
// inconclusive and should be retried: the copy outran a control command,
// another pull request held the lock, GitHub rate limited or failed a call, or
// a signal stopped the run. A run whose cleanup failed exits 1 even when it was
// inconclusive, because what it left behind may still hold the lock. Only a
// run that passed exits 0, so -h exits 1.
//
// The GitHub token comes from GITHUB_TOKEN and the test database DSN from
// SCHEMABOT_E2E_DSN.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/block/schemabot/pkg/livee2e"
)

const (
	exitPassed       = 0
	exitFailed       = 1
	exitInconclusive = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args, os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run takes ctx, cancelled by a signal, and its arguments, environment and
// output as parameters so tests can drive it.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	var cfg livee2e.Config
	// ContinueOnError keeps a bad flag from exiting 2, which means retry.
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	sweepOnly := fs.Bool("sweep-only", false, "only remove what earlier runs left behind; it still needs every flag a run needs")
	fs.StringVar(&cfg.Repo, "repo", "", "test repository, in owner/name form")
	fs.StringVar(&cfg.BaseBranch, "base", "main", "branch test pull requests target")
	fs.StringVar(&cfg.TableDir, "table-dir", "", "repository directory that holds the test database's table files")
	fs.StringVar(&cfg.Database, "database", "", "test database SchemaBot manages")
	fs.StringVar(&cfg.Environment, "environment", "", "SchemaBot environment to apply to")
	fs.StringVar(&cfg.CheckName, "check-name", "", "Check Run SchemaBot posts for the environment")
	fs.StringVar(&cfg.AppSlug, "app-slug", "", "slug of SchemaBot's GitHub App; only its comments and Check Runs count")
	fs.StringVar(&cfg.CommandPrefix, "command-prefix", "schemabot", "word every PR command starts with")
	fs.StringVar(&cfg.SeedTable, "seed-table", "", "existing table in the test database to copy rows from")
	fs.Int64Var(&cfg.SeedRows, "seed-rows", 2_000_000, "rows to copy into the run's table, enough for its copy to last past a stop")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", 5*time.Second, "how often to read the pull request")
	fs.DurationVar(&cfg.WaitTimeout, "wait-timeout", 15*time.Minute, "how long to wait for SchemaBot to act on each command")
	fs.DurationVar(&cfg.RunTimeout, "run-timeout", 2*time.Hour, "how long a whole run may take, opening the test database included; a sweep reaps the run once this and then the wait timeout have passed")
	if err := fs.Parse(args[1:]); err != nil {
		// -h fails too, so a gate misconfigured with it cannot pass.
		return exitFailed
	}

	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	if cfg.RunTimeout <= 0 {
		logger.Error("invalid configuration", "error", "--run-timeout must be positive")
		return exitFailed
	}
	// The run's deadline starts before the test database opens, so a slow
	// connection counts against it. The runner keeps its own cleanup budget.
	runCtx, cancelRun := context.WithTimeout(ctx, cfg.RunTimeout)
	defer cancelRun()

	gh, err := livee2e.NewGitHub(cfg.Repo, getenv("GITHUB_TOKEN"))
	if err != nil {
		logger.Error("cannot build the GitHub client", "error", err)
		return exitFailed
	}
	dsn := getenv("SCHEMABOT_E2E_DSN")
	if dsn == "" {
		logger.Error("SCHEMABOT_E2E_DSN is not set")
		return exitFailed
	}
	openCtx, cancelOpen := context.WithTimeout(runCtx, time.Minute)
	target, err := livee2e.OpenMySQLTarget(openCtx, dsn, cfg.Database, logger)
	cancelOpen()
	if err != nil {
		// A signal can land while the database opens, so this goes through
		// report like a run's own outcome.
		return report(logger, stdout, err, ctx.Err() != nil)
	}
	defer func() {
		if err := target.Close(); err != nil {
			logger.Warn("closing the test database failed", "database", cfg.Database, "error", err)
		}
	}()
	runner, err := livee2e.NewRunner(cfg, gh, target, logger)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		return exitFailed
	}

	if *sweepOnly {
		err = runner.Sweep(runCtx)
	} else {
		err = runner.Run(runCtx)
	}
	return report(logger, stdout, err, ctx.Err() != nil)
}

// report prints the outcome and returns the exit code. A run that stopped
// because of a signal, as when a pipeline cancels a superseded gate, proved
// nothing about the release, so it is inconclusive. A failed cleanup keeps
// the run's error out of the chain, so a stopped run whose cleanup failed
// still fails.
func report(logger *slog.Logger, out io.Writer, err error, signalled bool) int {
	if signalled && errors.Is(err, context.Canceled) {
		logger.Warn("live e2e stopped by a signal")
		err = fmt.Errorf("%w: %w", err, livee2e.ErrInconclusive)
	}
	code, verdict := exitFailed, "FAIL"
	switch {
	case err == nil:
		logger.Info("live e2e passed")
		code, verdict = exitPassed, "PASS"
	case errors.Is(err, livee2e.ErrInconclusive):
		logger.Warn("live e2e inconclusive; retry the run", "error", err)
		code, verdict = exitInconclusive, "INCONCLUSIVE"
	default:
		logger.Error("live e2e failed", "error", err)
	}
	if _, werr := fmt.Fprintln(out, verdict); werr != nil {
		logger.Warn("printing the outcome failed", "error", werr)
	}
	return code
}
