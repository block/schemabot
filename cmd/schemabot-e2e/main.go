// Binary schemabot-e2e runs a live end-to-end test against a deployed
// SchemaBot: it opens a test pull request, drives a schema change through it
// with PR commands, and checks the test database after every step. A deploy
// gate runs it after each SchemaBot release.
//
// It exits 0 when the run passed, 1 when a step failed, and 2 when the run was
// inconclusive and should be retried.
//
// The GitHub token comes from GITHUB_TOKEN and the test database DSN from
// SCHEMABOT_E2E_DSN.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	os.Exit(run())
}

func run() int {
	var cfg livee2e.Config
	// ContinueOnError keeps a bad flag from exiting 2, which means retry.
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	sweepOnly := fs.Bool("sweep-only", false, "only remove what earlier runs left behind")
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
	fs.DurationVar(&cfg.RunTimeout, "run-timeout", 2*time.Hour, "how long a whole run may take, opening the test database included; the sweep reaps only runs older than this plus the wait timeout")
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitPassed
		}
		return exitFailed
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if cfg.RunTimeout <= 0 {
		logger.Error("invalid configuration", "error", "--run-timeout must be positive")
		return exitFailed
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The run's deadline starts before the test database opens, so a slow
	// connection counts against it. The runner keeps its own cleanup budget.
	runCtx, cancelRun := context.WithTimeout(ctx, cfg.RunTimeout)
	defer cancelRun()

	gh, err := livee2e.NewGitHub(cfg.Repo, os.Getenv("GITHUB_TOKEN"))
	if err != nil {
		logger.Error("cannot build the GitHub client", "error", err)
		return exitFailed
	}
	dsn := os.Getenv("SCHEMABOT_E2E_DSN")
	if dsn == "" {
		logger.Error("SCHEMABOT_E2E_DSN is not set")
		return exitFailed
	}
	openCtx, cancelOpen := context.WithTimeout(runCtx, time.Minute)
	target, err := livee2e.OpenMySQLTarget(openCtx, dsn, cfg.Database, logger)
	cancelOpen()
	if err != nil {
		logger.Error("cannot open the test database", "database", cfg.Database, "error", err)
		return exitFailed
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
	return report(logger, err)
}

func report(logger *slog.Logger, err error) int {
	switch {
	case err == nil:
		logger.Info("live e2e passed")
		fmt.Println("PASS")
		return exitPassed
	case errors.Is(err, livee2e.ErrInconclusive):
		logger.Warn("live e2e inconclusive; retry the run", "error", err)
		fmt.Println("INCONCLUSIVE")
		return exitInconclusive
	default:
		logger.Error("live e2e failed", "error", err)
		fmt.Println("FAIL")
		return exitFailed
	}
}
