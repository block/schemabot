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
//
// With -github-calls-namespaces, a run in a pod also counts the GitHub API
// calls SchemaBot made during it, from every SchemaBot pod's metrics endpoint,
// and sends the count to DogStatsD as the schemabot.e2e.github_calls gauge,
// tagged with -metric-tags. When the count cannot be taken (a pod was added,
// removed or replaced, a container restarted, or a read failed) nothing is
// sent, so a gate that requires the gauge holds the release. The count does
// not change the exit code. It assumes the SchemaBot replicas stay fixed for
// the run: a pod that starts and goes away between the two reads is not seen.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"

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
	callNamespaces := fs.String("github-calls-namespaces", "", "comma-separated namespaces of the SchemaBot pods whose GitHub calls the run counts; empty counts nothing")
	callSelector := fs.String("github-calls-selector", "app.kubernetes.io/name=schemabot", "label selector for the SchemaBot pods whose GitHub calls the run counts")
	metricsPort := fs.Int("github-calls-metrics-port", 9102, "port of each SchemaBot pod's Prometheus endpoint")
	statsdAddr := fs.String("statsd-addr", "", "DogStatsD address for the GitHub call count; empty uses DD_DOGSTATSD_URL or DD_AGENT_HOST")
	metricTags := fs.String("metric-tags", "", "comma-separated key:value tags for the GitHub call count, such as the deploy's values SHA and stage")
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

	// The counter is built before the test database opens, so a gate
	// misconfigured to count calls fails without touching the database.
	var counter *livee2e.GitHubCallCounter
	closeCounter := func() {}
	if !*sweepOnly {
		var err error
		counter, closeCounter, err = newCallCounter(*callNamespaces, *callSelector, *metricsPort, *statsdAddr, *metricTags, getenv, logger)
		if err != nil {
			logger.Error("invalid configuration", "error", err)
			return exitFailed
		}
	}
	defer closeCounter()

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
		return report(logger, stdout, runner.Sweep(runCtx), ctx.Err() != nil)
	}
	if counter != nil {
		counter.Start(runCtx)
	}
	err = runner.Run(runCtx)
	if counter != nil {
		// The run's context may be cancelled by now; the count is still worth
		// sending, so it gets its own bound.
		countCtx, cancelCount := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		if _, cerr := counter.Finish(countCtx); cerr != nil {
			logger.Error("no GitHub call count sent; a gate that requires it will hold the release", "error", cerr)
		}
		cancelCount()
	}
	return report(logger, stdout, err, ctx.Err() != nil)
}

// newCallCounter builds the GitHub call counter, or returns nil when no
// namespaces are given. The returned close flushes the DogStatsD client.
func newCallCounter(namespaces, selector string, port int, statsdAddr, tags string, getenv func(string) string, logger *slog.Logger) (*livee2e.GitHubCallCounter, func(), error) {
	if namespaces == "" {
		return nil, func() {}, nil
	}
	pods, err := livee2e.NewInClusterPods(getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT"),
		splitList(namespaces), selector, port)
	if err != nil {
		return nil, nil, fmt.Errorf("count GitHub calls: %w", err)
	}
	client, err := statsd.New(statsdAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("count GitHub calls: DogStatsD client: %w", err)
	}
	closeClient := func() {
		if err := client.Close(); err != nil {
			logger.Warn("closing the DogStatsD client failed", "error", err)
		}
	}
	scrape := &http.Client{Timeout: 10 * time.Second}
	return livee2e.NewGitHubCallCounter(pods, scrape, client, splitList(tags), logger), closeClient, nil
}

// splitList splits a comma-separated flag value, dropping empty entries.
func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
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
