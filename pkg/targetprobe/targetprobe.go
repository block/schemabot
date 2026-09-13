// Package targetprobe checks connectivity to enumerable target databases at startup.
package targetprobe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/postgres"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/targetauth"
)

const (
	defaultPerTargetTimeout = 10 * time.Second
	defaultPoolSize         = 4
)

// openFunc opens a resolved target. It receives the whole target, not only
// its DSN, because the transport policy a target dials under can live in its
// metadata (the PostgreSQL CA reference), and the probe must verify the server
// under the same trust every request-path connection uses.
type openFunc func(context.Context, *inventory.Target) (*sql.DB, error)

// Prober checks each target exposed by an inventory Enumerator.
type Prober struct {
	resolver         inventory.Resolver
	logger           *slog.Logger
	perTargetTimeout time.Duration
	poolSize         int
	open             openFunc
}

// New constructs a startup target prober. Non-positive timeout and pool size
// values select safe defaults.
func New(resolver inventory.Resolver, logger *slog.Logger, perTargetTimeout time.Duration, poolSize int) *Prober {
	if logger == nil {
		logger = slog.Default()
	}
	if perTargetTimeout <= 0 {
		perTargetTimeout = defaultPerTargetTimeout
	}
	if poolSize <= 0 {
		poolSize = defaultPoolSize
	}
	return &Prober{
		resolver:         resolver,
		logger:           logger,
		perTargetTimeout: perTargetTimeout,
		poolSize:         poolSize,
		open:             openDatabase,
	}
}

// Run probes enumerable targets without affecting server health or readiness.
func (p *Prober) Run(ctx context.Context) {
	enumerator, ok := p.resolver.(inventory.Enumerator)
	if !ok {
		var attrs []any
		if typed, ok := p.resolver.(inventory.TypeReporter); ok {
			attrs = append(attrs, "database_types", []string{typed.DatabaseType()})
		}
		p.logger.Info("target probe: discovery-resolved targets are outside probe coverage", attrs...)
		return
	}
	p.logUnenumerableTypes()
	requests, err := enumerator.Enumerate(ctx)
	if err != nil {
		p.logger.Error("target probe: enumerate targets failed; no targets will be probed", "error", err)
		return
	}
	p.logger.Info("target probe: probing enumerated targets", "count", len(requests))

	outcomes := make(map[string]int)
	var outcomesMu sync.Mutex
	jobs := make(chan inventory.ProbeRequest)
	var wg sync.WaitGroup
	for range p.poolSize {
		wg.Go(func() {
			for request := range jobs {
				outcome, recorded := p.probeContained(ctx, request)
				if recorded {
					outcomesMu.Lock()
					outcomes[outcome]++
					outcomesMu.Unlock()
				}
			}
		})
	}
	for _, request := range requests {
		jobs <- request
	}
	close(jobs)
	wg.Wait()
	p.logger.Info("target probe: completed enumerated targets", "outcomes", outcomes)
}

func (p *Prober) logUnenumerableTypes() {
	router, ok := p.resolver.(*inventory.TypeRoutingResolver)
	if !ok {
		return
	}
	types := router.UnenumerableDatabaseTypes()
	if len(types) > 0 {
		p.logger.Info("target probe: discovery-resolved targets are outside probe coverage", "database_types", types)
	}
}

// probeContained is the panic containment boundary for one target (AV-5).
// Each probe runs on a pool worker, so a panic in resolution, dialing, or
// recording that escaped the worker would take the process down during
// startup; contained, it costs only that target's result. The panic value and
// stack stay in the server log.
func (p *Prober) probeContained(ctx context.Context, request inventory.ProbeRequest) (outcome string, recorded bool) {
	probePanic, _ := panicsafe.Catch(func() error {
		outcome, recorded = p.probe(ctx, request)
		return nil
	})
	if probePanic == nil {
		return outcome, recorded
	}
	p.logger.Error("target probe: probe panicked; the remaining targets continue",
		"target", request.Target,
		"database_type", request.DatabaseType,
		"panic", fmt.Sprint(probePanic.Value),
		"stack", string(probePanic.Stack))
	metrics.RecordRecoveredPanic(ctx, "target_probe")
	return "", false
}

func (p *Prober) probe(parent context.Context, request inventory.ProbeRequest) (string, bool) {
	if !supportsDSNProbe(request.DatabaseType) {
		p.logger.Debug("target probe: database type is outside DSN probe coverage; skipping target", "target", request.Target, "database_type", request.DatabaseType)
		return "", false
	}
	if parent.Err() != nil {
		p.logger.Debug("target probe: server is shutting down; skipping target", "target", request.Target, "database_type", request.DatabaseType)
		return "", false
	}
	ctx, cancel := context.WithTimeout(parent, p.perTargetTimeout)
	defer cancel()
	started := time.Now()
	resolved, err := p.resolver.ResolveTarget(ctx, inventory.Request{Target: request.Target, DatabaseType: request.DatabaseType})
	if err != nil {
		err = fmt.Errorf("resolve target %q for startup probe: %w", request.Target, err)
		outcome, classification := classifyResolve(ctx)
		return p.fail(parent, ctx, request, outcome, classification, started, err)
	}
	if resolved == nil {
		err = fmt.Errorf("resolver returned no target for %q", request.Target)
		return p.fail(parent, ctx, request, "resolve_error", "resolve_error", started, err)
	}
	db, err := p.open(ctx, resolved)
	if err != nil {
		err = fmt.Errorf("open target %q for startup probe: %w", request.Target, err)
		outcome, classification := classify(ctx, err)
		return p.fail(parent, ctx, request, outcome, classification, started, err)
	}
	defer utils.CloseAndLog(db)
	db.SetMaxOpenConns(1)
	if err = db.PingContext(ctx); err == nil {
		var one int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		if err != nil {
			err = fmt.Errorf("query target %q for startup probe: %w", request.Target, err)
		}
	} else {
		err = fmt.Errorf("ping target %q for startup probe: %w", request.Target, err)
	}
	if err != nil {
		outcome, classification := classify(ctx, err)
		return p.fail(parent, ctx, request, outcome, classification, started, err)
	}
	p.record(ctx, request, "success", "", started, nil)
	return "success", true
}

// fail records a failed probe unless the server began shutting down while it
// was in flight. A probe cut short by cancellation says nothing about the
// target — the same target reports a real outcome on the next start — so it is
// discarded rather than counted as a connection failure, exactly as a probe
// that had not yet started would have been skipped.
func (p *Prober) fail(parent, ctx context.Context, request inventory.ProbeRequest, outcome, classification string, started time.Time, err error) (string, bool) {
	if parent.Err() != nil {
		p.logger.Debug("target probe: server is shutting down; discarding the in-flight result", "target", request.Target, "database_type", request.DatabaseType, "error", err)
		return "", false
	}
	p.record(ctx, request, outcome, classification, started, err)
	return outcome, true
}

// classify maps a probe failure to its metric outcome and log classification.
// The per-target deadline wins over the driver error it caused, so a slow
// target reports timeout rather than the connection error the driver wrapped
// it in.
func classify(ctx context.Context, err error) (string, string) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout", "timeout"
	}
	classification := targetauth.Classify(err)
	if classification == targetauth.NotAuth {
		return "connection_error", classification.Label()
	}
	return classification.Label(), classification.Label()
}

// classifyResolve maps a resolution failure the same way: a resolver (for
// example a secret lookup) that ran past the per-target deadline reports
// timeout, and every other resolver failure reports resolve_error.
func classifyResolve(ctx context.Context) (string, string) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout", "timeout"
	}
	return "resolve_error", "resolve_error"
}

func (p *Prober) record(ctx context.Context, request inventory.ProbeRequest, outcome, classification string, started time.Time, err error) {
	// Static resolution does not use an environment, so the probe has none to
	// report; EnvironmentAttribute renders the empty value as unknown.
	metrics.RecordTargetProbe(ctx, request.Target, request.DatabaseType, "", outcome)
	attrs := []any{"target", request.Target, "database_type", request.DatabaseType, "environment", "", "outcome", outcome, "duration_ms", time.Since(started).Milliseconds()}
	if err == nil {
		p.logger.Info("target probe: succeeded", attrs...)
		return
	}
	attrs = append(attrs, "classification", classification, "error", err)
	p.logger.Warn("target probe: failed", attrs...)
}

// supportsDSNProbe reports whether targets of a database type are reached over
// a DSN the probe can dial. Vitess targets connect through the PlanetScale
// API, and a database type the probe does not know has no dial path it can
// vouch for; both are skipped up front rather than resolved and then reported
// as a connection failure they did not have.
func supportsDSNProbe(databaseType string) bool {
	switch databaseType {
	case storage.DatabaseTypeMySQL, storage.DatabaseTypeStrata, storage.DatabaseTypePostgres:
		return true
	default:
		return false
	}
}

// openDatabase dials a resolved target the way the request path does: MySQL
// through mysqlconn, PostgreSQL through postgresconn under the trust the
// target's CA reference names. A CA reference the engine would refuse fails
// the probe the same way it would fail the first plan. The resolver answers
// for the target's type, so a resolved type the probe cannot dial means the
// resolver returned a target of a different engine than it was asked for.
func openDatabase(_ context.Context, target *inventory.Target) (*sql.DB, error) {
	switch target.DatabaseType {
	case storage.DatabaseTypeMySQL, storage.DatabaseTypeStrata:
		return mysqlconn.Open(target.DSN)
	case storage.DatabaseTypePostgres:
		opts, err := postgres.ConnectionOptions(&engine.Credentials{DSN: target.DSN, Metadata: target.Metadata})
		if err != nil {
			return nil, err
		}
		return postgresconn.Open(target.DSN, opts...)
	default:
		return nil, fmt.Errorf("resolved database type %q does not support a DSN startup probe", target.DatabaseType)
	}
}
