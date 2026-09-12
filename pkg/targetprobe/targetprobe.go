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

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/targetauth"
)

const (
	defaultPerTargetTimeout = 10 * time.Second
	defaultPoolSize         = 4
)

type openFunc func(context.Context, string, string) (*sql.DB, error)

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
		p.logger.Info("target probe: discovery-resolved targets are outside probe coverage")
		return
	}
	p.logUnenumerableTypes()
	requests := enumerator.Enumerate()
	p.logger.Info("target probe: probing enumerated targets", "count", len(requests))

	outcomes := make(map[string]int)
	var outcomesMu sync.Mutex
	jobs := make(chan inventory.ProbeRequest)
	var wg sync.WaitGroup
	for range p.poolSize {
		wg.Go(func() {
			for request := range jobs {
				outcome, recorded := p.probe(ctx, request)
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

func (p *Prober) probe(parent context.Context, request inventory.ProbeRequest) (string, bool) {
	if request.DatabaseType == storage.DatabaseTypeVitess {
		p.logger.Debug("target probe: Vitess target is outside DSN probe coverage", "target", request.Target, "database_type", request.DatabaseType)
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
		p.record(ctx, request, "resolve_error", "resolve_error", started, err)
		return "resolve_error", true
	}
	if resolved == nil {
		err = fmt.Errorf("resolver returned no target for %q", request.Target)
		p.record(ctx, request, "resolve_error", "resolve_error", started, err)
		return "resolve_error", true
	}
	db, err := p.open(ctx, resolved.DatabaseType, resolved.DSN)
	if err != nil {
		err = fmt.Errorf("open target %q for startup probe: %w", request.Target, err)
		outcome, classification := classify(ctx, err)
		p.record(ctx, request, outcome, classification, started, err)
		return outcome, true
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
		p.record(ctx, request, outcome, classification, started, err)
		return outcome, true
	}
	p.record(ctx, request, "success", "", started, nil)
	return "success", true
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

func openDatabase(_ context.Context, databaseType, dsn string) (*sql.DB, error) {
	switch databaseType {
	case storage.DatabaseTypeMySQL, storage.DatabaseTypeStrata:
		return mysqlconn.Open(dsn)
	case storage.DatabaseTypePostgres:
		return postgresconn.Open(dsn)
	default:
		return nil, fmt.Errorf("database type %q does not support a DSN startup probe", databaseType)
	}
}
