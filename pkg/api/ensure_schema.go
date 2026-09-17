package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/schema"
)

// EnsureSchemaTimeout bounds a convergence nobody asked for: the one a pod runs
// on the way up. It covers acquiring the advisory lock, planning, and applying
// the storage schema change to completion. SchemaBot's storage tables are small,
// but Spirit applies an *online* DDL, and on Aurora that carries fixed overhead
// (binlog subscription, checksum, cutover MDL, and throttler poll loops) that can
// exceed a minute even for a tiny table. Trailing pods also wait up to this long
// on the advisory lock while the leader applies, then see no changes. Too short a
// value cancels the apply mid-copy ("failed to read chunk data: context
// canceled") and leaves storage uninitialized.
//
// It is short on purpose, and the reason is availability rather than DDL cost: a
// pod converging is a pod not yet serving, and one holding the advisory lock is
// every other pod not yet serving either. A boot must therefore give up and
// report rather than wait out work of unbounded length — which is why a
// convergence an operator asked for does not use this value. That one has
// somebody watching it and no deployment waiting on it, so it names its own
// budget through WithConvergenceTimeout, defaulting to
// DefaultStorageApplyTimeout.
//
// The same constant bounds the PostgreSQL bootstrap flow — its existence
// checks, advisory-lock wait, and transactional table creation — so tuning it
// for MySQL/Spirit reasons also changes how long a PostgreSQL pod waits. It is
// also the base of a fourth derivation on the boot path:
// postgresBootstrapDDLBudget subtracts a margin from the effective budget to
// bound one convergence DDL statement server-side, so lowering this shortens
// that budget too, down to its own floor.
const EnsureSchemaTimeout = 5 * time.Minute

// MinConvergenceTimeout is the shortest budget a convergence may run under. It
// is one whole second because that is the granularity the budget is honored
// at: the wire names a budget in whole seconds, and the coarsest lock wait an
// engine offers rounds up to whole seconds, so a shorter budget would be
// reported as one thing and enforced as another. It is also the shortest
// ceiling every statement budget derived from it stays strictly under; a
// ceiling that cannot be undercut in whole milliseconds cannot be bounded at
// all, and a budget that has stopped bounding is the one failure this
// constant exists to refuse.
const MinConvergenceTimeout = time.Second

// EnsureSchemaOption customizes EnsureSchema behavior.
type EnsureSchemaOption func(*ensureSchemaOptions)

type ensureSchemaOptions struct {
	allowDestructive bool
	dialect          schema.Dialect
	// convergenceTimeout bounds one whole convergence: the lock wait, the
	// diff under it, and the DDL. It defaults to EnsureSchemaTimeout, the
	// budget a boot needs, so a caller that never considered the question
	// converges the way a pod does.
	convergenceTimeout time.Duration
	// postgresStatementTimeout bounds a single ordinary query on the
	// PostgreSQL bootstrap's connection. Zero disables the budget explicitly;
	// negative means "not set", leaving the platform's ambient value in place.
	// It defaults to DefaultPostgresStatementTimeout rather than to "not set",
	// so a caller that never considered the question still bootstraps under a
	// budget SchemaBot states instead of one the platform imposed.
	postgresStatementTimeout time.Duration
}

// WithAllowDestructiveSchemaChanges controls whether EnsureSchema may execute
// destructive DDL against the storage database — any statement the plan's own
// linters report an error against, which for the storage schema means one that
// loses data (DROP TABLE, or an ALTER TABLE containing DROP COLUMN) and one
// that removes an index. It defaults to false: those statements are refused
// while the rest of the diff still applies. A mixed ALTER carrying an additive
// clause beside a drop runs the addition and withholds the drop, so a pod never
// starts missing a column its own binary needs.
//
// This is the only way to have the bootstrap execute one of those statements,
// so removing a table, column, or index from the embedded schema on purpose
// means setting this flag or running the DDL by hand. That is the
// intended trade: a surplus index left in place costs write throughput, while
// one dropped out from under the fleet's live queries costs availability.
// Wire this from StorageConfig.AllowDestructiveSchemaChanges.
func WithAllowDestructiveSchemaChanges(allow bool) EnsureSchemaOption {
	return func(o *ensureSchemaOptions) { o.allowDestructive = allow }
}

// WithDialect selects the database family of the storage database so
// EnsureSchema routes to the matching schema bootstrapper. It defaults to
// schema.DialectMySQL, which preserves the behavior of every existing call
// site. A dialect without a bootstrapper fails closed rather than falling back
// to the MySQL flow.
//
// The dispatch matches the schema.Dialect constants exactly and deliberately
// does not normalize case: schema.DialectForDatabaseType owns the conversion
// from raw config text (including lowercasing), so callers wiring config
// values must go through it. Any other value fails closed at startup.
func WithDialect(dialect schema.Dialect) EnsureSchemaOption {
	return func(o *ensureSchemaOptions) { o.dialect = dialect }
}

// WithPostgresStatementTimeout bounds a single ordinary query the PostgreSQL
// bootstrap issues — its catalog reads and existence checks. It deliberately
// does not bound the two statement classes the bootstrap runs that are
// expected to be slow: convergence DDL raises the budget per transaction to
// postgresBootstrapDDLBudget's value, and the advisory-lock wait runs with
// no statement budget at all. A zero duration disables the budget explicitly
// rather than inheriting the platform's, and a negative one leaves the
// platform's value in place. Unset, the budget is
// DefaultPostgresStatementTimeout. Wire this from
// PostgresConfig.StatementTimeoutOrDefault.
func WithPostgresStatementTimeout(d time.Duration) EnsureSchemaOption {
	return func(o *ensureSchemaOptions) { o.postgresStatementTimeout = d }
}

// WithConvergenceTimeout bounds one whole convergence — the advisory-lock wait,
// the diff taken under it, and the DDL — replacing the boot budget
// EnsureSchemaTimeout. A duration under MinConvergenceTimeout is refused rather
// than rounded: a non-positive one would read as "no limit", and a convergence
// with no ceiling holds the advisory lock forever on a statement that will never
// finish, so every pod that boots behind it fails its own lock wait; a
// sub-second one would be enforced at a granularity coarser than it was named
// at, and report a ceiling it did not run under.
//
// Raise it only for a convergence somebody is watching. The budget is what
// bounds how long this call can keep booting pods out of service, so the
// deliberate path trades that availability for the ability to finish work a boot
// could not — an index build over a storage table with a long history, say.
// Wire it from DefaultStorageApplyTimeout unless an operator named a value.
func WithConvergenceTimeout(d time.Duration) EnsureSchemaOption {
	return func(o *ensureSchemaOptions) { o.convergenceTimeout = d }
}

// EnsureSchema converges SchemaBot's own storage schema at startup, routing to
// the bootstrapper for the storage database's dialect (MySQL unless overridden
// with WithDialect). It is idempotent — no changes are made if the schema is
// already up-to-date.
//
// The dispatch fails closed: a dialect without a bootstrapper returns an error
// instead of running another family's DDL against the storage database. Each
// bootstrapper owns its dialect end to end — embedded schema files, diff/apply
// mechanism, and the advisory locker that serializes startup across pods — so
// adding a dialect means adding a bootstrapper here, not threading
// dialect-conditionals through the MySQL flow.
func EnsureSchema(dsn string, logger *slog.Logger, opts ...EnsureSchemaOption) error {
	// Background, not a caller's context, and the signature is what enforces
	// it: a boot has nobody to hang up on it, and there is no context a pod's
	// startup could pass that should be able to abandon a table copy partway.
	// The convergence budget is the only thing that stops this one.
	return ensureSchema(context.Background(), dsn, logger, opts...)
}

// ensureSchema is EnsureSchema with a caller's context, for the one path that
// has a caller: an operator sitting in front of a convergence they asked for.
// parent is what lets them stop it — Ctrl-C at the terminal reaches the DDL
// through here — and the convergence budget still bounds it either way.
//
// Nothing else should reach this. A convergence is the one write the storage
// bootstrap makes to the database every instance depends on, and a context
// that belongs to a request, a webhook, or an RPC can be cancelled by a
// network blip. Callers on those paths pass context.WithoutCancel so only
// their own deliberate stop can reach it.
func ensureSchema(parent context.Context, dsn string, logger *slog.Logger, opts ...EnsureSchemaOption) error {
	o := newEnsureSchemaOptions(opts...)
	if o.convergenceTimeout < MinConvergenceTimeout {
		return fmt.Errorf("converge storage schema: convergence timeout must be at least %s, got %s", MinConvergenceTimeout, o.convergenceTimeout)
	}
	switch o.dialect {
	case schema.DialectMySQL:
		return ensureMySQLSchema(parent, dsn, logger, o, namedlock.MySQL{})
	case schema.DialectPostgres:
		return ensurePostgresSchema(parent, dsn, logger, o, namedlock.Postgres{})
	default:
		return fmt.Errorf("no schema bootstrapper for storage dialect %q (supported: %q, %q)", o.dialect, schema.DialectMySQL, schema.DialectPostgres)
	}
}

// ensureMySQLSchema applies all embedded MySQL schema files to the database
// using Spirit — the same differ/Spirit mechanism as LocalClient, for
// consistency. locker serializes the bootstrap across pods; MySQL dispatch
// supplies the GET_LOCK/RELEASE_LOCK implementation.
//
// Concurrency-safe across pods: plans first without a lock (read-only diff),
// and returns immediately if no changes are needed and no stale Spirit tables
// are present. When changes or stale Spirit tables are detected, acquires a
// MySQL advisory lock to serialize cleanup and Spirit execution, then re-plans
// under the lock to confirm changes are still needed (another pod may have
// applied them while we waited for the lock).
//
// Destructive statements in the diff — those the plan's linters report an error
// against, which for the storage schema means losing data (DROP TABLE, or an
// ALTER TABLE containing DROP COLUMN) or removing an index — are refused unless
// WithAllowDestructiveSchemaChanges(true) is set. A statement carrying an
// addition beside a drop runs the addition, so a pod never starts missing a
// column its own binary needs. The statements and clauses that were not refused
// apply, and startup proceeds — a deliberate exception to fail-closed, because failing
// here would crash-loop every pod running an older binary during a rolling
// deploy or rollback where storage state was legitimately removed. The
// invariant is that an old binary can never destroy newer schema state: the
// surplus table, column, or index stays in place until an operator opts in.
func ensureMySQLSchema(parent context.Context, dsn string, logger *slog.Logger, o ensureSchemaOptions, locker namedlock.Locker) error {
	ctx, cancel := context.WithTimeout(parent, o.convergenceTimeout)
	defer cancel()

	// Diagnostic preamble: log the actual database target and current state
	// before doing any work. This is critical for debugging bootstrap issues
	// in embedded environments (e.g., Tern) where the DSN is constructed
	// dynamically and we need to confirm we're hitting the right database.
	var storageDatabase string
	if diag, err := diagnoseStorageTarget(ctx, dsn); err != nil {
		logger.Warn("storage target diagnostic failed", "error", err)
	} else {
		storageDatabase = diag.database
		logger.Info("EnsureSchema storage target",
			"hostname", diag.hostname,
			"database", diag.database,
			"existing_tables", diag.tableCount,
			"table_names", diag.tableNames,
		)
	}

	schemaFiles, err := readEmbeddedSchemaFiles()
	if err != nil {
		return err
	}
	logger.Info("loaded embedded storage schema files",
		"namespace_count", len(schemaFiles),
		"file_count", countSchemaFiles(schemaFiles),
		"files", schemaFileNames(schemaFiles),
	)

	// Use a quiet logger for Spirit — its internal operational messages
	// (table locks, checksums, metadata lock release) are noise for
	// EnsureSchema's small bootstrap DDL. SchemaBot logs the actual DDL
	// at info level separately.
	spiritLogger := slog.New(&levelFilterHandler{
		minLevel: slog.LevelWarn,
		handler:  logger.Handler(),
	})
	eng := spirit.New(spirit.Config{Logger: spiritLogger})

	// Fast path: plan without a lock. If no changes, return immediately.
	// This is the common case (99% of deploys) and avoids lock overhead.
	planResult, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:    storageSchemaNamespace,
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{DSN: dsn},
	})
	if err != nil {
		return fmt.Errorf("plan schema: %w", err)
	}
	if planResult.NoChanges {
		staleTables, err := staleSpiritTableNames(ctx, dsn)
		if err != nil {
			return fmt.Errorf("check stale Spirit tables: %w", err)
		}
		if len(staleTables) > 0 {
			logger.Info("stale Spirit tables found with storage schema up-to-date",
				"tables", staleTables,
			)
		} else {
			logger.Info("storage schema up-to-date")
			return nil
		}
	} else {
		// Log what the fast-path plan found before acquiring the lock.
		for _, tc := range planResult.FlatTableChanges() {
			logger.Info("schema change detected (pre-lock)",
				"table", tc.Table,
				"operation", tc.Operation,
				"ddl", tc.DDL,
			)
		}
	}

	if planResult.NoChanges {
		logger.Info("acquiring EnsureSchema advisory lock to clean stale Spirit tables")
	} else {
		logger.Info("acquiring EnsureSchema advisory lock to apply storage schema changes")
	}

	// Changes or stale Spirit tables detected — acquire advisory lock to
	// serialize cleanup and Spirit execution across pods.
	lockConn, err := acquireMySQLEnsureSchemaLock(ctx, dsn, logger, locker, o.convergenceTimeout)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	defer releaseEnsureSchemaLock(ctx, locker, lockConn, logger, schema.DialectMySQL, storageDatabase)

	// Clean up stale Spirit internal tables only while holding the advisory
	// lock. During a rolling deploy, another pod may be actively applying
	// SchemaBot storage DDL; cleaning before the lock can delete that pod's
	// shadow tables and make Spirit cancel with "table definition changed".
	if err := cleanStaleSpiritTables(ctx, dsn, logger); err != nil {
		return fmt.Errorf("clean stale Spirit tables: %w", err)
	}

	// Re-plan under the lock — another pod may have applied the changes
	// while we were waiting for the lock, or stale Spirit tables may have been
	// removed above.
	eng = spirit.New(spirit.Config{Logger: spiritLogger})
	planResult, err = eng.Plan(ctx, &engine.PlanRequest{
		Database:    storageSchemaNamespace,
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{DSN: dsn},
	})
	if err != nil {
		return fmt.Errorf("plan schema: %w", err)
	}
	if planResult.NoChanges {
		logger.Info("storage schema up-to-date")
		return nil
	}

	changes := planResult.Changes
	if !o.allowDestructive {
		allowed, refused := partitionDestructiveChanges(changes)
		for _, r := range refused {
			message, attrs := r.refusalTelemetry()
			logger.Warn(message, attrs...)
			metrics.RecordStorageSchemaDestructiveRefusal(ctx, r.change.Table, ddl.StatementTypeToOp(r.change.Operation))
		}
		if len(allowed) == 0 {
			logger.Warn("all planned storage schema changes are destructive and refused; storage schema left unchanged",
				"database", storageSchemaNamespace,
				"refused_count", len(refused),
			)
			return nil
		}
		changes = allowed
	}

	tableChanges := flatTableChanges(changes)
	logger.Info("applying storage schema changes", "ddl_count", len(tableChanges))
	for _, tc := range tableChanges {
		logger.Info("schema change",
			"table", tc.Table,
			"operation", tc.Operation,
			"ddl", tc.DDL,
		)
	}

	// Apply all DDL via Spirit (starts async schema change)
	applyStart := time.Now()
	_, err = eng.Apply(ctx, &engine.ApplyRequest{
		Database:    storageSchemaNamespace,
		Changes:     changes,
		Credentials: &engine.Credentials{DSN: dsn},
	})
	if err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	// Wait for schema change to complete by polling Progress.
	// Spirit runs asynchronously, so we need to wait for completion.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		progress, err := eng.Progress(ctx, &engine.ProgressRequest{
			Database:    storageSchemaNamespace,
			Credentials: &engine.Credentials{DSN: dsn},
		})
		if err != nil {
			// A cancelled context surfaces here as an opaque driver error
			// ("...context canceled"); name the timeout instead so the cause
			// is clear from the message line alone.
			if ctx.Err() != nil {
				return stopConvergence(ctx, eng, dsn, o.convergenceTimeout, len(tableChanges), logger)
			}
			return fmt.Errorf("check progress: %w", err)
		}

		if progress.State == engine.StateFailed {
			// Surface the cause in an Error log here — callers typically wrap the
			// returned error as a structured attribute, which is easy to miss in
			// log search. Include the DDL count and the underlying message so a
			// failed bootstrap is triageable from the message line alone.
			logger.Error("storage schema change failed; SchemaBot storage will not initialize",
				"database", storageSchemaNamespace,
				"ddl_count", len(tableChanges),
				"error", progress.ErrorMessage,
			)
			return fmt.Errorf("storage schema change failed (%d change(s)): %s", len(tableChanges), progress.ErrorMessage)
		}

		if progress.State.IsTerminal() {
			break
		}

		select {
		case <-ctx.Done():
			return ensureSchemaTimeoutError(ctx, o.convergenceTimeout, len(tableChanges), logger)
		case <-ticker.C:
		}
	}

	logger.Info("storage schema applied successfully",
		"ddl_count", len(tableChanges),
		"duration", time.Since(applyStart),
	)
	return nil
}

// stopConvergence ends a convergence whose context is gone: it releases what
// the schema change engine was holding, then reports why the run ended.
//
// The engine is cancelled rather than dropped, so the shadow table of the
// statement that was in flight goes with it. Leaving it behind would not
// corrupt anything — the next boot's stale-table cleanup reclaims it — but it
// would sit on the storage database until then, holding disk and shadowing a
// table name the next convergence wants to use.
func stopConvergence(ctx context.Context, eng engine.Engine, dsn string, budget time.Duration, ddlCount int, logger *slog.Logger) error {
	// The engine's own cancel path drops the artifacts on a context that
	// outlives this one, so it runs to completion on the context that just
	// ended. A failure here is not the operator's finding — the convergence
	// stopping is — so it is logged with what it leaves behind rather than
	// returned in place of the reason the run ended.
	if _, err := eng.Cancel(ctx, &engine.ControlRequest{
		Database:    storageSchemaNamespace,
		Credentials: &engine.Credentials{DSN: dsn},
	}); err != nil {
		logger.Warn("could not release the stopped storage schema change's artifacts; the next boot's stale-table cleanup will reclaim them",
			"database", storageSchemaNamespace, "error", err)
	}
	return convergenceStopReason(ctx, budget, ddlCount, logger)
}

// convergenceStopReason says which of the two ways a convergence's context
// ended. They leave the same storage state and are opposite findings: a budget
// that fired is a database too slow to converge inside the time it was given,
// and a cancellation is somebody deciding not to wait.
//
// Only the deliberate path can produce the second. A boot's convergence runs
// on a context nothing but its own budget can end, so a pod that reports this
// is reporting something that should not have been possible.
func convergenceStopReason(ctx context.Context, budget time.Duration, ddlCount int, logger *slog.Logger) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ensureSchemaTimeoutError(ctx, budget, ddlCount, logger)
	}
	// Not a failure of the storage, and it names no budget: nothing timed out,
	// and an operator sent looking for a timeout that never fired is an
	// operator not reading the state they actually left. What they need is the
	// next command — statements that finished before the stop stay finished,
	// so "how far did it get" is a question only another plan answers.
	logger.Warn("storage schema convergence stopped by its caller",
		"database", storageSchemaNamespace,
		"ddl_count", ddlCount,
	)
	return fmt.Errorf("storage schema convergence stopped (%d change(s) planned); statements that had already finished are still applied, so plan the storage schema again to see what is left: %w",
		ddlCount, ctx.Err())
}

// ensureSchemaTimeoutError builds and logs the error returned when the
// convergence budget fires before the storage schema change completes. Spirit
// cancels the online DDL mid-apply and storage stays uninitialized, so the
// message names the budget and the most likely cause (a backend throttling the
// online DDL) instead of surfacing a bare "context canceled" from the driver.
//
// budget is the one this convergence actually ran under, not the boot
// constant: a convergence an operator asked for runs under a longer one, and a
// failure reporting a budget it did not have sends them looking for a timeout
// that never fired.
func ensureSchemaTimeoutError(ctx context.Context, budget time.Duration, ddlCount int, logger *slog.Logger) error {
	logger.Error("storage schema change did not complete within the convergence budget; SchemaBot storage will not initialize",
		"database", storageSchemaNamespace,
		"timeout", budget,
		"ddl_count", ddlCount,
	)
	return fmt.Errorf("storage schema change did not complete within %s (%d change(s)); the database may be throttling the online DDL: %w",
		budget, ddlCount, ctx.Err())
}

// refusedStorageChange is storage-schema DDL EnsureSchema refused to execute,
// with the reason the plan classified its statement as unsafe.
type refusedStorageChange struct {
	// change carries the DDL that did not run. That is the whole planned
	// statement, or only its withheld clauses when the clauses that add were
	// split out and executed.
	change  engine.TableChange
	reason  string
	partial bool
	// splitErr records why a gated ALTER could not be reduced to the clauses
	// that only add, which is why the whole statement was refused rather than
	// part of it. It is nil for every other refusal.
	splitErr error
}

// refusalTelemetry returns the operator-facing telemetry for one refusal: the
// warning to log, and its structured attributes. The DDL is what did not run,
// so it narrows to the withheld clauses on a statement that was split.
func (r refusedStorageChange) refusalTelemetry() (message string, attrs []any) {
	message = "refusing destructive storage-schema change; the statement will not run and startup continues — set storage.allow_destructive_schema_changes: true to allow it"
	if r.partial {
		message = "withholding the destructive clauses of a storage-schema change; the statement's additions ran and its removals did not — set storage.allow_destructive_schema_changes: true to run them"
	}
	attrs = []any{
		"database", storageSchemaNamespace,
		"table", r.change.Table,
		"operation", ddl.StatementTypeToOp(r.change.Operation),
		"reason", r.reason,
		"ddl", r.change.DDL,
	}
	if r.splitErr != nil {
		attrs = append(attrs, "split_error", r.splitErr)
	}
	return message, attrs
}

// partitionDestructiveChanges splits planned storage-schema changes into the
// statements safe to execute and the unsafe statements to refuse, on the
// verdict the plan already carries.
//
// That verdict is the engine's, not this package's. Planning runs Spirit's
// linter registry over the diff alongside the live schema it was diffed
// against, and marks a change unsafe when any linter reports an error against
// it. Re-deriving a verdict here from the statement text would answer a
// narrower question than the plan already answered, and would answer it without
// the live schema — so a statement the plan knows is unsafe would execute
// because this package's vocabulary did not recognize it. Whatever the registry
// errors on is what the bootstrap refuses, which is how the operator-facing
// plan surface and the boot that follows it stay in agreement.
//
// The bootstrap's exposure is availability, not data loss alone. An index the
// fleet's live queries plan around is as load-bearing as a column: dropping it
// destroys no rows and completes in milliseconds because it is metadata-only,
// and it can still take the storage database down. The registry's error set
// spans both, so the same gate covers both.
//
// The verdict is per statement and Spirit's diff emits one combined ALTER per
// table, so a gated statement is reduced to the clauses that only add a schema
// object: those execute and the rest is withheld. The bootstrap exists to give
// the starting binary the tables, columns, and indexes it needs to run at all,
// and withholding an addition because it was bundled with a removal leaves a
// pod serving traffic against storage missing a column its own queries name.
// Nothing is gained by the bundling: the addition is the same statement's other
// half, not a consequence of the removal.
//
// The reduction is structural, not a second safety verdict. Which clauses add
// is a closed question about clause shapes (see ddl.SplitAdditiveAlter);
// whether the statement is gated at all stays the registry's answer. A gated
// statement that cannot be reduced — a DROP TABLE has no clauses, and an ALTER
// whose every clause is withheld has nothing left — is refused whole, as is one
// the parser cannot partition, which is the fail-closed direction.
//
// The diff emits an unsafe statement when the live storage database holds a
// table, column, or index the starting binary's embedded schema does not
// declare. During a rolling deploy or rollback that surplus state
// usually belongs to a newer binary, not to a removal the operator intended.
func partitionDestructiveChanges(changes []engine.SchemaChange) (allowed []engine.SchemaChange, refused []refusedStorageChange) {
	for _, sc := range changes {
		kept := sc
		kept.TableChanges = nil
		for _, tc := range sc.TableChanges {
			if !tc.IsUnsafe {
				kept.TableChanges = append(kept.TableChanges, tc)
				continue
			}
			// The caller logs every refusal below with its DDL and reason.
			additive, withheld, err := ddl.SplitAdditiveAlter(tc.DDL)
			switch {
			case errors.Is(err, ddl.ErrNotAlterTable):
				// A statement with no clauses — a DROP TABLE — is all or nothing.
				refused = append(refused, refusedStorageChange{change: tc, reason: tc.UnsafeReason})
				continue
			case err != nil:
				refused = append(refused, refusedStorageChange{change: tc, reason: tc.UnsafeReason, splitErr: err})
				continue
			case additive == "":
				// Every clause was withheld, so the statement is refused whole
				// rather than reported as a split that ran nothing.
				refused = append(refused, refusedStorageChange{change: tc, reason: tc.UnsafeReason})
				continue
			}

			addition := tc
			addition.DDL = additive
			addition.IsUnsafe = false
			addition.UnsafeReason = ""
			kept.TableChanges = append(kept.TableChanges, addition)

			withheldChange := tc
			withheldChange.DDL = withheld
			refused = append(refused, refusedStorageChange{
				change:  withheldChange,
				reason:  tc.UnsafeReason,
				partial: true,
			})
		}
		if len(kept.TableChanges) > 0 {
			allowed = append(allowed, kept)
		}
	}
	return allowed, refused
}

// flatTableChanges returns all table changes across the given schema changes.
func flatTableChanges(changes []engine.SchemaChange) []engine.TableChange {
	var tables []engine.TableChange
	for _, sc := range changes {
		tables = append(tables, sc.TableChanges...)
	}
	return tables
}

func staleSpiritTableNames(ctx context.Context, dsn string) ([]string, error) {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	// Load all tables here. table.WithoutUnderscoreTables would hide the
	// Spirit internal tables this path needs to detect.
	tables, err := table.LoadSchemaFromDB(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}

	var names []string
	for _, t := range tables {
		if ddl.IsSpiritInternalTable(t.Name) {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// readEmbeddedSchemaFiles reads the embedded MySQL schema files into a SchemaFiles map.
func readEmbeddedSchemaFiles() (schema.SchemaFiles, error) {
	entries, err := schema.MySQLFS.ReadDir("mysql")
	if err != nil {
		return nil, fmt.Errorf("read schema directory: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("read schema directory: no embedded schema files found in mysql/")
	}

	files := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := schema.MySQLFS.ReadFile("mysql/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read schema file %s: %w", entry.Name(), err)
		}
		files[entry.Name()] = string(content)
	}

	return schema.SchemaFiles{
		storageSchemaNamespace: &schema.Namespace{Files: files},
	}, nil
}

func countSchemaFiles(schemaFiles schema.SchemaFiles) int {
	total := 0
	for _, namespace := range schemaFiles {
		if namespace == nil {
			continue
		}
		total += len(namespace.Files)
	}
	return total
}

func schemaFileNames(schemaFiles schema.SchemaFiles) []string {
	names := make([]string, 0)
	for namespaceName, namespace := range schemaFiles {
		if namespace == nil {
			continue
		}
		for fileName := range namespace.Files {
			names = append(names, namespaceName+"/"+fileName)
		}
	}
	sort.Strings(names)
	return names
}

type storageDiagnostic struct {
	hostname   string
	database   string
	tableCount int
	tableNames []string
}

// diagnoseStorageTarget connects to the DSN and queries the actual database
// identity and existing table state. Used for diagnostic logging only.
func diagnoseStorageTarget(ctx context.Context, dsn string) (*storageDiagnostic, error) {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	var diag storageDiagnostic
	if err := db.QueryRowContext(ctx, "SELECT @@hostname, DATABASE()").Scan(&diag.hostname, &diag.database); err != nil {
		return nil, fmt.Errorf("query hostname and database: %w", err)
	}

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("show tables: %w", err)
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		diag.tableNames = append(diag.tableNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tables: %w", err)
	}
	diag.tableCount = len(diag.tableNames)

	return &diag, nil
}

// ensureSchemaLockName is the advisory lock name used to serialize
// EnsureSchema across concurrent pod startups.
const ensureSchemaLockName = "schemabot_ensure_schema"

// acquireMySQLEnsureSchemaLock acquires a session-scoped advisory lock to
// serialize EnsureSchema across pods. It serves the MySQL bootstrap flow only:
// the connection is opened with the Go MySQL driver via mysqlconn, so another
// dialect's bootstrapper needs its own lock helper alongside its
// namedlock.Locker. Returns the connection holding the lock — the lock is
// released when the connection is closed.
func acquireMySQLEnsureSchemaLock(ctx context.Context, dsn string, logger *slog.Logger, locker namedlock.Locker, wait time.Duration) (*sql.Conn, error) {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	// Advisory locks are per-connection, so we need a dedicated connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}

	// Wait up to this convergence's whole budget for the lock — a trailing pod
	// must outwait the leader's schema change, after which it re-plans and finds
	// no changes. A pod waits out a boot; it does not wait out an operator's
	// longer convergence, whose budget is its own and larger. That pod gives up
	// at its own ceiling and reports the contention by name, which is the
	// intended trade: converge ahead of a roll, not during one.
	acquired, err := locker.Acquire(ctx, conn, ensureSchemaLockName, wait)
	if err != nil {
		utils.CloseAndLog(conn)
		// The overall EnsureSchema deadline expires before the server-side
		// lock wait (which starts later, with the same duration), so a
		// contended timeout surfaces here as a context error — name the
		// likely cause instead of reporting only the raw cancellation.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("timed out waiting for advisory lock %q (another pod may be running EnsureSchema): %w", ensureSchemaLockName, err)
		}
		if ctx.Err() != nil {
			// Stopped by the operator watching it, before the lock was ever
			// taken. Nothing ran, and saying so is the whole answer: the run
			// they stopped changed nothing, and the one they were queued
			// behind is still going.
			return nil, fmt.Errorf("stopped while waiting for advisory lock %q; this run changed nothing, and the convergence it was queued behind is still running: %w", ensureSchemaLockName, err)
		}
		return nil, fmt.Errorf("acquire advisory lock: %w", err)
	}
	if !acquired {
		utils.CloseAndLog(conn)
		return nil, fmt.Errorf("timed out waiting for advisory lock %q (another pod may be running EnsureSchema)", ensureSchemaLockName)
	}

	logger.Info("acquired EnsureSchema advisory lock")
	return conn, nil
}

// ensureSchemaLockReleaseTimeout bounds the release issued as the bootstrap
// returns, so a storage database that has become unreachable delays startup by
// a bounded amount rather than by whatever the driver would wait.
const ensureSchemaLockReleaseTimeout = 10 * time.Second

// releaseEnsureSchemaLock releases the bootstrap advisory lock and closes the
// connection holding it. Closing alone already drops the lock — the pool behind
// this connection was closed at acquire time, so closing ends the session — but
// the release runs first for its answer: it is the only reading of whether the
// session SchemaBot converged storage under is still the session that took the
// lock.
//
// A negative answer is not a routine "the lock was not held". It means this pod
// converged storage without the exclusion the lock was supposed to give it, so
// another pod may have been converging the same schema alongside it, and it is
// reported with the identifiers needed to find the pod and database involved.
// It runs on a context detached from the bootstrap deadline, which is usually
// spent by the time a release is due.
func releaseEnsureSchemaLock(ctx context.Context, locker namedlock.Locker, conn *sql.Conn, logger *slog.Logger, dialect schema.Dialect, database string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ensureSchemaLockReleaseTimeout)
	defer cancel()

	released, err := locker.Release(releaseCtx, conn, ensureSchemaLockName)
	switch {
	case err != nil:
		logger.Warn("failed to release the EnsureSchema advisory lock",
			"lock", ensureSchemaLockName, "dialect", dialect, "database", database, "error", err)
	case !released:
		logger.Warn("the EnsureSchema advisory lock was not held at release; storage was converged without cross-instance exclusion and another instance may have been converging it at the same time",
			"lock", ensureSchemaLockName, "dialect", dialect, "database", database)
	}

	utils.CloseAndLog(conn)
}

// cleanStaleSpiritTables drops any Spirit internal tables left behind by a
// previous interrupted schema change on the SchemaBot storage database. Callers
// must hold the EnsureSchema advisory lock before invoking this helper. These
// are temporary tables (_tablename_new, _tablename_old, _tablename_chkpnt,
// _spirit_sentinel, _spirit_checkpoint) that Spirit normally cleans up after
// cutover. If a pod is killed mid-apply, they persist until the next startup.
//
// This is safe because EnsureSchema only targets SchemaBot's own storage
// database, and Spirit runs in-process — when the pod restarts, there is no
// active Spirit runner to resume. Spirit's checkpoint-based resume only works
// within a single runner lifetime. Cleaning these tables lets Spirit start
// fresh without logging confusing "successfully dropped old table" messages.
//
// This must NOT be used on target databases where user schema changes may be
// in progress or resumable.
func cleanStaleSpiritTables(ctx context.Context, dsn string, logger *slog.Logger) error {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	// Load all tables here. table.WithoutUnderscoreTables would hide the
	// Spirit internal tables this cleanup path needs to drop.
	tables, err := table.LoadSchemaFromDB(ctx, db)
	if err != nil {
		return fmt.Errorf("load schema: %w", err)
	}

	var staleCount int
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}
	logger.Info("cleanStaleSpiritTables loaded schema",
		"total_tables", len(tables),
		"table_names", tableNames,
	)

	for _, t := range tables {
		if !ddl.IsSpiritInternalTable(t.Name) {
			continue
		}
		staleCount++
		logger.Info("cleaning up stale Spirit temporary table from previous schema change",
			"table", t.Name,
		)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", t.Name)); err != nil {
			return fmt.Errorf("drop stale Spirit table %s: %w", t.Name, err)
		}
	}

	if staleCount == 0 {
		logger.Info("no stale Spirit tables found")
	} else {
		logger.Info("cleaned stale Spirit tables", "dropped", staleCount)
	}

	return nil
}

// levelFilterHandler wraps an slog.Handler and drops records below minLevel.
// Used to suppress Spirit's info-level operational logs during EnsureSchema.
type levelFilterHandler struct {
	minLevel slog.Level
	handler  slog.Handler
}

func (h *levelFilterHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.minLevel && h.handler.Enabled(ctx, level)
}

func (h *levelFilterHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.handler.Handle(ctx, r)
}

func (h *levelFilterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelFilterHandler{minLevel: h.minLevel, handler: h.handler.WithAttrs(attrs)}
}

func (h *levelFilterHandler) WithGroup(name string) slog.Handler {
	return &levelFilterHandler{minLevel: h.minLevel, handler: h.handler.WithGroup(name)}
}
