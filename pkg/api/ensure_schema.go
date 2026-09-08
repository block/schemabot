package api

import (
	"context"
	"database/sql"
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

// EnsureSchemaTimeout bounds the whole EnsureSchema operation: acquiring the
// advisory lock, planning, and applying the storage schema change to
// completion. SchemaBot's storage tables are small, but Spirit applies an
// *online* DDL, and on Aurora that carries fixed overhead (binlog subscription,
// checksum, cutover MDL, and throttler poll loops) that can exceed a minute even
// for a tiny table. Trailing pods also wait up to this long on the advisory lock
// while the leader applies, then see no changes. Too short a value cancels the
// apply mid-copy ("failed to read chunk data: context canceled") and leaves
// storage uninitialized.
//
// The same constant bounds the PostgreSQL bootstrap flow — its existence
// checks, advisory-lock wait, and transactional table creation — so tuning it
// for MySQL/Spirit reasons also changes how long a PostgreSQL pod waits. It is
// also the base of a fourth derivation: postgresBootstrapDDLStatementTimeout
// subtracts a margin from it to bound one convergence DDL statement
// server-side, so lowering this shortens that budget too, down to its own
// floor.
const EnsureSchemaTimeout = 5 * time.Minute

// EnsureSchemaOption customizes EnsureSchema behavior.
type EnsureSchemaOption func(*ensureSchemaOptions)

type ensureSchemaOptions struct {
	allowDestructive bool
	dialect          schema.Dialect
	// postgresStatementTimeout bounds a single ordinary query on the
	// PostgreSQL bootstrap's connection. Zero disables the budget explicitly;
	// negative means "not set", leaving the platform's ambient value in place.
	// It defaults to DefaultPostgresStatementTimeout rather than to "not set",
	// so a caller that never considered the question still bootstraps under a
	// budget SchemaBot states instead of one the platform imposed.
	postgresStatementTimeout time.Duration
}

// WithAllowDestructiveSchemaChanges controls whether EnsureSchema may execute
// destructive DDL (DROP TABLE, or an ALTER TABLE containing DROP COLUMN)
// against the storage database. It defaults to false: destructive statements
// are refused while the remaining non-destructive statements still apply. A
// mixed ALTER TABLE is split so its safe clauses execute and only the
// destructive clauses — plus any clause that cannot run without them, such
// as the ADD PRIMARY KEY behind a refused DROP PRIMARY KEY — are refused.
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
// postgresBootstrapDDLStatementTimeout, and the advisory-lock wait runs with
// no statement budget at all. A zero duration disables the budget explicitly
// rather than inheriting the platform's, and a negative one leaves the
// platform's value in place. Unset, the budget is
// DefaultPostgresStatementTimeout. Wire this from
// PostgresConfig.StatementTimeoutOrDefault.
func WithPostgresStatementTimeout(d time.Duration) EnsureSchemaOption {
	return func(o *ensureSchemaOptions) { o.postgresStatementTimeout = d }
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
	o := ensureSchemaOptions{
		dialect:                  schema.DialectMySQL,
		postgresStatementTimeout: DefaultPostgresStatementTimeout,
	}
	for _, opt := range opts {
		opt(&o)
	}
	switch o.dialect {
	case schema.DialectMySQL:
		return ensureMySQLSchema(dsn, logger, o, namedlock.MySQL{})
	case schema.DialectPostgres:
		return ensurePostgresSchema(dsn, logger, o, namedlock.Postgres{})
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
// Destructive statements in the diff (DROP TABLE, or an ALTER TABLE containing
// DROP COLUMN) are refused unless WithAllowDestructiveSchemaChanges(true) is
// set. A mixed ALTER TABLE is split so its safe clauses (an ADD COLUMN the
// starting binary needs) still execute and only its destructive clauses are
// refused. The remaining non-destructive statements apply,
// and startup proceeds — a deliberate exception to fail-closed, because
// failing here would crash-loop every pod running an older binary during a
// rolling deploy or rollback where a storage table or column was legitimately
// removed. The invariant is that an old binary can never destroy newer schema
// state: the surplus table or column stays in place until an operator opts in.
func ensureMySQLSchema(dsn string, logger *slog.Logger, o ensureSchemaOptions, locker namedlock.Locker) error {
	ctx, cancel := context.WithTimeout(context.Background(), EnsureSchemaTimeout)
	defer cancel()

	// Diagnostic preamble: log the actual database target and current state
	// before doing any work. This is critical for debugging bootstrap issues
	// in embedded environments (e.g., Tern) where the DSN is constructed
	// dynamically and we need to confirm we're hitting the right database.
	if diag, err := diagnoseStorageTarget(ctx, dsn); err != nil {
		logger.Warn("storage target diagnostic failed", "error", err)
	} else {
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
		Database:    "schemabot",
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
	lockConn, err := acquireMySQLEnsureSchemaLock(ctx, dsn, logger, locker)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	defer utils.CloseAndLog(lockConn)

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
		Database:    "schemabot",
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
		allowed, refused, err := partitionDestructiveChanges(changes)
		if err != nil {
			return fmt.Errorf("classify storage schema changes: %w", err)
		}
		for _, r := range refused {
			scope, message, attrs := r.refusalTelemetry()
			logger.Warn(message, attrs...)
			metrics.RecordStorageSchemaDestructiveRefusal(ctx, r.change.Table, ddl.StatementTypeToOp(r.change.Operation), scope)
		}
		if len(allowed) == 0 {
			logger.Warn("all planned storage schema changes are destructive and refused; storage schema left unchanged",
				"database", "schemabot",
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
		Database:    "schemabot",
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
			Database:    "schemabot",
			Credentials: &engine.Credentials{DSN: dsn},
		})
		if err != nil {
			// A cancelled context surfaces here as an opaque driver error
			// ("...context canceled"); name the timeout instead so the cause
			// is clear from the message line alone.
			if ctx.Err() != nil {
				return ensureSchemaTimeoutError(ctx, len(tableChanges), logger)
			}
			return fmt.Errorf("check progress: %w", err)
		}

		if progress.State == engine.StateFailed {
			// Surface the cause in an Error log here — callers typically wrap the
			// returned error as a structured attribute, which is easy to miss in
			// log search. Include the DDL count and the underlying message so a
			// failed bootstrap is triageable from the message line alone.
			logger.Error("storage schema change failed; SchemaBot storage will not initialize",
				"database", "schemabot",
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
			return ensureSchemaTimeoutError(ctx, len(tableChanges), logger)
		case <-ticker.C:
		}
	}

	logger.Info("storage schema applied successfully",
		"ddl_count", len(tableChanges),
		"duration", time.Since(applyStart),
	)
	return nil
}

// ensureSchemaTimeoutError builds and logs the error returned when
// EnsureSchemaTimeout fires before the storage schema change completes. Spirit
// cancels the online DDL mid-apply and storage stays uninitialized, so the
// message names the timeout and the most likely cause (a backend throttling the
// online DDL) instead of surfacing a bare "context canceled" from the driver.
func ensureSchemaTimeoutError(ctx context.Context, ddlCount int, logger *slog.Logger) error {
	logger.Error("storage schema change did not complete before EnsureSchemaTimeout; SchemaBot storage will not initialize",
		"database", "schemabot",
		"timeout", EnsureSchemaTimeout,
		"ddl_count", ddlCount,
	)
	return fmt.Errorf("storage schema change did not complete within %s (%d change(s)); the database may be throttling the online DDL: %w",
		EnsureSchemaTimeout, ddlCount, ctx.Err())
}

// refusedStorageChange is a planned storage-schema statement EnsureSchema
// refused to execute, with the reason it was classified as unsafe.
type refusedStorageChange struct {
	change engine.TableChange
	reason string
	// splitFrom is the combined ALTER TABLE statement the refused clauses
	// were split out of; empty when the whole statement was refused.
	splitFrom string
	// splitErr is the error that prevented partitioning an unsafe ALTER into
	// safe and destructive clauses; when set, the statement was refused whole
	// so no clause of it executed.
	splitErr error
}

// refusalTelemetry returns the operator-facing telemetry for one refusal: the
// metrics scope saying whether any of the statement still ran, the warning to
// log, and its structured attributes. A split refusal carries the combined
// ALTER its destructive clauses were split out of; a whole refusal of an
// unsplittable ALTER carries the error that prevented the split.
func (r refusedStorageChange) refusalTelemetry() (scope, message string, attrs []any) {
	attrs = []any{
		"database", "schemabot",
		"table", r.change.Table,
		"operation", ddl.StatementTypeToOp(r.change.Operation),
		"reason", r.reason,
		"ddl", r.change.DDL,
	}
	switch {
	case r.splitErr != nil:
		return metrics.StorageSchemaRefusalWhole,
			"refusing an unsafe storage-schema ALTER whole because its clauses could not be partitioned; no clause of it will run and startup continues — set storage.allow_destructive_schema_changes: true to allow it",
			append(attrs, "split_error", r.splitErr)
	case r.splitFrom != "":
		return metrics.StorageSchemaRefusalSplit,
			"refusing destructive clauses of a mixed storage-schema ALTER; the destructive clauses will not run, the safe clauses still execute, and startup continues — set storage.allow_destructive_schema_changes: true to allow them",
			append(attrs, "split_from_ddl", r.splitFrom)
	default:
		return metrics.StorageSchemaRefusalWhole,
			"refusing destructive storage-schema change; the statement will not run and startup continues — set storage.allow_destructive_schema_changes: true to allow it",
			attrs
	}
}

// partitionDestructiveChanges splits planned storage-schema changes into the
// statements safe to execute and the unsafe statements to refuse, using
// Spirit's unsafe vocabulary (ddl.UnsafeStatement). The vocabulary is
// Spirit's, not a local list: every statement its UnsafeLinter flags as
// destroying data — dropping a table, column, partition, or primary key,
// truncating or coalescing partitions, discarding a tablespace — is refused,
// while structural statements that lose nothing (DROP INDEX, renames) are
// allowed. A statement Spirit's parser cannot classify fails startup rather
// than executing unclassified: a classification failure can land on a
// statement the starting binary needs — an additive ALTER in a syntax a
// bumped parser trips on — and skipping it would trade a loud startup
// failure for a missing column at query time. The Spirit diff emits an
// unsafe statement when the live storage database holds a table or column the
// starting binary's embedded schema does not declare — during a rolling
// deploy or rollback that surplus state usually belongs to a newer binary,
// not to a removal the operator intended. Spirit's diff emits one combined
// ALTER per table, so an unsafe ALTER that also carries additive clauses is
// split (ddl.SplitUnsafeAlter): the safe clauses the starting binary needs
// still execute, and only the destructive clauses are refused. Clauses that
// cannot run without a refused clause (the ADD PRIMARY KEY half of a
// primary-key change) are refused with it, so the executed remainder is
// always independently runnable. A split that cannot be performed falls back
// to refusing the statement whole rather than failing startup — the opposite
// disposition from a classification failure, because a split failure only
// ever happens on a statement already classified unsafe: the starting binary
// demonstrably does not need it, so refusing it whole is the established
// answer, and it executes strictly less than any split would, so the failed
// split cannot widen what the bootstrap executes. Startup survives it, where
// failing would crash-loop every pod whose pending ALTER the splitter cannot
// partition.
func partitionDestructiveChanges(changes []engine.SchemaChange) (allowed []engine.SchemaChange, refused []refusedStorageChange, err error) {
	for _, sc := range changes {
		kept := sc
		kept.TableChanges = nil
		for _, tc := range sc.TableChanges {
			unsafe, reason, err := ddl.UnsafeStatement(tc.DDL)
			if err != nil {
				return nil, nil, fmt.Errorf("classify storage schema change for table %q (%s): %w", tc.Table, tc.DDL, err)
			}
			if !unsafe {
				kept.TableChanges = append(kept.TableChanges, tc)
				continue
			}
			if tc.Operation == ddl.StatementAlterTable {
				safeDDL, unsafeDDL, splitErr := ddl.SplitUnsafeAlter(tc.DDL)
				if splitErr != nil {
					// Refusing the statement whole executes strictly less
					// than any split would, so the failed split cannot widen
					// what the bootstrap executes — and startup proceeds,
					// which is the reason this path exists. The caller logs
					// the fallback with the split error.
					refused = append(refused, refusedStorageChange{change: tc, reason: reason, splitErr: splitErr})
					continue
				}
				if safeDDL != "" {
					// A mixed ALTER: execute the clauses that lose nothing and
					// refuse only the destructive remainder. The caller logs the
					// refusal with the destructive clauses' exact DDL.
					keptChange := tc
					keptChange.DDL = safeDDL
					kept.TableChanges = append(kept.TableChanges, keptChange)
					refusedChange := tc
					refusedChange.DDL = unsafeDDL
					refused = append(refused, refusedStorageChange{change: refusedChange, reason: reason, splitFrom: tc.DDL})
					continue
				}
			}
			// The caller logs each refusal with the exact DDL and reason.
			refused = append(refused, refusedStorageChange{change: tc, reason: reason})
		}
		if len(kept.TableChanges) > 0 {
			allowed = append(allowed, kept)
		}
	}
	return allowed, refused, nil
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
		"schemabot": &schema.Namespace{Files: files},
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
func acquireMySQLEnsureSchemaLock(ctx context.Context, dsn string, logger *slog.Logger, locker namedlock.Locker) (*sql.Conn, error) {
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

	// Wait up to the full timeout for the lock — a trailing pod must outwait the
	// leader's schema change, after which it re-plans and finds no changes.
	acquired, err := locker.Acquire(ctx, conn, ensureSchemaLockName, EnsureSchemaTimeout)
	if err != nil {
		utils.CloseAndLog(conn)
		// The overall EnsureSchema deadline expires before the server-side
		// lock wait (which starts later, with the same duration), so a
		// contended timeout surfaces here as a context error — name the
		// likely cause instead of reporting only the raw cancellation.
		if ctx.Err() != nil {
			return nil, fmt.Errorf("timed out waiting for advisory lock %q (another pod may be running EnsureSchema): %w", ensureSchemaLockName, err)
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
