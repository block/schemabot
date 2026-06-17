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
	"github.com/block/schemabot/pkg/mysqlconn"
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
const EnsureSchemaTimeout = 5 * time.Minute

// EnsureSchema applies all embedded MySQL schema files to the database using Spirit.
// It is idempotent - no changes are made if the schema is already up-to-date.
// Uses the same differ/Spirit mechanism as LocalClient for consistency.
//
// Concurrency-safe across pods: plans first without a lock (read-only diff),
// and returns immediately if no changes are needed and no stale Spirit tables
// are present. When changes or stale Spirit tables are detected, acquires a
// MySQL advisory lock to serialize cleanup and Spirit execution, then re-plans
// under the lock to confirm changes are still needed (another pod may have
// applied them while we waited for the lock).
func EnsureSchema(dsn string, logger *slog.Logger) error {
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
	lockConn, err := acquireEnsureSchemaLock(ctx, dsn, logger)
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

	tableChanges := planResult.FlatTableChanges()
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
		Changes:     planResult.Changes,
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
		"timeout", EnsureSchemaTimeout,
		"ddl_count", ddlCount,
	)
	return fmt.Errorf("storage schema change did not complete within %s (%d change(s)); the database may be throttling the online DDL: %w",
		EnsureSchemaTimeout, ddlCount, ctx.Err())
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

// ensureSchemaLockName is the MySQL advisory lock name used to serialize
// EnsureSchema across concurrent pod startups.
const ensureSchemaLockName = "schemabot_ensure_schema"

// acquireEnsureSchemaLock acquires a MySQL advisory lock to serialize
// EnsureSchema across pods. Returns the connection holding the lock — the
// lock is released when the connection is closed.
func acquireEnsureSchemaLock(ctx context.Context, dsn string, logger *slog.Logger) (*sql.Conn, error) {
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

	// GET_LOCK returns 1 on success, 0 on timeout, NULL on error.
	// Wait up to the full timeout for the lock — a trailing pod must outwait the
	// leader's schema change, after which it re-plans and finds no changes.
	var result sql.NullInt64
	err = conn.QueryRowContext(ctx,
		"SELECT GET_LOCK(?, ?)", ensureSchemaLockName, int(EnsureSchemaTimeout.Seconds()),
	).Scan(&result)
	if err != nil {
		utils.CloseAndLog(conn)
		return nil, fmt.Errorf("GET_LOCK: %w", err)
	}
	if !result.Valid || result.Int64 != 1 {
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
