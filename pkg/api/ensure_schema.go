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
	"github.com/block/schemabot/pkg/schema"
)

// EnsureSchemaTimeout is the maximum time EnsureSchema will wait for schema changes to complete.
// Schema changes on SchemaBot's own storage tables are small DDL on metadata tables.
const EnsureSchemaTimeout = 1 * time.Minute

// EnsureSchema applies all embedded MySQL schema files to the database using Spirit.
// It is idempotent - no changes are made if the schema is already up-to-date.
// Uses the same differ/Spirit mechanism as LocalClient for consistency.
//
// Concurrency-safe across pods: plans first without a lock (read-only diff),
// and returns immediately if no changes are needed. When changes are detected,
// acquires a MySQL advisory lock to serialize Spirit execution, then re-plans
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

	// Clean up stale Spirit internal tables before planning. These are
	// leftover from a previous interrupted schema change (e.g., pod killed
	// mid-apply). Must happen before Plan so the diff sees the actual table
	// state without stale artifacts like _tablename_new or _tablename_old.
	if err := cleanStaleSpiritTables(ctx, dsn, logger); err != nil {
		return fmt.Errorf("clean stale Spirit tables: %w", err)
	}

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
		logger.Info("storage schema up-to-date")
		return nil
	}

	// Log what the fast-path plan found before acquiring the lock.
	for _, tc := range planResult.FlatTableChanges() {
		logger.Info("schema change detected (pre-lock)",
			"table", tc.Table,
			"operation", tc.Operation,
			"ddl", tc.DDL,
		)
	}

	// Changes detected — acquire advisory lock to serialize across pods.
	lockConn, err := acquireEnsureSchemaLock(ctx, dsn, logger)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	defer utils.CloseAndLog(lockConn)

	// Re-plan under the lock — another pod may have applied the changes
	// while we were waiting for the lock.
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
		logger.Info("storage schema up-to-date (applied by another pod)")
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
			return fmt.Errorf("check progress: %w", err)
		}

		if progress.State == engine.StateFailed {
			return fmt.Errorf("schema change failed: %s", progress.ErrorMessage)
		}

		if progress.State.IsTerminal() {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	logger.Info("storage schema applied successfully")
	return nil
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
	db, err := sql.Open("mysql", dsn)
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
	db, err := sql.Open("mysql", dsn)
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
	// Use the full EnsureSchemaTimeout as the lock wait — if another pod is
	// running EnsureSchema, it should finish well within a minute.
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
// previous interrupted schema change on the SchemaBot storage database. These
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
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

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
