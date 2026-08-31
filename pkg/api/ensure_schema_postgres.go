package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
)

// ensurePostgresSchema converges additive drift in SchemaBot's PostgreSQL
// storage schema. It creates missing tables, columns, and standalone indexes.
// Extra objects are tolerated for binary rollback, and column comparison is
// presence-only: type, length, and nullability drift are outside its scope.
//
// The flow never destroys or alters existing objects, so the destructive-change
// refusal that guards the MySQL flow does not apply here.
//
// Concurrency-safe across pods: discovers drift without a lock, acquires the
// PostgreSQL advisory lock only when needed, then re-discovers under the lock.
// Each table's changes execute in one transaction.
func ensurePostgresSchema(dsn string, logger *slog.Logger, locker namedlock.Locker) error {
	ctx, cancel := context.WithTimeout(context.Background(), EnsureSchemaTimeout)
	defer cancel()

	tables, files, err := readEmbeddedPostgresSchemaFiles()
	if err != nil {
		return err
	}

	db, err := postgresconn.Open(dsn)
	if err != nil {
		return fmt.Errorf("open storage database: %w", err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping storage database: %w", err)
	}

	// Diagnostic preamble: log the actual database target before doing any
	// work, so a bootstrap against the wrong database is visible from the
	// startup logs alone. Best-effort like the MySQL flow — a failed
	// diagnostic must not abort the bootstrap. current_schema() is NULL when
	// the role's search_path resolves to no schema, so coalesce it rather
	// than failing the scan; the real failure surfaces at table creation
	// with the server's error.
	var database, schemaName string
	if err := db.QueryRowContext(ctx, "SELECT current_database(), COALESCE(current_schema(), '')").Scan(&database, &schemaName); err != nil {
		logger.Warn("storage target diagnostic failed", "error", err)
	} else {
		logger.Info("EnsureSchema storage target",
			"dialect", schema.DialectPostgres,
			"database", database,
			"schema", schemaName,
			"embedded_tables", len(tables),
		)
	}

	// Fast path: discover drift without a lock. This is the common case and
	// avoids advisory-lock overhead when the schema is already converged.
	drift, err := postgresSchemaDriftFor(ctx, db, tables, files)
	if err != nil {
		return fmt.Errorf("inspect storage schema: %w", err)
	}
	if len(drift) == 0 {
		if err := verifyAndLogPostgresSchemaShape(ctx, db, tables, files, logger, database, schemaName); err != nil {
			return fmt.Errorf("validate existing storage tables: %w", err)
		}
		logger.Info("storage schema up-to-date", "database", database)
		return nil
	}
	logger.Info("storage schema drift detected (pre-lock); acquiring EnsureSchema advisory lock to converge it",
		"database", database,
		"change_count", len(drift),
	)

	lockConn, err := acquirePostgresEnsureSchemaLock(ctx, dsn, logger, locker)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	defer utils.CloseAndLog(lockConn)

	// Re-check under the lock — another pod may have converged the schema while
	// this pod waited.
	drift, err = postgresSchemaDriftFor(ctx, db, tables, files)
	if err != nil {
		return fmt.Errorf("inspect storage schema under lock: %w", err)
	}
	if len(drift) == 0 {
		if err := verifyAndLogPostgresSchemaShape(ctx, db, tables, files, logger, database, schemaName); err != nil {
			return fmt.Errorf("validate storage tables after lock: %w", err)
		}
		logger.Info("storage schema up-to-date", "database", database)
		return nil
	}

	applyStart := time.Now()
	for _, table := range tables {
		changes := drift[table]
		if len(changes) == 0 {
			continue
		}
		if err := applyPostgresTableChanges(ctx, db, table, changes, logger); err != nil {
			return fmt.Errorf("converge storage table %q: %w", table, err)
		}
	}
	logger.Info("storage schema applied successfully",
		"database", database,
		"change_count", len(drift),
		"duration", time.Since(applyStart),
	)
	if err := verifyAndLogPostgresSchemaShape(ctx, db, tables, files, logger, database, schemaName); err != nil {
		return fmt.Errorf("validate converged storage tables: %w", err)
	}
	return nil
}

type postgresSchemaChange struct {
	operation string
	object    string
	ddl       string
	manual    bool
}

type postgresSchemaDrift map[string][]postgresSchemaChange

func postgresSchemaDriftFor(ctx context.Context, db *sql.DB, tables []string, files map[string]string) (postgresSchemaDrift, error) {
	missingTables, err := missingPostgresTables(ctx, db, tables)
	if err != nil {
		return nil, err
	}
	missingTable := make(map[string]bool, len(missingTables))
	for _, table := range missingTables {
		missingTable[table] = true
	}

	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return nil, fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}
	drift := make(postgresSchemaDrift)
	for _, table := range tables {
		statements, err := parser.Split(files[table])
		if err != nil {
			return nil, fmt.Errorf("split schema file for table %q: %w", table, err)
		}
		if len(statements) == 0 {
			return nil, fmt.Errorf("schema file for table %q has no statements", table)
		}
		if missingTable[table] {
			drift[table] = []postgresSchemaChange{{operation: "create_table", object: table, ddl: files[table]}}
			continue
		}

		expectedColumns, err := parser.CreateTableColumns(statements[0])
		if err != nil {
			return nil, fmt.Errorf("extract expected columns for table %q: %w", table, err)
		}
		existingColumns, err := postgresTableColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		for _, column := range expectedColumns {
			if existingColumns[column] {
				continue
			}
			statement, err := ddl.SynthesizePostgresAddColumn(statements[0], column)
			if err != nil {
				return nil, fmt.Errorf("synthesize ADD COLUMN for %q.%q: %w", table, column, err)
			}
			drift[table] = append(drift[table], postgresSchemaChange{
				operation: "add_column",
				object:    column,
				ddl:       statement,
				manual:    postgresAddColumnRequiresManualBackfill(statement),
			})
		}

		existingIndexes, err := postgresTableIndexes(ctx, db, table)
		if err != nil {
			return nil, err
		}
		for _, statement := range statements[1:] {
			indexName, indexTable, unique, err := parser.CreateIndex(statement)
			if err != nil {
				return nil, fmt.Errorf("extract expected indexes for table %q: %w", table, err)
			}
			if indexName == "" {
				continue
			}
			if indexTable != table {
				return nil, fmt.Errorf("schema file for table %q declares index %q on table %q", table, indexName, indexTable)
			}
			existingUnique, present := existingIndexes[indexName]
			if present && (!unique || existingUnique) {
				continue
			}
			// A non-unique live index cannot satisfy a unique expectation. CREATE
			// INDEX would collide by name, so fail closed rather than altering it.
			if present {
				return nil, fmt.Errorf("storage table %q has non-unique index %q where the embedded schema requires a unique index; replace it manually", table, indexName)
			}
			drift[table] = append(drift[table], postgresSchemaChange{operation: "create_index", object: indexName, ddl: statement})
		}
	}
	return drift, nil
}

// SynthesizePostgresAddColumn returns canonical deparsed SQL, making these
// clause checks insensitive to formatting in the embedded schema file. The
// statement has already passed PostgreSQL's real parser before this guard.
func postgresAddColumnRequiresManualBackfill(statement string) bool {
	upper := strings.ToUpper(statement)
	return strings.Contains(upper, " NOT NULL") && !strings.Contains(upper, " DEFAULT ")
}

func verifyAndLogPostgresSchemaShape(ctx context.Context, db *sql.DB, tables []string, files map[string]string, logger *slog.Logger, database, schemaName string) error {
	if err := verifyPostgresSchemaShape(ctx, db, tables, files, logger); err != nil {
		logger.Error("PostgreSQL storage schema shape check failed",
			"dialect", schema.DialectPostgres,
			"database", database,
			"schema", schemaName,
			"operation", "verify_storage_shape",
			"error", err,
		)
		return fmt.Errorf("verify PostgreSQL storage schema shape: %w", err)
	}
	return nil
}

// verifyPostgresSchemaShape checks the additive convergence result. Columns
// remain presence-only; type, length, and nullability drift are not detected.
func verifyPostgresSchemaShape(ctx context.Context, db *sql.DB, tables []string, files map[string]string, logger *slog.Logger) error {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}
	for _, table := range tables {
		statements, err := parser.Split(files[table])
		if err != nil {
			return fmt.Errorf("split schema file for table %q: %w", table, err)
		}
		if len(statements) == 0 {
			return fmt.Errorf("schema file for table %q has no statements", table)
		}
		expected, err := parser.CreateTableColumns(statements[0])
		if err != nil {
			return fmt.Errorf("extract expected columns for table %q: %w", table, err)
		}
		existing, err := postgresTableColumns(ctx, db, table)
		if err != nil {
			return err
		}

		var missing []string
		for _, column := range expected {
			if !existing[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("storage table %q is missing expected columns: %s", table, strings.Join(missing, ", "))
		}

		expectedUnique := make([]string, 0)
		expectedNonUnique := make([]string, 0)
		for _, statement := range statements[1:] {
			indexName, indexTable, unique, err := parser.CreateIndex(statement)
			if err != nil {
				return fmt.Errorf("extract expected indexes for table %q: %w", table, err)
			}
			if indexName == "" {
				// Not a standalone CREATE INDEX statement, so it declares no
				// index expectation.
				continue
			}
			if indexTable != table {
				return fmt.Errorf("schema file for table %q declares index %q on table %q", table, indexName, indexTable)
			}
			if unique {
				expectedUnique = append(expectedUnique, indexName)
			} else {
				expectedNonUnique = append(expectedNonUnique, indexName)
			}
		}
		existingIndexes, err := postgresTableIndexes(ctx, db, table)
		if err != nil {
			return err
		}
		missing = nil
		for _, indexName := range expectedUnique {
			if !existingIndexes[indexName] {
				missing = append(missing, indexName)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("storage table %q is missing expected unique indexes: %s", table, strings.Join(missing, ", "))
		}
		var missingNonUnique []string
		for _, indexName := range expectedNonUnique {
			if _, present := existingIndexes[indexName]; !present {
				missingNonUnique = append(missingNonUnique, indexName)
			}
		}
		if len(missingNonUnique) > 0 {
			logger.Warn("storage table is missing non-unique indexes after additive convergence",
				"dialect", schema.DialectPostgres,
				"table", table,
				"indexes", strings.Join(missingNonUnique, ", "),
			)
		}
	}
	return nil
}

func postgresTableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1", table)
	if err != nil {
		return nil, fmt.Errorf("list columns for table %q: %w", table, err)
	}
	defer utils.CloseAndLog(rows)

	existing := make(map[string]bool)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan column for table %q: %w", table, err)
		}
		existing[column] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns for table %q: %w", table, err)
	}
	return existing, nil
}

// postgresTableIndexes returns the table's live indexes as a name→uniqueness
// map. A unique expectation requires its name to map to true; a non-unique
// expectation is satisfied by presence under either uniqueness, since a
// unique index answers the same reads.
func postgresTableIndexes(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT index_class.relname, index_info.indisunique
		FROM pg_index AS index_info
		JOIN pg_class AS table_class ON table_class.oid = index_info.indrelid
		JOIN pg_namespace AS table_namespace ON table_namespace.oid = table_class.relnamespace
		JOIN pg_class AS index_class ON index_class.oid = index_info.indexrelid
		WHERE table_namespace.nspname = current_schema()
		  AND table_class.relname = $1`, table)
	if err != nil {
		return nil, fmt.Errorf("list indexes for table %q: %w", table, err)
	}
	defer utils.CloseAndLog(rows)

	existing := make(map[string]bool)
	for rows.Next() {
		var indexName string
		var unique bool
		if err := rows.Scan(&indexName, &unique); err != nil {
			return nil, fmt.Errorf("scan index for table %q: %w", table, err)
		}
		existing[indexName] = unique
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexes for table %q: %w", table, err)
	}
	return existing, nil
}

// readEmbeddedPostgresSchemaFiles reads the embedded PostgreSQL schema files,
// returning the sorted table names and a table→file-content map. Each file is
// named after the single storage table it creates — an invariant the
// MySQL↔PostgreSQL schema parity tests pin.
func readEmbeddedPostgresSchemaFiles() ([]string, map[string]string, error) {
	entries, err := schema.PostgresFS.ReadDir("postgres")
	if err != nil {
		return nil, nil, fmt.Errorf("read schema directory: %w", err)
	}
	if len(entries) == 0 {
		return nil, nil, fmt.Errorf("read schema directory: no embedded schema files found in postgres/")
	}

	files := make(map[string]string, len(entries))
	tables := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := schema.PostgresFS.ReadFile("postgres/" + entry.Name())
		if err != nil {
			return nil, nil, fmt.Errorf("read schema file %s: %w", entry.Name(), err)
		}
		table := strings.TrimSuffix(entry.Name(), ".sql")
		files[table] = string(content)
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables, files, nil
}

// missingPostgresTables returns the storage tables from want that do not
// exist in the connection's current schema, in want's order.
func missingPostgresTables(ctx context.Context, db *sql.DB, want []string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'")
	if err != nil {
		return nil, fmt.Errorf("list storage tables: %w", err)
	}
	defer utils.CloseAndLog(rows)

	existing := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		existing[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate storage tables: %w", err)
	}

	var missing []string
	for _, table := range want {
		if !existing[table] {
			missing = append(missing, table)
		}
	}
	return missing, nil
}

// applyPostgresTableChanges executes one table's additive changes atomically.
// A CREATE TABLE change contains its complete embedded schema file, including
// indexes; pgx's simple query protocol executes that multi-statement string.
func applyPostgresTableChanges(ctx context.Context, db *sql.DB, table string, changes []postgresSchemaChange, logger *slog.Logger) error {
	for _, change := range changes {
		if change.manual {
			return fmt.Errorf("storage table %q is missing column %q whose definition is NOT NULL without a DEFAULT; add it manually or ship the column with a DEFAULT", table, change.object)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Rollback after a successful commit is a no-op returning sql.ErrTxDone;
	// any other rollback failure is worth surfacing.
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			logger.Warn("failed to roll back transaction", "table", table, "error", err)
		}
	}()

	for _, change := range changes {
		logger.Info("schema change",
			"table", table,
			"operation", change.operation,
			"object", change.object,
			"ddl", change.ddl,
		)
		if _, err := tx.ExecContext(ctx, change.ddl); err != nil {
			return fmt.Errorf("execute %s for %q: %w", change.operation, change.object, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// acquirePostgresEnsureSchemaLock acquires a session-scoped advisory lock to
// serialize EnsureSchema across pods, mirroring acquireMySQLEnsureSchemaLock
// for the PostgreSQL bootstrap flow. It opens a dedicated *sql.DB whose pool
// is closed before returning, so closing the returned connection terminates
// the underlying session and releases the advisory lock — returning the
// connection to a shared pool would leave the session (and the lock) alive.
func acquirePostgresEnsureSchemaLock(ctx context.Context, dsn string, logger *slog.Logger, locker namedlock.Locker) (*sql.Conn, error) {
	db, err := postgresconn.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	// Advisory locks are per-session, so we need a dedicated connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}

	// Wait up to the full timeout for the lock — a trailing pod must outwait
	// the leader's table creation, after which it re-checks and finds every
	// table present.
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
