package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
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
// A change that needs manual remediation aborts the whole convergence before
// any DDL executes. Each transaction bounds its lock wait with lock_timeout.
// Plain CREATE INDEX holds a SHARE lock for the full build, blocking writes;
// EnsureSchemaTimeout is the build's only duration ceiling.
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
	// Log what the fast-path scan found before parking on the lock, so the
	// last pre-lock line names the work this pod is waiting to do.
	for _, table := range tables {
		for _, change := range drift[table] {
			logger.Info("schema change detected (pre-lock)",
				"table", table,
				"operation", change.operation,
				"object", change.object,
				"ddl", change.ddl,
			)
		}
	}
	logger.Info("storage schema drift detected (pre-lock); acquiring EnsureSchema advisory lock to converge it",
		"database", database,
		"change_count", drift.changeCount(),
		"table_count", len(drift),
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

	// Gate on manual remediation across the whole drift set before touching
	// any table, so a change that cannot run automatically never leaves the
	// schema half-converged.
	if err := postgresManualRemediation(tables, drift); err != nil {
		return err
	}

	applyStart := time.Now()
	for _, table := range tables {
		changes := drift[table]
		if len(changes) == 0 {
			logger.Debug("storage table already converged", "table", table)
			continue
		}
		if err := applyPostgresTableChanges(ctx, db, table, changes, logger); err != nil {
			return fmt.Errorf("converge storage table %q: %w", table, err)
		}
	}
	logger.Info("storage schema applied successfully",
		"database", database,
		"change_count", drift.changeCount(),
		"table_count", len(drift),
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
	// manualReason is non-empty when the change cannot run automatically and
	// names why plus the remediation. Any non-empty reason aborts convergence
	// before any DDL executes.
	manualReason string
}

type postgresSchemaDrift map[string][]postgresSchemaChange

// changeCount returns the total number of planned changes across all tables.
func (d postgresSchemaDrift) changeCount() int {
	total := 0
	for _, changes := range d {
		total += len(changes)
	}
	return total
}

// postgresIndexExpectation is one standalone CREATE INDEX statement's
// expectation: an index under this name must exist, and must be unique when
// unique is set. Indexes are matched by name only — column composition is
// not compared, so a same-named index over different columns reads as
// converged.
type postgresIndexExpectation struct {
	name   string
	unique bool
	ddl    string
}

// postgresTableExpectations is the shape one embedded schema file declares
// for its table: the CREATE TABLE statement followed by named standalone
// CREATE INDEX statements.
type postgresTableExpectations struct {
	createTable string
	columns     []string
	indexes     []postgresIndexExpectation
}

// postgresExpectationsFor parses one table's embedded schema file into the
// expectations the drift scan and the shape verification both consume. A
// trailing statement that is not a named standalone CREATE INDEX on the
// file's own table fails closed: the additive convergence could neither
// create nor verify it, so a schema file carrying one would silently stop
// being the source of truth for the live schema.
func postgresExpectationsFor(parser ddl.StatementParser, table, file string) (postgresTableExpectations, error) {
	statements, err := parser.Split(file)
	if err != nil {
		return postgresTableExpectations{}, fmt.Errorf("split schema file for table %q: %w", table, err)
	}
	if len(statements) == 0 {
		return postgresTableExpectations{}, fmt.Errorf("schema file for table %q has no statements", table)
	}
	columns, err := parser.CreateTableColumns(statements[0])
	if err != nil {
		return postgresTableExpectations{}, fmt.Errorf("extract expected columns for table %q: %w", table, err)
	}
	expectations := postgresTableExpectations{createTable: statements[0], columns: columns}
	for _, statement := range statements[1:] {
		indexName, indexTable, unique, err := parser.CreateIndex(statement)
		if err != nil {
			return postgresTableExpectations{}, fmt.Errorf("extract expected indexes for table %q: %w", table, err)
		}
		if indexName == "" {
			return postgresTableExpectations{}, fmt.Errorf(
				"schema file for table %q contains a statement the additive convergence cannot track: %q; only named standalone CREATE INDEX statements may follow CREATE TABLE",
				table, statement)
		}
		if indexTable != table {
			return postgresTableExpectations{}, fmt.Errorf("schema file for table %q declares index %q on table %q", table, indexName, indexTable)
		}
		expectations.indexes = append(expectations.indexes, postgresIndexExpectation{name: indexName, unique: unique, ddl: statement})
	}
	return expectations, nil
}

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
		expected, err := postgresExpectationsFor(parser, table, files[table])
		if err != nil {
			return nil, err
		}
		if missingTable[table] {
			drift[table] = []postgresSchemaChange{{operation: "create_table", object: table, ddl: files[table]}}
			continue
		}

		existingColumns, err := postgresTableColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		for _, column := range expected.columns {
			if existingColumns[column] {
				continue
			}
			statement, err := ddl.SynthesizePostgresAddColumn(expected.createTable, column)
			if err != nil {
				return nil, fmt.Errorf("synthesize ADD COLUMN for %q.%q: %w", table, column, err)
			}
			manualReason, err := ddl.PostgresAddColumnManualReason(expected.createTable, column)
			if err != nil {
				return nil, fmt.Errorf("classify ADD COLUMN safety for %q.%q: %w", table, column, err)
			}
			drift[table] = append(drift[table], postgresSchemaChange{
				operation:    "add_column",
				object:       column,
				ddl:          statement,
				manualReason: manualReason,
			})
		}

		existingIndexes, err := postgresTableIndexes(ctx, db, table)
		if err != nil {
			return nil, err
		}
		for _, index := range expected.indexes {
			existingUnique, present := existingIndexes[index.name]
			if present && (!index.unique || existingUnique) {
				continue
			}
			// A non-unique live index cannot satisfy a unique expectation. CREATE
			// INDEX would collide by name, so fail closed rather than altering it.
			if present {
				return nil, fmt.Errorf("storage table %q has non-unique index %q where the embedded schema requires a unique index; replace it manually", table, index.name)
			}
			drift[table] = append(drift[table], postgresSchemaChange{operation: "create_index", object: index.name, ddl: index.ddl})
		}
	}
	return drift, nil
}

// postgresManualRemediation returns an error naming every planned change that
// needs manual remediation, or nil when all planned changes can run
// automatically. It scans the whole drift set so the gate fires before any
// table's DDL executes — an operator sees every problem at once rather than
// one per crashloop restart.
func postgresManualRemediation(tables []string, drift postgresSchemaDrift) error {
	var problems []string
	for _, table := range tables {
		for _, change := range drift[table] {
			if change.manualReason == "" {
				continue
			}
			problems = append(problems, fmt.Sprintf("storage table %q is missing column %q whose %s", table, change.object, change.manualReason))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

func verifyAndLogPostgresSchemaShape(ctx context.Context, db *sql.DB, tables []string, files map[string]string, logger *slog.Logger, database, schemaName string) error {
	if err := verifyPostgresSchemaShape(ctx, db, tables, files); err != nil {
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
// Indexes are matched by name and uniqueness only, not column composition.
func verifyPostgresSchemaShape(ctx context.Context, db *sql.DB, tables []string, files map[string]string) error {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}
	for _, table := range tables {
		expected, err := postgresExpectationsFor(parser, table, files[table])
		if err != nil {
			return err
		}
		existing, err := postgresTableColumns(ctx, db, table)
		if err != nil {
			return err
		}

		var missing []string
		for _, column := range expected.columns {
			if !existing[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("storage table %q is missing expected columns: %s", table, strings.Join(missing, ", "))
		}

		existingIndexes, err := postgresTableIndexes(ctx, db, table)
		if err != nil {
			return err
		}
		var missingUnique []string
		for _, index := range expected.indexes {
			if index.unique && !existingIndexes[index.name] {
				missingUnique = append(missingUnique, index.name)
			}
		}
		if len(missingUnique) > 0 {
			return fmt.Errorf("storage table %q is missing expected unique indexes: %s", table, strings.Join(missingUnique, ", "))
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

// postgresDDLLockTimeout bounds how long a convergence DDL statement waits
// for its table lock. Without it, one long-running reader queues the ALTER
// TABLE's AccessExclusiveLock request indefinitely, and every later reader
// queues behind that request — a table-wide stall. With it, the statement
// fails, the transaction rolls back, and the startup attempt retries or
// fails visibly instead.
const postgresDDLLockTimeout = 10 * time.Second

// applyPostgresTableChanges executes one table's additive changes. A CREATE
// TABLE change contains its complete embedded schema file, including indexes,
// executed as one transaction; pgx's simple query protocol executes that
// multi-statement string. For an existing table, column changes share one
// transaction — each is metadata-only after the manual-remediation gate — and
// each index builds in its own transaction. Plain CREATE INDEX holds a SHARE
// lock for the full build and blocks writes; lock_timeout bounds only the wait
// to acquire that lock, while EnsureSchemaTimeout bounds the build itself.
// CREATE INDEX CONCURRENTLY cannot run inside these transactions, so the write
// block is the accepted cost. Cross-transaction atomicity is unnecessary: a
// startup killed between transactions leaves additive drift the next run
// re-discovers and converges.
func applyPostgresTableChanges(ctx context.Context, db *sql.DB, table string, changes []postgresSchemaChange, logger *slog.Logger) error {
	if changes[0].operation == "create_table" {
		return execPostgresChanges(ctx, db, table, changes, logger)
	}
	var columnChanges []postgresSchemaChange
	var indexChanges []postgresSchemaChange
	for _, change := range changes {
		if change.operation == "create_index" {
			indexChanges = append(indexChanges, change)
		} else {
			columnChanges = append(columnChanges, change)
		}
	}
	if len(columnChanges) > 0 {
		if err := execPostgresChanges(ctx, db, table, columnChanges, logger); err != nil {
			return err
		}
	}
	for _, index := range indexChanges {
		if err := execPostgresChanges(ctx, db, table, []postgresSchemaChange{index}, logger); err != nil {
			return err
		}
	}
	return nil
}

// execPostgresChanges executes one batch of changes in a single transaction
// with a bounded lock wait.
func execPostgresChanges(ctx context.Context, db *sql.DB, table string, changes []postgresSchemaChange, logger *slog.Logger) error {
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

	if _, err := tx.ExecContext(ctx, "SELECT set_config('lock_timeout', $1, true)",
		strconv.FormatInt(postgresDDLLockTimeout.Milliseconds(), 10)); err != nil {
		return fmt.Errorf("set lock_timeout for table %q: %w", table, err)
	}
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
