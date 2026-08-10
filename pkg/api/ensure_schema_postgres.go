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
	_ "github.com/jackc/pgx/v5/stdlib" // pgx database/sql driver for the storage database

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/schema"
)

// ensurePostgresSchema converges SchemaBot's storage schema on PostgreSQL by
// creating every storage table whose embedded schema file has no matching
// table in the current schema. Existing tables are checked for every expected
// column, but are never altered; extra columns and index differences are
// tolerated. That bound is deliberate — PostgreSQL has
// no in-process diff/apply mechanism here (Spirit is MySQL-only), and
// create-only convergence is sufficient to bootstrap a fresh storage
// database. Evolving an already-bootstrapped PostgreSQL storage schema
// requires a schema diff mechanism, which lands separately.
//
// Because the flow only ever creates missing tables, it can never destroy
// existing data, so the destructive-change refusal that guards the MySQL flow
// (WithAllowDestructiveSchemaChanges) does not apply here.
//
// Concurrency-safe across pods: checks table existence first without a lock
// (read-only, the common case on 99% of deploys), and returns immediately when
// every table exists. When tables are missing, acquires a PostgreSQL advisory
// lock to serialize creation across pods, then re-checks under the lock —
// another pod may have created the tables while we waited. Each table's file
// executes inside one transaction, so a killed pod leaves either the whole
// table with its indexes or nothing.
func ensurePostgresSchema(dsn string, logger *slog.Logger, locker namedlock.Locker) error {
	ctx, cancel := context.WithTimeout(context.Background(), EnsureSchemaTimeout)
	defer cancel()

	tables, files, err := readEmbeddedPostgresSchemaFiles()
	if err != nil {
		return err
	}

	db, err := sql.Open("pgx", dsn)
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

	// Fast path: check existence without a lock. If every table exists,
	// return immediately.
	missing, err := missingPostgresTables(ctx, db, tables)
	if err != nil {
		return fmt.Errorf("check storage tables: %w", err)
	}
	if len(missing) == 0 {
		if err := verifyAndLogPostgresTableColumns(ctx, db, tables, files, logger, database, schemaName); err != nil {
			return fmt.Errorf("validate existing storage tables: %w", err)
		}
		logger.Info("storage schema up-to-date", "database", database)
		return nil
	}
	logger.Info("storage tables missing (pre-lock); acquiring EnsureSchema advisory lock to create them",
		"database", database,
		"tables", missing,
	)

	lockConn, err := acquirePostgresEnsureSchemaLock(ctx, dsn, logger, locker)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	defer utils.CloseAndLog(lockConn)

	// Re-check under the lock — another pod may have created the tables
	// while we waited.
	missing, err = missingPostgresTables(ctx, db, tables)
	if err != nil {
		return fmt.Errorf("check storage tables: %w", err)
	}
	if len(missing) == 0 {
		if err := verifyAndLogPostgresTableColumns(ctx, db, tables, files, logger, database, schemaName); err != nil {
			return fmt.Errorf("validate storage tables after lock: %w", err)
		}
		logger.Info("storage schema up-to-date", "database", database)
		return nil
	}

	createStart := time.Now()
	for _, table := range missing {
		if err := createPostgresTable(ctx, db, table, files[table], logger); err != nil {
			return fmt.Errorf("create storage table %q: %w", table, err)
		}
	}
	logger.Info("storage schema applied successfully",
		"database", database,
		"tables_created", len(missing),
		"duration", time.Since(createStart),
	)
	if err := verifyAndLogPostgresTableColumns(ctx, db, tables, files, logger, database, schemaName); err != nil {
		return fmt.Errorf("validate converged storage tables: %w", err)
	}
	return nil
}

func verifyAndLogPostgresTableColumns(ctx context.Context, db *sql.DB, tables []string, files map[string]string, logger *slog.Logger, database, schemaName string) error {
	if err := verifyPostgresTableColumns(ctx, db, tables, files); err != nil {
		logger.Error("PostgreSQL storage schema column check failed",
			"dialect", schema.DialectPostgres,
			"database", database,
			"schema", schemaName,
			"operation", "verify_storage_columns",
			"error", err,
		)
		return fmt.Errorf("verify PostgreSQL storage schema columns: %w", err)
	}
	return nil
}

var postgresTableConstraintKeywords = map[string]bool{
	"PRIMARY":    true,
	"UNIQUE":     true,
	"CONSTRAINT": true,
	"CHECK":      true,
	"FOREIGN":    true,
	"EXCLUDE":    true,
	"INDEX":      true,
}

// postgresCreateTableColumns extracts column names from the single plain
// CREATE TABLE statement at the start of an embedded PostgreSQL schema file.
// Nested type and constraint expressions are kept together while the table
// body is split at top-level commas.
func postgresCreateTableColumns(content string) ([]string, error) {
	open := strings.IndexByte(content, '(')
	if open < 0 {
		return nil, fmt.Errorf("CREATE TABLE has no column list")
	}
	close, err := matchingPostgresParen(content, open)
	if err != nil {
		return nil, err
	}

	entries, err := splitPostgresTableEntries(content[open+1 : close])
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(entries))
	for _, entry := range entries {
		first, remainder, err := postgresEntryFirstToken(entry)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(remainder) == "" {
			return nil, fmt.Errorf("unrecognized CREATE TABLE entry %q", entry)
		}
		if postgresTableConstraintKeywords[strings.ToUpper(first)] {
			continue
		}
		if strings.HasPrefix(first, `"`) {
			columns = append(columns, strings.ReplaceAll(first[1:len(first)-1], `""`, `"`))
			continue
		}
		if first != strings.ToLower(first) {
			return nil, fmt.Errorf("unrecognized CREATE TABLE entry %q", entry)
		}
		for _, r := range first {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
				return nil, fmt.Errorf("invalid column name %q", first)
			}
		}
		columns = append(columns, first)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("CREATE TABLE has no columns")
	}
	return columns, nil
}

func postgresEntryFirstToken(entry string) (string, string, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return "", "", fmt.Errorf("empty CREATE TABLE entry")
	}
	if entry[0] != '"' {
		if end := strings.IndexAny(entry, " \t\r\n"); end >= 0 {
			return entry[:end], entry[end:], nil
		}
		return entry, "", nil
	}
	for i := 1; i < len(entry); i++ {
		if entry[i] != '"' {
			continue
		}
		if i+1 < len(entry) && entry[i+1] == '"' {
			i++
			continue
		}
		return entry[:i+1], entry[i+1:], nil
	}
	return "", "", fmt.Errorf("invalid quoted column name in entry %q", entry)
}

func matchingPostgresParen(content string, open int) (int, error) {
	depth := 0
	inSingle, inDouble := false, false
	for i := open; i < len(content); i++ {
		switch content[i] {
		case '\'':
			if !inDouble && inSingle && i+1 < len(content) && content[i+1] == '\'' {
				i++
			} else if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && inDouble && i+1 < len(content) && content[i+1] == '"' {
				i++
			} else if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				depth--
				if depth == 0 {
					return i, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("CREATE TABLE has an unterminated column list")
}

func splitPostgresTableEntries(body string) ([]string, error) {
	var entries []string
	start, depth := 0, 0
	inSingle, inDouble := false, false
	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '\'':
			if !inDouble && inSingle && i+1 < len(body) && body[i+1] == '\'' {
				i++
			} else if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && inDouble && i+1 < len(body) && body[i+1] == '"' {
				i++
			} else if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				depth--
				if depth < 0 {
					return nil, fmt.Errorf("unexpected closing parenthesis in CREATE TABLE body")
				}
			}
		case ',':
			if !inSingle && !inDouble && depth == 0 {
				entries = append(entries, strings.TrimSpace(body[start:i]))
				start = i + 1
			}
		}
	}
	if inSingle || inDouble || depth != 0 {
		return nil, fmt.Errorf("unterminated expression in CREATE TABLE body")
	}
	entries = append(entries, strings.TrimSpace(body[start:]))
	return entries, nil
}

func verifyPostgresTableColumns(ctx context.Context, db *sql.DB, tables []string, files map[string]string) error {
	for _, table := range tables {
		expected, err := postgresCreateTableColumns(files[table])
		if err != nil {
			return fmt.Errorf("extract expected columns for table %q: %w", table, err)
		}
		rows, err := db.QueryContext(ctx,
			"SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1", table)
		if err != nil {
			return fmt.Errorf("list columns for table %q: %w", table, err)
		}
		existing := make(map[string]bool)
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				utils.CloseAndLog(rows)
				return fmt.Errorf("scan column for table %q: %w", table, err)
			}
			existing[column] = true
		}
		if err := rows.Err(); err != nil {
			utils.CloseAndLog(rows)
			return fmt.Errorf("iterate columns for table %q: %w", table, err)
		}
		utils.CloseAndLog(rows)

		var missing []string
		for _, column := range expected {
			if !existing[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("storage table %q is missing expected columns: %s", table, strings.Join(missing, ", "))
		}
	}
	return nil
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

// createPostgresTable executes one embedded schema file — a CREATE TABLE
// followed by its CREATE INDEX statements — inside a single transaction.
// PostgreSQL DDL is transactional, so the table appears with all of its
// indexes or not at all; an interrupted bootstrap never leaves a
// partially-indexed table behind.
//
// The file executes whole in one Exec: pgx uses the simple query protocol for
// zero-argument Execs, and the simple protocol runs a multi-statement string
// natively, so no client-side statement splitting is needed.
func createPostgresTable(ctx context.Context, db *sql.DB, table, content string, logger *slog.Logger) error {
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

	logger.Info("schema change",
		"table", table,
		"operation", "create",
		"ddl", content,
	)
	if _, err := tx.ExecContext(ctx, content); err != nil {
		return fmt.Errorf("execute schema file: %w", err)
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
	db, err := sql.Open("pgx", dsn)
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
