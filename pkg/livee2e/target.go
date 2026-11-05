package livee2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/block/schemabot/pkg/mysqlconn"
)

// Target is the test database the runner seeds and checks. The runner reads
// the schema directly so it can confirm what SchemaBot reported rather than
// trust the PR comment alone.
type Target interface {
	TableExists(ctx context.Context, table string) (bool, error)
	ColumnType(ctx context.Context, table, column string) (string, error)
	RowCount(ctx context.Context, table string) (int64, error)
	// Seed copies rows from source into table until table holds rows rows.
	Seed(ctx context.Context, table, source string, rows int64) error
	// LeftoverCopyTables lists the shadow and checkpoint tables a schema
	// change on table leaves behind if it does not clean up.
	LeftoverCopyTables(ctx context.Context, table string) ([]string, error)
	// RunTables lists the tables earlier runs created.
	RunTables(ctx context.Context) ([]string, error)
	DropTable(ctx context.Context, table string) error
	Close() error
}

// runTableName matches the tables the runner creates, and only those, so the
// sweep can never drop a table it does not own.
var runTableName = regexp.MustCompile(`^e2e_[0-9]{14}$`)

// seedBatchRows bounds each seeding transaction so one statement never holds
// a long transaction open on the test database.
const seedBatchRows = 50000

type mysqlTarget struct {
	db       *sql.DB
	database string
	logger   *slog.Logger
}

// OpenMySQLTarget connects to the test database named database.
func OpenMySQLTarget(dsn, database string, logger *slog.Logger) (Target, error) {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open test database %s: %w", database, err)
	}
	return &mysqlTarget{db: db, database: database, logger: logger}, nil
}

func (t *mysqlTarget) Close() error {
	return t.db.Close()
}

func (t *mysqlTarget) TableExists(ctx context.Context, table string) (bool, error) {
	var n int
	err := t.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		t.database, table).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("look up table %s.%s: %w", t.database, table, err)
	}
	return n > 0, nil
}

func (t *mysqlTarget) ColumnType(ctx context.Context, table, column string) (string, error) {
	var columnType string
	err := t.db.QueryRowContext(ctx,
		"SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
		t.database, table, column).Scan(&columnType)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("column %s.%s.%s does not exist", t.database, table, column)
	}
	if err != nil {
		return "", fmt.Errorf("read type of column %s.%s.%s: %w", t.database, table, column, err)
	}
	return columnType, nil
}

func (t *mysqlTarget) RowCount(ctx context.Context, table string) (int64, error) {
	var n int64
	if err := t.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+t.qualified(table)).Scan(&n); err != nil {
		return 0, fmt.Errorf("count rows in %s.%s: %w", t.database, table, err)
	}
	return n, nil
}

func (t *mysqlTarget) Seed(ctx context.Context, table, source string, rows int64) error {
	var lastID int64
	for copied := int64(0); copied < rows; {
		batch := min(seedBatchRows, rows-copied)
		var maxID sql.NullInt64
		err := t.db.QueryRowContext(ctx,
			"SELECT MAX(id) FROM (SELECT id FROM "+t.qualified(source)+" WHERE id > ? ORDER BY id LIMIT ?) AS batch",
			lastID, batch).Scan(&maxID)
		if err != nil {
			return fmt.Errorf("find the next seed batch in %s.%s after id %d: %w", t.database, source, lastID, err)
		}
		if !maxID.Valid {
			return fmt.Errorf("seed table %s.%s has only %d rows; the run needs %d", t.database, source, copied, rows)
		}
		res, err := t.db.ExecContext(ctx,
			"INSERT INTO "+t.qualified(table)+" (payload, created_at) SELECT payload, created_at FROM "+t.qualified(source)+" WHERE id > ? AND id <= ?",
			lastID, maxID.Int64)
		if err != nil {
			return fmt.Errorf("copy %s.%s rows %d..%d into %s: %w", t.database, source, lastID+1, maxID.Int64, table, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("read rows copied into %s.%s: %w", t.database, table, err)
		}
		copied += n
		lastID = maxID.Int64
	}
	t.logger.Info("seeded test table", "database", t.database, "table", table, "source", source, "rows", rows)
	return nil
}

func (t *mysqlTarget) LeftoverCopyTables(ctx context.Context, table string) ([]string, error) {
	// The shadow and checkpoint tables Spirit creates for a copy. The table it
	// renames away at cutover is left out: SchemaBot drops that one on its own
	// schedule, after the apply finishes.
	candidates := []string{"_" + table + "_new", "_" + table + "_chkpnt"}
	var found []string
	for _, name := range candidates {
		exists, err := t.TableExists(ctx, name)
		if err != nil {
			return nil, err
		}
		if exists {
			found = append(found, name)
		}
	}
	return found, nil
}

func (t *mysqlTarget) RunTables(ctx context.Context) ([]string, error) {
	rows, err := t.db.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE 'e2e\\_%'",
		t.database)
	if err != nil {
		return nil, fmt.Errorf("list run tables in %s: %w", t.database, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.logger.Warn("closing run table listing failed", "database", t.database, "error", err)
		}
	}()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read run table name in %s: %w", t.database, err)
		}
		if !runTableName.MatchString(name) {
			t.logger.Debug("skipping table that a run did not create", "database", t.database, "table", name)
			continue
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list run tables in %s: %w", t.database, err)
	}
	return tables, nil
}

func (t *mysqlTarget) DropTable(ctx context.Context, table string) error {
	if !runTableName.MatchString(table) {
		return fmt.Errorf("refusing to drop %s.%s: a run did not create it", t.database, table)
	}
	if _, err := t.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+t.qualified(table)); err != nil {
		return fmt.Errorf("drop %s.%s: %w", t.database, table, err)
	}
	t.logger.Info("dropped run table", "database", t.database, "table", table)
	return nil
}

func (t *mysqlTarget) qualified(table string) string {
	return quoteIdentifier(t.database) + "." + quoteIdentifier(table)
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
