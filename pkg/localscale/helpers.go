package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// querier is the common interface satisfied by *sql.DB, *sql.Conn, and *sql.Tx.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// execer is the interface for SQL exec operations.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// queryExecer combines query and exec interfaces.
type queryExecer interface {
	querier
	execer
}

// QueryResult holds the result of a SQL query execution via admin endpoints.
type QueryResult struct {
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
	RowsAffected int64    `json:"rows_affected,omitempty"`
}

// executeQuery runs a SQL query and returns a QueryResult. For SELECT queries,
// it returns columns and rows. For INSERT/UPDATE/DELETE, it returns rows_affected.
func executeQuery(ctx context.Context, db queryExecer, query string, args ...any) (*QueryResult, error) {
	trimmed := strings.TrimSpace(strings.ToUpper(query))
	isSelect := strings.HasPrefix(trimmed, "SELECT") || strings.HasPrefix(trimmed, "SHOW") ||
		strings.HasPrefix(trimmed, "DESCRIBE") || strings.HasPrefix(trimmed, "EXPLAIN")

	if isSelect {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
		defer utils.CloseAndLog(rows)

		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("get columns: %w", err)
		}

		var resultRows [][]any
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			ptrs := make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
			row := make([]any, len(columns))
			for i, v := range values {
				if v.Valid {
					row[i] = v.String
				}
			}
			resultRows = append(resultRows, row)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate rows: %w", err)
		}
		return &QueryResult{Columns: columns, Rows: resultRows}, nil
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}
	affected, _ := result.RowsAffected()
	return &QueryResult{RowsAffected: affected}, nil
}

// showCreateAllFromConn reads all CREATE TABLE statements from a connection.
// This is the *sql.Conn variant needed for shard-targeted queries where
// USE keyspace:shard must be pinned to a single connection. Filtering uses
// Spirit's table.FilterOption pattern.
func showCreateAllFromConn(ctx context.Context, conn *sql.Conn, opts ...table.FilterOption) ([]table.TableSchema, error) {
	optSet := make(map[table.FilterOption]bool, len(opts))
	for _, o := range opts {
		optSet[o] = true
	}

	rows, err := conn.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("show tables: %w", err)
	}

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			utils.CloseAndLog(rows)
			return nil, fmt.Errorf("scan table: %w", err)
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		utils.CloseAndLog(rows)
		return nil, fmt.Errorf("iterate tables: %w", err)
	}
	utils.CloseAndLog(rows)

	var result []table.TableSchema
	for _, name := range tableNames {
		if optSet[table.WithoutUnderscoreTables] && strings.HasPrefix(name, "_") {
			continue
		}
		if optSet[table.WithoutArchiveTables] && table.IsArchiveTable(name) {
			continue
		}
		var tbl, createStmt string
		if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+quoteIdentifier(name)).Scan(&tbl, &createStmt); err != nil {
			return nil, fmt.Errorf("show create table %s: %w", name, err)
		}
		if optSet[table.WithStrippedAutoIncrement] {
			createStmt = table.StripAutoIncrement(createStmt)
		}
		result = append(result, table.TableSchema{Name: tbl, Schema: createStmt})
	}
	return result, nil
}

// hasVSchemaData returns true if the given NullString contains non-empty, non-null VSchema data.
func hasVSchemaData(s sql.NullString) bool {
	return s.Valid && s.String != "" && s.String != "null"
}

// scanDynamicRows scans all rows from a result set with dynamic columns into
// a slice of maps. Each map has column name → value for non-NULL columns.
func scanDynamicRows(rows *sql.Rows) ([]map[string]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}
	var result []map[string]string
	for rows.Next() {
		colValues := make([]sql.NullString, len(columns))
		colPtrs := make([]any, len(columns))
		for i := range colValues {
			colPtrs[i] = &colValues[i]
		}
		if err := rows.Scan(colPtrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		colMap := make(map[string]string)
		for i, col := range columns {
			if colValues[i].Valid {
				colMap[col] = colValues[i].String
			}
		}
		result = append(result, colMap)
	}
	return result, rows.Err()
}

// vtgateShardConn returns a vtgate connection targeted at a specific shard for the
// given keyspace. Shard-targeted connections bypass vtgate's schema tracker cache,
// ensuring SHOW CREATE TABLE works even when the cache is stale after recent DDL.
// The caller must call the returned cleanup function to close the connection.
func (s *Server) vtgateShardConn(ctx context.Context, backend *databaseBackend, keyspace string) (_ *sql.Conn, cleanup func(), _ error) {
	if err := validateIdentifier(keyspace); err != nil {
		return nil, nil, fmt.Errorf("invalid keyspace: %w", err)
	}
	// Discover first shard via vtctld.
	resp, err := backend.vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{
		Keyspace: keyspace,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("find shards for keyspace %s: %w", keyspace, err)
	}
	var firstShard string
	for name := range resp.Shards {
		firstShard = name
		break
	}
	if firstShard == "" {
		return nil, nil, fmt.Errorf("no shards found for keyspace %s", keyspace)
	}

	conn, err := backend.unscopedVtgateDB.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get vtgate connection: %w", err)
	}

	// Target the shard. After USE keyspace:shard, all queries on this connection
	// bypass vtgate's planner and go directly to the tablet.
	target := fmt.Sprintf("%s:%s", keyspace, firstShard)
	if err := validateIdentifier(target); err != nil {
		utils.CloseAndLog(conn)
		return nil, nil, fmt.Errorf("invalid shard target %s: %w", target, err)
	}
	if _, err := conn.ExecContext(ctx, "USE "+quoteIdentifier(target)); err != nil {
		utils.CloseAndLog(conn)
		return nil, nil, fmt.Errorf("target shard %s: %w", target, err)
	}

	return conn, func() { utils.CloseAndLog(conn) }, nil
}

// forEachShard executes fn on a shard-targeted connection for each shard of a keyspace.
// This is needed for commands like ALTER VITESS_MIGRATION CANCEL ALL and SHOW
// VITESS_MIGRATIONS which fail on keyspace-scoped connections for multi-shard keyspaces.
func (s *Server) forEachShard(ctx context.Context, backend *databaseBackend, keyspace string, fn func(conn *sql.Conn) error) error {
	resp, err := backend.vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{
		Keyspace: keyspace,
	})
	if err != nil {
		return fmt.Errorf("find shards for %s: %w", keyspace, err)
	}
	for shard := range resp.Shards {
		conn, err := backend.unscopedVtgateDB.Conn(ctx)
		if err != nil {
			return fmt.Errorf("get connection: %w", err)
		}
		target := fmt.Sprintf("%s:%s", keyspace, shard)
		if _, err := conn.ExecContext(ctx, "USE "+quoteIdentifier(target)); err != nil {
			utils.CloseAndLog(conn)
			return fmt.Errorf("target %s: %w", target, err)
		}
		fnErr := fn(conn)
		utils.CloseAndLog(conn)
		if fnErr != nil {
			return fnErr
		}
	}
	return nil
}

// snapshotKeyspaceSchema reads all CREATE TABLE statements from a vtgate keyspace.
// Uses shard-targeting to bypass vtgate's schema tracker cache.
func (s *Server) snapshotKeyspaceSchema(ctx context.Context, backend *databaseBackend, keyspace string) ([]string, error) {
	conn, cleanup, err := s.vtgateShardConn(ctx, backend, keyspace)
	if err != nil {
		return nil, fmt.Errorf("shard-targeted conn: %w", err)
	}
	defer cleanup()

	tables, err := showCreateAllFromConn(ctx, conn, table.WithoutUnderscoreTables)
	if err != nil {
		return nil, fmt.Errorf("snapshot schema for %s: %w", keyspace, err)
	}
	stmts := make([]string, len(tables))
	for i, t := range tables {
		stmts[i] = t.Schema
	}
	return stmts, nil
}

// addAlgorithmInstant rewrites an ALTER TABLE statement to include ALGORITHM=INSTANT.
// Returns "" for non-ALTER TABLE statements (CREATE TABLE, DROP TABLE, etc.).
//
// Example: "ALTER TABLE users ADD COLUMN age INT" → "ALTER TABLE users ALGORITHM=INSTANT, ADD COLUMN age INT"
func addAlgorithmInstant(stmt string) string {
	trimmed := strings.TrimSpace(stmt)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "ALTER TABLE ") {
		return ""
	}
	// Find end of "ALTER TABLE <name>" — skip past table name
	// Format: ALTER TABLE <name> <rest>
	rest := trimmed[len("ALTER TABLE "):]
	if len(rest) == 0 {
		return ""
	}
	// Table name might be backtick-quoted
	var tableName string
	if rest[0] == '`' {
		end := strings.Index(rest[1:], "`")
		if end < 0 {
			return ""
		}
		tableName = rest[:end+2] // includes backticks
		rest = rest[end+2:]
	} else {
		parts := strings.SplitN(rest, " ", 2)
		tableName = parts[0]
		if len(parts) > 1 {
			rest = " " + parts[1]
		} else {
			rest = ""
		}
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE %s ALGORITHM=INSTANT, %s", tableName, rest)
}

func branchDBName(branch, keyspace string) string {
	return fmt.Sprintf("branch_%s_%s", branch, keyspace)
}

// openBranchDB opens a temporary connection to a branch database in localscale-mysql.
// Callers must close the returned DB when done.
func (s *Server) openBranchDB(ctx context.Context, branch, keyspace string) (*sql.DB, error) {
	dsn := s.branchDSNBase + branchDBName(branch, keyspace)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open branch db %s/%s: %w", branch, keyspace, err)
	}
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, fmt.Errorf("ping branch db %s/%s: %w", branch, keyspace, err)
	}
	return db, nil
}

// sanitizeDDL parses a SQL statement with Spirit's classifier and verifies it's
// DDL (ALTER TABLE, CREATE TABLE, DROP TABLE, etc.). Returns an error for non-DDL.
func sanitizeDDL(stmt string) error {
	classifications, err := statement.Classify(stmt)
	if err != nil {
		return fmt.Errorf("parse statement: %w", err)
	}
	for _, c := range classifications {
		if !c.Type.IsDDL() {
			return fmt.Errorf("not a DDL statement: %s", c.Type)
		}
	}
	return nil
}

// quoteIdentifier returns a MySQL backtick-quoted identifier, escaping any
// embedded backticks by doubling them (the MySQL convention).
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// validateIdentifier checks that a string is safe for use as a SQL identifier.
// Allows alphanumeric, underscore, hyphen, and dollar sign.
func validateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("identifier is required")
	}
	for _, c := range name {
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		isAllowed := c == '_' || c == '-' || c == '$' || c == '.' || c == ':'
		if !isAlpha && !isDigit && !isAllowed {
			return fmt.Errorf("identifier contains invalid character: %q", c)
		}
	}
	return nil
}

// validateSessionString rejects strings containing characters that could break
// SQL session variable SET or LIKE queries when interpolated into single-quoted contexts.
func validateSessionString(s string) error {
	if strings.ContainsAny(s, "'\"\\`") {
		return fmt.Errorf("contains unsafe characters")
	}
	return nil
}

// validateBranchName checks that a branch name is safe for use in SQL identifiers
// and filesystem paths. Allows alphanumeric, hyphen, underscore, and dot.
func validateBranchName(name string) error {
	if name == "" {
		return fmt.Errorf("branch name is required")
	}
	for _, c := range name {
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		isAllowed := c == '-' || c == '_' || c == '.'
		if !isAlpha && !isDigit && !isAllowed {
			return fmt.Errorf("branch name contains invalid character: %q", c)
		}
	}
	return nil
}

// execLog executes a SQL statement and logs any error. Use this instead of
// `_, _ = s.metadataDB.ExecContext(...)` to ensure DB errors are never silently lost.
func (s *Server) execLog(ctx context.Context, query string, args ...any) {
	if _, err := s.metadataDB.ExecContext(ctx, query, args...); err != nil {
		s.logger.Error("exec failed", "query", query[:min(len(query), 80)], "error", err)
	}
}

// decodeJSON decodes the request body into v. Returns false and writes a 400 error if decoding fails.
func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		s.writeError(w, http.StatusBadRequest, "decode body: %v", err)
		return false
	}
	return true
}

func (s *Server) writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("write json", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, code int, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	s.logger.Warn("api error", "code", code, "message", msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// PS SDK expects "code" as a string, not a number
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code":    fmt.Sprintf("%d", code),
		"message": msg,
	})
}
