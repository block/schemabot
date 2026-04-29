package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// httpError carries an HTTP status code with an error message. Helpers return
// these so callers can write the response explicitly, keeping control flow visible.
type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func newHTTPError(code int, format string, args ...any) *httpError {
	return &httpError{code: code, msg: fmt.Sprintf(format, args...)}
}

// writeHTTPError writes an HTTP error response from an error. If the error is
// an *httpError, uses its status code; otherwise defaults to 500.
func (s *Server) writeHTTPError(w http.ResponseWriter, err error) {
	var he *httpError
	if errors.As(err, &he) {
		s.writeError(w, he.code, "%s", he.msg)
	} else {
		s.writeError(w, http.StatusInternalServerError, "%v", err)
	}
}

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

// buildDDLStrategy constructs the Vitess online DDL strategy string.
// If instantDDL is true, --prefer-instant-ddl is used; otherwise --postpone-completion.
func buildDDLStrategy(instantDDL bool) string {
	const baseFlags = " --in-order-completion --allow-zero-in-date --analyze-table" +
		" --force-cut-over-after=1ms --cut-over-threshold=15s" +
		" --singleton-context --allow-concurrent"

	if instantDDL {
		return "vitess --prefer-instant-ddl" + baseFlags
	}
	return "vitess --postpone-completion" + baseFlags
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

// getBranchSchema reads all CREATE TABLE statements from a branch database in localscale-mysql.
func (s *Server) getBranchSchemaFromBackend(ctx context.Context, backend *databaseBackend, branch, keyspace string) ([]string, error) {
	return s.getBranchSchemaWithDSN(ctx, backend.mysqlDSNBase, branch, keyspace)
}

func (s *Server) getBranchSchemaWithDSN(ctx context.Context, dsnBase, branch, keyspace string) ([]string, error) {
	db, err := sql.Open("mysql", dsnBase+branchDBName(branch, keyspace))
	if err != nil {
		return nil, fmt.Errorf("open branch db %s/%s: %w", branch, keyspace, err)
	}
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, fmt.Errorf("ping branch db %s/%s: %w", branch, keyspace, err)
	}
	defer utils.CloseAndLog(db)

	tables, err := table.LoadSchemaFromDB(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("load branch schema for %s/%s: %w", branch, keyspace, err)
	}
	stmts := make([]string, len(tables))
	for i, t := range tables {
		stmts[i] = t.Schema
	}
	return stmts, nil
}

// submitOnlineDDL submits DDL statements as Vitess online DDL migrations.
func (s *Server) submitOnlineDDL(ctx context.Context, backend *databaseBackend, ddlByKeyspace map[string][]string, strategy, migrationContext string) error {
	if err := validateSessionString(strategy); err != nil {
		return fmt.Errorf("invalid ddl_strategy: %w", err)
	}
	if err := validateSessionString(migrationContext); err != nil {
		return fmt.Errorf("invalid migration_context: %w", err)
	}
	for keyspace, stmts := range ddlByKeyspace {
		db, ok := backend.vtgateDBs[keyspace]
		if !ok {
			return fmt.Errorf("no vtgate connection for keyspace %s", keyspace)
		}
		for _, stmt := range stmts {
			conn, err := db.Conn(ctx)
			if err != nil {
				return fmt.Errorf("get connection for %s: %w", keyspace, err)
			}
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET @@ddl_strategy='%s'", strategy)); err != nil {
				utils.CloseAndLog(conn)
				return fmt.Errorf("set ddl_strategy for %s: %w", keyspace, err)
			}
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET @@migration_context='%s'", migrationContext)); err != nil {
				utils.CloseAndLog(conn)
				return fmt.Errorf("set migration_context for %s: %w", keyspace, err)
			}
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				utils.CloseAndLog(conn)
				return fmt.Errorf("submit online DDL to %s: %w\nstatement: %s", keyspace, err, stmt)
			}
			utils.CloseAndLog(conn)
			s.logger.Info("submitted online DDL", "keyspace", keyspace, "stmt", stmt, "context", migrationContext)
		}
	}
	return nil
}

// extractTableNames parses DDL statements and returns the set of table names they reference.
func extractTableNames(stmts []string) (map[string]struct{}, error) {
	tables := make(map[string]struct{})
	for _, stmt := range stmts {
		parsed, err := statement.New(stmt)
		if err != nil {
			return nil, fmt.Errorf("parse DDL statement: %w", err)
		}
		if len(parsed) == 0 {
			continue
		}
		if tableName := parsed[0].Table; tableName != "" {
			tables[tableName] = struct{}{}
		}
	}
	return tables, nil
}

// snapshotSchema captures SHOW CREATE TABLE for all tables affected by the DDL.
// Returns a map of keyspace → []createTableStatement for use in DDL-based revert.
func (s *Server) snapshotSchema(ctx context.Context, backend *databaseBackend, ddlByKeyspace map[string][]string) (map[string][]string, error) {
	snapshot := make(map[string][]string)
	for keyspace, stmts := range ddlByKeyspace {
		db, ok := backend.vtgateDBs[keyspace]
		if !ok {
			return nil, fmt.Errorf("snapshot schema: no vtgate connection for keyspace %s", keyspace)
		}
		tables, err := extractTableNames(stmts)
		if err != nil {
			return nil, fmt.Errorf("extract table names for keyspace %s: %w", keyspace, err)
		}
		// Get current CREATE TABLE for each affected table.
		for table := range tables {
			var tName, createStmt string
			err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+quoteIdentifier(table)).Scan(&tName, &createStmt)
			if err != nil {
				// Table might not exist yet (CREATE TABLE DDL) — skip.
				s.logger.Debug("snapshot: table not found", "keyspace", keyspace, "table", table, "error", err)
				continue
			}
			snapshot[keyspace] = append(snapshot[keyspace], createStmt)
		}
	}
	return snapshot, nil
}

// computeReverseDDL computes the DDL needed to revert a deploy by diffing the
// current schema against the stored pre-deploy schema snapshot.
//
// Only tables affected by the deploy are queried from the current schema (not
// the entire keyspace). Affected tables are those present in schemaBefore plus
// those referenced by ddlByKeyspace (to handle CREATE TABLE → DROP TABLE).
func (s *Server) computeReverseDDL(ctx context.Context, backend *databaseBackend, schemaBefore map[string][]string, ddlByKeyspace map[string][]string) (map[string][]string, error) {
	differ := ddl.NewDiffer()
	reverseDDL := make(map[string][]string)

	// Process all keyspaces that have either a before snapshot or DDL.
	keyspaces := make(map[string]struct{})
	for ks := range schemaBefore {
		keyspaces[ks] = struct{}{}
	}
	for ks := range ddlByKeyspace {
		keyspaces[ks] = struct{}{}
	}

	for keyspace := range keyspaces {
		beforeStmts := schemaBefore[keyspace]

		// Collect affected table names from both before snapshot and original DDL.
		affectedTables, err := extractTableNames(beforeStmts)
		if err != nil {
			return nil, fmt.Errorf("extract table names from before snapshot for %s: %w", keyspace, err)
		}
		ddlTables, err := extractTableNames(ddlByKeyspace[keyspace])
		if err != nil {
			return nil, fmt.Errorf("extract table names from DDL for %s: %w", keyspace, err)
		}
		for t := range ddlTables {
			affectedTables[t] = struct{}{}
		}

		// Get current CREATE TABLE only for affected tables via shard-targeted connection.
		conn, cleanup, err := s.vtgateShardConn(ctx, backend, keyspace)
		if err != nil {
			return nil, fmt.Errorf("shard conn for %s: %w", keyspace, err)
		}
		var currentStmts []string
		for table := range affectedTables {
			var tName, createStmt string
			if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+quoteIdentifier(table)).Scan(&tName, &createStmt); err != nil {
				continue // Table doesn't exist in current schema (e.g., was DROPped by the deploy)
			}
			currentStmts = append(currentStmts, createStmt)
		}
		cleanup()

		// Diff current → before to get reverse DDL.
		result, err := differ.DiffStatements(currentStmts, beforeStmts)
		if err != nil {
			return nil, fmt.Errorf("diff %s: %w", keyspace, err)
		}
		if len(result.Statements) > 0 {
			reverseDDL[keyspace] = result.Statements
		}
	}
	return reverseDDL, nil
}

// parseDeployNumber parses the deploy request number from the URL path.
func (s *Server) parseDeployNumber(r *http.Request) (uint64, error) {
	number, err := strconv.ParseUint(r.PathValue("number"), 10, 64)
	if err != nil {
		return 0, newHTTPError(http.StatusBadRequest, "invalid deploy request number: %v", err)
	}
	return number, nil
}

type deployRequest struct {
	org      string
	database string
	number   uint64
}

// resolveDeployAction is shared preamble for deploy action handlers: resolves the
// backend from URL path values and parses the deploy number.
func (s *Server) resolveDeployAction(r *http.Request) (*databaseBackend, deployRequest, error) {
	org := r.PathValue("org")
	database := r.PathValue("db")
	backend, err := s.backendFor(org, database)
	if err != nil {
		return nil, deployRequest{}, newHTTPError(http.StatusNotFound, "%v", err)
	}
	number, err := s.parseDeployNumber(r)
	if err != nil {
		return nil, deployRequest{}, err // already an *httpError
	}
	return backend, deployRequest{org: org, database: database, number: number}, nil
}

// deployResponse returns the standard deploy request response map with number, branch, and state.
func deployResponse(number uint64, branch, state string) map[string]any {
	return map[string]any{
		"number":           number,
		"branch":           branch,
		"deployment_state": state,
	}
}

// deployRequestInfo holds commonly needed fields from a deploy request row.
type deployRequestInfo struct {
	branch           string
	migrationContext string
	deploymentState  string
}

// getDeployRequestInfo fetches common deploy request fields.
func (s *Server) getDeployRequestInfo(ctx context.Context, ref deployRequest) (*deployRequestInfo, error) {
	var info deployRequestInfo
	err := s.metadataDB.QueryRowContext(ctx,
		`SELECT branch, migration_context, deployment_state
		 FROM localscale_deploy_requests
		 WHERE org = ? AND database_name = ? AND number = ?`,
		ref.org, ref.database, ref.number,
	).Scan(&info.branch, &info.migrationContext, &info.deploymentState)
	if err != nil {
		return nil, newHTTPError(http.StatusNotFound, "deploy request not found: %d", ref.number)
	}
	return &info, nil
}

// dropBranchDatabases drops all branch databases for a given branch name
// and shuts down any TCP proxy associated with the branch. Branches live on
// the backend's mysqld (not the metadata DB), so we connect there for the DROP.
func (s *Server) dropBranchDatabases(ctx context.Context, backend *databaseBackend, branch string) {
	db, err := sql.Open("mysql", backend.mysqlDSNBase)
	if err != nil {
		s.logger.Error("dropBranchDatabases: open backend", "branch", branch, "error", err)
		return
	}
	if err := db.PingContext(ctx); err != nil {
		s.logger.Error("dropBranchDatabases: ping backend", "branch", branch, "error", err)
		utils.CloseAndLog(db)
		return
	}
	defer utils.CloseAndLog(db)

	for keyspace := range backend.vtgateDBs {
		dbName := branchDBName(branch, keyspace)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteIdentifier(dbName))); err != nil {
			s.logger.Warn("drop branch database", "branch", branch, "keyspace", keyspace, "error", err)
		}
	}
	s.closeProxy(branch)
}

// normalizeJSON re-marshals JSON to normalize formatting for comparison.
func normalizeJSON(data []byte) string {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return string(data)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// checkInstantEligibility tests whether all ALTER TABLE statements in a deploy
// can use ALGORITHM=INSTANT. It creates a temporary scratch database per
// keyspace, copies table schemas from main, and tests each ALTER with
// ALGORITHM=INSTANT. Non-ALTER statements (CREATE/DROP TABLE) are skipped since
// they don't involve row copy, but a deploy with no ALTERs is not instant DDL.
func (s *Server) checkInstantEligibility(ctx context.Context, backend *databaseBackend, ddlByKeyspace map[string][]string) bool {
	sawAlter := false
	keyspaceIndex := 0
	for keyspace, stmts := range ddlByKeyspace {
		var alterStmts []string
		for _, stmt := range stmts {
			if addAlgorithmInstant(stmt) != "" {
				alterStmts = append(alterStmts, stmt)
			}
		}
		if len(alterStmts) == 0 {
			continue
		}
		sawAlter = true
		scratchDB := fmt.Sprintf("_ls_instant_%d_%d", time.Now().UnixNano(), keyspaceIndex)
		keyspaceIndex++

		if _, err := s.metadataDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", scratchDB)); err != nil {
			s.logger.Warn("instant check: create scratch db", "keyspace", keyspace, "error", err)
			return false
		}
		defer func() {
			if _, err := s.metadataDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", scratchDB)); err != nil {
				s.logger.Warn("instant check: drop scratch db", "keyspace", keyspace, "error", err)
			}
		}()

		mainSchemas, err := s.snapshotKeyspaceSchema(ctx, backend, keyspace)
		if err != nil {
			s.logger.Warn("instant check: snapshot schema", "keyspace", keyspace, "error", err)
			return false
		}

		// Copy table schemas into scratch database
		for _, createStmt := range mainSchemas {
			prefixed := strings.Replace(createStmt, "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", scratchDB), 1)
			if _, err := s.metadataDB.ExecContext(ctx, prefixed); err != nil {
				s.logger.Warn("instant check: copy table", "keyspace", keyspace, "error", err)
				return false
			}
		}

		// Test each ALTER with ALGORITHM=INSTANT.
		// Non-ALTER statements (CREATE/DROP TABLE) are skipped — they don't
		// affect instant eligibility since they don't involve row copy.
		for _, stmt := range alterStmts {
			instantStmt := addAlgorithmInstant(stmt)
			tableName := extractAlterTableName(stmt)
			if tableName == "" {
				s.logger.Info("instant check: parse failed", "keyspace", keyspace)
				return false
			}
			scratchStmt := qualifyAlterTableName(instantStmt, scratchDB)
			if scratchStmt == "" {
				s.logger.Info("instant check: qualify failed", "keyspace", keyspace, "table", tableName)
				return false
			}
			if _, err := s.metadataDB.ExecContext(ctx, scratchStmt); err != nil {
				s.logger.Info("instant check: not eligible", "keyspace", keyspace, "table", tableName, "error", err)
				return false
			}
			s.logger.Info("instant check: eligible", "keyspace", keyspace, "table", tableName)
		}
	}
	return sawAlter
}

// hasAlterTableStatements returns true if any DDL statement is an ALTER TABLE.
func hasAlterTableStatements(ddlByKeyspace map[string][]string) bool {
	for _, stmts := range ddlByKeyspace {
		for _, stmt := range stmts {
			if addAlgorithmInstant(stmt) != "" {
				return true
			}
		}
	}
	return false
}

// extractAlterTableName extracts the table name from an ALTER TABLE statement
// using Spirit's SQL parser.
func extractAlterTableName(stmt string) string {
	results, err := statement.Classify(stmt)
	if err != nil || len(results) == 0 {
		return ""
	}
	if results[0].Type != statement.StatementAlterTable {
		return ""
	}
	return results[0].Table
}

// qualifyAlterTableName rewrites an ALTER TABLE statement to use a fully-qualified
// table name (schema.table) using Spirit's parsed Alter clause.
func qualifyAlterTableName(stmt, schemaName string) string {
	parsed, err := statement.New(stmt)
	if err != nil || len(parsed) == 0 {
		return ""
	}
	r := parsed[0]
	if r.Table == "" || r.Alter == "" {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", schemaName, r.Table, r.Alter)
}

// addAlgorithmInstant rewrites an ALTER TABLE statement to include ALGORITHM=INSTANT.
// Returns "" for non-ALTER TABLE statements (CREATE TABLE, DROP TABLE, etc.).
func addAlgorithmInstant(stmt string) string {
	parsed, err := statement.New(strings.TrimSpace(stmt))
	if err != nil || len(parsed) == 0 {
		return ""
	}
	r := parsed[0]
	if r.Table == "" || r.Alter == "" {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE `%s` ALGORITHM=INSTANT, %s", r.Table, r.Alter)
}

func branchDBName(branch, keyspace string) string {
	return fmt.Sprintf("branch_%s_%s", branch, keyspace)
}

// openBranchDB opens a temporary connection to a branch database on the correct
// backend mysqld. Each org/database has its own managed cluster with its own mysqld,
// so the branch's org/database is looked up from metadata to find the right backend.
// Callers must close the returned DB when done.
func (s *Server) openBranchDB(ctx context.Context, branch, keyspace string) (*sql.DB, error) {
	// Look up the branch's org/database to find the correct backend mysqld.
	var org, database string
	err := s.metadataDB.QueryRowContext(ctx,
		"SELECT org, database_name FROM localscale_branches WHERE name = ?", branch,
	).Scan(&org, &database)
	if err != nil {
		return nil, fmt.Errorf("look up branch %s: %w", branch, err)
	}

	backend, err := s.backendFor(org, database)
	if err != nil {
		return nil, fmt.Errorf("find backend for branch %s (%s/%s): %w", branch, org, database, err)
	}

	dsn := backend.mysqlDSNBase + branchDBName(branch, keyspace)
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

// waitForOnlineDDLReady polls each keyspace's sidecar database until Vitess's
// online DDL executor is operational. The executor requires the per-shard sidecar
// databases to be initialized before it can process migrations. Without this check,
// the first deploy request's DDL submission can hang waiting for a sidecar that
// hasn't been created yet.
func (s *Server) waitForOnlineDDLReady(ctx context.Context) error {
	for key, backend := range s.backends {
		for keyspace, db := range backend.vtgateDBs {
			deadline := time.Now().Add(60 * time.Second)
			ready := false
			for time.Now().Before(deadline) {
				rows, err := db.QueryContext(ctx, "SHOW VITESS_MIGRATIONS")
				if err == nil {
					utils.CloseAndLog(rows)
					s.logger.Info("online DDL ready", "database", key.database, "keyspace", keyspace)
					ready = true
					break
				}
				s.logger.Debug("waiting for online DDL readiness", "database", key.database, "keyspace", keyspace, "error", err)
				time.Sleep(time.Second)
			}
			if !ready {
				return fmt.Errorf("online DDL not ready after 60s for %s/%s", key.database, keyspace)
			}
		}
	}
	return nil
}

// execLog executes a SQL statement and logs any error. Use this instead of
// `_, _ = s.metadataDB.ExecContext(...)` to ensure DB errors are never silently lost.
func (s *Server) execLog(ctx context.Context, query string, args ...any) error {
	if _, err := s.metadataDB.ExecContext(ctx, query, args...); err != nil {
		s.logger.Error("exec failed", "query", query[:min(len(query), 80)], "error", err)
		return fmt.Errorf("exec %s: %w", query[:min(len(query), 80)], err)
	}
	return nil
}

// decodeJSON decodes the request body into v.
func (s *Server) decodeJSON(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return newHTTPError(http.StatusBadRequest, "decode body: %v", err)
	}
	return nil
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
	// Use PlanetScale SDK error code strings so the SDK's error parser
	// produces typed *ps.Error values (e.g., ErrNotFound for 404).
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code":    httpStatusToPSCode(code),
		"message": msg,
	})
}

// httpStatusToPSCode maps HTTP status codes to PlanetScale SDK error code
// strings. The SDK parses these in its error handler to set ps.Error.Code.
func httpStatusToPSCode(status int) string {
	switch status {
	case http.StatusNotFound:
		return "not_found"
	case http.StatusUnauthorized, http.StatusForbidden:
		return "unauthorized"
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnprocessableEntity:
		return "unprocessable"
	default:
		return fmt.Sprintf("%d", status)
	}
}
