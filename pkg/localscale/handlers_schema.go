package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) handleListKeyspaces(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	s.logger.Debug("list keyspaces", "org", org, "database", database)

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	resp, err := backend.vtctld.GetKeyspaces(r.Context(), &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "list keyspaces: %v", err)
	}

	type keyspaceJSON struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Shards  int    `json:"shards"`
		Sharded bool   `json:"sharded"`
		Ready   bool   `json:"ready"`
	}

	var keyspaces []keyspaceJSON
	for _, ks := range resp.Keyspaces {
		shards := backend.shardCounts[ks.Name]
		if shards == 0 {
			shards = 1
		}
		keyspaces = append(keyspaces, keyspaceJSON{
			ID:      ks.Name,
			Name:    ks.Name,
			Shards:  shards,
			Sharded: shards > 1,
			Ready:   true,
		})
	}

	s.writeJSON(w, map[string]any{"data": keyspaces})
	return nil
}

func (s *Server) handleGetBranchSchema(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")
	s.logger.Info("get branch schema", "org", org, "database", database, "branch", branch)

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	keyspace := r.URL.Query().Get("keyspace")
	if keyspace == "" {
		return newHTTPError(http.StatusBadRequest, "keyspace query parameter required")
	}

	var tables []table.TableSchema
	if branch == "main" {
		// Use a shard-targeted connection to bypass vtgate's schema tracker cache.
		conn, cleanup, err := s.vtgateShardConn(r.Context(), backend, keyspace)
		if err != nil {
			return newHTTPError(http.StatusInternalServerError, "shard-targeted conn for %s: %v", keyspace, err)
		}
		defer cleanup()

		tables, err = showCreateAllFromConn(r.Context(), conn, table.WithoutUnderscoreTables)
		if err != nil {
			return newHTTPError(http.StatusInternalServerError, "%v", err)
		}
	} else {
		// Read schema from the branch database.
		branchDB, err := s.openBranchDB(r.Context(), branch, keyspace)
		if err != nil {
			return newHTTPError(http.StatusNotFound, "branch database not found: %v", err)
		}
		defer utils.CloseAndLog(branchDB)

		tables, err = table.LoadSchemaFromDB(r.Context(), branchDB, table.WithoutUnderscoreTables)
		if err != nil {
			return newHTTPError(http.StatusInternalServerError, "%v", err)
		}
	}

	type schemaEntry struct {
		Name string `json:"name"`
		Raw  string `json:"raw"`
		HTML string `json:"html"`
	}

	schemas := make([]schemaEntry, len(tables))
	for i, t := range tables {
		schemas[i] = schemaEntry{Name: t.Name, Raw: t.Schema}
	}

	s.writeJSON(w, map[string]any{"data": schemas})
	return nil
}

// handleBranchTableMetrics serves the branch table metrics endpoint, which the
// PlanetScale SDK does not wrap: GET .../branches/{branch}/metrics/tables. The
// response is one flat JSON object keyed by table name — every keyspace on the
// branch in a single call — each entry carrying that table's storage_bytes.
// Bytes are computed from the backing cluster's table statistics (data plus
// index length), so seeded tables report a real, non-zero footprint and the
// figure is the whole-branch total: summed across a keyspace's shards on main,
// and read from the branch databases on other branches.
func (s *Server) handleBranchTableMetrics(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")
	s.logger.Debug("branch table metrics", "org", org, "database", database, "branch", branch)

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	resp, err := backend.vtctld.GetKeyspaces(r.Context(), &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "list keyspaces: %v", err)
	}

	totals := make(map[string]int64)
	for _, ks := range resp.Keyspaces {
		if branch == "main" {
			err := s.forEachShard(r.Context(), backend, ks.Name, func(conn *sql.Conn) error {
				return accumulateTableStorageBytes(r.Context(), conn, totals)
			})
			if err != nil {
				return newHTTPError(http.StatusInternalServerError, "table metrics for keyspace %s: %v", ks.Name, err)
			}
			continue
		}

		branchDB, err := s.openBranchDB(r.Context(), branch, ks.Name)
		if err != nil {
			return newHTTPError(http.StatusNotFound, "branch database not found: %v", err)
		}
		accErr := accumulateTableStorageBytes(r.Context(), branchDB, totals)
		utils.CloseAndLog(branchDB)
		if accErr != nil {
			return newHTTPError(http.StatusInternalServerError, "table metrics for branch %s keyspace %s: %v", branch, ks.Name, accErr)
		}
	}

	type tableMetricsJSON struct {
		StorageBytes int64 `json:"storage_bytes"`
	}
	payload := make(map[string]tableMetricsJSON, len(totals))
	for name, storageBytes := range totals {
		payload[name] = tableMetricsJSON{StorageBytes: storageBytes}
	}
	s.writeJSON(w, payload)
	return nil
}

// rowQuerier abstracts the query surface shared by *sql.DB and *sql.Conn so
// table statistics can be read from a shard-targeted connection or a branch
// database pool alike.
type rowQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// accumulateTableStorageBytes adds each user table's data-plus-index footprint
// from SHOW TABLE STATUS into totals, keyed by table name. Adding lets callers
// sum one table's footprint across shards. Underscore-prefixed tables (Vitess
// internals and online DDL artifacts) are excluded, matching the schema
// handlers' table filtering.
func accumulateTableStorageBytes(ctx context.Context, q rowQuerier, totals map[string]int64) error {
	rows, err := q.QueryContext(ctx, "SHOW TABLE STATUS")
	if err != nil {
		return fmt.Errorf("show table status: %w", err)
	}
	statuses, err := scanDynamicRows(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return fmt.Errorf("scan table status: %w", err)
	}
	for _, st := range statuses {
		name := st["Name"]
		if strings.HasPrefix(name, "_") {
			continue
		}
		var storageBytes int64
		for _, col := range []string{"Data_length", "Index_length"} {
			v, ok := st[col]
			if !ok || v == "" {
				// NULL statistics (views, or a storage engine that reports
				// none) contribute nothing to the footprint.
				continue
			}
			n, parseErr := strconv.ParseInt(v, 10, 64)
			if parseErr != nil {
				return fmt.Errorf("parse %s for table %s: %w", col, name, parseErr)
			}
			storageBytes += n
		}
		totals[name] += storageBytes
	}
	return nil
}

func (s *Server) handleGetBranchVSchema(w http.ResponseWriter, r *http.Request) error {
	keyspace := r.URL.Query().Get("keyspace")
	if keyspace == "" {
		return newHTTPError(http.StatusBadRequest, "keyspace query parameter required")
	}
	return s.serveKeyspaceVSchema(w, r, keyspace, false)
}

// handleGetKeyspaceVSchema serves the standard PS SDK path: /branches/{branch}/keyspaces/{keyspace}/vschema
func (s *Server) handleGetKeyspaceVSchema(w http.ResponseWriter, r *http.Request) error {
	keyspace := r.PathValue("keyspace")
	return s.serveKeyspaceVSchema(w, r, keyspace, true)
}

// serveKeyspaceVSchema is the shared implementation for VSchema GET handlers.
// includeHTML controls whether the response includes an "html" field (PS SDK compat).
func (s *Server) serveKeyspaceVSchema(w http.ResponseWriter, r *http.Request, keyspace string, includeHTML bool) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")

	emptyResp := map[string]any{"raw": "{}"}
	if includeHTML {
		emptyResp["html"] = ""
	}

	if branch == "main" {
		backend, err := s.backendFor(org, database)
		if err != nil {
			return newHTTPError(http.StatusNotFound, "%v", err)
		}

		resp, err := backend.vtctld.GetVSchema(r.Context(), &vtctldatapb.GetVSchemaRequest{
			Keyspace: keyspace,
		})
		if err != nil {
			s.writeJSON(w, emptyResp)
			return nil
		}
		data, err := vschemaMarshaler.Marshal(resp.VSchema)
		if err != nil {
			s.writeJSON(w, emptyResp)
			return nil
		}
		result := map[string]any{"raw": string(data)}
		if includeHTML {
			result["html"] = ""
		}
		s.writeJSON(w, result)
		return nil
	}

	// For non-main branches, read vschema_data from the branch row.
	var vschemaSQL sql.NullString
	err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT vschema_data FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branch,
	).Scan(&vschemaSQL)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "branch not found: %s", branch)
	}

	if !hasVSchemaData(vschemaSQL) {
		s.writeJSON(w, emptyResp)
		return nil
	}

	var vschemaMap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(vschemaSQL.String), &vschemaMap); err != nil {
		s.writeJSON(w, emptyResp)
		return nil
	}

	ksData, ok := vschemaMap[keyspace]
	if !ok {
		s.writeJSON(w, emptyResp)
		return nil
	}

	result := map[string]any{"raw": string(ksData)}
	if includeHTML {
		result["html"] = ""
	}
	s.writeJSON(w, result)
	return nil
}

// handleUpdateKeyspaceVSchema serves PATCH /branches/{branch}/keyspaces/{keyspace}/vschema
func (s *Server) handleUpdateKeyspaceVSchema(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")
	keyspace := r.PathValue("keyspace")

	var body struct {
		VSchema string `json:"vschema"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	if !json.Valid([]byte(body.VSchema)) {
		return newHTTPError(http.StatusBadRequest, "invalid VSchema JSON")
	}

	// Use a transaction with row locking to prevent concurrent updates
	// from overwriting each other (read-modify-write race).
	tx, err := s.metadataDB.BeginTx(r.Context(), nil)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var vschemaSQL sql.NullString
	err = tx.QueryRowContext(r.Context(),
		"SELECT vschema_data FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ? FOR UPDATE",
		org, database, branch,
	).Scan(&vschemaSQL)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "branch not found: %s", branch)
	}

	existing := make(map[string]json.RawMessage)
	if hasVSchemaData(vschemaSQL) {
		_ = json.Unmarshal([]byte(vschemaSQL.String), &existing)
	}

	existing[keyspace] = json.RawMessage(body.VSchema)
	merged, _ := json.Marshal(existing)

	_, err = tx.ExecContext(r.Context(),
		"UPDATE localscale_branches SET vschema_data = ? WHERE org = ? AND database_name = ? AND name = ?",
		string(merged), org, database, branch,
	)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "update branch vschema: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return newHTTPError(http.StatusInternalServerError, "commit vschema update: %v", err)
	}

	s.logger.Info("updated branch vschema", "org", org, "database", database, "branch", branch, "keyspace", keyspace)
	s.writeJSON(w, map[string]any{"raw": body.VSchema, "html": ""})
	return nil
}
