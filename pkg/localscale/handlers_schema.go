package localscale

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) handleListKeyspaces(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	database := r.PathValue("db")
	s.logger.Debug("list keyspaces", "org", org, "database", database)

	backend, err := s.backendFor(org, database)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "%v", err)
		return
	}

	resp, err := backend.vtctld.GetKeyspaces(r.Context(), &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "list keyspaces: %v", err)
		return
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
}

func (s *Server) handleGetBranchSchema(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")
	s.logger.Info("get branch schema", "org", org, "database", database, "branch", branch)

	backend, err := s.backendFor(org, database)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "%v", err)
		return
	}

	keyspace := r.URL.Query().Get("keyspace")
	if keyspace == "" {
		s.writeError(w, http.StatusBadRequest, "keyspace query parameter required")
		return
	}

	var tables []table.TableSchema
	if branch == "main" {
		// Use a shard-targeted connection to bypass vtgate's schema tracker cache.
		conn, cleanup, err := s.vtgateShardConn(r.Context(), backend, keyspace)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "shard-targeted conn for %s: %v", keyspace, err)
			return
		}
		defer cleanup()

		tables, err = showCreateAllFromConn(r.Context(), conn, table.WithoutUnderscoreTables)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "%v", err)
			return
		}
	} else {
		// Read schema from the branch database.
		branchDB, err := s.openBranchDB(r.Context(), branch, keyspace)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "branch database not found: %v", err)
			return
		}
		defer utils.CloseAndLog(branchDB)

		tables, err = table.LoadSchemaFromDB(r.Context(), branchDB, table.WithoutUnderscoreTables)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "%v", err)
			return
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
}

func (s *Server) handleGetBranchVSchema(w http.ResponseWriter, r *http.Request) {
	keyspace := r.URL.Query().Get("keyspace")
	if keyspace == "" {
		s.writeError(w, http.StatusBadRequest, "keyspace query parameter required")
		return
	}
	s.serveKeyspaceVSchema(w, r, keyspace, false)
}

// handleGetKeyspaceVSchema serves the standard PS SDK path: /branches/{branch}/keyspaces/{keyspace}/vschema
func (s *Server) handleGetKeyspaceVSchema(w http.ResponseWriter, r *http.Request) {
	keyspace := r.PathValue("keyspace")
	s.serveKeyspaceVSchema(w, r, keyspace, true)
}

// serveKeyspaceVSchema is the shared implementation for VSchema GET handlers.
// includeHTML controls whether the response includes an "html" field (PS SDK compat).
func (s *Server) serveKeyspaceVSchema(w http.ResponseWriter, r *http.Request, keyspace string, includeHTML bool) {
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
			s.writeError(w, http.StatusNotFound, "%v", err)
			return
		}

		resp, err := backend.vtctld.GetVSchema(r.Context(), &vtctldatapb.GetVSchemaRequest{
			Keyspace: keyspace,
		})
		if err != nil {
			s.writeJSON(w, emptyResp)
			return
		}
		data, err := vschemaMarshaler.Marshal(resp.VSchema)
		if err != nil {
			s.writeJSON(w, emptyResp)
			return
		}
		result := map[string]any{"raw": string(data)}
		if includeHTML {
			result["html"] = ""
		}
		s.writeJSON(w, result)
		return
	}

	// For non-main branches, read vschema_data from the branch row.
	var vschemaSQL sql.NullString
	err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT vschema_data FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branch,
	).Scan(&vschemaSQL)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "branch not found: %s", branch)
		return
	}

	if !hasVSchemaData(vschemaSQL) {
		s.writeJSON(w, emptyResp)
		return
	}

	var vschemaMap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(vschemaSQL.String), &vschemaMap); err != nil {
		s.writeJSON(w, emptyResp)
		return
	}

	ksData, ok := vschemaMap[keyspace]
	if !ok {
		s.writeJSON(w, emptyResp)
		return
	}

	result := map[string]any{"raw": string(ksData)}
	if includeHTML {
		result["html"] = ""
	}
	s.writeJSON(w, result)
}

// handleUpdateKeyspaceVSchema serves PATCH /branches/{branch}/keyspaces/{keyspace}/vschema
func (s *Server) handleUpdateKeyspaceVSchema(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")
	keyspace := r.PathValue("keyspace")

	var body struct {
		VSchema string `json:"vschema"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}

	if !json.Valid([]byte(body.VSchema)) {
		s.writeError(w, http.StatusBadRequest, "invalid VSchema JSON")
		return
	}

	// Read existing vschema_data from branch row.
	var vschemaSQL sql.NullString
	err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT vschema_data FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branch,
	).Scan(&vschemaSQL)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "branch not found: %s", branch)
		return
	}

	existing := make(map[string]json.RawMessage)
	if hasVSchemaData(vschemaSQL) {
		_ = json.Unmarshal([]byte(vschemaSQL.String), &existing)
	}

	existing[keyspace] = json.RawMessage(body.VSchema)
	merged, _ := json.Marshal(existing)

	_, err = s.metadataDB.ExecContext(r.Context(),
		"UPDATE localscale_branches SET vschema_data = ? WHERE org = ? AND database_name = ? AND name = ?",
		string(merged), org, database, branch,
	)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "update branch vschema: %v", err)
		return
	}

	s.logger.Info("updated branch vschema", "org", org, "database", database, "branch", branch, "keyspace", keyspace)
	s.writeJSON(w, map[string]any{"raw": body.VSchema, "html": ""})
}
