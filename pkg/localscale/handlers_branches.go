package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/utils"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) handleGetBranch(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branchName := r.PathValue("branch")

	// The "main" branch is virtual — it always exists and represents the live database.
	if branchName == "main" {
		if _, err := s.backendFor(org, database); err != nil {
			s.writeError(w, http.StatusNotFound, "%v", err)
			return
		}
		s.writeJSON(w, map[string]any{
			"name":            "main",
			"parent_branch":   "",
			"ready":           true,
			"safe_migrations": true,
			"region":          map[string]string{"slug": "us-east-1"},
		})
		return
	}

	var name, parentBranch, region string
	var ready bool
	var errorMessage sql.NullString
	err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT name, parent_branch, region, ready, error_message FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branchName,
	).Scan(&name, &parentBranch, &region, &ready, &errorMessage)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "branch not found: %s", branchName)
		return
	}

	resp := map[string]any{
		"name":            name,
		"parent_branch":   parentBranch,
		"ready":           ready,
		"safe_migrations": true,
		"region":          map[string]string{"slug": region},
	}
	if errorMessage.Valid && errorMessage.String != "" {
		resp["error_message"] = errorMessage.String
	}
	s.writeJSON(w, resp)
}

func (s *Server) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	database := r.PathValue("db")

	backend, err := s.backendFor(org, database)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "%v", err)
		return
	}

	var body struct {
		Name         string `json:"name"`
		ParentBranch string `json:"parent_branch"`
		Region       string `json:"region"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}
	if body.Region == "" {
		body.Region = "us-east-1"
	}

	if err := validateBranchName(body.Name); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid branch name: %v", err)
		return
	}

	_, err = s.metadataDB.ExecContext(r.Context(),
		"INSERT INTO localscale_branches (org, database_name, name, parent_branch, region, ready) VALUES (?, ?, ?, ?, ?, FALSE)",
		org, database, body.Name, body.ParentBranch, body.Region,
	)
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") {
			s.writeError(w, http.StatusConflict, "Name has already been taken")
		} else {
			s.writeError(w, http.StatusInternalServerError, "insert branch: %v", err)
		}
		return
	}

	// Background goroutine snapshots schema from vtgate into branch databases,
	// snapshots VSchema from vtctld onto the branch row, then marks ready=true.
	s.wg.Go(func() {
		bgCtx := context.Background()
		snapshotFailed := false
		var snapshotErrors []string

		// Snapshot schema from vtgate into branch databases for each keyspace.
		for keyspace := range backend.vtgateDBs {
			dbName := branchDBName(body.Name, keyspace)

			// Create branch database in localscale-mysql
			if err := validateIdentifier(dbName); err != nil {
				s.logger.Error("invalid branch database name", "name", dbName, "error", err)
				snapshotFailed = true
				snapshotErrors = append(snapshotErrors, fmt.Sprintf("invalid branch database name %s: %v", dbName, err))
				continue
			}
			if _, err := s.metadataDB.ExecContext(bgCtx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(dbName)); err != nil {
				s.logger.Error("create branch database", "branch", body.Name, "keyspace", keyspace, "error", err)
				snapshotFailed = true
				snapshotErrors = append(snapshotErrors, fmt.Sprintf("create branch database %s: %v", keyspace, err))
				continue
			}

			// Get schema from vtgate
			stmts, err := s.snapshotKeyspaceSchema(bgCtx, backend, keyspace)
			if err != nil {
				s.logger.Error("snapshot keyspace schema", "branch", body.Name, "keyspace", keyspace, "error", err)
				snapshotFailed = true
				snapshotErrors = append(snapshotErrors, fmt.Sprintf("snapshot keyspace schema %s: %v", keyspace, err))
				continue
			}

			// Execute CREATE TABLEs in branch database
			if len(stmts) > 0 {
				branchDB, err := s.openBranchDB(bgCtx, body.Name, keyspace)
				if err != nil {
					s.logger.Error("open branch database", "branch", body.Name, "keyspace", keyspace, "error", err)
					snapshotFailed = true
					snapshotErrors = append(snapshotErrors, fmt.Sprintf("open branch database %s: %v", keyspace, err))
					continue
				}
				for _, stmt := range stmts {
					if _, err := branchDB.ExecContext(bgCtx, stmt); err != nil {
						s.logger.Error("execute CREATE TABLE in branch", "branch", body.Name, "keyspace", keyspace, "stmt", stmt, "error", err)
						snapshotFailed = true
						snapshotErrors = append(snapshotErrors, fmt.Sprintf("execute CREATE TABLE in branch %s: %v", keyspace, err))
						break
					}
				}
				utils.CloseAndLog(branchDB)
			}
		}

		// Snapshot VSchema from vtctld for each keyspace
		vschemaSnapshot := make(map[string]json.RawMessage)
		for keyspace := range backend.vtgateDBs {
			resp, err := backend.vtctld.GetVSchema(bgCtx, &vtctldatapb.GetVSchemaRequest{Keyspace: keyspace})
			if err != nil {
				s.logger.Warn("snapshot vschema", "branch", body.Name, "keyspace", keyspace, "error", err)
				continue
			}
			data, err := vschemaMarshaler.Marshal(resp.VSchema)
			if err != nil {
				s.logger.Warn("marshal vschema snapshot", "branch", body.Name, "keyspace", keyspace, "error", err)
				continue
			}
			vschemaSnapshot[keyspace] = data
		}

		// Store VSchema snapshot on branch row
		if len(vschemaSnapshot) > 0 {
			vschemaJSON, _ := json.Marshal(vschemaSnapshot)
			s.execLog(bgCtx,
				"UPDATE localscale_branches SET vschema_data = ? WHERE org = ? AND database_name = ? AND name = ?",
				string(vschemaJSON), org, database, body.Name,
			)
		}

		if snapshotFailed {
			errMsg := strings.Join(snapshotErrors, "; ")
			s.logger.Error("branch snapshot failed, not marking ready", "name", body.Name, "errors", errMsg)
			s.execLog(bgCtx,
				"UPDATE localscale_branches SET error_message = ? WHERE org = ? AND database_name = ? AND name = ?",
				errMsg, org, database, body.Name,
			)
			return
		}

		// Mark branch as ready
		s.execLog(bgCtx,
			"UPDATE localscale_branches SET ready = TRUE WHERE org = ? AND database_name = ? AND name = ?",
			org, database, body.Name,
		)
		s.logger.Info("branch ready", "name", body.Name)
	})

	s.writeJSON(w, map[string]any{
		"name":          body.Name,
		"parent_branch": body.ParentBranch,
		"ready":         false,
		"region":        map[string]string{"slug": body.Region},
	})
}

// handleApplyBranchSchema executes DDL statements against branch databases and updates
// VSchema on the branch row. The PlanetScale engine calls this before CreateDeployRequest.
//
// For each ALTER TABLE, it first tries ALGORITHM=INSTANT against the branch database.
// If all ALTERs succeed with ALGORITHM=INSTANT and there are no non-ALTER statements,
// the branch is marked instant_ddl_eligible=true. If any ALTER fails ALGORITHM=INSTANT
// or any non-ALTER statement (CREATE TABLE, DROP TABLE) is present, the branch is marked
// instant_ddl_eligible=false. This matches PlanetScale behavior.
func (s *Server) handleApplyBranchSchema(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")

	var body struct {
		DDL     map[string][]string        `json:"ddl"`
		VSchema map[string]json.RawMessage `json:"vschema"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}

	// Verify branch exists and is ready before applying schema.
	var branchReady bool
	err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT ready FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branch,
	).Scan(&branchReady)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "branch not found: %s", branch)
		return
	}
	if !branchReady {
		s.writeError(w, http.StatusConflict, "branch %s is not ready", branch)
		return
	}

	// Execute DDL against branch databases, detecting instant DDL eligibility.
	// For ALTER TABLE statements, try ALGORITHM=INSTANT first. If it succeeds,
	// the DDL is already applied. If it fails, fall back to normal execution.
	// Non-ALTER statements (CREATE TABLE, DROP TABLE) are not instant-eligible
	// — matching PlanetScale behavior where only ALTER TABLE can be instant.
	// Validate all statements are DDL before executing any.
	for _, stmts := range body.DDL {
		for _, stmt := range stmts {
			if err := sanitizeDDL(stmt); err != nil {
				s.writeError(w, http.StatusBadRequest, "invalid DDL: %v", err)
				return
			}
		}
	}

	totalDDL := 0
	allInstant := true
	for keyspace, stmts := range body.DDL {
		branchDB, err := s.openBranchDB(r.Context(), branch, keyspace)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "open branch db for %s: %v", keyspace, err)
			return
		}
		for _, stmt := range stmts {
			if instantStmt := addAlgorithmInstant(stmt); instantStmt != "" {
				// ALTER TABLE: try ALGORITHM=INSTANT first
				if _, err := branchDB.ExecContext(r.Context(), instantStmt); err == nil {
					totalDDL++
					continue // Instant succeeded — DDL already applied
				}
				// Not instant-eligible, fall back to normal execution
				allInstant = false
			} else {
				// Non-ALTER statements (CREATE TABLE, DROP TABLE) are not
				// instant-eligible, matching PlanetScale behavior.
				allInstant = false
			}
			if _, err := branchDB.ExecContext(r.Context(), stmt); err != nil {
				utils.CloseAndLog(branchDB)
				s.writeError(w, http.StatusInternalServerError, "execute DDL in branch %s/%s: %v\nstatement: %s", branch, keyspace, err, stmt)
				return
			}
			totalDDL++
		}
		utils.CloseAndLog(branchDB)
	}

	// If there are no DDL statements, instant eligibility is false.
	// VSchema-only changes are not instant-eligible per PlanetScale behavior.
	if totalDDL == 0 {
		allInstant = false
	}

	// Store instant eligibility on the branch row
	s.execLog(r.Context(),
		"UPDATE localscale_branches SET instant_ddl_eligible = ? WHERE org = ? AND database_name = ? AND name = ?",
		allInstant, org, database, branch,
	)

	// Update VSchema on branch row (merge with existing snapshot from branch creation)
	if len(body.VSchema) > 0 {
		var existingVSchemaSQL sql.NullString
		_ = s.metadataDB.QueryRowContext(r.Context(),
			"SELECT vschema_data FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
			org, database, branch,
		).Scan(&existingVSchemaSQL)

		existing := make(map[string]json.RawMessage)
		if existingVSchemaSQL.Valid && existingVSchemaSQL.String != "" {
			if err := json.Unmarshal([]byte(existingVSchemaSQL.String), &existing); err != nil {
				s.logger.Warn("unmarshal existing branch vschema", "branch", branch, "error", err)
			}
		}
		maps.Copy(existing, body.VSchema)
		merged, _ := json.Marshal(existing)
		_, err := s.metadataDB.ExecContext(r.Context(),
			"UPDATE localscale_branches SET vschema_data = ? WHERE org = ? AND database_name = ? AND name = ?",
			string(merged), org, database, branch,
		)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "update branch vschema: %v", err)
			return
		}
	}

	s.logger.Info("applied DDL to branch", "org", org, "database", database, "branch", branch, "keyspace_count", len(body.DDL), "total_ddl", totalDDL, "vschema_count", len(body.VSchema))
	s.writeJSON(w, map[string]any{"ok": true, "total_ddl": totalDDL, "vschema_count": len(body.VSchema)})
}

func (s *Server) handleCreateBranchPassword(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")

	var body struct {
		Name string `json:"name"`
		Role string `json:"role"`
		TTL  int    `json:"ttl"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}

	backend, err := s.backendFor(org, database)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "%v", err)
		return
	}

	// Verify the branch exists (main is always valid).
	if branch != "main" {
		var ready bool
		err := s.metadataDB.QueryRowContext(r.Context(),
			"SELECT ready FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
			org, database, branch,
		).Scan(&ready)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "branch not found: %s", branch)
			return
		}
		if !ready {
			s.writeError(w, http.StatusConflict, "branch %s is not ready", branch)
			return
		}
	}

	// Create a TCP proxy for this branch.
	//
	// Main branch: routes to vtgate MySQL (supports SHOW VITESS_MIGRATIONS, VSchema
	// queries, etc). No DB name rewriting — vtgate uses real keyspace names.
	//
	// Other branches: routes to mysqld with DB name rewriting (keyspace → branch_X_keyspace).
	// This gives each branch its own network endpoint, matching production PlanetScale.
	var keyspaces []string
	for ks := range backend.vtgateDBs {
		keyspaces = append(keyspaces, ks)
	}
	listenAddr, err := s.proxyListenAddr()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "allocate proxy port: %v", err)
		return
	}

	var upstreamDSN string
	var proxyBranch string
	if branch == "main" {
		// Main branch → vtgate (no DB name rewriting).
		upstreamDSN = fmt.Sprintf("root@tcp(%s)/", backend.vtgateMySQLAddr)
		proxyBranch = "" // empty branch name = identity mapping in newBranchProxy
	} else {
		upstreamDSN = fmt.Sprintf("%s@tcp(%s)/", managedMySQLTCPUser, backend.mysqlTCPAddr)
		proxyBranch = branch
	}
	proxy, err := newBranchProxy(r.Context(), listenAddr, upstreamDSN, proxyBranch, keyspaces, s.logger)
	if err != nil {
		if s.portAlloc != nil {
			// Return port to pool on failure.
			if _, portStr, splitErr := net.SplitHostPort(listenAddr); splitErr == nil {
				if port, convErr := strconv.Atoi(portStr); convErr == nil {
					s.portAlloc.release(port)
				}
			}
		}
		s.writeError(w, http.StatusInternalServerError, "create branch proxy: %v", err)
		return
	}
	s.trackProxy(branch, proxy)

	s.writeJSON(w, map[string]any{
		"name":            body.Name,
		"role":            body.Role,
		"ttl_seconds":     body.TTL,
		"access_host_url": s.proxyAdvertiseAddr(proxy, r),
		"username":        managedMySQLTCPUser,
		"plain_text":      "",
	})
}
