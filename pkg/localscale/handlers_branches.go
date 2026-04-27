package localscale

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/block/spirit/pkg/utils"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) handleGetBranch(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branchName := r.PathValue("branch")

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	// The "main" branch is virtual — it always exists and represents the live database.
	if branchName == "main" {
		s.writeJSON(w, map[string]any{
			"name":            "main",
			"parent_branch":   "",
			"ready":           true,
			"safe_migrations": backend.safeMigrations,
			"region":          map[string]string{"slug": "us-east-1"},
		})
		return nil
	}

	var name, parentBranch, region string
	var ready bool
	var errorMessage sql.NullString
	err = s.metadataDB.QueryRowContext(r.Context(),
		"SELECT name, parent_branch, region, ready, error_message FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branchName,
	).Scan(&name, &parentBranch, &region, &ready, &errorMessage)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "branch not found: %s", branchName)
	}

	resp := map[string]any{
		"name":            name,
		"parent_branch":   parentBranch,
		"ready":           ready,
		"safe_migrations": backend.safeMigrations,
		"region":          map[string]string{"slug": region},
	}
	if errorMessage.Valid && errorMessage.String != "" {
		resp["error_message"] = errorMessage.String
	}
	s.writeJSON(w, resp)
	return nil
}

func (s *Server) handleCreateBranch(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	var body struct {
		Name         string `json:"name"`
		ParentBranch string `json:"parent_branch"`
		Region       string `json:"region"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}
	if body.Region == "" {
		body.Region = "us-east-1"
	}

	if err := validateBranchName(body.Name); err != nil {
		return newHTTPError(http.StatusBadRequest, "invalid branch name: %v", err)
	}

	_, err = s.metadataDB.ExecContext(r.Context(),
		"INSERT INTO localscale_branches (org, database_name, name, parent_branch, region, ready) VALUES (?, ?, ?, ?, ?, FALSE)",
		org, database, body.Name, body.ParentBranch, body.Region,
	)
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") {
			return newHTTPError(http.StatusConflict, "Name has already been taken")
		}
		return newHTTPError(http.StatusInternalServerError, "insert branch: %v", err)
	}

	// Background goroutine snapshots schema from vtgate into branch databases,
	// snapshots VSchema from vtctld onto the branch row, then marks ready=true.
	s.wg.Go(func() {
		bgCtx := s.shutdownCtx
		if s.branchCreationDelay > 0 {
			select {
			case <-time.After(s.branchCreationDelay):
			case <-bgCtx.Done():
				return
			}
		}
		if err := s.snapshotBranch(bgCtx, backend, org, database, body.Name); err != nil {
			s.logger.Error("branch snapshot failed", "branch", body.Name, "error", err)
			// Best-effort: record error on branch row
			_ = s.execLog(bgCtx,
				"UPDATE localscale_branches SET error_message = ? WHERE org = ? AND database_name = ? AND name = ?",
				err.Error(), org, database, body.Name)
		}
	})

	s.writeJSON(w, map[string]any{
		"name":          body.Name,
		"parent_branch": body.ParentBranch,
		"ready":         false,
		"region":        map[string]string{"slug": body.Region},
	})
	return nil
}

// handleRefreshSchema re-snapshots a branch from main. Drops existing branch
// databases, re-copies schema from vtgate, and marks the branch ready.
// This is the LocalScale equivalent of PlanetScale's refresh-schema API.
func (s *Server) handleRefreshSchema(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branchName := r.PathValue("branch")

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	// Verify branch exists
	var ready bool
	if err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT ready FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branchName,
	).Scan(&ready); err != nil {
		return newHTTPError(http.StatusNotFound, "branch %s not found", branchName)
	}

	// Mark not ready during schema refresh
	if err := s.execLog(r.Context(),
		"UPDATE localscale_branches SET ready = FALSE WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branchName); err != nil {
		return newHTTPError(http.StatusInternalServerError, "update branch: %v", err)
	}

	// Drop existing branch databases and re-snapshot from main
	s.dropBranchDatabases(r.Context(), backend, branchName)

	s.wg.Go(func() {
		bgCtx := s.shutdownCtx
		if err := s.snapshotBranch(bgCtx, backend, org, database, branchName); err != nil {
			s.logger.Error("branch schema refresh failed", "branch", branchName, "error", err)
			_ = s.execLog(bgCtx,
				"UPDATE localscale_branches SET error_message = ? WHERE org = ? AND database_name = ? AND name = ?",
				err.Error(), org, database, branchName)
		}
	})

	s.writeJSON(w, map[string]any{
		"name":  branchName,
		"ready": false,
	})
	return nil
}

// snapshotBranch creates branch databases, copies schema from vtgate, snapshots
// VSchema from vtctld, and marks the branch as ready. Called as a background
// goroutine after the branch row is inserted.
func (s *Server) snapshotBranch(ctx context.Context, backend *databaseBackend, org, database, branchName string) error {
	// Open a connection to the backend's mysqld for branch database creation.
	// Each org/database has its own managed cluster with its own mysqld.
	s.logger.Info("branch snapshot: opening backend mysqld", "dsn_prefix", backend.mysqlDSNBase)
	backendDB, err := sql.Open("mysql", backend.mysqlDSNBase)
	if err != nil {
		return fmt.Errorf("open backend mysqld: %w", err)
	}
	defer utils.CloseAndLog(backendDB)

	if err := backendDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping backend mysqld: %w", err)
	}
	s.logger.Info("branch snapshot: backend mysqld connected", "dsn_prefix", backend.mysqlDSNBase)

	// Snapshot schema from vtgate into branch databases for each keyspace.
	s.logger.Info("branch snapshot: iterating keyspaces", "count", len(backend.vtgateDBs), "branch", branchName)
	for keyspace := range backend.vtgateDBs {
		s.logger.Info("branch snapshot: processing keyspace", "keyspace", keyspace, "branch", branchName)
		dbName := branchDBName(branchName, keyspace)

		// Create branch database on the backend's mysqld
		if err := validateIdentifier(dbName); err != nil {
			return fmt.Errorf("invalid branch database name %s: %w", dbName, err)
		}
		if _, err := backendDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(dbName)); err != nil {
			return fmt.Errorf("create branch database %s: %w", keyspace, err)
		}

		// Get schema from vtgate
		stmts, err := s.snapshotKeyspaceSchema(ctx, backend, keyspace)
		if err != nil {
			return fmt.Errorf("snapshot keyspace schema %s: %w", keyspace, err)
		}

		// Execute CREATE TABLEs in branch database (on the backend's mysqld)
		if len(stmts) > 0 {
			branchDB, err := sql.Open("mysql", backend.mysqlDSNBase+branchDBName(branchName, keyspace))
			if err != nil {
				return fmt.Errorf("open branch database %s: %w", keyspace, err)
			}
			for _, stmt := range stmts {
				if _, err := branchDB.ExecContext(ctx, stmt); err != nil {
					utils.CloseAndLog(branchDB)
					return fmt.Errorf("execute CREATE TABLE in branch %s: %w", keyspace, err)
				}
			}
			utils.CloseAndLog(branchDB)
		}
	}

	// Snapshot VSchema from vtctld for each keyspace
	vschemaSnapshot := make(map[string]json.RawMessage)
	for keyspace := range backend.vtgateDBs {
		resp, err := backend.vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{Keyspace: keyspace})
		if err != nil {
			return fmt.Errorf("get vschema for keyspace %s: %w", keyspace, err)
		}
		data, err := vschemaMarshaler.Marshal(resp.VSchema)
		if err != nil {
			return fmt.Errorf("marshal vschema for keyspace %s: %w", keyspace, err)
		}
		vschemaSnapshot[keyspace] = data
	}

	// Store VSchema snapshot on branch row
	if len(vschemaSnapshot) > 0 {
		vschemaJSON, err := json.Marshal(vschemaSnapshot)
		if err != nil {
			return fmt.Errorf("marshal vschema snapshot: %w", err)
		}
		if err := s.execLog(ctx,
			"UPDATE localscale_branches SET vschema_data = ? WHERE org = ? AND database_name = ? AND name = ?",
			string(vschemaJSON), org, database, branchName,
		); err != nil {
			return fmt.Errorf("persist vschema snapshot for branch %s: %w", branchName, err)
		}
	}

	// Mark branch as ready
	if err := s.execLog(ctx,
		"UPDATE localscale_branches SET ready = TRUE WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branchName,
	); err != nil {
		return fmt.Errorf("mark branch %s as ready: %w", branchName, err)
	}
	s.logger.Info("branch ready", "name", branchName)
	return nil
}

// handleApplyBranchSchema executes DDL statements against branch databases and updates
// VSchema on the branch row. The PlanetScale engine calls this before CreateDeployRequest.
//
// For each ALTER TABLE, it first tries ALGORITHM=INSTANT against the branch database.
// If all ALTERs succeed with ALGORITHM=INSTANT and there are no non-ALTER statements,
// the branch is marked instant_ddl_eligible=true. If any ALTER fails ALGORITHM=INSTANT
// or any non-ALTER statement (CREATE TABLE, DROP TABLE) is present, the branch is marked
// instant_ddl_eligible=false. This matches PlanetScale behavior.
func (s *Server) handleApplyBranchSchema(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")

	var body struct {
		DDL     map[string][]string        `json:"ddl"`
		VSchema map[string]json.RawMessage `json:"vschema"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	// Verify branch exists and is ready before applying schema.
	var branchReady bool
	err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT ready FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
		org, database, branch,
	).Scan(&branchReady)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "branch not found: %s", branch)
	}
	if !branchReady {
		return newHTTPError(http.StatusConflict, "branch %s is not ready", branch)
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
				return newHTTPError(http.StatusBadRequest, "invalid DDL: %v", err)
			}
		}
	}

	totalDDL := 0
	allInstant := true
	for keyspace, stmts := range body.DDL {
		branchDB, err := s.openBranchDB(r.Context(), branch, keyspace)
		if err != nil {
			return newHTTPError(http.StatusInternalServerError, "open branch db for %s: %v", keyspace, err)
		}
		for _, stmt := range stmts {
			if instantStmt := addAlgorithmInstant(stmt); instantStmt != "" {
				// ALTER TABLE: try ALGORITHM=INSTANT first
				if _, instantErr := branchDB.ExecContext(r.Context(), instantStmt); instantErr == nil {
					totalDDL++
					s.logger.Info("instant DDL succeeded on branch", "branch", branch, "keyspace", keyspace, "stmt", instantStmt[:min(len(instantStmt), 100)])
					continue // Instant succeeded — DDL already applied
				} else {
					s.logger.Info("instant DDL failed on branch, falling back", "branch", branch, "keyspace", keyspace, "error", instantErr, "stmt", instantStmt[:min(len(instantStmt), 100)])
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
				return newHTTPError(http.StatusInternalServerError, "execute DDL in branch %s/%s: %v\nstatement: %s", branch, keyspace, err, stmt)
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
	if err := s.execLog(r.Context(),
		"UPDATE localscale_branches SET instant_ddl_eligible = ? WHERE org = ? AND database_name = ? AND name = ?",
		allInstant, org, database, branch,
	); err != nil {
		return newHTTPError(http.StatusInternalServerError, "update instant eligibility: %v", err)
	}

	// Update VSchema on branch row (merge with existing snapshot from branch creation)
	if len(body.VSchema) > 0 {
		var existingVSchemaSQL sql.NullString
		if err := s.metadataDB.QueryRowContext(r.Context(),
			"SELECT vschema_data FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
			org, database, branch,
		).Scan(&existingVSchemaSQL); err != nil {
			return newHTTPError(http.StatusInternalServerError, "read existing branch vschema: %v", err)
		}

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
			return newHTTPError(http.StatusInternalServerError, "update branch vschema: %v", err)
		}
	}

	s.logger.Info("applied DDL to branch", "org", org, "database", database, "branch", branch, "keyspace_count", len(body.DDL), "total_ddl", totalDDL, "vschema_count", len(body.VSchema))
	s.writeJSON(w, map[string]any{"ok": true, "total_ddl": totalDDL, "vschema_count": len(body.VSchema)})
	return nil
}

func (s *Server) handleCreateBranchPassword(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	branch := r.PathValue("branch")

	var body struct {
		Name string `json:"name"`
		Role string `json:"role"`
		TTL  int    `json:"ttl"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	// Verify the branch exists (main is always valid).
	if branch != "main" {
		var ready bool
		err := s.metadataDB.QueryRowContext(r.Context(),
			"SELECT ready FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?",
			org, database, branch,
		).Scan(&ready)
		if err != nil {
			return newHTTPError(http.StatusNotFound, "branch not found: %s", branch)
		}
		if !ready {
			return newHTTPError(http.StatusConflict, "branch %s is not ready", branch)
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
		return newHTTPError(http.StatusInternalServerError, "allocate proxy port: %v", err)
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
	var branchTLS *tls.Config
	if s.tlsBundle != nil {
		branchTLS = s.tlsBundle.TLSConfig
	}
	proxy, err := newBranchProxy(r.Context(), listenAddr, upstreamDSN, proxyBranch, keyspaces, s.logger, branchTLS)
	if err != nil {
		if s.portAlloc != nil {
			// Return port to pool on failure.
			if _, portStr, splitErr := net.SplitHostPort(listenAddr); splitErr == nil {
				if port, convErr := strconv.Atoi(portStr); convErr == nil {
					s.portAlloc.release(port)
				}
			}
		}
		return newHTTPError(http.StatusInternalServerError, "create branch proxy: %v", err)
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
	return nil
}
