package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/spirit/pkg/utils"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) handleCreateDeployRequest(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")

	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	var body struct {
		Branch           string `json:"branch"`
		IntoBranch       string `json:"into_branch"`
		AutoCutover      bool   `json:"auto_cutover"`
		AutoDeleteBranch bool   `json:"auto_delete_branch"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}
	if body.IntoBranch == "" {
		body.IntoBranch = "main"
	}

	// Extract token name from Authorization header (format: "tokenName:tokenValue")
	var tokenName string
	if auth := r.Header.Get("Authorization"); auth != "" {
		if parts := strings.SplitN(auth, ":", 2); len(parts) == 2 {
			tokenName = parts[0]
		}
	}

	// Verify the branch exists
	var branchExists bool
	err = s.metadataDB.QueryRowContext(r.Context(),
		`SELECT 1 FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?`,
		org, database, body.Branch,
	).Scan(&branchExists)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "branch not found: %s", body.Branch)
	}

	// Insert deploy request with atomically computed sequential number.
	result, err := s.metadataDB.ExecContext(r.Context(),
		`INSERT INTO localscale_deploy_requests (number, org, database_name, token_name, branch, into_branch, auto_cutover, ddl_statements, deployment_state)
		 SELECT COALESCE(MAX(number), 0) + 1, ?, ?, ?, ?, ?, ?, '{}', ?
		 FROM localscale_deploy_requests WHERE database_name = ?`,
		org, database, tokenName, body.Branch, body.IntoBranch, body.AutoCutover, dr.Pending, database,
	)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "insert deploy request: %v", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "get deploy request id: %v", err)
	}

	// Read back the assigned number.
	var number uint64
	if err := s.metadataDB.QueryRowContext(r.Context(),
		"SELECT number FROM localscale_deploy_requests WHERE id = ?", id,
	).Scan(&number); err != nil {
		return newHTTPError(http.StatusInternalServerError, "read deploy request number: %v", err)
	}

	// Return immediately with "pending" — background goroutine will compute diff
	// and transition to "ready" or "no_changes".
	s.writeJSON(w, map[string]any{
		"number":           number,
		"branch":           body.Branch,
		"into_branch":      body.IntoBranch,
		"deployment_state": dr.Pending,
		"html_url":         fmt.Sprintf("%s/%s/%s/deploy-requests/%d", s.baseURL, org, database, number),
		"approved":         false,
		"deployment": map[string]any{
			"instant_ddl_eligible": false,
		},
	})

	// Background goroutine: compute schema + VSchema diff, then update the row.
	drNumber := number
	s.wg.Go(func() {
		bgCtx := s.shutdownCtx
		if s.deployRequestDelay > 0 {
			select {
			case <-time.After(s.deployRequestDelay):
			case <-bgCtx.Done():
				return
			}
		}
		if err := s.computeDeployRequestDiff(bgCtx, backend, drNumber, org, database, body.Branch); err != nil {
			s.logger.Error("deploy request diff failed", "number", drNumber, "error", err)
			if stateErr := s.updateDeployState(bgCtx, drNumber, dr.Error); stateErr != nil {
				s.logger.Error("failed to set error state", "number", drNumber, "error", stateErr)
			}
		}
	})

	return nil
}

func (s *Server) handleDeployDeployRequest(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	backend, err := s.backendFor(org, database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	number, err := s.parseDeployNumber(r)
	if err != nil {
		return err
	}

	var body struct {
		InstantDDL bool `json:"instant_ddl"`
	}
	// Decode optional body — empty body is valid (InstantDDL defaults to false).
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && err != io.EOF {
		return newHTTPError(http.StatusBadRequest, "invalid request body: %v", err)
	}

	// Read deploy request
	var branch, ddlJSON, deployState string
	var vschemaDataSQL sql.NullString
	var autoCutover, deployed bool
	err = s.metadataDB.QueryRowContext(r.Context(),
		"SELECT branch, ddl_statements, vschema_data, auto_cutover, deployed, deployment_state FROM localscale_deploy_requests WHERE number = ?",
		number,
	).Scan(&branch, &ddlJSON, &vschemaDataSQL, &autoCutover, &deployed, &deployState)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "deploy request not found: %d", number)
	}

	if deployState == dr.Pending {
		return newHTTPError(http.StatusConflict, "deploy request is still being prepared")
	}

	// Simulate PlanetScale's approval requirement when configured per-database.
	backend, backendErr := s.backendFor(org, database)
	if backendErr != nil {
		return newHTTPError(http.StatusNotFound, "%v", backendErr)
	}
	if backend.requireApproval {
		return newHTTPError(http.StatusForbidden, "Deploy request must be approved before deploying.")
	}

	if deployed {
		return newHTTPError(http.StatusConflict, "deploy request already deployed")
	}

	// Only one deploy can be active per database. A deploy in complete_pending_revert
	// still has an open revert window — the user must skip-revert or wait for expiry
	// before deploying again. This matches PlanetScale's gated deployment behavior.
	var prevNumber uint64
	err = s.metadataDB.QueryRowContext(r.Context(),
		`SELECT number FROM localscale_deploy_requests
		 WHERE database_name = ? AND deployed = TRUE AND cancelled = FALSE
		 AND number != ?
		 ORDER BY number DESC LIMIT 1`,
		database, number,
	).Scan(&prevNumber)
	if err == nil {
		prevState, stateErr := s.getDeployRequestState(r.Context(), prevNumber)
		if stateErr == nil && !terminalDeployStates[prevState] {
			return newHTTPError(http.StatusConflict, "deploy request %d is still active (state: %s)", prevNumber, prevState)
		}
	}

	// Parse DDL per keyspace
	var ddlByKeyspace map[string][]string
	if err := json.Unmarshal([]byte(ddlJSON), &ddlByKeyspace); err != nil {
		return newHTTPError(http.StatusInternalServerError, "unmarshal ddl: %v", err)
	}

	// Check for VSchema data
	hasVSchema := hasVSchemaData(vschemaDataSQL)

	// Count total DDL
	totalDDL := 0
	for _, stmts := range ddlByKeyspace {
		totalDDL += len(stmts)
	}

	if totalDDL == 0 && !hasVSchema {
		if err := s.execLog(r.Context(),
			"UPDATE localscale_deploy_requests SET deployed = TRUE, deployment_state = ? WHERE number = ?", dr.NoChanges, number,
		); err != nil {
			return newHTTPError(http.StatusInternalServerError, "update deploy state: %v", err)
		}
		s.writeJSON(w, deployResponse(number, branch, dr.NoChanges))
		return nil
	}

	// Atomically mark as deployed — deployed=FALSE prevents double-deploy races.
	migrationContext := fmt.Sprintf("localscale:%d", number)
	result, err := s.metadataDB.ExecContext(r.Context(),
		`UPDATE localscale_deploy_requests
		 SET deployed = TRUE, migration_context = ?, deployment_state = ?
		 WHERE number = ? AND deployed = FALSE`,
		migrationContext, dr.Submitting, number,
	)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "update deploy request: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return newHTTPError(http.StatusConflict, "deploy request already deployed")
	}

	resp := deployResponse(number, branch, dr.Submitting)
	resp["approved"] = true
	resp["html_url"] = fmt.Sprintf("%s/%s/%s/deploy-requests/%d", s.baseURL, org, database, number)
	s.writeJSON(w, resp)

	// Background goroutine: apply VSchema, snapshot schema, submit DDL, advance state.
	instantDDL := body.InstantDDL
	s.wg.Go(func() {
		params := deployExecParams{
			backend:          backend,
			number:           number,
			hasVSchema:       hasVSchema,
			vschemaData:      vschemaDataSQL.String,
			totalDDL:         totalDDL,
			ddlByKeyspace:    ddlByKeyspace,
			instantDDL:       instantDDL,
			migrationContext: migrationContext,
			autoCutover:      autoCutover,
		}
		if err := s.executeDeployRequest(s.shutdownCtx, params); err != nil {
			s.logger.Error("deploy execution failed", "number", number, "error", err)
			if stateErr := s.updateDeployState(s.shutdownCtx, number, dr.Error); stateErr != nil {
				s.logger.Error("failed to set error state", "number", number, "error", stateErr)
			}
		}
	})

	return nil
}

func (s *Server) handleListDeployRequests(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")

	rows, err := s.metadataDB.QueryContext(r.Context(),
		`SELECT number, branch, into_branch, deployment_state, instant_ddl_eligible
		 FROM localscale_deploy_requests ORDER BY number DESC`)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "query deploy requests: %v", err)
	}
	defer utils.CloseAndLog(rows)

	var results []map[string]any
	for rows.Next() {
		var number int64
		var branch, intoBranch, state string
		var instantEligible bool
		if err := rows.Scan(&number, &branch, &intoBranch, &state, &instantEligible); err != nil {
			return newHTTPError(http.StatusInternalServerError, "scan deploy request: %v", err)
		}
		results = append(results, map[string]any{
			"number":           number,
			"branch":           branch,
			"into_branch":      intoBranch,
			"deployment_state": state,
			"html_url":         fmt.Sprintf("%s/%s/%s/deploy-requests/%d", s.baseURL, org, database, number),
			"deployment": map[string]any{
				"instant_ddl_eligible": instantEligible,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return newHTTPError(http.StatusInternalServerError, "iterate deploy requests: %v", err)
	}

	s.writeJSON(w, map[string]any{"data": results})
	return nil
}

func (s *Server) handleGetDeployRequest(w http.ResponseWriter, r *http.Request) error {
	org := r.PathValue("org")
	database := r.PathValue("db")
	number, err := s.parseDeployNumber(r)
	if err != nil {
		return err
	}

	var branch, intoBranch, state string
	var instantEligible bool
	err = s.metadataDB.QueryRowContext(r.Context(),
		`SELECT branch, into_branch, deployment_state, instant_ddl_eligible
		 FROM localscale_deploy_requests WHERE number = ?`, number,
	).Scan(&branch, &intoBranch, &state, &instantEligible)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "deploy request not found: %d", number)
	}

	resp := map[string]any{
		"number":           number,
		"branch":           branch,
		"into_branch":      intoBranch,
		"deployment_state": state,
		"html_url":         fmt.Sprintf("%s/%s/%s/deploy-requests/%d", s.baseURL, org, database, number),
		"deployment": map[string]any{
			"instant_ddl_eligible": instantEligible,
		},
	}
	s.writeJSON(w, resp)
	return nil
}

// computeDeployRequestDiff computes the schema and VSchema diff for a deploy request
// and persists the results. Called as a background goroutine after the deploy request
// is created in "pending" state.
func (s *Server) computeDeployRequestDiff(ctx context.Context, backend *databaseBackend, number uint64, org, database, branch string) error {
	// Read branch VSchema from branch row
	var vschemaDataSQL sql.NullString
	err := s.metadataDB.QueryRowContext(ctx,
		`SELECT vschema_data FROM localscale_branches WHERE org = ? AND database_name = ? AND name = ?`,
		org, database, branch,
	).Scan(&vschemaDataSQL)
	if err != nil {
		return fmt.Errorf("read branch %s vschema: %w", branch, err)
	}

	// Diff branch schema vs main schema for each keyspace
	differ := ddl.NewDiffer()
	ddlByKeyspace := make(map[string][]string)
	for keyspace := range backend.vtgateDBs {
		mainStmts, err := s.snapshotKeyspaceSchema(ctx, backend, keyspace)
		if err != nil {
			return fmt.Errorf("snapshot main schema for keyspace %s: %w", keyspace, err)
		}

		branchStmts, err := s.getBranchSchemaFromBackend(ctx, backend, branch, keyspace)
		if err != nil {
			return fmt.Errorf("get branch schema for keyspace %s: %w", keyspace, err)
		}

		diffResult, err := differ.DiffStatements(mainStmts, branchStmts)
		if err != nil {
			return fmt.Errorf("diff schema for keyspace %s: %w", keyspace, err)
		}
		if len(diffResult.Statements) > 0 {
			ddlByKeyspace[keyspace] = diffResult.Statements
		}
	}

	// Diff VSchema
	var vschemaChanges map[string]json.RawMessage
	if hasVSchemaData(vschemaDataSQL) {
		var branchVSchema map[string]json.RawMessage
		if err := json.Unmarshal([]byte(vschemaDataSQL.String), &branchVSchema); err != nil {
			return fmt.Errorf("unmarshal branch vschema data: %w", err)
		}

		vschemaChanges = make(map[string]json.RawMessage)
		for keyspace, branchVS := range branchVSchema {
			resp, err := backend.vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{Keyspace: keyspace})
			if err != nil {
				return fmt.Errorf("get main vschema for keyspace %s: %w", keyspace, err)
			}
			mainVS, err := vschemaMarshaler.Marshal(resp.VSchema)
			if err != nil {
				return fmt.Errorf("marshal main vschema for keyspace %s: %w", keyspace, err)
			}

			if normalizeJSON(branchVS) != normalizeJSON(mainVS) {
				vschemaChanges[keyspace] = branchVS
			}
		}
		if len(vschemaChanges) == 0 {
			vschemaChanges = nil
		}
	}

	ddlJSON, err := json.Marshal(ddlByKeyspace)
	if err != nil {
		return fmt.Errorf("marshal ddl_statements: %w", err)
	}
	var vschemaJSON string
	if len(vschemaChanges) > 0 {
		vschemaBytes, err := json.Marshal(vschemaChanges)
		if err != nil {
			return fmt.Errorf("marshal vschema changes: %w", err)
		}
		vschemaJSON = string(vschemaBytes)
	}

	totalDDL := 0
	for _, stmts := range ddlByKeyspace {
		totalDDL += len(stmts)
	}

	// Detect instant DDL eligibility by testing ALGORITHM=INSTANT on a
	// temporary scratch database. Creates a temp DB, copies table schemas
	// from main, tries each ALTER with ALGORITHM=INSTANT, and drops the
	// temp DB. This matches real PlanetScale behavior where the server
	// evaluates instant eligibility when preparing the deploy request.
	instantEligible := totalDDL > 0
	if instantEligible {
		instantEligible = s.checkInstantEligibility(ctx, backend, ddlByKeyspace)
	}

	newState := dr.Ready
	if totalDDL == 0 && len(vschemaChanges) == 0 {
		newState = dr.NoChanges
	}

	if err := s.execLog(ctx,
		`UPDATE localscale_deploy_requests
		 SET ddl_statements = ?, vschema_data = ?, instant_ddl_eligible = ?, deployment_state = ?
		 WHERE number = ?`,
		string(ddlJSON), vschemaJSON, instantEligible, newState, number,
	); err != nil {
		return fmt.Errorf("persist diff results for deploy request %d: %w", number, err)
	}
	s.logger.Info("deploy request diff complete", "number", number, "state", newState, "ddl_count", totalDDL)
	return nil
}

// deployExecParams holds the parameters for executeDeployRequest.
type deployExecParams struct {
	backend          *databaseBackend
	number           uint64
	hasVSchema       bool
	vschemaData      string
	totalDDL         int
	ddlByKeyspace    map[string][]string
	instantDDL       bool
	migrationContext string
	autoCutover      bool
}

// executeDeployRequest applies VSchema, snapshots schema, submits online DDL,
// and advances the deploy state. Called as a background goroutine after the deploy
// request is marked as deployed.
func (s *Server) executeDeployRequest(ctx context.Context, p deployExecParams) error {
	// Apply VSchema BEFORE DDL submission. Vtgate needs VSchema (sharded: true)
	// to route DDL correctly to multi-shard keyspaces. Without it, vtgate treats
	// the keyspace as unsharded and rejects DDL with "Keyspace does not have
	// exactly one shard".
	if p.hasVSchema {
		if err := s.applyPendingVSchema(ctx, p.backend, p.number, p.vschemaData); err != nil {
			return fmt.Errorf("apply vschema for deploy request %d: %w", p.number, err)
		}
	}

	if p.totalDDL > 0 {
		// Snapshot current schema before deploying DDL (enables DDL-based revert).
		schemaBefore, err := s.snapshotSchema(ctx, p.backend, p.ddlByKeyspace)
		if err != nil {
			s.logger.Warn("schema snapshot failed", "error", err)
		} else if len(schemaBefore) > 0 {
			schemaJSON, err := json.Marshal(schemaBefore)
			if err != nil {
				s.logger.Warn("marshal schema_before", "number", p.number, "error", err)
			} else {
				if err := s.execLog(ctx,
					"UPDATE localscale_deploy_requests SET schema_before = ? WHERE number = ?",
					string(schemaJSON), p.number); err != nil {
					s.logger.Warn("failed to save schema_before snapshot", "number", p.number, "error", err)
				}
			}
		}

		// Build ddl_strategy
		ddlStrategy := buildDDLStrategy(p.instantDDL)

		if err := s.submitOnlineDDL(ctx, p.backend, p.ddlByKeyspace, ddlStrategy, p.migrationContext); err != nil {
			return fmt.Errorf("submit online DDL for deploy request %d: %w", p.number, err)
		}
	}

	// Apply default throttle to the online-ddl app if configured.
	if s.defaultThrottleRatio > 0 && p.totalDDL > 0 {
		if err := s.applyThrottle(ctx, p.backend, p.number, s.defaultThrottleRatio); err != nil {
			s.logger.Warn("default throttle failed", "number", p.number, "error", err)
		}
	}

	// Advance state based on what the deploy contains.
	// VSchema is already applied above, so VSchema-only deploys go straight to complete.
	initialState := dr.Queued
	if p.totalDDL == 0 {
		initialState = dr.CompletePendingRevert
	}
	if err := s.updateDeployState(ctx, p.number, initialState); err != nil {
		return fmt.Errorf("update deploy state to %s for deploy request %d: %w", initialState, p.number, err)
	}

	s.logger.Info("deployed deploy request",
		"number", p.number,
		"ddl_count", p.totalDDL,
		"has_vschema", p.hasVSchema,
		"migration_context", p.migrationContext,
		"auto_cutover", p.autoCutover,
		"instant_ddl", p.instantDDL,
	)
	return nil
}

// The background processor keeps deployment_state up to date.
func (s *Server) getDeployRequestState(ctx context.Context, number uint64) (string, error) {
	var state string
	err := s.metadataDB.QueryRowContext(ctx,
		"SELECT deployment_state FROM localscale_deploy_requests WHERE number = ?", number,
	).Scan(&state)
	return state, err
}
