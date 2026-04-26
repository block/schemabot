package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/block/spirit/pkg/utils"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) handleCancelDeployRequest(w http.ResponseWriter, r *http.Request) error {
	backend, ref, err := s.resolveDeployAction(r)
	if err != nil {
		return err
	}
	number := ref.number

	info, err := s.getDeployRequestInfo(r.Context(), ref)
	if err != nil {
		return err
	}

	switch info.deploymentState {
	case dr.Queued, dr.InProgress, dr.PendingCutover, dr.Submitting:
		// valid states for cancel
	default:
		return newHTTPError(http.StatusConflict, "cannot cancel: deploy request is in state %q", info.deploymentState)
	}

	// Cancel all Vitess migrations for this deploy
	if info.migrationContext != "" {
		if err := s.alterVitessMigrations(r.Context(), backend, info.migrationContext, "CANCEL"); err != nil {
			return newHTTPError(http.StatusInternalServerError, "cancel migrations: %v", err)
		}
	}

	// Set in_progress_cancel — the processor will advance to complete_cancel
	// once all Vitess migrations reach terminal state.
	if err := s.execLog(r.Context(),
		`UPDATE localscale_deploy_requests
		 SET deployment_state = ?
		 WHERE org = ? AND database_name = ? AND number = ?`,
		dr.InProgressCancel, ref.org, ref.database, number,
	); err != nil {
		return newHTTPError(http.StatusInternalServerError, "update deploy state: %v", err)
	}

	s.writeJSON(w, deployResponse(number, info.branch, dr.InProgressCancel))
	return nil
}

func (s *Server) handleApplyDeployRequest(w http.ResponseWriter, r *http.Request) error {
	backend, ref, err := s.resolveDeployAction(r)
	if err != nil {
		return err
	}
	number := ref.number

	info, err := s.getDeployRequestInfo(r.Context(), ref)
	if err != nil {
		return err
	}

	if info.deploymentState != dr.PendingCutover {
		return newHTTPError(http.StatusConflict, "cannot cutover: deploy request is in state %q, expected pending_cutover", info.deploymentState)
	}

	// Cutover: complete all Vitess migrations for this deploy.
	// ALTER VITESS_MIGRATION ... COMPLETE triggers the cutover for all
	// ready_to_complete migrations matching this context.
	if info.migrationContext != "" {
		if err := s.alterVitessMigrations(r.Context(), backend, info.migrationContext, "COMPLETE"); err != nil {
			return newHTTPError(http.StatusInternalServerError, "complete migrations: %v", err)
		}
	}

	// Mark cutover as requested so the processor can distinguish
	// "pending_cutover" from "in_progress_cutover".
	if err := s.execLog(r.Context(),
		`UPDATE localscale_deploy_requests
		 SET cutover_requested = TRUE, deployment_state = ?
		 WHERE org = ? AND database_name = ? AND number = ?`,
		dr.InProgressCutover, ref.org, ref.database, number,
	); err != nil {
		return newHTTPError(http.StatusInternalServerError, "update deploy state: %v", err)
	}

	s.writeJSON(w, deployResponse(number, info.branch, dr.InProgressCutover))
	return nil
}

func (s *Server) handleRevertDeployRequest(w http.ResponseWriter, r *http.Request) error {
	backend, ref, err := s.resolveDeployAction(r)
	if err != nil {
		return err
	}
	number := ref.number

	var branch, migrationContext, ddlJSON, currentState string
	var vschemaOriginalSQL, schemaBeforeSQL sql.NullString
	var vschemaReverted bool
	err = s.metadataDB.QueryRowContext(r.Context(),
		`SELECT branch, migration_context, ddl_statements, vschema_data_original, vschema_reverted, schema_before, deployment_state
		 FROM localscale_deploy_requests
		 WHERE org = ? AND database_name = ? AND number = ?`,
		ref.org, ref.database, number,
	).Scan(&branch, &migrationContext, &ddlJSON, &vschemaOriginalSQL, &vschemaReverted, &schemaBeforeSQL, &currentState)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "deploy request not found: %d", number)
	}

	if currentState != dr.CompletePendingRevert {
		return newHTTPError(http.StatusConflict, "cannot revert: deploy request is in state %q, expected complete_pending_revert", currentState)
	}

	// Revert order: VSchema first, then DDL.
	// VSchema must be reverted before DDL because reverting a CREATE TABLE (= dropping it)
	// will fail if VSchema still references the table. Conversely, reverting a DROP TABLE
	// (= recreating it) is safe even if VSchema doesn't reference it yet.
	hasOriginalVSchema := hasVSchemaData(vschemaOriginalSQL)
	if hasOriginalVSchema && !vschemaReverted {
		// Set transitional state before VSchema revert
		if err := s.updateDeployState(r.Context(), ref, dr.InProgressRevertVSchema); err != nil {
			return newHTTPError(http.StatusInternalServerError, "update deploy state: %v", err)
		}
		if err := s.revertPendingVSchema(r.Context(), backend, ref, vschemaOriginalSQL.String); err != nil {
			s.logger.Error("revert vschema failed", "number", number, "error", err)
			// Best-effort state restore so the operation can be retried.
			if restoreErr := s.updateDeployState(r.Context(), ref, dr.CompletePendingRevert); restoreErr != nil {
				s.logger.Error("failed to restore state after revert failure", "number", number, "error", restoreErr)
			}
			return newHTTPError(http.StatusInternalServerError, "revert vschema: %v", err)
		}
	}

	// DDL revert: compute reverse DDL from stored pre-deploy schema snapshot and
	// submit as new online DDL. This avoids REVERT VITESS_MIGRATION which doesn't
	// work for instant DDL (no vreplication stream to revert from).
	revertContext := fmt.Sprintf("localscale-revert:%d", number)
	var revertCount int
	if schemaBeforeSQL.Valid && schemaBeforeSQL.String != "" {
		var schemaBefore map[string][]string
		if err := json.Unmarshal([]byte(schemaBeforeSQL.String), &schemaBefore); err != nil {
			return newHTTPError(http.StatusInternalServerError, "parse schema_before: %v", err)
		}
		var ddlByKeyspace map[string][]string
		if ddlJSON != "" {
			if err := json.Unmarshal([]byte(ddlJSON), &ddlByKeyspace); err != nil {
				return newHTTPError(http.StatusInternalServerError, "unmarshal ddl_statements: %v", err)
			}
		}
		reverseDDL, err := s.computeReverseDDL(r.Context(), backend, schemaBefore, ddlByKeyspace)
		if err != nil {
			return newHTTPError(http.StatusInternalServerError, "compute reverse DDL: %v", err)
		}
		if len(reverseDDL) > 0 {
			strategy := buildDDLStrategy(true) // always use --prefer-instant-ddl for revert
			if err := s.submitOnlineDDL(r.Context(), backend, reverseDDL, strategy, revertContext); err != nil {
				return newHTTPError(http.StatusInternalServerError, "submit reverse DDL: %v", err)
			}
			for _, stmts := range reverseDDL {
				revertCount += len(stmts)
			}
			s.logger.Info("issued reverse DDL for revert", "number", number, "ddl_count", revertCount, "revert_context", revertContext)
		}
	}

	// VSchema-only reverts (no DDL) are already complete at this point.
	revertState := dr.InProgressRevert
	if revertCount == 0 {
		revertState = dr.CompleteRevert
	}

	if err := s.execLog(r.Context(),
		`UPDATE localscale_deploy_requests
		 SET reverted = TRUE, revert_migration_context = ?, deployment_state = ?
		 WHERE org = ? AND database_name = ? AND number = ?`,
		revertContext, revertState, ref.org, ref.database, number,
	); err != nil {
		return newHTTPError(http.StatusInternalServerError, "update deploy state: %v", err)
	}

	s.writeJSON(w, deployResponse(number, branch, revertState))
	return nil
}

func (s *Server) handleSkipRevertDeployRequest(w http.ResponseWriter, r *http.Request) error {
	backend, ref, err := s.resolveDeployAction(r)
	if err != nil {
		return err
	}
	number := ref.number

	info, err := s.getDeployRequestInfo(r.Context(), ref)
	if err != nil {
		return err
	}

	if info.deploymentState != dr.CompletePendingRevert {
		return newHTTPError(http.StatusConflict, "cannot skip revert: deploy request is in state %q, expected complete_pending_revert", info.deploymentState)
	}

	if err := s.execLog(r.Context(),
		`UPDATE localscale_deploy_requests
		 SET revert_skipped = TRUE, deployment_state = ?
		 WHERE org = ? AND database_name = ? AND number = ?`,
		dr.Complete, ref.org, ref.database, number,
	); err != nil {
		return newHTTPError(http.StatusInternalServerError, "update deploy state: %v", err)
	}

	// Drop branch databases — revert window is closed, branch data no longer needed.
	s.dropBranchDatabases(r.Context(), backend, info.branch)

	s.writeJSON(w, deployResponse(number, info.branch, dr.Complete))
	return nil
}

func (s *Server) handleThrottleDeployRequest(w http.ResponseWriter, r *http.Request) error {
	backend, ref, err := s.resolveDeployAction(r)
	if err != nil {
		return err
	}
	number := ref.number

	var body struct {
		ThrottleRatio float64 `json:"throttle_ratio"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	if body.ThrottleRatio < 0 || body.ThrottleRatio > 0.95 {
		return newHTTPError(http.StatusUnprocessableEntity, "throttle_ratio must be between 0.0 and 0.95, got %f", body.ThrottleRatio)
	}

	// Store in metadata for query purposes
	_, err = s.metadataDB.ExecContext(r.Context(),
		`UPDATE localscale_deploy_requests
		 SET throttle_ratio = ?
		 WHERE org = ? AND database_name = ? AND number = ?`,
		body.ThrottleRatio, ref.org, ref.database, number)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "update throttle: %v", err)
	}

	if err := s.applyThrottle(r.Context(), backend, number, body.ThrottleRatio); err != nil {
		return newHTTPError(http.StatusInternalServerError, "apply throttle: %v", err)
	}
	s.writeJSON(w, map[string]string{"status": "ok"})
	return nil
}

// applyThrottle sets the throttle ratio for online DDL migrations across all keyspaces.
// Ratio 0.0 = full speed, 0.95 = max throttle (PlanetScale caps at 0.95).
//
// Uses ALTER VITESS_MIGRATION THROTTLE/UNTHROTTLE ALL which operates at the online-ddl
// level without enabling the global tablet throttler. This avoids vstreamer blocks from
// "metric not collected yet" errors on vtcombo/vttestserver environments.
func (s *Server) applyThrottle(ctx context.Context, backend *databaseBackend, number uint64, ratio float64) error {
	var firstErr error
	for keyspace, db := range backend.vtgateDBs {
		var stmt string
		if ratio > 0 {
			stmt = fmt.Sprintf("ALTER VITESS_MIGRATION THROTTLE ALL EXPIRE '876000h' RATIO %g", ratio)
		} else {
			stmt = "ALTER VITESS_MIGRATION UNTHROTTLE ALL"
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			s.logger.Warn("failed to apply throttle", "keyspace", keyspace, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("apply throttle to %s: %w", keyspace, err)
			}
		}
	}
	s.logger.Info("throttle updated", "number", number, "ratio", ratio, "keyspace_count", len(backend.vtgateDBs))
	return firstErr
}

// applyPendingVSchema reads the VSchema data for a deploy request and applies each
// keyspace's VSchema via vtctldclient.ApplyVSchema, then marks vschema_applied=true.
// Before applying, it captures the current (original) VSchema for each keyspace so
// it can be restored on revert.
func (s *Server) applyPendingVSchema(ctx context.Context, backend *databaseBackend, ref deployRequest, vschemaDataJSON string) error {
	var vschemaByKeyspace map[string]json.RawMessage
	if err := json.Unmarshal([]byte(vschemaDataJSON), &vschemaByKeyspace); err != nil {
		return fmt.Errorf("unmarshal vschema data: %w", err)
	}

	// Capture original VSchema for each keyspace before applying new one.
	originalVSchema := make(map[string]json.RawMessage)
	for keyspace := range vschemaByKeyspace {
		resp, err := backend.vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{Keyspace: keyspace})
		if err != nil {
			return fmt.Errorf("capture original vschema for %s: %w", keyspace, err)
		}
		data, err := vschemaMarshaler.Marshal(resp.VSchema)
		if err != nil {
			return fmt.Errorf("marshal original vschema for %s: %w", keyspace, err)
		}
		originalVSchema[keyspace] = data
	}

	// Store original VSchema in metadata for revert
	if len(originalVSchema) > 0 {
		origJSON, err := json.Marshal(originalVSchema)
		if err != nil {
			return fmt.Errorf("marshal original vschema: %w", err)
		}
		_, err = s.metadataDB.ExecContext(ctx,
			`UPDATE localscale_deploy_requests
			 SET vschema_data_original = ?
			 WHERE org = ? AND database_name = ? AND number = ?`,
			string(origJSON), ref.org, ref.database, ref.number,
		)
		if err != nil {
			return fmt.Errorf("store original vschema: %w", err)
		}
	}

	for keyspace, vschemaJSON := range vschemaByKeyspace {
		if err := s.applyVSchemaInternal(ctx, backend, keyspace, vschemaJSON); err != nil {
			return fmt.Errorf("apply vschema to %s: %w", keyspace, err)
		}
		s.logger.Info("applied vschema for deploy request", "number", ref.number, "keyspace", keyspace)
	}

	_, err := s.metadataDB.ExecContext(ctx,
		`UPDATE localscale_deploy_requests
		 SET vschema_applied = TRUE
		 WHERE org = ? AND database_name = ? AND number = ?`,
		ref.org, ref.database, ref.number,
	)
	if err != nil {
		return fmt.Errorf("mark vschema applied: %w", err)
	}

	return nil
}

// revertPendingVSchema restores the original VSchema for each keyspace and marks
// vschema_reverted=true.
func (s *Server) revertPendingVSchema(ctx context.Context, backend *databaseBackend, ref deployRequest, originalVSchemaJSON string) error {
	var vschemaByKeyspace map[string]json.RawMessage
	if err := json.Unmarshal([]byte(originalVSchemaJSON), &vschemaByKeyspace); err != nil {
		return fmt.Errorf("unmarshal original vschema data: %w", err)
	}

	for keyspace, vschemaJSON := range vschemaByKeyspace {
		if err := s.applyVSchemaInternal(ctx, backend, keyspace, vschemaJSON); err != nil {
			return fmt.Errorf("revert vschema for %s: %w", keyspace, err)
		}
		s.logger.Info("reverted vschema for deploy request", "number", ref.number, "keyspace", keyspace)
	}

	_, err := s.metadataDB.ExecContext(ctx,
		`UPDATE localscale_deploy_requests
		 SET vschema_reverted = TRUE
		 WHERE org = ? AND database_name = ? AND number = ?`,
		ref.org, ref.database, ref.number,
	)
	if err != nil {
		return fmt.Errorf("mark vschema reverted: %w", err)
	}

	return nil
}

// alterVitessMigrations runs ALTER VITESS_MIGRATION '<uuid>' <action> against
// each migration's owning keyspace. action is "CANCEL", "COMPLETE", or "RETRY".
func (s *Server) alterVitessMigrations(ctx context.Context, backend *databaseBackend, migrationContext, action string) error {
	migrations, err := s.showMigrations(ctx, backend, migrationContext)
	if err != nil {
		return err
	}
	var firstErr error
	for _, m := range migrations {
		uuid := m["migration_uuid"]
		keyspace := m["_keyspace"]
		if uuid == "" || keyspace == "" {
			s.logger.Warn("skipping migration with missing uuid or keyspace", "uuid", uuid, "keyspace", keyspace)
			continue
		}
		if err := validateSessionString(uuid); err != nil {
			s.logger.Warn("skipping migration with invalid UUID", "uuid", uuid, "error", err)
			continue
		}
		db, ok := backend.vtgateDBs[keyspace]
		if !ok {
			s.logger.Warn("unknown keyspace for migration", "uuid", uuid, "keyspace", keyspace)
			continue
		}
		stmt := fmt.Sprintf("ALTER VITESS_MIGRATION '%s' %s", uuid, action)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			s.logger.Warn("alter vitess_migration failed", "keyspace", keyspace, "action", action, "uuid", uuid, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("alter vitess_migration %s %s on %s: %w", uuid, action, keyspace, err)
			}
		} else {
			s.logger.Info("alter vitess_migration", "keyspace", keyspace, "action", action, "uuid", uuid)
		}
	}
	return firstErr
}

// showMigrations queries SHOW VITESS_MIGRATIONS for a context across all keyspaces
// and returns the raw column maps with an added "_keyspace" field.
func (s *Server) showMigrations(ctx context.Context, backend *databaseBackend, migrationContext string) ([]map[string]string, error) {
	if err := validateSessionString(migrationContext); err != nil {
		return nil, fmt.Errorf("invalid migration context: %w", err)
	}
	var result []map[string]string
	var lastErr error
	for keyspace, db := range backend.vtgateDBs {
		rows, err := db.QueryContext(ctx, "SHOW VITESS_MIGRATIONS LIKE '"+migrationContext+"'")
		if err != nil {
			s.logger.Warn("show vitess_migrations failed", "keyspace", keyspace, "error", err)
			lastErr = fmt.Errorf("show vitess_migrations for %s: %w", keyspace, err)
			continue
		}
		rowMaps, err := scanDynamicRows(rows)
		utils.CloseAndLog(rows)
		if err != nil {
			s.logger.Warn("scan vitess_migrations failed", "keyspace", keyspace, "error", err)
			lastErr = fmt.Errorf("scan vitess_migrations for %s: %w", keyspace, err)
			continue
		}
		for _, rm := range rowMaps {
			rm["_keyspace"] = keyspace
		}
		result = append(result, rowMaps...)
	}
	// Return error only if ALL keyspaces failed
	if len(result) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return result, nil
}

func (s *Server) getMigrationInfos(ctx context.Context, backend *databaseBackend, migrationContext string) []migrationInfo {
	colMaps, err := s.showMigrations(ctx, backend, migrationContext)
	if err != nil {
		s.logger.Warn("getMigrationInfos: show migrations failed", "migration_context", migrationContext, "error", err)
		return nil
	}
	var migrations []migrationInfo
	for _, colMap := range colMaps {
		migrations = append(migrations, migrationInfo{
			status:          colMap["migration_status"],
			readyToComplete: colMap["ready_to_complete"] == "1",
			ddlAction:       colMap["ddl_action"],
			message:         colMap["message"],
		})
	}
	return migrations
}

// deriveRevertState determines the revert progress for a deploy request.
// Queries SHOW VITESS_MIGRATIONS by the revert context to find the reverse DDL
// migrations and derive overall state.
//
// States: in_progress_revert → complete_revert or complete_error.
func (s *Server) deriveRevertState(ctx context.Context, backend *databaseBackend, revertMigrationContext string) string {
	if revertMigrationContext == "" {
		return dr.CompleteRevert
	}

	migrations := s.getMigrationInfos(ctx, backend, revertMigrationContext)
	if len(migrations) == 0 {
		return dr.InProgressRevert // revert migrations not yet visible
	}

	revertDDLState := deriveDeployState(migrations, true) // cutoverRequested=true (auto-cutover)
	switch revertDDLState {
	case dr.CompleteError:
		return dr.CompleteRevertError
	case dr.CompletePendingRevert:
		return dr.CompleteRevert
	default:
		return dr.InProgressRevert
	}
}
