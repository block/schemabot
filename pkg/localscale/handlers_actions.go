package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/block/spirit/pkg/utils"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) handleCancelDeployRequest(w http.ResponseWriter, r *http.Request) error {
	_, ref, err := s.resolveDeployAction(r)
	if err != nil {
		return err
	}
	number := ref.number

	info, err := s.getDeployRequestInfo(r.Context(), ref)
	if err != nil {
		return err
	}

	if !CanCancelDeployRequest(info.deploymentState) {
		return newHTTPError(http.StatusConflict, "cannot cancel: deploy request is in state %q", info.deploymentState)
	}

	// Recording the state is the whole of the handler's job. The processor
	// issues CANCEL to Vitess on every tick a request spends in
	// in_progress_cancel, so asking the engine here as well would only add a
	// first attempt — one made on the caller's connection, against the shard
	// whose silence is the reason the cancel was sent. A cancel must be
	// answerable while the engine is not.
	//
	// The timestamp comes from Go rather than NOW() because the deadline it
	// feeds is measured against a Go clock. A stored NOW() is the database
	// session's wall clock, and an offset between the two reads as a cancel
	// from the future: a deadline that never passes, on the one request that
	// needs it to.
	if err := s.execLog(r.Context(),
		`UPDATE localscale_deploy_requests
		 SET deployment_state = ?, cancel_requested_at = ?
		 WHERE org = ? AND database_name = ? AND number = ?`,
		dr.InProgressCancel, time.Now().UTC().Format(storedTimestampLayout),
		ref.org, ref.database, number,
	); err != nil {
		return newHTTPError(http.StatusInternalServerError, "update deploy state: %v", err)
	}
	s.logger.Info("deploy request cancel recorded", "number", number, "org", ref.org,
		"database", ref.database, "from_state", info.deploymentState)

	s.writeJSON(w, deployResponse(number, info.branch, dr.InProgressCancel, info.createdAt))
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
		if err := s.alterVitessMigrations(r.Context(), backend, info.migrationContext, migrationActionComplete); err != nil {
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

	s.writeJSON(w, deployResponse(number, info.branch, dr.InProgressCutover, info.createdAt))
	return nil
}

func (s *Server) handleRevertDeployRequest(w http.ResponseWriter, r *http.Request) error {
	backend, ref, err := s.resolveDeployAction(r)
	if err != nil {
		return err
	}
	number := ref.number

	var branch, migrationContext, ddlJSON, currentState, createdAtRaw string
	var vschemaOriginalSQL, schemaBeforeSQL sql.NullString
	var vschemaReverted bool
	err = s.metadataDB.QueryRowContext(r.Context(),
		`SELECT branch, migration_context, ddl_statements, vschema_data_original, vschema_reverted, schema_before, deployment_state, created_at
		 FROM localscale_deploy_requests
		 WHERE org = ? AND database_name = ? AND number = ?`,
		ref.org, ref.database, number,
	).Scan(&branch, &migrationContext, &ddlJSON, &vschemaOriginalSQL, &vschemaReverted, &schemaBeforeSQL, &currentState, &createdAtRaw)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "deploy request not found: %d", number)
	}
	createdAt, err := deployRequestCreatedAt(createdAtRaw)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "deploy request %d: %v", number, err)
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

	s.writeJSON(w, deployResponse(number, branch, revertState, createdAt))
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

	s.writeJSON(w, deployResponse(number, info.branch, dr.Complete, info.createdAt))
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

	// Apply unsharded keyspaces first — they define sequence tables that
	// sharded keyspaces reference via auto_increment. Vtgate's
	// resolveAutoIncrement deletes tables with unresolvable sequence
	// references, so the defining keyspace must be in the VSchema before
	// the referencing keyspace is applied.
	var unsharded, sharded []string
	for keyspace, vschemaJSON := range vschemaByKeyspace {
		var ks struct {
			Sharded bool `json:"sharded"`
		}
		if json.Unmarshal(vschemaJSON, &ks) == nil && ks.Sharded {
			sharded = append(sharded, keyspace)
		} else {
			unsharded = append(unsharded, keyspace)
		}
	}
	for _, keyspace := range append(unsharded, sharded...) {
		if err := s.applyVSchemaInternal(ctx, backend, keyspace, vschemaByKeyspace[keyspace]); err != nil {
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

// ALTER VITESS_MIGRATION actions SchemaBot issues.
const (
	migrationActionCancel   = "CANCEL"
	migrationActionComplete = "COMPLETE"
)

// awaitingCompletion reports whether a row still has to be told to cut over.
// Vitess clears postpone_completion the moment it accepts a COMPLETE, so a row
// that is no longer postponed is already cutting over or already done.
func awaitingCompletion(row map[string]string) bool {
	return row["postpone_completion"] == "1"
}

// filterMigrations returns the rows that keep satisfies.
func filterMigrations(migrations []map[string]string, keep func(map[string]string) bool) []map[string]string {
	var kept []map[string]string
	for _, m := range migrations {
		if keep(m) {
			kept = append(kept, m)
		}
	}
	return kept
}

// distinctMigrationsByKeyspace reduces the rows to the distinct schema changes
// of each keyspace. SHOW VITESS_MIGRATIONS reports a schema change once per
// shard, and all of those rows carry the same UUID, so a statement addressed
// at the keyspace covers every one of them.
//
// known reports whether a keyspace belongs to this backend; rows for any other
// keyspace, and rows whose UUID would not be safe to interpolate, are dropped
// with a warning. A row missing an identifier is an error: it cannot be
// addressed at all, and acting on the rest would half-apply the operation.
func distinctMigrationsByKeyspace(
	migrations []map[string]string,
	migrationContext string,
	known func(keyspace string) bool,
	logger *slog.Logger,
) (map[string][]string, error) {
	byKeyspace := make(map[string][]string)
	seen := make(map[string]bool)
	for _, m := range migrations {
		uuid := m["migration_uuid"]
		keyspace := m["_keyspace"]
		if uuid == "" {
			err := fmt.Errorf("schema change for context %s is missing uuid: keyspace=%q", migrationContext, keyspace)
			logger.Warn("schema change control will fail because a row is missing its uuid", "keyspace", keyspace, "error", err)
			return nil, err
		}
		if keyspace == "" {
			err := fmt.Errorf("schema change for context %s is missing keyspace: uuid=%q", migrationContext, uuid)
			logger.Warn("schema change control will fail because a row is missing its keyspace", "uuid", uuid, "error", err)
			return nil, err
		}
		if err := validateSessionString(uuid); err != nil {
			logger.Warn("skipping a row with an invalid UUID", "uuid", uuid, "error", err)
			continue
		}
		if !known(keyspace) {
			logger.Warn("unknown keyspace for schema change", "uuid", uuid, "keyspace", keyspace)
			continue
		}
		if seen[keyspace+"/"+uuid] {
			continue
		}
		seen[keyspace+"/"+uuid] = true
		byKeyspace[keyspace] = append(byKeyspace[keyspace], uuid)
	}
	return byKeyspace, nil
}

// alterVitessMigrations runs ALTER VITESS_MIGRATION '<uuid>' <action> once per
// schema change, on a keyspace-scoped connection. vtgate scatters it from
// there, so every shard of the keyspace acts on it together: a cutover is one
// command for the whole keyspace, not one command per shard.
//
// SHOW VITESS_MIGRATIONS reports a schema change once per shard, and all those
// rows carry the same UUID, so they are reduced to the distinct UUIDs first.
// Issuing the statement per row would send it to each shard as many times as
// the keyspace has shards, and a duplicate that lands while a shard is cutting
// over contends with the cutover holding that shard's tables locked — the two
// sit on each other until the cutover's deadline, leaving the deploy request
// in in_progress_cutover.
func (s *Server) alterVitessMigrations(ctx context.Context, backend *databaseBackend, migrationContext, action string) error {
	migrations, err := s.showMigrations(ctx, backend, migrationContext)
	if err != nil {
		return err
	}
	// A schema change Vitess has already unpostponed is cutting over or done,
	// and telling it again is the duplicate described above.
	if action == migrationActionComplete {
		migrations = filterMigrations(migrations, awaitingCompletion)
		if len(migrations) == 0 {
			s.logger.Debug("no schema change is waiting to be told to cut over",
				"migration_context", migrationContext)
			return nil
		}
	}
	known := func(keyspace string) bool {
		_, ok := backend.vtgateDBs[keyspace]
		return ok
	}
	byKeyspace, err := distinctMigrationsByKeyspace(migrations, migrationContext, known, s.logger)
	if err != nil {
		return err
	}

	var firstErr error
	for keyspace, uuids := range byKeyspace {
		db := backend.vtgateDBs[keyspace]
		for _, uuid := range uuids {
			stmt := fmt.Sprintf("ALTER VITESS_MIGRATION '%s' %s", uuid, action)
			execCtx, cancel := context.WithTimeout(ctx, vitessQueryTimeout)
			_, err := db.ExecContext(execCtx, stmt)
			cancel()
			if err != nil {
				s.logger.Warn("alter vitess_migration failed", "keyspace", keyspace, "uuid", uuid,
					"action", action, "error", err)
				if firstErr == nil {
					firstErr = fmt.Errorf("alter vitess_migration %s %s on %s: %w", uuid, action, keyspace, err)
				}
				continue
			}
			s.logger.Info("alter vitess_migration", "keyspace", keyspace, "uuid", uuid, "action", action)
		}
	}
	return firstErr
}

// vitessQueryTimeout bounds one statement SchemaBot sends a shard about its
// schema changes. A healthy shard answers in milliseconds; a shard whose
// cutover has stopped responding answers never, and the caller that waits on
// it is the single processor goroutine that drives every deploy request. One
// unresponsive shard costs a tick under this bound and stalls the state
// machine without it.
const vitessQueryTimeout = 10 * time.Second

// showMigrations queries SHOW VITESS_MIGRATIONS for a context on every shard
// and returns the raw column maps with an added "_keyspace" field.
//
// The read is addressed to one shard at a time. Each shard keeps its own row
// for a schema change, and vtgate will not scatter this statement: asked on a
// connection scoped to a keyspace with more than one shard it answers
// "Keyspace does not have exactly one shard" rather than reading any of them.
// Control statements are the other way round and go to the keyspace, which
// vtgate does scatter, so a cutover stays one command for all of its shards.
//
// A shard that does not answer fails the whole call. Its schema changes are
// indistinguishable from schema changes that do not exist, and both callers
// read the absence as a fact: the control path would address the shards that
// answered and leave the rest of the operation unissued, and state derivation
// would call a deploy complete on the strength of the shards it could see.
func (s *Server) showMigrations(ctx context.Context, backend *databaseBackend, migrationContext string) ([]map[string]string, error) {
	if err := validateSessionString(migrationContext); err != nil {
		return nil, fmt.Errorf("invalid migration context: %w", err)
	}
	var result []map[string]string
	for keyspace := range backend.vtgateDBs {
		for _, shard := range buildShards(backend.shardCounts[keyspace]) {
			rowMaps, err := s.showMigrationsOnShard(ctx, backend, keyspace, shard.Name, migrationContext)
			if err != nil {
				return nil, fmt.Errorf("show vitess_migrations for %s/%s: %w", keyspace, shard.Name, err)
			}
			for _, rm := range rowMaps {
				rm["_keyspace"] = keyspace
			}
			result = append(result, rowMaps...)
		}
	}
	return result, nil
}

// showMigrationsOnShard reads one shard's rows for a context.
func (s *Server) showMigrationsOnShard(
	ctx context.Context,
	backend *databaseBackend,
	keyspace, shard, migrationContext string,
) ([]map[string]string, error) {
	conn, cleanup, err := s.vtgateTargetConn(ctx, backend, keyspace, shard)
	if err != nil {
		return nil, fmt.Errorf("shard-targeted conn: %w", err)
	}
	defer cleanup()
	return showMigrationsOn(ctx, conn, migrationContext)
}

// showMigrationsOn reads one keyspace's rows within vitessQueryTimeout. The
// scan shares the deadline with the query: rows are streamed, so a shard that
// stops answering part way through blocks the scan exactly as it would block
// the query.
// rowQuerier is satisfied by both *sql.DB and the *sql.Conn a shard-targeted
// read runs on, which has to stay one connection to keep its USE keyspace:shard.
type rowQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func showMigrationsOn(ctx context.Context, db rowQuerier, migrationContext string) ([]map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, vitessQueryTimeout)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SHOW VITESS_MIGRATIONS LIKE '"+migrationContext+"'")
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	return scanDynamicRows(rows)
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
