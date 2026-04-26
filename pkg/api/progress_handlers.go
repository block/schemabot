package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// changeTypeToString converts a proto ChangeType enum to a lowercase string.
func changeTypeToString(ct ternv1.ChangeType) string {
	switch ct {
	case ternv1.ChangeType_CHANGE_TYPE_CREATE:
		return "create"
	case ternv1.ChangeType_CHANGE_TYPE_ALTER:
		return "alter"
	case ternv1.ChangeType_CHANGE_TYPE_DROP:
		return "drop"
	default:
		return ""
	}
}

// deriveErrorCode returns an error code based on apply state
// and error message. Returns empty string when no error code applies.
func deriveErrorCode(applyState, errorMessage string) string {
	if errorMessage != "" && state.IsState(applyState, state.Apply.Failed) {
		return apitypes.ErrCodeEngineError
	}
	return ""
}

// engineName converts a protobuf Engine enum to a display-friendly name.
func engineName(e ternv1.Engine) string {
	switch e {
	case ternv1.Engine_ENGINE_SPIRIT:
		return "Spirit"
	case ternv1.Engine_ENGINE_PLANETSCALE:
		return "PlanetScale"
	default:
		return "Unknown"
	}
}

const progressTableKeySep = "\x00"

func progressTableKey(namespace, table string) string {
	return namespace + progressTableKeySep + table
}

// resolveDeployment determines the deployment name from a database and explicit deployment.
// In local mode (config-based databases), the database name is used as the deployment.
// In gRPC mode, falls back to DefaultDeployment.
func (s *Service) resolveDeployment(database, deployment string) string {
	if deployment != "" {
		return deployment
	}
	if s.config.Database(database) != nil {
		return database
	}
	return DefaultDeployment
}

// progressResponseFromProto converts a protobuf ProgressResponse to an HTTP ProgressResponse.
func progressResponseFromProto(resp *ternv1.ProgressResponse) *apitypes.ProgressResponse {
	progressState := tern.ProtoStateToStorage(resp.State)
	httpResp := &apitypes.ProgressResponse{
		State:        progressState,
		Engine:       engineName(resp.Engine),
		ApplyID:      resp.ApplyId,
		StartedAt:    resp.StartedAt,
		CompletedAt:  resp.CompletedAt,
		ErrorCode:    deriveErrorCode(progressState, resp.ErrorMessage),
		ErrorMessage: resp.ErrorMessage,
		Summary:      resp.Summary,
		Volume:       resp.Volume,
	}

	for _, t := range resp.Tables {
		tpr := &apitypes.TableProgressResponse{
			TableName:       t.TableName,
			Keyspace:        t.Namespace,
			ChangeType:      changeTypeToString(t.ChangeType),
			DDL:             t.Ddl,
			Status:          t.Status,
			RowsCopied:      t.RowsCopied,
			RowsTotal:       t.RowsTotal,
			PercentComplete: t.PercentComplete,
			ETASeconds:      t.EtaSeconds,
			IsInstant:       t.IsInstant,
			ProgressDetail:  t.ProgressDetail,
			TaskID:          t.TaskId,
		}
		for _, sh := range t.Shards {
			var pct int32
			if sh.RowsTotal > 0 {
				pct = int32(sh.RowsCopied * 100 / sh.RowsTotal)
			}
			tpr.Shards = append(tpr.Shards, &apitypes.ShardProgressResponse{
				Shard:           sh.Shard,
				Status:          state.NormalizeShardStatus(sh.Status),
				RowsCopied:      sh.RowsCopied,
				RowsTotal:       sh.RowsTotal,
				ETASeconds:      sh.EtaSeconds,
				PercentComplete: pct,
				CutoverAttempts: sh.CutoverAttempts,
			})
		}
		httpResp.Tables = append(httpResp.Tables, tpr)
	}

	return httpResp
}

// GetProgress fetches the current progress for a database from its tern client.
// This is used by the webhook handler to populate table-level progress in PR comments.
func (s *Service) GetProgress(ctx context.Context, database, environment string) (*apitypes.ProgressResponse, error) {
	ternApplyID, activeApply, err := s.findActiveApplyID(ctx, database, environment)
	if err != nil {
		return nil, fmt.Errorf("resolve active apply for %s: %w", database, err)
	}

	// Use deployment from the stored apply if available.
	var storedDeployment string
	if activeApply != nil {
		storedDeployment = activeApply.Deployment
	}
	deployment := s.resolveDeployment(database, storedDeployment)

	client, err := s.TernClient(deployment, environment)
	if err != nil {
		return nil, fmt.Errorf("get tern client for %s/%s: %w", database, environment, err)
	}

	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:  ternApplyID,
		Database: database,
	})
	if err != nil {
		return nil, fmt.Errorf("progress for %s: %w", database, err)
	}

	httpResp := progressResponseFromProto(resp)
	httpResp.Database = database
	httpResp.Environment = environment

	if activeApply != nil {
		httpResp.ApplyID = activeApply.ApplyIdentifier
		overlayApplyOptions(httpResp, activeApply)
		s.overlayVitessMetadata(ctx, httpResp, activeApply)
	}

	return httpResp, nil
}

// handleProgress handles GET /api/progress/{database} requests.
func (s *Service) handleProgress(w http.ResponseWriter, r *http.Request) {
	database := r.PathValue("database")
	if database == "" {
		s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, "database is required")
		return
	}

	s.logger.Info("progress by database", "database", database)
	environment := r.URL.Query().Get("environment")
	if environment == "" {
		environment = "staging"
	}

	// Resolve the Tern-facing apply_id. Prefer explicit apply_id query param
	// (resolves external_id from storage), fall back to active apply lookup.
	var ternApplyID string
	var activeApply *storage.Apply
	if qApplyID := r.URL.Query().Get("apply_id"); qApplyID != "" {
		resolved, err := s.resolveApplyID(r.Context(), qApplyID)
		if err != nil {
			s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to resolve apply_id: "+err.Error())
			return
		}
		ternApplyID = resolved
		activeApply, _ = s.storage.Applies().GetByApplyIdentifier(r.Context(), qApplyID)
	} else {
		var err error
		ternApplyID, activeApply, err = s.findActiveApplyID(r.Context(), database, environment)
		if err != nil {
			s.logger.Error("failed to look up active apply from storage", "database", database, "environment", environment, "error", err)
			s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to look up active apply: "+err.Error())
			return
		}
	}

	// Deployment is a plan-time decision stored on the apply record.
	// Use the stored deployment if an apply exists, otherwise fall back to default resolution.
	var deployment string
	if activeApply != nil && activeApply.Deployment != "" {
		deployment = activeApply.Deployment
	}
	deployment = s.resolveDeployment(database, deployment)

	client, err := s.TernClient(deployment, environment)
	if err != nil {
		s.writeErrorCode(w, http.StatusNotFound, apitypes.ErrCodeDeploymentNotFound, err.Error())
		return
	}

	resp, err := client.Progress(r.Context(), &ternv1.ProgressRequest{
		ApplyId:  ternApplyID,
		Database: database,
	})
	if err != nil {
		s.logger.Error("progress failed", "database", database, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeEngineUnavailable, "progress failed: "+err.Error())
		return
	}

	httpResp := progressResponseFromProto(resp)

	// Overlay apply metadata from storage.
	if activeApply != nil {
		httpResp.ApplyID = activeApply.ApplyIdentifier
		overlayApplyOptions(httpResp, activeApply)
		s.overlayVitessMetadata(r.Context(), httpResp, activeApply)
	}

	s.writeJSON(w, http.StatusOK, httpResp)
}

// handleProgressByApplyID handles GET /api/progress/apply/{apply_id} requests.
// Returns progress for a specific apply by its external identifier.
func (s *Service) handleProgressByApplyID(w http.ResponseWriter, r *http.Request) {
	applyID := r.PathValue("apply_id")
	if applyID == "" {
		s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, "apply_id is required")
		return
	}

	s.logger.Info("progress by apply-id", "apply_id", applyID)

	// Look up the apply by its external identifier
	apply, err := s.storage.Applies().GetByApplyIdentifier(r.Context(), applyID)
	if err != nil {
		s.logger.Error("failed to get apply", "apply_id", applyID, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to get apply: "+err.Error())
		return
	}
	if apply == nil {
		s.writeErrorCode(w, http.StatusNotFound, apitypes.ErrCodeNotFound, "apply not found: "+applyID)
		return
	}

	// For terminal applies, serve from local storage (no RPC needed).
	if state.IsTerminalApplyState(apply.State) {
		httpResp, err := s.progressFromLocalStorage(r.Context(), apply)
		if err != nil {
			s.logger.Error("failed to read tasks from storage for terminal apply", "apply_id", applyID, "error", err)
			s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to read tasks: "+err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, httpResp)
		return
	}

	// Active apply — use the deployment stored on the apply record.
	deployment := s.resolveDeployment(apply.Database, apply.Deployment)
	s.logger.Debug("progress by apply-id: resolving client", "apply_id", applyID, "database", apply.Database, "deployment", deployment, "environment", apply.Environment)

	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("no tern client for active apply — server is misconfigured",
			"apply_id", applyID, "database", apply.Database, "deployment", deployment, "environment", apply.Environment, "error", err)
		s.writeErrorCode(w, http.StatusNotFound, apitypes.ErrCodeDeploymentNotFound,
			fmt.Sprintf("no tern client configured for database %q (deployment=%q, environment=%q) — add this database to the server config", apply.Database, deployment, apply.Environment))
		return
	}
	s.logger.Debug("progress by apply-id: got client", "apply_id", applyID, "is_remote", client.IsRemote())

	// Resolve to the Tern-facing ID: external_id (remote engine's apply identifier) or apply_identifier (local mode).
	ternApplyID := apply.ApplyIdentifier
	if apply.ExternalID != "" {
		ternApplyID = apply.ExternalID
	}

	resp, err := client.Progress(r.Context(), &ternv1.ProgressRequest{
		ApplyId:  ternApplyID,
		Database: apply.Database,
	})
	if err != nil {
		s.logger.Error("progress failed", "apply_id", applyID, "database", apply.Database, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeEngineUnavailable, "progress failed: "+err.Error())
		return
	}

	httpResp := progressResponseFromProto(resp)
	httpResp.ApplyID = apply.ApplyIdentifier
	httpResp.Database = apply.Database
	httpResp.Environment = apply.Environment
	httpResp.Caller = apply.Caller
	if apply.Repository != "" && apply.PullRequest > 0 {
		httpResp.PullRequest = fmt.Sprintf("https://github.com/%s/pull/%d", apply.Repository, apply.PullRequest)
	}

	// Re-read the apply record — the tern client's Progress call may have
	// updated state and timestamps (e.g., CompletedAt set when engine reports
	// terminal state).
	if freshApply, err := s.storage.Applies().GetByApplyIdentifier(r.Context(), applyID); err == nil && freshApply != nil {
		apply = freshApply
	}
	if apply.StartedAt != nil {
		httpResp.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		httpResp.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}

	overlayApplyOptions(httpResp, apply)

	s.overlayVitessMetadata(r.Context(), httpResp, apply)

	// Overlay per-table timestamps from task records. The proto response
	// doesn't carry task timestamps, but storage has them from engine
	// progress polling (e.g., SHOW VITESS_MIGRATIONS started_timestamp).
	if tasks, err := s.storage.Tasks().GetByApplyID(r.Context(), apply.ID); err == nil {
		taskByTable := make(map[string]*storage.Task, len(tasks))
		for _, t := range tasks {
			taskByTable[progressTableKey(t.Namespace, t.TableName)] = t
		}
		for _, tpr := range httpResp.Tables {
			task, ok := taskByTable[progressTableKey(tpr.Keyspace, tpr.TableName)]
			if ok {
				if task.StartedAt != nil && tpr.StartedAt == "" {
					tpr.StartedAt = task.StartedAt.Format(time.RFC3339)
				}
				if task.CompletedAt != nil && tpr.CompletedAt == "" {
					tpr.CompletedAt = task.CompletedAt.Format(time.RFC3339)
				}
			}
		}
	}

	s.writeJSON(w, http.StatusOK, httpResp)
}

// overlayVitessMetadata loads VitessApplyData for the given apply and merges
// engine-specific metadata (deploy request URL, revert status) into the response.
func (s *Service) overlayVitessMetadata(ctx context.Context, resp *apitypes.ProgressResponse, apply *storage.Apply) {
	if apply == nil {
		slog.Warn("overlayVitessMetadata: no apply record")
		return
	}
	if apply.Engine != storage.EnginePlanetScale {
		return
	}
	vad, err := s.storage.VitessApplyData().GetByApplyID(ctx, apply.ID)
	if err != nil {
		slog.Error("overlayVitessMetadata: failed to load vitess apply data", "apply_id", apply.ApplyIdentifier, "error", err)
		return
	}
	if resp.Metadata == nil {
		resp.Metadata = make(map[string]string)
	}
	if vad.BranchName != "" {
		resp.Metadata["branch_name"] = vad.BranchName
	}
	if vad.DeployRequestURL != "" {
		resp.Metadata["deploy_request_url"] = vad.DeployRequestURL
	}
	if vad.RevertSkippedAt != nil {
		resp.Metadata["revert_skipped"] = "true"
	}
}

// handleDatabaseHistory handles GET /api/history/{database} requests.
// Returns all applies for a database, sorted by created_at desc.
func (s *Service) handleDatabaseHistory(w http.ResponseWriter, r *http.Request) {
	database := r.PathValue("database")
	if database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}

	environment := r.URL.Query().Get("environment")

	applies, err := s.storage.Applies().GetByDatabase(r.Context(), database, "", environment)
	if err != nil {
		s.logger.Error("failed to get applies", "database", database, "error", err)
		s.writeError(w, http.StatusInternalServerError, "failed to get applies: "+err.Error())
		return
	}

	resp := &apitypes.DatabaseHistoryResponse{
		Database: database,
		Applies:  make([]*apitypes.ApplyHistoryResponse, 0, len(applies)),
	}

	for _, apply := range applies {
		caller := apply.Caller
		if caller == "" {
			caller = "cli"
			if apply.PullRequest > 0 && apply.Repository != "" {
				caller = fmt.Sprintf("%s#%d", apply.Repository, apply.PullRequest)
			} else if apply.PullRequest > 0 {
				caller = fmt.Sprintf("PR %d", apply.PullRequest)
			}
		}
		applyResp := &apitypes.ApplyHistoryResponse{
			ApplyID:     apply.ApplyIdentifier,
			Environment: apply.Environment,
			State:       apply.State,
			Engine:      apply.Engine,
			Caller:      caller,
			Error:       apply.ErrorMessage,
			ErrorCode:   deriveErrorCode(apply.State, apply.ErrorMessage),
		}
		if apply.StartedAt != nil {
			applyResp.StartedAt = apply.StartedAt.Format(time.RFC3339)
		}
		if apply.CompletedAt != nil {
			applyResp.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
		}
		resp.Applies = append(resp.Applies, applyResp)
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// handleDatabaseEnvironments returns the list of environments for a database.
// This is used by the CLI to discover environments when -e flag is not specified.
func (s *Service) handleDatabaseEnvironments(w http.ResponseWriter, r *http.Request) {
	database := r.PathValue("database")
	if database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}

	var environments []string

	// Check local mode config (Databases)
	if dbConfig := s.config.Database(database); dbConfig != nil {
		for env := range dbConfig.Environments {
			environments = append(environments, env)
		}
	}

	// Check gRPC mode config (TernDeployments)
	if len(environments) == 0 && len(s.config.TernDeployments) > 0 {
		deploymentParam := r.URL.Query().Get("deployment")
		deployment := s.resolveDeployment(database, deploymentParam)
		if endpoints, ok := s.config.TernDeployments[deployment]; ok {
			for env := range endpoints {
				environments = append(environments, env)
			}
		}
	}

	if len(environments) == 0 {
		available := make([]string, 0, len(s.config.TernDeployments))
		for name := range s.config.TernDeployments {
			available = append(available, name)
		}
		sort.Strings(available)
		s.logger.Warn("no environments found for database",
			"database", database,
			"available_deployments", available)
		if len(available) > 0 {
			s.writeError(w, http.StatusNotFound,
				fmt.Sprintf("no environments found for database %q — no matching deployment (available: %v). "+
					"Add a 'deployment' field to schemabot.yaml or configure a 'default' deployment on the server.", database, available))
		} else {
			s.writeError(w, http.StatusNotFound,
				fmt.Sprintf("no environments found for database %q — no databases or deployments configured on this server", database))
		}
		return
	}

	sort.Strings(environments)

	s.writeJSON(w, http.StatusOK, map[string]any{
		"database":     database,
		"environments": environments,
	})
}

// handleStatus handles GET /api/status requests.
// Returns recent schema changes (active first, then completed/failed).
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	applies, err := s.storage.Applies().GetRecent(r.Context(), 20)
	if err != nil {
		s.logger.Error("get recent applies failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "failed to get recent applies")
		return
	}

	activeCount := 0
	for _, apply := range applies {
		if !state.IsTerminalApplyState(apply.State) {
			activeCount++
		}
	}

	resp := &apitypes.StatusResponse{
		ActiveCount: activeCount,
		Applies:     make([]*apitypes.ActiveApplyResponse, 0, len(applies)),
	}

	for _, apply := range applies {
		caller := apply.Caller
		if caller == "" {
			caller = "cli"
			if apply.PullRequest > 0 && apply.Repository != "" {
				caller = fmt.Sprintf("%s#%d", apply.Repository, apply.PullRequest)
			}
		}

		active := &apitypes.ActiveApplyResponse{
			ApplyID:     apply.ApplyIdentifier,
			Database:    apply.Database,
			Environment: apply.Environment,
			State:       apply.State,
			Engine:      apply.Engine,
			Caller:      caller,
			UpdatedAt:   apply.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
		if apply.StartedAt != nil {
			active.StartedAt = apply.StartedAt.Format("2006-01-02T15:04:05Z07:00")
		}
		if apply.CompletedAt != nil {
			active.CompletedAt = apply.CompletedAt.Format("2006-01-02T15:04:05Z07:00")
		}
		opts := storage.ParseApplyOptions(apply.Options)
		if opts.Volume > 0 {
			active.Volume = opts.Volume
		}
		resp.Applies = append(resp.Applies, active)
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// progressFromLocalStorage builds a ProgressResponse from local apply + task
// records. Used for terminal applies where the Tern RPC is unnecessary.
//
// If any local task records are stale (non-terminal state on a terminal apply),
// this method syncs them from a one-time Tern RPC before building the response.
// Subsequent calls serve entirely from local storage.
func (s *Service) progressFromLocalStorage(ctx context.Context, apply *storage.Apply) (*apitypes.ProgressResponse, error) {
	tasks, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("get tasks for apply %d: %w", apply.ID, err)
	}

	// Check if any tasks are stale (non-terminal and not matching the apply
	// state). A stopped task on a stopped apply is expected, not stale.
	stale := false
	for _, task := range tasks {
		if !state.IsTerminalTaskState(task.State) && task.State != apply.State {
			stale = true
			break
		}
	}

	// Sync stale tasks from Tern (one-time RPC, no-op on subsequent calls).
	if stale && apply.ExternalID != "" {
		if err := s.syncTasksFromTern(ctx, apply, tasks); err != nil {
			s.logger.Warn("task sync from Tern failed, serving stale data",
				"apply_id", apply.ApplyIdentifier, "error", err)
		} else {
			// Re-read tasks after sync; keep original on failure.
			if refreshed, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID); err == nil {
				tasks = refreshed
			}
		}
	}

	// Build response from local records
	httpResp := &apitypes.ProgressResponse{
		State:       apply.State,
		Engine:      apply.Engine,
		ApplyID:     apply.ApplyIdentifier,
		Database:    apply.Database,
		Environment: apply.Environment,
		Caller:      apply.Caller,
	}
	if apply.Repository != "" && apply.PullRequest > 0 {
		httpResp.PullRequest = fmt.Sprintf("https://github.com/%s/pull/%d", apply.Repository, apply.PullRequest)
	}
	if apply.StartedAt != nil {
		httpResp.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		httpResp.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}
	if apply.ErrorMessage != "" {
		httpResp.ErrorCode = deriveErrorCode(apply.State, apply.ErrorMessage)
		httpResp.ErrorMessage = apply.ErrorMessage
	}
	overlayApplyOptions(httpResp, apply)
	s.overlayVitessMetadata(ctx, httpResp, apply)

	for _, task := range tasks {
		tpr := &apitypes.TableProgressResponse{
			TableName:       task.TableName,
			Keyspace:        task.Namespace,
			ChangeType:      task.DDLAction,
			DDL:             task.DDL,
			Status:          task.State,
			RowsCopied:      task.RowsCopied,
			RowsTotal:       task.RowsTotal,
			PercentComplete: int32(task.ProgressPercent),
			IsInstant:       task.IsInstant,
			TaskID:          task.TaskIdentifier,
		}
		if task.StartedAt != nil {
			tpr.StartedAt = task.StartedAt.Format(time.RFC3339)
		}
		if task.CompletedAt != nil {
			tpr.CompletedAt = task.CompletedAt.Format(time.RFC3339)
		}
		httpResp.Tables = append(httpResp.Tables, tpr)
	}

	return httpResp, nil
}

// syncTasksFromTern calls the remote Tern's Progress RPC and syncs the
// per-table state into local task records. Called once for gRPC-mode applies
// with stale task state; subsequent reads are served from local storage.
func (s *Service) syncTasksFromTern(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error {
	deployment := s.resolveDeployment(apply.Database, apply.Deployment)
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		return fmt.Errorf("get tern client: %w", err)
	}

	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:  apply.ExternalID,
		Database: apply.Database,
	})
	if err != nil {
		return fmt.Errorf("progress RPC: %w", err)
	}

	// Build namespace/table → proto progress lookup. Vitess applies commonly
	// include the same table name in multiple keyspaces.
	tableProgress := make(map[string]*ternv1.TableProgress, len(resp.Tables))
	for _, tp := range resp.Tables {
		tableProgress[progressTableKey(tp.Namespace, tp.TableName)] = tp
	}

	now := time.Now()
	var synced int
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		tp, ok := tableProgress[progressTableKey(task.Namespace, task.TableName)]
		if !ok {
			s.logger.Error("task has no matching table in Tern progress response",
				"task_id", task.TaskIdentifier, "namespace", task.Namespace, "table", task.TableName, "apply_id", apply.ApplyIdentifier)
			continue
		}
		task.State = state.NormalizeTaskStatus(tp.Status)
		task.RowsCopied = tp.RowsCopied
		task.RowsTotal = tp.RowsTotal
		task.ProgressPercent = int(tp.PercentComplete)
		task.UpdatedAt = now
		if err := s.storage.Tasks().Update(ctx, task); err != nil {
			s.logger.Error("sync task failed", "task_id", task.TaskIdentifier, "error", err)
			continue
		}
		synced++
	}
	s.logger.Info("synced stale task records from Tern",
		"apply_id", apply.ApplyIdentifier, "synced", synced, "total", len(tasks))
	return nil
}

// overlayApplyOptions populates volume and options on the response from the apply record.
func overlayApplyOptions(resp *apitypes.ProgressResponse, apply *storage.Apply) {
	opts := storage.ParseApplyOptions(apply.Options)
	if opts.Volume > 0 {
		resp.Volume = int32(opts.Volume)
	}
	optMap := make(map[string]string)
	if opts.DeferCutover {
		optMap["defer_cutover"] = "true"
	}
	if opts.SkipRevert {
		optMap["skip_revert"] = "true"
	}
	if opts.AllowUnsafe {
		optMap["allow_unsafe"] = "true"
	}
	if len(optMap) > 0 {
		resp.Options = optMap
	}
}
