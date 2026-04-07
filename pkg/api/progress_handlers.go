package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// deriveErrorCode returns a machine-readable error code based on apply state
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
		httpResp.Tables = append(httpResp.Tables, &apitypes.TableProgressResponse{
			TableName:       t.TableName,
			DDL:             t.Ddl,
			Status:          t.Status,
			RowsCopied:      t.RowsCopied,
			RowsTotal:       t.RowsTotal,
			PercentComplete: t.PercentComplete,
			ETASeconds:      t.EtaSeconds,
			IsInstant:       t.IsInstant,
			ProgressDetail:  t.ProgressDetail,
			TaskID:          t.TaskId,
		})
	}

	return httpResp
}

// GetProgress fetches the current progress for a database from its tern client.
// This is used by the webhook handler to populate table-level progress in PR comments.
func (s *Service) GetProgress(ctx context.Context, database, environment string) (*apitypes.ProgressResponse, error) {
	deployment := s.resolveDeployment(database, "")

	client, err := s.TernClient(deployment, environment)
	if err != nil {
		return nil, fmt.Errorf("get tern client for %s/%s: %w", database, environment, err)
	}

	ternApplyID, activeApply, err := s.findActiveApplyID(ctx, database, environment)
	if err != nil {
		return nil, fmt.Errorf("resolve active apply for %s: %w", database, err)
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
		opts := storage.ParseApplyOptions(activeApply.Options)
		if opts.Volume > 0 {
			httpResp.Volume = int32(opts.Volume)
		}
	}

	return httpResp, nil
}

// handleProgress handles GET /api/progress/{database} requests.
func (s *Service) handleProgress(w http.ResponseWriter, r *http.Request) {
	database := r.PathValue("database")
	if database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}

	deployment := s.resolveDeployment(database, r.URL.Query().Get("deployment"))
	environment := r.URL.Query().Get("environment")
	if environment == "" {
		environment = "staging"
	}

	client, err := s.TernClient(deployment, environment)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Resolve the Tern-facing apply_id. Prefer explicit apply_id query param
	// (resolves external_id from storage), fall back to active apply lookup.
	var ternApplyID string
	var activeApply *storage.Apply
	if qApplyID := r.URL.Query().Get("apply_id"); qApplyID != "" {
		resolved, err := s.resolveApplyID(r.Context(), qApplyID)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "failed to resolve apply_id: "+err.Error())
			return
		}
		ternApplyID = resolved
		// Look up the apply for metadata overlay.
		activeApply, _ = s.storage.Applies().GetByApplyIdentifier(r.Context(), qApplyID)
	} else {
		var err error
		ternApplyID, activeApply, err = s.findActiveApplyID(r.Context(), database, environment)
		if err != nil {
			s.logger.Error("failed to resolve active apply", "database", database, "error", err)
			s.writeError(w, http.StatusInternalServerError, "failed to resolve active apply: "+err.Error())
			return
		}
	}

	resp, err := client.Progress(r.Context(), &ternv1.ProgressRequest{
		ApplyId:  ternApplyID,
		Database: database,
	})
	if err != nil {
		s.logger.Error("progress failed", "database", database, "error", err)
		s.writeError(w, http.StatusInternalServerError, "progress failed: "+err.Error())
		return
	}

	httpResp := progressResponseFromProto(resp)

	// Overlay apply metadata from storage.
	if activeApply != nil {
		httpResp.ApplyID = activeApply.ApplyIdentifier
		opts := storage.ParseApplyOptions(activeApply.Options)
		if opts.Volume > 0 {
			httpResp.Volume = int32(opts.Volume)
		}
	}

	s.writeJSON(w, http.StatusOK, httpResp)
}

// handleProgressByApplyID handles GET /api/progress/apply/{apply_id} requests.
// Returns progress for a specific apply by its external identifier.
func (s *Service) handleProgressByApplyID(w http.ResponseWriter, r *http.Request) {
	applyID := r.PathValue("apply_id")
	if applyID == "" {
		s.writeError(w, http.StatusBadRequest, "apply_id is required")
		return
	}

	// Look up the apply by its external identifier
	apply, err := s.storage.Applies().GetByApplyIdentifier(r.Context(), applyID)
	if err != nil {
		s.logger.Error("failed to get apply", "apply_id", applyID, "error", err)
		s.writeError(w, http.StatusInternalServerError, "failed to get apply: "+err.Error())
		return
	}
	if apply == nil {
		s.writeError(w, http.StatusNotFound, "apply not found: "+applyID)
		return
	}

	// For terminal applies, serve from local storage (no RPC needed).
	if state.IsTerminalApplyState(apply.State) {
		httpResp, err := s.progressFromLocalStorage(r.Context(), apply)
		if err != nil {
			s.logger.Error("local progress failed", "apply_id", applyID, "error", err)
			s.writeError(w, http.StatusInternalServerError, "progress failed: "+err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, httpResp)
		return
	}

	// Active apply — call Tern for live progress.
	deployment := s.resolveDeployment(apply.Database, "")

	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

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
		s.writeError(w, http.StatusInternalServerError, "progress failed: "+err.Error())
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

	// Use apply record as source of truth for timestamps
	if apply.StartedAt != nil {
		httpResp.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		httpResp.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}

	// Add volume from apply options
	opts := storage.ParseApplyOptions(apply.Options)
	if opts.Volume > 0 {
		httpResp.Volume = int32(opts.Volume)
	}

	s.writeJSON(w, http.StatusOK, httpResp)
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
		deployment := s.resolveDeployment(database, "")
		if endpoints, ok := s.config.TernDeployments[deployment]; ok {
			for env := range endpoints {
				environments = append(environments, env)
			}
		}
	}

	if len(environments) == 0 {
		s.writeError(w, http.StatusNotFound, "database not found: "+database)
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
	opts := storage.ParseApplyOptions(apply.Options)
	if opts.Volume > 0 {
		httpResp.Volume = int32(opts.Volume)
	}

	for _, task := range tasks {
		httpResp.Tables = append(httpResp.Tables, &apitypes.TableProgressResponse{
			TableName:       task.TableName,
			DDL:             task.DDL,
			Status:          task.State,
			RowsCopied:      task.RowsCopied,
			RowsTotal:       task.RowsTotal,
			PercentComplete: int32(task.ProgressPercent),
			IsInstant:       task.IsInstant,
			TaskID:          task.TaskIdentifier,
		})
	}

	return httpResp, nil
}

// syncTasksFromTern calls the remote Tern's Progress RPC and syncs the
// per-table state into local task records. Called once for gRPC-mode applies
// with stale task state; subsequent reads are served from local storage.
func (s *Service) syncTasksFromTern(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error {
	deployment := s.resolveDeployment(apply.Database, "")
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

	// Build table name → proto progress lookup
	tableProgress := make(map[string]*ternv1.TableProgress, len(resp.Tables))
	for _, tp := range resp.Tables {
		tableProgress[tp.TableName] = tp
	}

	now := time.Now()
	var synced int
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		tp, ok := tableProgress[task.TableName]
		if !ok {
			s.logger.Error("task has no matching table in Tern progress response",
				"task_id", task.TaskIdentifier, "table", task.TableName, "apply_id", apply.ApplyIdentifier)
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
