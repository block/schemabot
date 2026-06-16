package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const (
	defaultStatusLimit = 20
	maxStatusLimit     = 1000
)

// changeTypeToString converts a proto ChangeType enum to a lowercase string.
func changeTypeToString(ct ternv1.ChangeType) string {
	switch ct {
	case ternv1.ChangeType_CHANGE_TYPE_CREATE:
		return ddl.StatementTypeToOp(statement.StatementCreateTable)
	case ternv1.ChangeType_CHANGE_TYPE_ALTER:
		return ddl.StatementTypeToOp(statement.StatementAlterTable)
	case ternv1.ChangeType_CHANGE_TYPE_DROP:
		return ddl.StatementTypeToOp(statement.StatementDropTable)
	case ternv1.ChangeType_CHANGE_TYPE_VSCHEMA:
		return "vschema_update"
	default:
		return ""
	}
}

// deriveErrorCode returns an error code based on apply state
// and error message. Returns empty string when no error code applies.
func deriveErrorCode(applyState, errorMessage string) string {
	if errorMessage == "" {
		return ""
	}
	if state.IsState(applyState, state.Apply.FailedRetryable) {
		return apitypes.ErrCodeEngineErrorRetryable
	}
	if state.IsState(applyState, state.Apply.Failed) {
		return apitypes.ErrCodeEngineError
	}
	return ""
}

// shouldServeProgressFromStorage returns true when storage has the authoritative
// progress state and there is no active Tern work to poll.
func shouldServeProgressFromStorage(applyState string) bool {
	return state.IsTerminalApplyState(applyState) || state.IsState(applyState, state.Apply.FailedRetryable)
}

// A remote apply may be accepted into control-plane storage before an operator
// worker dispatches it and stores the data-plane ID. During that handoff window,
// progress must use local storage instead of asking the data plane about a
// control-plane apply identifier it cannot know.
func shouldServeRemoteProgressFromStorage(apply *storage.Apply, client tern.Client) bool {
	if apply == nil {
		return false
	}
	if client == nil || !client.IsRemote() {
		return false
	}
	return apply.ExternalID == ""
}

// queuedRemoteProgressApply returns the user-visible apply state while a gRPC
// apply is claimed by an operator worker but has not been accepted by the
// remote data plane yet. The storage row is already running for recovery
// purposes, but operators should still see pending until external_id exists.
func queuedRemoteProgressApply(apply *storage.Apply) *storage.Apply {
	if apply == nil {
		return nil
	}
	queued := *apply
	if state.IsState(queued.State, state.Apply.Running) {
		queued.State = state.Apply.Pending
	}
	return &queued
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

func storedDeploymentForApply(apply *storage.Apply) (string, error) {
	if apply == nil {
		return "", fmt.Errorf("apply is required")
	}
	if apply.Deployment == "" {
		return "", fmt.Errorf("apply %q is missing stored deployment metadata", apply.ApplyIdentifier)
	}
	return apply.Deployment, nil
}

// deploymentForDatabaseEnvironment resolves the Tern deployment that owns a
// database/environment. The apply's stored deployment is authoritative for
// existing work because it records the route used when the apply started.
func (s *Service) deploymentForDatabaseEnvironment(database, deployment, environment string) (string, error) {
	if deployment != "" {
		return deployment, nil
	}
	resolved, err := s.config.ResolveDatabaseTarget(database, environment)
	if err != nil {
		return "", err
	}
	return resolved.Deployment, nil
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

func progressOperationResponseFromStorage(op *storage.ApplyOperation) *apitypes.ProgressOperationResponse {
	resp := &apitypes.ProgressOperationResponse{
		Deployment:    op.Deployment,
		Target:        op.Target,
		State:         op.State,
		CutoverPolicy: op.CutoverPolicy,
		OnFailure:     op.OnFailure,
		ErrorCode:     deriveErrorCode(op.State, op.ErrorMessage),
		ErrorMessage:  op.ErrorMessage,
	}
	if op.StartedAt != nil {
		resp.StartedAt = op.StartedAt.Format(time.RFC3339)
	}
	if op.CompletedAt != nil {
		resp.CompletedAt = op.CompletedAt.Format(time.RFC3339)
	}
	return resp
}

func (s *Service) progressOperationsForApply(ctx context.Context, apply *storage.Apply) ([]*apitypes.ProgressOperationResponse, map[int64]string, error) {
	if apply == nil {
		return nil, nil, fmt.Errorf("apply is required")
	}
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("list apply operations for apply %d (%s): %w", apply.ID, apply.ApplyIdentifier, err)
	}
	responses := make([]*apitypes.ProgressOperationResponse, 0, len(ops))
	deploymentByOperationID := make(map[int64]string, len(ops))
	for _, op := range ops {
		responses = append(responses, progressOperationResponseFromStorage(op))
		deploymentByOperationID[op.ID] = op.Deployment
	}
	return responses, deploymentByOperationID, nil
}

func (s *Service) bestEffortProgressOperations(ctx context.Context, apply *storage.Apply) ([]*apitypes.ProgressOperationResponse, map[int64]string) {
	if apply == nil {
		s.logger.Warn("progress response will omit per-deployment operations: apply is nil")
		return nil, nil
	}
	operations, deploymentByOperationID, err := s.progressOperationsForApply(ctx, apply)
	if err != nil {
		// Operation rows are observability enrichment, not an apply safety gate.
		// Serve progress without the enrichment and log the storage uncertainty.
		s.logger.Warn("progress response will omit per-deployment operations",
			"apply_id", apply.ApplyIdentifier,
			"apply_db_id", apply.ID,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		return nil, nil
	}
	return operations, deploymentByOperationID
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

	if shouldServeProgressFromStorage(apply.State) {
		httpResp, err := s.progressFromLocalStorage(r.Context(), apply)
		if err != nil {
			s.logger.Error("failed to read apply progress from storage", "apply_id", applyID, "state", apply.State, "error", err)
			s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to read tasks: "+err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, httpResp)
		return
	}

	// Active apply — use the deployment stored on the apply record.
	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		s.logger.Error("active apply is missing stored deployment metadata",
			"apply_id", applyID, "database", apply.Database, "environment", apply.Environment, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, err.Error())
		return
	}
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

	if shouldServeRemoteProgressFromStorage(apply, client) {
		httpResp, err := s.progressFromLocalStorage(r.Context(), queuedRemoteProgressApply(apply))
		if err != nil {
			s.logger.Error("failed to read queued remote apply progress from storage", "apply_id", applyID, "state", apply.State, "error", err)
			s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to read tasks: "+err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, httpResp)
		return
	}

	resp, err := client.Progress(r.Context(), &ternv1.ProgressRequest{
		ApplyId:     ternApplyIDForStoredApply(apply),
		Environment: apply.Environment,
	})
	if err != nil {
		s.logger.Error("progress failed", "apply_id", applyID, "database", apply.Database, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeEngineUnavailable, "progress failed: "+err.Error())
		return
	}

	httpResp := progressResponseFromProto(resp)
	httpResp.Operations, _ = s.bestEffortProgressOperations(r.Context(), apply)
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
		slog.Warn("progress response will omit vitess metadata: no apply record",
			"apply_id", resp.ApplyID,
			"database", resp.Database,
			"environment", resp.Environment)
		return
	}
	if apply.Engine != storage.EnginePlanetScale {
		return
	}
	// The overlay is best-effort enrichment — the progress response is still
	// served without the engine metadata, so log at Warn rather than Error.
	vad, err := s.storage.VitessApplyData().GetByApplyID(ctx, apply.ID)
	if err != nil {
		slog.Warn("progress response will omit vitess metadata: failed to load vitess apply data",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
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
	if vad.IsInstant {
		resp.Metadata["is_instant"] = "true"
	}
	if vad.DeferredDeploy {
		resp.Metadata["deferred_deploy"] = "true"
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

	environments, err := s.config.DatabaseEnvironments(database)
	if err != nil {
		available := make([]string, 0, len(s.config.Databases))
		for name := range s.config.Databases {
			available = append(available, name)
		}
		sort.Strings(available)
		s.logger.Warn("database environments not found",
			"database", database,
			"available_databases", available,
			"error", err)
		if len(available) > 0 {
			s.writeError(w, http.StatusNotFound,
				fmt.Sprintf("no environments found for database %q - configure this database in the SchemaBot server config (available: %v)", database, available))
		} else {
			s.writeError(w, http.StatusNotFound,
				fmt.Sprintf("no environments found for database %q - no databases configured on this server", database))
		}
		return
	}

	if len(environments) == 0 {
		available := make([]string, 0, len(s.config.Databases))
		for name := range s.config.Databases {
			available = append(available, name)
		}
		sort.Strings(available)
		s.logger.Warn("no environments found for database",
			"database", database,
			"available_databases", available)
		if len(available) > 0 {
			s.writeError(w, http.StatusNotFound,
				fmt.Sprintf("no environments found for database %q - configure this database in the SchemaBot server config (available: %v)", database, available))
		} else {
			s.writeError(w, http.StatusNotFound,
				fmt.Sprintf("no environments found for database %q - no databases configured on this server", database))
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"database":     database,
		"environments": environments,
	})
}

// handleDatabaseList returns the sanitized databases registered on this
// server. It intentionally exposes topology metadata only; connection
// strings, opaque execution targets, and endpoint addresses stay server-side.
func (s *Service) handleDatabaseList(w http.ResponseWriter, r *http.Request) {
	databaseType, err := parseDatabaseListTypeFilter(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := databaseListResponse(s.config, databaseType)
	if err != nil {
		s.logger.Error("database list failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "failed to list databases: "+err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, resp)
}

func parseDatabaseListTypeFilter(r *http.Request) (string, error) {
	databaseType := r.URL.Query().Get("type")
	switch databaseType {
	case "", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata:
		return databaseType, nil
	default:
		return "", fmt.Errorf("type must be %q, %q, or %q", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata)
	}
}

func databaseListResponse(config *ServerConfig, databaseType string) (*apitypes.DatabaseListResponse, error) {
	if config == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	databaseNames := make([]string, 0, len(config.Databases))
	for database, dbConfig := range config.Databases {
		if databaseType != "" && dbConfig.Type != databaseType {
			continue
		}
		databaseNames = append(databaseNames, database)
	}
	sort.Strings(databaseNames)

	resp := &apitypes.DatabaseListResponse{Databases: make([]*apitypes.DatabaseResponse, 0, len(databaseNames))}
	for _, database := range databaseNames {
		dbConfig := config.Databases[database]
		environments, err := config.DatabaseEnvironments(database)
		if err != nil {
			return nil, fmt.Errorf("list database environments for database %q: %w", database, err)
		}
		databaseResp := &apitypes.DatabaseResponse{
			Database:     database,
			Type:         dbConfig.Type,
			Environments: make([]*apitypes.DatabaseEnvironmentResponse, 0, len(environments)),
		}
		for _, environment := range environments {
			envRoute, err := databaseEnvironmentResponse(database, dbConfig, environment)
			if err != nil {
				return nil, err
			}
			databaseResp.Environments = append(databaseResp.Environments, envRoute)
		}
		resp.Databases = append(resp.Databases, databaseResp)
	}
	return resp, nil
}

func databaseEnvironmentResponse(database string, dbConfig DatabaseConfig, environment string) (*apitypes.DatabaseEnvironmentResponse, error) {
	envConfig, ok := dbConfig.Environments[environment]
	if !ok {
		return nil, fmt.Errorf("database %q environment %q is not configured on this server", database, environment)
	}
	deployments, err := sanitizedDatabaseDeployments(database, environment, envConfig)
	if err != nil {
		return nil, err
	}
	return &apitypes.DatabaseEnvironmentResponse{
		Environment: environment,
		Deployments: deployments,
	}, nil
}

func sanitizedDatabaseDeployments(database, environment string, envConfig EnvironmentConfig) ([]string, error) {
	if envConfig.HasLocalDSN() {
		return nil, nil
	}
	if envConfig.Deployments != nil {
		if len(envConfig.Deployments) == 0 {
			return nil, fmt.Errorf("database %q environment %q deployments map is empty", database, environment)
		}
		deployments, err := orderedDeploymentKeys(envConfig.Deployments, envConfig.DeploymentOrder, fmt.Sprintf("database %q environment %q", database, environment))
		if err != nil {
			return nil, err
		}
		return deployments, nil
	}
	if envConfig.Deployment == "" {
		return nil, fmt.Errorf("database %q environment %q missing server-side deployment", database, environment)
	}
	return []string{envConfig.Deployment}, nil
}

// handleStatus handles GET /api/status requests.
// Returns recent schema changes.
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	limit, err := parseStatusLimit(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	failuresOnly, err := parseStatusFailuresOnly(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	filter := storage.RecentAppliesFilter{
		Limit:       limit + 1,
		Environment: r.URL.Query().Get("environment"),
	}
	if failuresOnly {
		filter.States = []string{state.Apply.Failed, state.Apply.FailedRetryable}
	}
	applies, err := s.storage.Applies().GetRecent(r.Context(), filter)
	if err != nil {
		s.logger.Error("get recent applies failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "failed to get recent applies")
		return
	}
	hasMore := len(applies) > limit
	if hasMore {
		applies = applies[:limit]
	}

	activeCount := 0
	for _, apply := range applies {
		if !failuresOnly && !state.IsTerminalApplyState(apply.State) {
			activeCount++
		}
	}

	resp := &apitypes.StatusResponse{
		ActiveCount:  activeCount,
		Limit:        limit,
		MaxLimit:     maxStatusLimit,
		HasMore:      hasMore,
		FailuresOnly: failuresOnly,
		Applies:      make([]*apitypes.ActiveApplyResponse, 0, len(applies)),
	}

	for _, apply := range applies {
		resp.Applies = append(resp.Applies, activeApplyResponseFromStorage(apply))
	}

	s.writeJSON(w, http.StatusOK, resp)
}

func activeApplyResponseFromStorage(apply *storage.Apply) *apitypes.ActiveApplyResponse {
	caller := apply.Caller
	if caller == "" {
		caller = "cli"
		if apply.PullRequest > 0 && apply.Repository != "" {
			caller = fmt.Sprintf("%s#%d", apply.Repository, apply.PullRequest)
		}
	}

	active := &apitypes.ActiveApplyResponse{
		ApplyID:      apply.ApplyIdentifier,
		ExternalID:   apply.ExternalID,
		Database:     apply.Database,
		Environment:  apply.Environment,
		State:        apply.State,
		Engine:       apply.Engine,
		Caller:       caller,
		ErrorMessage: apply.ErrorMessage,
		UpdatedAt:    apply.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
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
	return active
}

func parseStatusLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultStatusLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}
	if limit > maxStatusLimit {
		return maxStatusLimit, nil
	}
	return limit, nil
}

func parseStatusFailuresOnly(r *http.Request) (bool, error) {
	raw := r.URL.Query().Get("failed")
	if raw == "" {
		return false, nil
	}
	failed, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("failed must be a boolean")
	}
	return failed, nil
}

// progressFromLocalStorage builds a ProgressResponse from local apply + task
// records when there is no active Tern work to poll.
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
	if !state.IsState(apply.State, state.Apply.FailedRetryable) {
		for _, task := range tasks {
			if !state.IsTerminalTaskState(task.State) && task.State != apply.State {
				stale = true
				break
			}
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
	operations, deploymentByOperationID := s.bestEffortProgressOperations(ctx, apply)
	httpResp.Operations = operations

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
		if task.ApplyOperationID != nil {
			if deployment, ok := deploymentByOperationID[*task.ApplyOperationID]; ok {
				tpr.Deployment = deployment
			}
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
	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		return err
	}
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		return fmt.Errorf("get tern client: %w", err)
	}

	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ExternalID,
		Environment: apply.Environment,
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
	if opts.DeferDeploy {
		optMap["defer_deploy"] = "true"
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
