package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// controlStatus returns "success" if accepted, "rejected" otherwise.
func controlStatus(accepted bool) string {
	if accepted {
		return "success"
	}
	return "rejected"
}

// logControlOperation appends an apply log entry for a control operation (cutover, stop, start, revert, etc.).
func (s *Service) logControlOperation(r *http.Request, applyID, caller, eventType, message string) {
	if applyID == "" {
		s.logger.Debug("skipping control operation log — no apply ID", "event", eventType)
		return
	}
	applyStore := s.storage.Applies()
	if applyStore == nil {
		s.logger.Error("apply store not available for control operation log", "apply_id", applyID, "event", eventType)
		return
	}
	apply, err := applyStore.GetByApplyIdentifier(r.Context(), applyID)
	if err != nil {
		s.logger.Error("failed to look up apply for control operation log", "apply_id", applyID, "event", eventType, "error", err)
		return
	}
	if apply == nil {
		s.logger.Warn("apply not found for control operation log", "apply_id", applyID, "event", eventType)
		return
	}
	logStore := s.storage.ApplyLogs()
	if logStore == nil {
		s.logger.Error("apply log store not available for control operation log", "apply_id", applyID, "event", eventType)
		return
	}
	logMessage := fmt.Sprintf("%s (caller: %s)", message, caller)
	if err := logStore.Append(r.Context(), &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: eventType,
		Source:    storage.LogSourceSchemaBot,
		Message:   logMessage,
	}); err != nil {
		s.logger.Error("failed to append control operation log", "apply_id", apply.ID, "event", eventType, "error", err)
	}
}

// writeControlError logs and writes an HTTP error for a control operation.
func (s *Service) writeControlError(w http.ResponseWriter, opName string, apply *storage.Apply, err error) {
	attrs := []any{"error", err}
	if apply != nil {
		attrs = append(attrs,
			"apply_id", apply.ApplyIdentifier,
			"external_apply_id", apply.ExternalID,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"environment", apply.Environment,
		)
	}
	s.logger.Error(opName+" failed", attrs...)
	s.writeError(w, http.StatusInternalServerError, opName+" failed: "+err.Error())
}

// decodeControlRequest decodes a control request (stop/start/cutover/volume),
// loads the apply record, and returns a Tern client using the deployment stored
// on that apply. Control operations are scoped by apply_id + environment; the
// database is derived from storage so callers cannot target a different
// database than the one originally planned.
func (s *Service) decodeControlRequest(w http.ResponseWriter, r *http.Request, dest any,
	environment, applyID *string) (tern.Client, *storage.Apply, string, bool) {

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dest); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return nil, nil, "", false
	}
	if *applyID == "" {
		s.writeError(w, http.StatusBadRequest, "apply_id is required")
		return nil, nil, "", false
	}
	if *environment == "" {
		s.writeError(w, http.StatusBadRequest, "environment is required")
		return nil, nil, "", false
	}

	applyIdentifier := *applyID
	if s.storage == nil {
		s.logger.Error("storage not available for control request", "path", r.URL.Path, "apply_id", applyIdentifier, "environment", *environment)
		s.writeError(w, http.StatusInternalServerError, "storage is not available")
		return nil, nil, "", false
	}
	applyStore := s.storage.Applies()
	if applyStore == nil {
		s.logger.Error("apply store not available for control request", "path", r.URL.Path, "apply_id", applyIdentifier, "environment", *environment)
		s.writeError(w, http.StatusInternalServerError, "apply store is not available")
		return nil, nil, "", false
	}
	apply, err := applyStore.GetByApplyIdentifier(r.Context(), applyIdentifier)
	if err != nil {
		s.logger.Error("failed to load apply for control request", "path", r.URL.Path, "apply_id", applyIdentifier, "environment", *environment, "error", err)
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to look up apply %q: %v", applyIdentifier, err))
		return nil, nil, "", false
	}
	if apply == nil {
		s.writeError(w, http.StatusNotFound, "apply not found: "+applyIdentifier)
		return nil, nil, "", false
	}
	resolvedApplyID := ternApplyIDForStoredApply(apply)
	if apply.Environment != *environment {
		s.writeError(w, http.StatusBadRequest,
			fmt.Sprintf("apply %q belongs to environment %q, not %q", applyIdentifier, apply.Environment, *environment))
		return nil, nil, "", false
	}
	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		s.logger.Error("control request apply is missing stored deployment metadata",
			"apply_id", applyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"environment", apply.Environment,
			"error", err)
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return nil, nil, "", false
	}

	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("failed to create Tern client",
			"deployment", deployment,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		s.writeError(w, http.StatusNotFound, err.Error())
		return nil, nil, "", false
	}

	return client, apply, resolvedApplyID, true
}

func ternApplyIDForStoredApply(apply *storage.Apply) string {
	if apply.ExternalID != "" {
		return apply.ExternalID
	}
	return apply.ApplyIdentifier
}

// CutoverRequest is the HTTP request body for POST /api/cutover.
type CutoverRequest struct {
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller,omitempty"`
}

// handleCutover handles POST /api/cutover requests.
func (s *Service) handleCutover(w http.ResponseWriter, r *http.Request) {
	var req CutoverRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.Cutover(r.Context(), &ternv1.CutoverRequest{
		ApplyId:     applyID,
		Database:    apply.Database,
		Environment: apply.Environment,
	})
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "cutover", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "cutover", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "cutover", apply.Database, apply.Environment, controlStatus(resp.Accepted))
	if resp.Accepted {
		s.logControlOperation(r, apply.ApplyIdentifier, req.Caller, storage.LogEventCutoverTriggered, "Cutover triggered by user")
	}

	s.writeJSON(w, http.StatusOK, &apitypes.ControlResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
	})
}

// StopRequest is the HTTP request body for POST /api/stop.
type StopRequest struct {
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller,omitempty"`
}

// handleStop handles POST /api/stop requests.
// Stops all non-terminal tasks for the database.
func (s *Service) handleStop(w http.ResponseWriter, r *http.Request) {
	var req StopRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.Stop(r.Context(), &ternv1.StopRequest{
		ApplyId:     applyID,
		Database:    apply.Database,
		Environment: apply.Environment,
	})
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "stop", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "stop", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "stop", apply.Database, apply.Environment, controlStatus(resp.Accepted))
	if resp.Accepted && resp.StoppedCount > 0 && client.IsRemote() {
		if syncErr := s.syncRemoteStopState(r.Context(), client, apply); syncErr != nil {
			metrics.RecordControlOperation(r.Context(), "stop", apply.Database, apply.Environment, "error")
			s.writeControlError(w, "stop", apply, syncErr)
			return
		}
	}
	if resp.Accepted {
		s.logControlOperation(r, apply.ApplyIdentifier, req.Caller, storage.LogEventStopRequested, "Stop requested by user")
	}

	s.writeJSON(w, http.StatusOK, &apitypes.StopResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
		StoppedCount: resp.StoppedCount,
		SkippedCount: resp.SkippedCount,
	})
}

func (s *Service) syncRemoteStopState(ctx context.Context, client tern.Client, apply *storage.Apply) error {
	tasks, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("get tasks for stopped apply %s: %w", apply.ApplyIdentifier, err)
	}
	remoteProgressByTask := s.remoteStopProgressByTask(ctx, client, apply)

	// The remote data plane has already accepted the stop. Mirror that state
	// into control-plane storage so a follow-up start can be queued immediately
	// without waiting for the scheduler progress poller to observe it.
	now := time.Now()
	taskState := state.Task.Stopped
	applyState := state.Apply.Stopped
	completedAt := (*time.Time)(nil)
	if apply.DatabaseType == storage.DatabaseTypeVitess {
		taskState = state.Task.Cancelled
		applyState = state.Apply.Cancelled
		completedAt = &now
	}
	for _, task := range tasks {
		nextTaskState := taskState
		nextCompletedAt := completedAt
		remoteProgress, hasRemoteProgress := remoteProgressByTask[progressTableKey(task.Namespace, task.TableName)]
		if hasRemoteProgress {
			remoteTaskState := state.NormalizeTaskStatus(remoteProgress.Status)
			if remoteTaskState != "" {
				nextTaskState = remoteTaskState
				if state.IsTerminalTaskState(remoteTaskState) {
					nextCompletedAt = &now
				} else {
					nextCompletedAt = nil
				}
			}
			task.RowsCopied = remoteProgress.RowsCopied
			task.RowsTotal = remoteProgress.RowsTotal
			task.ProgressPercent = int(remoteProgress.PercentComplete)
			task.ETASeconds = int(remoteProgress.EtaSeconds)
		}
		if state.IsTerminalTaskState(task.State) && (!hasRemoteProgress || !state.IsState(nextTaskState, state.Task.Stopped)) {
			continue
		}
		task.State = nextTaskState
		task.ErrorMessage = ""
		task.CompletedAt = nextCompletedAt
		task.UpdatedAt = now
		if err := s.storage.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("mark task %s %s after remote stop: %w", task.TaskIdentifier, nextTaskState, err)
		}
	}
	apply.State = applyState
	apply.ErrorMessage = ""
	apply.CompletedAt = completedAt
	apply.UpdatedAt = now
	if err := s.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("mark apply %s %s after remote stop: %w", apply.ApplyIdentifier, applyState, err)
	}
	return nil
}

func (s *Service) remoteStopProgressByTask(ctx context.Context, client tern.Client, apply *storage.Apply) map[string]*ternv1.TableProgress {
	if client == nil || !client.IsRemote() || apply == nil || apply.ExternalID == "" || apply.DatabaseType != storage.DatabaseTypeMySQL {
		return nil
	}
	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ExternalID,
		Database:    apply.Database,
		Environment: apply.Environment,
	})
	if err != nil {
		s.logger.Warn("remote stop progress refresh failed, using local task snapshot",
			"apply_id", apply.ApplyIdentifier,
			"external_apply_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		return nil
	}
	if resp == nil {
		s.logger.Warn("remote stop progress refresh returned empty response, using local task snapshot",
			"apply_id", apply.ApplyIdentifier,
			"external_apply_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment)
		return nil
	}
	progressByTask := make(map[string]*ternv1.TableProgress, len(resp.Tables))
	for _, tableProgress := range resp.Tables {
		progressByTask[progressTableKey(tableProgress.Namespace, tableProgress.TableName)] = tableProgress
	}
	return progressByTask
}

// StartRequest is the HTTP request body for POST /api/start.
type StartRequest struct {
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller,omitempty"`
}

// handleStart handles POST /api/start requests.
func (s *Service) handleStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	var resp *ternv1.StartResponse
	var err error
	queuedForScheduler := false
	switch {
	case state.IsState(apply.State, state.Apply.WaitingForDeploy):
		resp, err = client.Start(r.Context(), &ternv1.StartRequest{
			ApplyId:     applyID,
			Database:    apply.Database,
			Environment: apply.Environment,
		})
	case state.IsState(apply.State, state.Apply.Stopped):
		resp, err = s.queueStoppedApplyForScheduler(r.Context(), apply)
		queuedForScheduler = err == nil && resp.Accepted
	case apply.GetOptions().DeferDeploy:
		err = fmt.Errorf("schema change is not ready for deploy (current state: %s)", apply.State)
	default:
		err = fmt.Errorf("schema change is not stopped (current state: %s)", apply.State)
	}
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "start", apply, err)
		return
	}

	metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Environment, controlStatus(resp.Accepted))
	if resp.Accepted {
		s.logControlOperation(r, apply.ApplyIdentifier, req.Caller, storage.LogEventStartRequested, "Start requested by user")
		if queuedForScheduler {
			s.wakeScheduler(apply.ApplyIdentifier, apply.Database, apply.Environment)
		}
	}

	httpResp := &apitypes.StartResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
		StartedCount: resp.StartedCount,
		SkippedCount: resp.SkippedCount,
	}

	s.writeJSON(w, http.StatusOK, httpResp)
}

func (s *Service) queueStoppedApplyForScheduler(ctx context.Context, apply *storage.Apply) (*ternv1.StartResponse, error) {
	if !state.IsState(apply.State, state.Apply.Stopped) {
		return nil, fmt.Errorf("schema change is not stopped (current state: %s)", apply.State)
	}
	tasks, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("get tasks for apply %s: %w", apply.ApplyIdentifier, err)
	}
	var startedCount int64
	var skippedCount int64
	for _, task := range tasks {
		switch {
		case state.IsState(task.State, state.Task.Stopped):
			startedCount++
		case state.IsTerminalTaskState(task.State):
			skippedCount++
		}
	}
	if startedCount == 0 {
		return nil, fmt.Errorf("no stopped tasks for apply %s", apply.ApplyIdentifier)
	}
	apply.State = state.Apply.Pending
	apply.CompletedAt = nil
	now := time.Now()
	if apply.StartedAt == nil {
		apply.StartedAt = &now
	}
	apply.UpdatedAt = now
	if err := s.storage.Applies().Update(ctx, apply); err != nil {
		return nil, fmt.Errorf("queue apply %s for scheduler start: %w", apply.ApplyIdentifier, err)
	}
	return &ternv1.StartResponse{
		Accepted:     true,
		StartedCount: startedCount,
		SkippedCount: skippedCount,
	}, nil
}

// VolumeRequest is the HTTP request body for POST /api/volume.
type VolumeRequest struct {
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Volume      int32  `json:"volume"` // 1-11 (1=conservative, 11=aggressive)
}

// handleVolume handles POST /api/volume requests.
func (s *Service) handleVolume(w http.ResponseWriter, r *http.Request) {
	var req VolumeRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	if req.Volume < 1 || req.Volume > 11 {
		s.writeError(w, http.StatusBadRequest, "volume must be between 1 and 11")
		return
	}

	resp, err := client.Volume(r.Context(), &ternv1.VolumeRequest{
		ApplyId:     applyID,
		Database:    apply.Database,
		Environment: apply.Environment,
		Volume:      req.Volume,
	})
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "volume", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "volume", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "volume", apply.Database, apply.Environment, controlStatus(resp.Accepted))

	s.writeJSON(w, http.StatusOK, &apitypes.VolumeResponse{
		Accepted:       resp.Accepted,
		ErrorMessage:   resp.ErrorMessage,
		PreviousVolume: resp.PreviousVolume,
		NewVolume:      resp.NewVolume,
	})
}

// RevertRequest is the HTTP request body for POST /api/revert.
type RevertRequest struct {
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller,omitempty"`
}

// handleRevert handles POST /api/revert requests.
func (s *Service) handleRevert(w http.ResponseWriter, r *http.Request) {
	var req RevertRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.Revert(r.Context(), &ternv1.RevertRequest{
		ApplyId:  applyID,
		Database: apply.Database,
	})
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "revert", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "revert", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "revert", apply.Database, apply.Environment, controlStatus(resp.Accepted))
	if resp.Accepted {
		s.logControlOperation(r, apply.ApplyIdentifier, req.Caller, storage.LogEventRevertTriggered, "Revert triggered by user")
	}

	s.writeJSON(w, http.StatusOK, &apitypes.ControlResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
	})
}

// SkipRevertRequest is the HTTP request body for POST /api/skip-revert.
type SkipRevertRequest struct {
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller,omitempty"`
}

// handleSkipRevert handles POST /api/skip-revert requests.
func (s *Service) handleSkipRevert(w http.ResponseWriter, r *http.Request) {
	var req SkipRevertRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.SkipRevert(r.Context(), &ternv1.SkipRevertRequest{
		ApplyId:  applyID,
		Database: apply.Database,
	})
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "skip_revert", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "skip-revert", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "skip_revert", apply.Database, apply.Environment, controlStatus(resp.Accepted))

	// Record skip-revert on VitessApplyData for progress visibility
	if resp.Accepted && apply.Engine == storage.EnginePlanetScale {
		vitessDataStore := s.storage.VitessApplyData()
		if vitessDataStore == nil {
			s.logger.Error("vitess apply data store not available after skip-revert", "apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier)
		} else {
			vad, err := vitessDataStore.GetByApplyID(r.Context(), apply.ID)
			switch {
			case err != nil:
				s.logger.Error("failed to load vitess apply data after skip-revert", "apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier, "error", err)
			case vad == nil:
				s.logger.Warn("vitess apply data missing after skip-revert", "apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier)
			default:
				now := time.Now()
				vad.RevertSkippedAt = &now
				if err := vitessDataStore.Save(r.Context(), vad); err != nil {
					s.logger.Error("failed to save vitess apply data after skip-revert", "apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier, "error", err)
				}
			}
		}
	}
	if resp.Accepted {
		s.logControlOperation(r, apply.ApplyIdentifier, req.Caller, storage.LogEventSkipRevertTriggered, "Skip-revert triggered by user")
	}

	s.writeJSON(w, http.StatusOK, &apitypes.ControlResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
	})
}

// RollbackPlanRequest is the HTTP request body for POST /api/rollback/plan.
type RollbackPlanRequest struct {
	ApplyID string `json:"apply_id"`
}

// handleRollbackPlan handles POST /api/rollback/plan requests.
// Looks up the specified apply to determine database/environment, then generates
// a plan to revert to the schema state before that apply.
func (s *Service) handleRollbackPlan(w http.ResponseWriter, r *http.Request) {
	var req RollbackPlanRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.ApplyID == "" {
		s.writeError(w, http.StatusBadRequest, "apply_id is required")
		return
	}

	// Look up the apply to get database/environment
	apply, err := s.storage.Applies().GetByApplyIdentifier(r.Context(), req.ApplyID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to look up apply: "+err.Error())
		return
	}
	if apply == nil {
		s.writeError(w, http.StatusNotFound, "apply not found: "+req.ApplyID)
		return
	}

	resp, err := s.ExecuteRollbackPlan(r.Context(), apply.Database, apply.Environment, apply.Deployment)
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "rollback_plan", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "rollback plan", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "rollback_plan", apply.Database, apply.Environment, "success")

	// Include database metadata so the caller doesn't need to look it up separately
	resp.Database = apply.Database
	resp.DatabaseType = apply.DatabaseType
	resp.Environment = apply.Environment

	s.writeJSON(w, http.StatusOK, resp)
}
