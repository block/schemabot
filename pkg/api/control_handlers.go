package api

import (
	"encoding/json"
	"errors"
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

type controlOperationHTTPError struct {
	status int
	err    error
}

func (e *controlOperationHTTPError) Error() string {
	return e.err.Error()
}

func (e *controlOperationHTTPError) Unwrap() error {
	return e.err
}

// controlConflictf marks an operator request as rejected rather than failed.
// Storage, Tern, and scheduler errors still fall back to 500s.
func controlConflictf(format string, args ...any) error {
	return &controlOperationHTTPError{
		status: http.StatusConflict,
		err:    fmt.Errorf(format, args...),
	}
}

func controlOperationHTTPStatus(err error) int {
	var httpErr *controlOperationHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.status
	}
	return http.StatusInternalServerError
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
	status := controlOperationHTTPStatus(err)
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
	if status >= http.StatusInternalServerError {
		s.logger.Error(opName+" failed", attrs...)
		s.writeError(w, status, opName+" failed: "+err.Error())
	} else {
		s.logger.Warn(opName+" rejected", attrs...)
		s.writeError(w, status, opName+" rejected: "+err.Error())
	}
}

// decodeControlRequest decodes a control request (stop/start/cutover/volume),
// loads the apply record, and returns a Tern client using the deployment stored
// on that apply. Control operations are scoped by apply_id + environment; the
// database is derived from storage so callers cannot target a different
// database than the one originally planned.
func (s *Service) decodeControlRequest(w http.ResponseWriter, r *http.Request, dest any,
	applyID, environment *string) (tern.Client, *storage.Apply, string, bool) {

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
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Caller      string `json:"caller,omitempty"`
}

// handleCutover handles POST /api/cutover requests.
func (s *Service) handleCutover(w http.ResponseWriter, r *http.Request) {
	var req CutoverRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	resp, err := client.Cutover(r.Context(), &ternv1.CutoverRequest{
		ApplyId:     applyID,
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
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Caller      string `json:"caller,omitempty"`
}

// handleStop handles POST /api/stop requests.
// Stops all non-terminal tasks for the database.
func (s *Service) handleStop(w http.ResponseWriter, r *http.Request) {
	var req StopRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	resp, err := client.Stop(r.Context(), &ternv1.StopRequest{
		ApplyId:     applyID,
		Environment: apply.Environment,
	})
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "stop", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "stop", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "stop", apply.Database, apply.Environment, controlStatus(resp.Accepted))
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

// StartRequest is the HTTP request body for POST /api/start.
type StartRequest struct {
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Caller      string `json:"caller,omitempty"`
}

// handleStart handles POST /api/start requests.
func (s *Service) handleStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	if err := validateStartRequestState(apply); err != nil {
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Environment, "rejected")
		s.writeControlError(w, "start", apply, err)
		return
	}

	resp, err := client.Start(r.Context(), &ternv1.StartRequest{
		ApplyId:     applyID,
		Environment: apply.Environment,
	})
	if err != nil {
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Environment, "error")
		s.writeControlError(w, "start", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Environment, controlStatus(resp.Accepted))
	if resp.Accepted {
		s.logControlOperation(r, apply.ApplyIdentifier, req.Caller, storage.LogEventStartRequested, "Start requested by user")
	}

	// For remote (gRPC) clients, update local apply state and restart the
	// background progress poller. Without this, the local applies.state stays
	// "stopped" permanently because the old poller exited when it saw the
	// terminal state.
	var syncErr string
	if resp.Accepted && client.IsRemote() {
		// Mark running before ResumeApply so it doesn't re-issue a Start RPC.
		apply.State = state.Apply.Running
		if resumeErr := client.ResumeApply(r.Context(), apply); resumeErr != nil {
			s.logger.Error("failed to resume apply tracking after start", "apply_id", apply.ApplyIdentifier, "error", resumeErr)
			syncErr = "schema change was started successfully, but the status and progress endpoints may show stale state until the next recovery cycle"
		}
	}

	httpResp := &apitypes.StartResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
		StartedCount: resp.StartedCount,
		SkippedCount: resp.SkippedCount,
	}
	if syncErr != "" {
		httpResp.ErrorCode = apitypes.ErrCodeStateSyncFailed
		httpResp.ErrorMessage = syncErr
	}

	s.writeJSON(w, http.StatusOK, httpResp)
}

func validateStartRequestState(apply *storage.Apply) error {
	if apply.GetOptions().DeferDeploy && !state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return controlConflictf("schema change is not ready for deploy (current state: %s)", apply.State)
	}
	if !isStartRequestAllowedState(apply.State) {
		return startNotAllowedForState(apply)
	}
	return nil
}

// isStartRequestAllowedState is an allowlist for states where /start has a
// concrete action. New apply states must opt in here before they can reach Tern.
func isStartRequestAllowedState(applyState string) bool {
	return state.IsState(
		applyState,
		state.Apply.WaitingForDeploy,
		state.Apply.Running,
		state.Apply.Stopped,
	)
}

func startNotAllowedForState(apply *storage.Apply) error {
	switch {
	case state.IsState(apply.State, state.Apply.Pending):
		return controlConflictf("schema change is pending and no start request is queued")
	case state.IsState(apply.State, state.Apply.WaitingForCutover):
		return controlConflictf("schema change is waiting for cutover; use cutover instead of start")
	case state.IsState(apply.State, state.Apply.CuttingOver):
		return controlConflictf("schema change is cutting over; start is not allowed")
	case state.IsState(apply.State, state.Apply.RevertWindow):
		return controlConflictf("schema change is in revert window; use revert or skip-revert instead of start")
	case state.IsState(apply.State, state.Apply.FailedRetryable):
		return controlConflictf("schema change is waiting for scheduler retry; start is not allowed")
	case state.IsState(apply.State, state.Apply.Failed):
		return controlConflictf("schema change failed and cannot be started")
	case state.IsState(apply.State, state.Apply.Completed):
		return controlConflictf("schema change already completed and cannot be started")
	case state.IsState(apply.State, state.Apply.Cancelled):
		return controlConflictf("schema change was cancelled and cannot be started")
	case state.IsState(apply.State, state.Apply.Reverted):
		return controlConflictf("schema change was reverted and cannot be started")
	case state.IsState(apply.State,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
	):
		return controlConflictf("schema change is in setup state %s; start is not allowed", state.NormalizeState(apply.State))
	default:
		return controlConflictf("schema change is not stopped (current state: %s)", apply.State)
	}
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
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	if req.Volume < 1 || req.Volume > 11 {
		s.writeError(w, http.StatusBadRequest, "volume must be between 1 and 11")
		return
	}

	resp, err := client.Volume(r.Context(), &ternv1.VolumeRequest{
		ApplyId:     applyID,
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
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Caller      string `json:"caller,omitempty"`
}

// handleRevert handles POST /api/revert requests.
func (s *Service) handleRevert(w http.ResponseWriter, r *http.Request) {
	var req RevertRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	resp, err := client.Revert(r.Context(), &ternv1.RevertRequest{
		ApplyId:     applyID,
		Environment: apply.Environment,
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
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Caller      string `json:"caller,omitempty"`
}

// handleSkipRevert handles POST /api/skip-revert requests.
func (s *Service) handleSkipRevert(w http.ResponseWriter, r *http.Request) {
	var req SkipRevertRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	resp, err := client.SkipRevert(r.Context(), &ternv1.SkipRevertRequest{
		ApplyId:     applyID,
		Environment: apply.Environment,
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
