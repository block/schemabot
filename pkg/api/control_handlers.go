package api

import (
	"context"
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
// Storage, Tern, and operator errors still fall back to 500s.
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

func controlHTTPErrorf(status int, format string, args ...any) error {
	return &controlOperationHTTPError{
		status: status,
		err:    fmt.Errorf(format, args...),
	}
}

func controlOperationCaller(caller string) string {
	if caller == "" {
		return "unknown"
	}
	return caller
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
	s.logControlOperationForApply(r.Context(), apply, caller, eventType, message)
}

func (s *Service) logControlOperationForApply(ctx context.Context, apply *storage.Apply, caller, eventType, message string) {
	logStore := s.storage.ApplyLogs()
	if logStore == nil {
		s.logger.Error("apply log store not available for control operation log", "apply_id", apply.ApplyIdentifier, "event", eventType)
		return
	}
	logMessage := fmt.Sprintf("%s (caller: %s)", message, controlOperationCaller(caller))
	if err := logStore.Append(ctx, &storage.ApplyLog{
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
			"deployment", apply.Deployment,
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

func (s *Service) rejectControlIfStopPending(ctx context.Context, opName string, apply *storage.Apply) error {
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return fmt.Errorf("load pending stop control request for apply %s before %s: %w", apply.ApplyIdentifier, opName, err)
	}
	if controlReq == nil {
		return nil
	}
	s.logControlOperationForApply(ctx, apply, controlReq.RequestedBy, storage.LogEventStopRequested,
		fmt.Sprintf("Pending stop request blocked %s", opName))
	return controlConflictf("schema change has a pending stop request; %s is blocked until stop is processed", opName)
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

func (s *Service) controlTarget(ctx context.Context, operation, applyIdentifier, environment string) (tern.Client, *storage.Apply, string, error) {
	if applyIdentifier == "" {
		return nil, nil, "", controlHTTPErrorf(http.StatusBadRequest, "apply_id is required")
	}
	if environment == "" {
		return nil, nil, "", controlHTTPErrorf(http.StatusBadRequest, "environment is required")
	}
	if s.storage == nil {
		s.logger.Error("storage not available for control request", "operation", operation, "apply_id", applyIdentifier, "environment", environment)
		return nil, nil, "", fmt.Errorf("storage is not available")
	}
	applyStore := s.storage.Applies()
	if applyStore == nil {
		s.logger.Error("apply store not available for control request", "operation", operation, "apply_id", applyIdentifier, "environment", environment)
		return nil, nil, "", fmt.Errorf("apply store is not available")
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, applyIdentifier)
	if err != nil {
		s.logger.Error("failed to load apply for control request", "operation", operation, "apply_id", applyIdentifier, "environment", environment, "error", err)
		return nil, nil, "", fmt.Errorf("failed to look up apply %q: %w", applyIdentifier, err)
	}
	if apply == nil {
		return nil, nil, "", controlHTTPErrorf(http.StatusNotFound, "apply not found: %s", applyIdentifier)
	}
	if apply.Environment != environment {
		return nil, apply, "", controlHTTPErrorf(http.StatusBadRequest,
			"apply %q belongs to environment %q, not %q", applyIdentifier, apply.Environment, environment)
	}
	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		s.logger.Error("control request apply is missing stored deployment metadata",
			"operation", operation,
			"apply_id", applyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"environment", apply.Environment,
			"error", err)
		return nil, apply, "", err
	}
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("failed to create Tern client",
			"operation", operation,
			"deployment", deployment,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		return nil, apply, "", controlHTTPErrorf(http.StatusNotFound, "%s", err.Error())
	}
	return client, apply, ternApplyIDForStoredApply(apply), nil
}

// ternApplyIDForStoredApply returns the identifier that a Tern RPC expects for
// an apply-scoped control or progress request.
//
// HTTP callers use SchemaBot's apply_identifier. In remote gRPC mode,
// SchemaBot queues work locally first; after the operator dispatches it, Tern
// returns its own apply ID and SchemaBot stores that value as external_id.
// Subsequent RPCs to remote Tern must use external_id. In local mode, the API
// layer and LocalClient share storage, so external_id stays empty and the
// apply_identifier is already the Tern-facing ID.
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
	client, apply, ternApplyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	if err := s.rejectControlIfStopPending(r.Context(), "cutover", apply); err != nil {
		status := "error"
		if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
			status = "rejected"
		}
		metrics.RecordControlOperation(r.Context(), "cutover", apply.Database, apply.Deployment, apply.Environment, status)
		s.writeControlError(w, "cutover", apply, err)
		return
	}

	resp, httpStatus, err := s.executeCutoverForApply(r.Context(), client, apply, ternApplyID, req.Caller)
	if err != nil {
		s.writeControlError(w, "cutover", apply, err)
		return
	}
	s.writeJSON(w, httpStatus, resp)
}

// ExecuteCutover records durable cutover intent for an apply. The operator
// owner is responsible for completing cutover against the data plane.
func (s *Service) ExecuteCutover(ctx context.Context, req apitypes.ControlRequest) (*apitypes.ControlResponse, error) {
	client, apply, ternApplyID, err := s.controlTarget(ctx, "cutover", req.ApplyID, req.Environment)
	if err != nil {
		return nil, err
	}
	if err := s.rejectControlIfStopPending(ctx, "cutover", apply); err != nil {
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "rejected")
		return nil, err
	}
	resp, _, err := s.executeCutoverForApply(ctx, client, apply, ternApplyID, req.Caller)
	return resp, err
}

func (s *Service) executeCutoverForApply(ctx context.Context, client tern.Client, apply *storage.Apply, ternApplyID, caller string) (*apitypes.ControlResponse, int, error) {
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		err := fmt.Errorf("control request store is not available")
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "error")
		return nil, http.StatusOK, err
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationCutover)
	if err != nil {
		err := fmt.Errorf("load pending cutover control request for apply %s: %w", apply.ApplyIdentifier, err)
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "error")
		return nil, http.StatusOK, err
	}
	if controlReq != nil {
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "success")
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventCutoverTriggered,
			"Cutover requested by user while cutover request already pending")
		s.logger.Info("cutover request already pending",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller,
			"original_requested_by", controlReq.RequestedBy)
		s.wakeOperator(apply.ApplyIdentifier, apply.Database, apply.Environment)
		return &apitypes.ControlResponse{Accepted: true}, http.StatusAccepted, nil
	}
	readiness, err := s.cutoverRequestReadiness(ctx, client, apply, ternApplyID)
	if err != nil {
		err := fmt.Errorf("check cutover readiness for apply %s: %w", apply.ApplyIdentifier, err)
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "error")
		return nil, http.StatusOK, err
	}
	if readiness == cutoverRequestAlreadyInProgress {
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "success")
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventCutoverTriggered,
			"Cutover requested by user while cutover already in progress")
		s.logger.Info("cutover request accepted because cutover is already in progress",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller,
			"state", apply.State)
		return &apitypes.ControlResponse{Accepted: true, Status: apitypes.ControlStatusAlreadyInProgress}, http.StatusAccepted, nil
	}
	if readiness == cutoverRequestRecovering {
		s.logger.Info("cutover request rejected while apply is recovering",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller,
			"state", apply.State)
		err := controlConflictf("schema change is recovering after restart; cutover will be available once recovery completes")
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "rejected")
		return nil, http.StatusOK, err
	}
	if readiness == cutoverRequestNotReady {
		err := controlConflictf("schema change is not waiting for cutover (current state: %s)", apply.State)
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "rejected")
		return nil, http.StatusOK, err
	}
	_, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: caller,
	})
	if err != nil {
		err := fmt.Errorf("record cutover control request for apply %s: %w", apply.ApplyIdentifier, err)
		metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "error")
		return nil, http.StatusOK, err
	}
	metrics.RecordControlOperation(ctx, "cutover", apply.Database, apply.Deployment, apply.Environment, "success")
	message := "Cutover requested by user"
	if alreadyPending {
		message = "Cutover requested by user while cutover request already pending"
	}
	s.logControlOperationForApply(ctx, apply, caller, storage.LogEventCutoverTriggered, message)
	s.logger.Info("cutover request queued for operator",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"deployment", apply.Deployment,
		"environment", apply.Environment,
		"requested_by", caller)
	s.wakeOperator(apply.ApplyIdentifier, apply.Database, apply.Environment)
	if alreadyPending {
		return &apitypes.ControlResponse{Accepted: true}, http.StatusAccepted, nil
	}
	return &apitypes.ControlResponse{Accepted: true}, http.StatusOK, nil
}

type cutoverRequestReadiness int

const (
	cutoverRequestNotReady cutoverRequestReadiness = iota
	cutoverRequestReady
	cutoverRequestAlreadyInProgress
	cutoverRequestRecovering
)

func (s *Service) cutoverRequestReadiness(ctx context.Context, client tern.Client, apply *storage.Apply, ternApplyID string) (cutoverRequestReadiness, error) {
	if state.IsState(apply.State, state.Apply.WaitingForCutover) {
		return cutoverRequestReady, nil
	}
	if state.IsState(apply.State, state.Apply.CuttingOver) {
		return cutoverRequestAlreadyInProgress, nil
	}
	if state.IsState(apply.State, state.Apply.Recovering) {
		return cutoverRequestRecovering, nil
	}
	if client != nil && client.IsRemote() && ternApplyID != "" {
		progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
			ApplyId:     ternApplyID,
			Environment: apply.Environment,
		})
		if err != nil {
			return cutoverRequestNotReady, fmt.Errorf("check remote apply %s before cutover: %w", apply.ApplyIdentifier, err)
		}
		remoteState := tern.ProtoStateToStorage(progress.State)
		if state.IsState(remoteState, state.Apply.WaitingForCutover) {
			return cutoverRequestReady, nil
		}
		if state.IsState(remoteState, state.Apply.Recovering) {
			return cutoverRequestRecovering, nil
		}
		if state.IsState(remoteState, state.Apply.CuttingOver) {
			return cutoverRequestReady, nil
		}
		return cutoverRequestNotReady, nil
	}
	if !state.IsState(apply.State, state.Apply.Running) {
		return cutoverRequestNotReady, nil
	}
	taskStore := s.storage.Tasks()
	if taskStore == nil {
		return cutoverRequestNotReady, fmt.Errorf("task store is not available")
	}
	tasks, err := taskStore.GetByApplyID(ctx, apply.ID)
	if err != nil {
		return cutoverRequestNotReady, fmt.Errorf("load tasks for apply %s before cutover: %w", apply.ApplyIdentifier, err)
	}
	readyForCutover := false
	for _, task := range tasks {
		if state.IsState(task.State, state.Task.CuttingOver) {
			return cutoverRequestAlreadyInProgress, nil
		}
		if state.IsState(task.State, state.Task.WaitingForCutover) {
			readyForCutover = true
		}
	}
	if readyForCutover {
		return cutoverRequestReady, nil
	}
	return cutoverRequestNotReady, nil
}

// StopRequest is the HTTP request body for POST /api/stop.
type StopRequest struct {
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Caller      string `json:"caller,omitempty"`
}

type stopControlRequestMetadata struct {
	StoppedCount int64 `json:"stopped_count,omitempty"`
	SkippedCount int64 `json:"skipped_count,omitempty"`
}

const stopResponseStatusAlreadyRequested = "already_requested"

// handleStop handles POST /api/stop requests.
// Records durable stop intent for the apply owner to process.
func (s *Service) handleStop(w http.ResponseWriter, r *http.Request) {
	var req StopRequest
	client, apply, ternApplyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}
	stopResp, httpStatus, err := s.executeStopForApply(r.Context(), client, apply, ternApplyID, req.Environment, req.Caller)
	if err != nil {
		s.writeControlError(w, "stop", apply, err)
		return
	}
	s.writeJSON(w, httpStatus, stopResp)
}

// ExecuteStop records durable stop intent for an apply. The operator owner is
// responsible for completing the stop if the immediate local attempt cannot.
func (s *Service) ExecuteStop(ctx context.Context, req apitypes.ControlRequest) (*apitypes.StopResponse, error) {
	client, apply, ternApplyID, err := s.controlTarget(ctx, "stop", req.ApplyID, req.Environment)
	if err != nil {
		return nil, err
	}
	resp, _, err := s.executeStopForApply(ctx, client, apply, ternApplyID, req.Environment, req.Caller)
	return resp, err
}

func (s *Service) executeStopForApply(ctx context.Context, client tern.Client, apply *storage.Apply, ternApplyID, environment, caller string) (*apitypes.StopResponse, int, error) {
	resp, responseStatus, err := s.queueStopForApplyOwner(ctx, apply, caller)
	if err != nil {
		status := "error"
		if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
			status = "rejected"
		}
		metrics.RecordControlOperation(ctx, "stop", apply.Database, apply.Deployment, apply.Environment, status)
		return nil, http.StatusOK, err
	}
	metrics.RecordControlOperation(ctx, "stop", apply.Database, apply.Deployment, apply.Environment, controlStatus(resp.Accepted))
	if resp.Accepted {
		logMessage := "Stop requested by user"
		if responseStatus == stopResponseStatusAlreadyRequested {
			logMessage = "Stop requested by user while stop request already pending"
		}
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested, logMessage)
		if responseStatus == stopResponseStatusAlreadyRequested {
			s.logger.Info("immediate stop skipped because stop request is already pending",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"deployment", apply.Deployment,
				"environment", apply.Environment,
				"requested_by", caller)
		} else {
			s.tryImmediateStopAfterQueue(ctx, client, apply, ternApplyID, environment, caller)
		}
		s.wakeOperator(apply.ApplyIdentifier, apply.Database, apply.Environment)
	}

	httpStatus := http.StatusOK
	if responseStatus == stopResponseStatusAlreadyRequested {
		httpStatus = http.StatusAccepted
	}
	return &apitypes.StopResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
		StoppedCount: resp.StoppedCount,
		SkippedCount: resp.SkippedCount,
		Status:       responseStatus,
	}, httpStatus, nil
}

func (s *Service) tryImmediateStopAfterQueue(ctx context.Context, client tern.Client, apply *storage.Apply, ternApplyID, environment, caller string) {
	if client == nil {
		s.logger.Warn("immediate stop not attempted because Tern client is unavailable; durable stop request remains pending for apply owner retry",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller)
		return
	}
	if client.IsRemote() {
		s.logger.Info("immediate stop skipped for remote Tern client; durable stop request remains pending for operator-owned reconciliation",
			"apply_id", apply.ApplyIdentifier,
			"tern_apply_id", ternApplyID,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller)
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested,
			"Immediate stop skipped for remote Tern client; durable operator owner will reconcile stop state")
		return
	}
	resp, err := client.Stop(ctx, &ternv1.StopRequest{
		ApplyId:     ternApplyID,
		Environment: environment,
	})
	if err != nil {
		s.logger.Warn("immediate stop failed; durable stop request remains pending for apply owner retry",
			"apply_id", apply.ApplyIdentifier,
			"tern_apply_id", ternApplyID,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller,
			"error", err)
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested,
			fmt.Sprintf("Immediate stop attempt failed; durable stop request remains pending: %v", err))
		return
	}
	if resp == nil {
		s.logger.Warn("immediate stop returned nil response; durable stop request remains pending for apply owner retry",
			"apply_id", apply.ApplyIdentifier,
			"tern_apply_id", ternApplyID,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller)
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested,
			"Immediate stop attempt returned no response; durable stop request remains pending")
		return
	}
	if !resp.Accepted {
		s.logger.Warn("immediate stop was not accepted; durable stop request remains pending for apply owner retry",
			"apply_id", apply.ApplyIdentifier,
			"tern_apply_id", ternApplyID,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller,
			"error_message", resp.ErrorMessage,
			"stopped_count", resp.StoppedCount,
			"skipped_count", resp.SkippedCount)
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested,
			fmt.Sprintf("Immediate stop attempt was not accepted; durable stop request remains pending: %s", resp.ErrorMessage))
		return
	}
	stopCompleted, err := s.completeImmediateStopRequestIfStopped(ctx, apply, caller)
	if err != nil {
		s.logger.Warn("immediate stop accepted but durable stop request completion failed; durable stop request remains pending for apply owner retry",
			"apply_id", apply.ApplyIdentifier,
			"tern_apply_id", ternApplyID,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller,
			"stopped_count", resp.StoppedCount,
			"skipped_count", resp.SkippedCount,
			"error", err)
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested,
			fmt.Sprintf("Immediate stop accepted but durable stop request completion failed; durable stop request remains pending: %v", err))
		return
	}
	if !stopCompleted {
		s.logger.Info("immediate stop accepted; durable apply owner will reconcile final stop state",
			"apply_id", apply.ApplyIdentifier,
			"tern_apply_id", ternApplyID,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"requested_by", caller,
			"stopped_count", resp.StoppedCount,
			"skipped_count", resp.SkippedCount)
		s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested,
			"Immediate stop accepted; durable apply owner will reconcile final stop state")
		return
	}
	s.logger.Info("immediate stop accepted and durable stop request completed",
		"apply_id", apply.ApplyIdentifier,
		"tern_apply_id", ternApplyID,
		"database", apply.Database,
		"deployment", apply.Deployment,
		"environment", apply.Environment,
		"requested_by", caller,
		"stopped_count", resp.StoppedCount,
		"skipped_count", resp.SkippedCount)
	s.logControlOperationForApply(ctx, apply, caller, storage.LogEventStopRequested,
		"Immediate stop accepted and durable stop request completed")
}

func (s *Service) completeImmediateStopRequestIfStopped(ctx context.Context, apply *storage.Apply, caller string) (bool, error) {
	storedApply, err := s.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		return false, fmt.Errorf("load apply %s after immediate stop: %w", apply.ApplyIdentifier, err)
	}
	if storedApply == nil {
		return false, fmt.Errorf("load apply %s after immediate stop: %w", apply.ApplyIdentifier, storage.ErrApplyNotFound)
	}
	if !stopRequestCompletedByApplyState(storedApply.State) {
		s.logger.Info("immediate stop accepted but stored apply is not stopped; durable stop request remains pending for apply owner retry",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", caller,
			"state", storedApply.State)
		return false, nil
	}
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		return false, fmt.Errorf("control request store is not available")
	}
	if err := controlStore.CompletePending(ctx, apply.ID, storage.ControlOperationStop); err != nil {
		return false, fmt.Errorf("complete pending stop control request for apply %s after immediate stop: %w", apply.ApplyIdentifier, err)
	}
	return true, nil
}

func stopRequestCompletedByApplyState(applyState string) bool {
	return state.IsState(applyState, state.Apply.Stopped, state.Apply.Cancelled) || state.IsTerminalApplyState(applyState)
}

func (s *Service) queueStopForApplyOwner(ctx context.Context, apply *storage.Apply, caller string) (*ternv1.StopResponse, string, error) {
	if resp, responseStatus, found, err := s.pendingStopResponseIfPresent(ctx, apply); err != nil || found {
		return resp, responseStatus, err
	}
	if state.IsTerminalApplyState(apply.State) {
		return nil, "", controlConflictf("schema change is already terminal (current state: %s)", apply.State)
	}
	tasks, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return nil, "", fmt.Errorf("get tasks for apply %s: %w", apply.ApplyIdentifier, err)
	}
	stoppedCount, skippedCount := stopRequestCounts(tasks)
	if stoppedCount == 0 && skippedCount == 0 {
		return nil, "", controlConflictf("no active schema change for apply %s", apply.ApplyIdentifier)
	}
	controlReq, alreadyPending, err := s.createStopControlRequest(ctx, apply, caller, stoppedCount, skippedCount)
	if err != nil {
		return nil, "", err
	}
	if alreadyPending {
		resp, err := stopResponseFromControlRequest(controlReq)
		if err != nil {
			return nil, "", err
		}
		return resp, stopResponseStatusAlreadyRequested, nil
	}
	return &ternv1.StopResponse{
		Accepted:     true,
		StoppedCount: stoppedCount,
		SkippedCount: skippedCount,
	}, "", nil
}

func stopRequestCounts(tasks []*storage.Task) (int64, int64) {
	var stoppedCount int64
	var skippedCount int64
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			skippedCount++
			continue
		}
		stoppedCount++
	}
	return stoppedCount, skippedCount
}

func (s *Service) createStopControlRequest(ctx context.Context, apply *storage.Apply, caller string, stoppedCount, skippedCount int64) (*storage.ApplyControlRequest, bool, error) {
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		return nil, false, fmt.Errorf("control request store is not available")
	}
	metadata, err := json.Marshal(stopControlRequestMetadata{
		StoppedCount: stoppedCount,
		SkippedCount: skippedCount,
	})
	if err != nil {
		return nil, false, fmt.Errorf("marshal stop control request metadata for apply %s: %w", apply.ApplyIdentifier, err)
	}
	controlReq, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: caller,
		Metadata:    metadata,
	})
	if err != nil {
		return nil, false, fmt.Errorf("record stop control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return controlReq, alreadyPending, nil
}

func (s *Service) pendingStopResponseIfPresent(ctx context.Context, apply *storage.Apply) (*ternv1.StopResponse, string, bool, error) {
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		return nil, "", false, fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return nil, "", false, fmt.Errorf("load pending stop control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if controlReq == nil {
		return nil, "", false, nil
	}
	resp, err := stopResponseFromControlRequest(controlReq)
	if err != nil {
		return nil, "", false, err
	}
	return resp, stopResponseStatusAlreadyRequested, true, nil
}

func stopResponseFromControlRequest(controlReq *storage.ApplyControlRequest) (*ternv1.StopResponse, error) {
	resp := &ternv1.StopResponse{}
	if controlReq == nil {
		return resp, nil
	}
	var metadata stopControlRequestMetadata
	if len(controlReq.Metadata) > 0 {
		if err := json.Unmarshal(controlReq.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("decode stop control request metadata for request %d: %w", controlReq.ID, err)
		}
	}
	resp.StoppedCount = metadata.StoppedCount
	resp.SkippedCount = metadata.SkippedCount
	switch controlReq.Status {
	case storage.ControlRequestPending, storage.ControlRequestCompleted:
		resp.Accepted = true
	case storage.ControlRequestFailed:
		resp.ErrorMessage = controlReq.ErrorMessage
	default:
		resp.ErrorMessage = fmt.Sprintf("stop control request has unknown status: %s", controlReq.Status)
	}
	return resp, nil
}

// StartRequest is the HTTP request body for POST /api/start.
type StartRequest struct {
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Caller      string `json:"caller,omitempty"`
}

type startControlRequestMetadata struct {
	StartedCount int64 `json:"started_count,omitempty"`
	// SkippedCount preserves how many task rows were already terminal when the
	// start was accepted. Duplicate start calls use this metadata so clients see
	// the same "already complete" count as the original request.
	SkippedCount int64 `json:"skipped_count,omitempty"`
}

const (
	startResponseStatusQueued           = "queued"
	startResponseStatusAlreadyRequested = "already_requested"
)

// handleStart handles POST /api/start requests.
func (s *Service) handleStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	client, apply, applyID, ok := s.decodeControlRequest(w, r, &req, &req.ApplyID, &req.Environment)
	if !ok {
		return
	}

	if err := validateStartRequestState(apply); err != nil {
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Deployment, apply.Environment, "rejected")
		s.writeControlError(w, "start", apply, err)
		return
	}
	if err := s.completeResolvedStopBeforeStart(r.Context(), client, apply, req.Caller); err != nil {
		status := "error"
		if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
			status = "rejected"
		}
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Deployment, apply.Environment, status)
		s.writeControlError(w, "start", apply, err)
		return
	}
	if err := s.rejectControlIfStopPending(r.Context(), "start", apply); err != nil {
		status := "error"
		if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
			status = "rejected"
		}
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Deployment, apply.Environment, status)
		s.writeControlError(w, "start", apply, err)
		return
	}

	var resp *ternv1.StartResponse
	var err error
	responseStatus := ""
	queuedForOperator := false
	switch {
	case state.IsState(apply.State, state.Apply.WaitingForDeploy):
		if err := s.rejectControlIfStopPending(r.Context(), "start dispatch", apply); err != nil {
			status := "error"
			if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
				status = "rejected"
			}
			metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Deployment, apply.Environment, status)
			s.writeControlError(w, "start", apply, err)
			return
		}
		resp, err = client.Start(r.Context(), &ternv1.StartRequest{
			ApplyId:     applyID,
			Environment: apply.Environment,
		})
	case state.IsState(apply.State, state.Apply.Pending):
		resp, responseStatus, err = s.startResponseForPendingStartRequest(r.Context(), apply)
		queuedForOperator = err == nil && resp.Accepted
	case storedApplyMayHaveStoppedTasksForStart(apply.State):
		// A queued or operator-claimed start can leave the durable request
		// pending while the stored apply is stopped or running. Treat retries in
		// either state as idempotent duplicates instead of revalidating remote
		// state or recording another request.
		var foundPendingStart bool
		resp, responseStatus, foundPendingStart, err = s.pendingStartResponseIfPresent(r.Context(), apply)
		queuedForOperator = err == nil && foundPendingStart && resp.Accepted
		if err == nil && !foundPendingStart && client.IsRemote() && apply.ExternalID != "" {
			resp, responseStatus, err = s.queueRemoteStoppedApplyForOperator(r.Context(), client, apply, req.Caller)
		} else if err == nil && !foundPendingStart {
			resp, responseStatus, err = s.queueStoppedApplyForOperator(r.Context(), apply, req.Caller)
		}
		queuedForOperator = queuedForOperator || (err == nil && resp.Accepted)
	default:
		err = startNotAllowedForState(apply)
	}
	if err != nil {
		status := "error"
		if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
			status = "rejected"
		}
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Deployment, apply.Environment, status)
		s.writeControlError(w, "start", apply, err)
		return
	}

	metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Deployment, apply.Environment, controlStatus(resp.Accepted))
	if resp.Accepted {
		s.logControlOperation(r, apply.ApplyIdentifier, req.Caller, storage.LogEventStartRequested, "Start requested by user")
		if queuedForOperator {
			s.wakeOperator(apply.ApplyIdentifier, apply.Database, apply.Environment)
		}
	}

	httpResp := &apitypes.StartResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
		StartedCount: resp.StartedCount,
		SkippedCount: resp.SkippedCount,
		Status:       responseStatus,
	}

	httpStatus := http.StatusOK
	if responseStatus == startResponseStatusAlreadyRequested {
		httpStatus = http.StatusAccepted
	}
	s.writeJSON(w, httpStatus, httpResp)
}

func (s *Service) completeResolvedStopBeforeStart(ctx context.Context, client tern.Client, apply *storage.Apply, caller string) error {
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return fmt.Errorf("load pending stop control request for apply %s before start: %w", apply.ApplyIdentifier, err)
	}
	if controlReq == nil {
		return nil
	}
	stopCaller := controlReq.RequestedBy
	if stopCaller == "" {
		stopCaller = "unknown"
	}

	if stopRequestCompletedByApplyState(apply.State) {
		if err := controlStore.CompletePending(ctx, apply.ID, storage.ControlOperationStop); err != nil {
			return fmt.Errorf("complete pending stop control request for apply %s before start: %w", apply.ApplyIdentifier, err)
		}
		s.logger.Info("completed resolved stop request before start",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", stopCaller,
			"start_requested_by", caller,
			"state", apply.State)
		return nil
	}

	if !client.IsRemote() {
		s.logger.Info("pending stop request not completed before start because local apply is not stopped yet",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", stopCaller,
			"start_requested_by", caller,
			"state", apply.State)
		return nil
	}
	if apply.ExternalID == "" {
		s.logger.Warn("pending stop request not completed before start because remote apply has no external id",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", stopCaller,
			"start_requested_by", caller,
			"state", apply.State)
		return nil
	}
	progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ExternalID,
		Environment: apply.Environment,
	})
	if err != nil {
		return fmt.Errorf("check remote apply %s before completing pending stop for start: %w", apply.ApplyIdentifier, err)
	}
	remoteState := tern.ProtoStateToStorage(progress.State)
	if !state.IsState(remoteState, state.Apply.Stopped) {
		return nil
	}

	now := time.Now()
	oldState := apply.State
	apply.State = state.Apply.Stopped
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := s.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("sync remote stopped apply %s before start: %w", apply.ApplyIdentifier, err)
	}
	if err := controlStore.CompletePending(ctx, apply.ID, storage.ControlOperationStop); err != nil {
		return fmt.Errorf("complete pending remote stop control request for apply %s before start: %w", apply.ApplyIdentifier, err)
	}
	s.logger.Info("completed remote stop request before start after remote state check",
		"apply_id", apply.ApplyIdentifier,
		"external_apply_id", apply.ExternalID,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", stopCaller,
		"start_requested_by", caller,
		"old_state", oldState,
		"new_state", apply.State)
	s.logControlOperationForApply(ctx, apply, stopCaller, storage.LogEventStopRequested,
		fmt.Sprintf("Pending remote stop request completed before start (caller: %s)", stopCaller))
	return nil
}

// queueStoppedApplyForOperator makes a user start request claimable by a
// operator worker. Resuming stopped table work can outlive the HTTP request,
// so the handler records intent, normalizes a lagging stored apply row to
// stopped, and wakes the operator.
func (s *Service) queueStoppedApplyForOperator(ctx context.Context, apply *storage.Apply, caller string) (*ternv1.StartResponse, string, error) {
	if !storedApplyMayHaveStoppedTasksForStart(apply.State) {
		return nil, "", startNotAllowedForState(apply)
	}
	tasks, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return nil, "", fmt.Errorf("get tasks for apply %s: %w", apply.ApplyIdentifier, err)
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
		if state.IsState(apply.State, state.Apply.Stopped) {
			return nil, "", controlConflictf("no stopped tasks for apply %s", apply.ApplyIdentifier)
		}
		return nil, "", startNotAllowedForState(apply)
	}
	if state.IsState(apply.State, state.Apply.Running) {
		s.logger.Info("queueing start for stopped tasks while stored apply is still running",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"stopped_count", startedCount,
			"terminal_count", skippedCount)
		if err := s.ensureStoredApplyStoppedForStartClaim(ctx, apply); err != nil {
			return nil, "", err
		}
	}
	return s.persistStartRequestForOperator(ctx, apply, caller, startedCount, skippedCount)
}

// queueRemoteStoppedApplyForOperator validates remote stopped state before
// recording operator work. In gRPC mode, progress can show the data plane as
// stopped before the control-plane task rows have synced to stopped, so the
// remote apply state is the start gate.
func (s *Service) queueRemoteStoppedApplyForOperator(ctx context.Context, client tern.Client, apply *storage.Apply, caller string) (*ternv1.StartResponse, string, error) {
	if !storedApplyMayHaveStoppedTasksForStart(apply.State) {
		return nil, "", startNotAllowedForState(apply)
	}
	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ExternalID,
		Environment: apply.Environment,
	})
	if err != nil {
		return nil, "", fmt.Errorf("check remote apply %s before start: %w", apply.ApplyIdentifier, err)
	}
	remoteState := tern.ProtoStateToStorage(resp.State)
	if !state.IsState(remoteState, state.Apply.Stopped) {
		if remoteState == "" {
			remoteState = resp.State.String()
		}
		return nil, "", controlConflictf("schema change is not stopped (remote state: %s, current state: %s)", remoteState, apply.State)
	}
	startedCount, skippedCount := remoteStoppedApplyStartCounts(resp.Tables)
	if startedCount == 0 {
		startedCount = 1
	}
	if state.IsState(apply.State, state.Apply.Running) {
		s.logger.Info("queueing remote start while stored apply is still running",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"remote_state", remoteState,
			"stopped_count", startedCount,
			"terminal_count", skippedCount)
		if err := s.ensureStoredApplyStoppedForStartClaim(ctx, apply); err != nil {
			return nil, "", err
		}
	}
	return s.persistStartRequestForOperator(ctx, apply, caller, startedCount, skippedCount)
}

func (s *Service) ensureStoredApplyStoppedForStartClaim(ctx context.Context, apply *storage.Apply) error {
	if !state.IsState(apply.State, state.Apply.Running) {
		return nil
	}
	applyStore := s.storage.Applies()
	if applyStore == nil {
		return fmt.Errorf("apply store is not available")
	}

	previousApply := *apply
	now := time.Now()
	apply.State = state.Apply.Stopped
	apply.CompletedAt = nil
	apply.UpdatedAt = now
	if err := applyStore.Update(ctx, apply); err != nil {
		*apply = previousApply
		return fmt.Errorf("mark stored apply %s stopped before operator start: %w", apply.ApplyIdentifier, err)
	}
	return nil
}

func remoteStoppedApplyStartCounts(tables []*ternv1.TableProgress) (int64, int64) {
	var startedCount int64
	var skippedCount int64
	for _, table := range tables {
		taskState := state.NormalizeTaskStatus(table.Status)
		switch {
		case state.IsState(taskState, state.Task.Stopped):
			startedCount++
		case state.IsTerminalTaskState(taskState):
			skippedCount++
		}
	}
	return startedCount, skippedCount
}

func (s *Service) persistStartRequestForOperator(ctx context.Context, apply *storage.Apply, caller string, startedCount, skippedCount int64) (*ternv1.StartResponse, string, error) {
	controlReq, alreadyPending, err := s.createStartControlRequest(ctx, apply, caller, startedCount, skippedCount)
	if err != nil {
		return nil, "", err
	}
	if alreadyPending {
		resp, err := startResponseFromControlRequest(controlReq)
		if err != nil {
			return nil, "", err
		}
		return resp, startResponseStatusAlreadyRequested, nil
	}

	return &ternv1.StartResponse{
		Accepted:     true,
		StartedCount: startedCount,
		SkippedCount: skippedCount,
	}, startResponseStatusQueued, nil
}

func (s *Service) createStartControlRequest(ctx context.Context, apply *storage.Apply, caller string, startedCount, skippedCount int64) (*storage.ApplyControlRequest, bool, error) {
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		return nil, false, fmt.Errorf("control request store is not available")
	}
	metadata, err := json.Marshal(startControlRequestMetadata{
		StartedCount: startedCount,
		SkippedCount: skippedCount,
	})
	if err != nil {
		return nil, false, fmt.Errorf("marshal start control request metadata for apply %s: %w", apply.ApplyIdentifier, err)
	}
	controlReq, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: caller,
		Metadata:    metadata,
	})
	if err != nil {
		return nil, false, fmt.Errorf("record start control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return controlReq, alreadyPending, nil
}

func (s *Service) startResponseForPendingStartRequest(ctx context.Context, apply *storage.Apply) (*ternv1.StartResponse, string, error) {
	resp, responseStatus, found, err := s.pendingStartResponseIfPresent(ctx, apply)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return nil, "", startNotAllowedForState(apply)
	}
	return resp, responseStatus, nil
}

func (s *Service) pendingStartResponseIfPresent(ctx context.Context, apply *storage.Apply) (*ternv1.StartResponse, string, bool, error) {
	controlStore := s.storage.ControlRequests()
	if controlStore == nil {
		return nil, "", false, fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationStart)
	if err != nil {
		return nil, "", false, fmt.Errorf("load pending start control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if controlReq == nil {
		return nil, "", false, nil
	}
	resp, err := startResponseFromControlRequest(controlReq)
	if err != nil {
		return nil, "", false, err
	}
	return resp, startResponseStatusAlreadyRequested, true, nil
}

func startResponseFromControlRequest(controlReq *storage.ApplyControlRequest) (*ternv1.StartResponse, error) {
	resp := &ternv1.StartResponse{}
	if controlReq == nil {
		return resp, nil
	}
	var metadata startControlRequestMetadata
	if len(controlReq.Metadata) > 0 {
		if err := json.Unmarshal(controlReq.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("decode start control request metadata for request %d: %w", controlReq.ID, err)
		}
	}
	resp.StartedCount = metadata.StartedCount
	resp.SkippedCount = metadata.SkippedCount
	switch controlReq.Status {
	case storage.ControlRequestPending, storage.ControlRequestCompleted:
		resp.Accepted = true
	case storage.ControlRequestFailed:
		resp.ErrorMessage = controlReq.ErrorMessage
	default:
		resp.ErrorMessage = fmt.Sprintf("start control request has unknown status: %s", controlReq.Status)
	}
	return resp, nil
}

// storedApplyMayHaveStoppedTasksForStart keeps start requests aligned with the
// durable task rows, not only the derived apply row. Stop persists stopped task
// rows before the apply row necessarily reflects the derived stopped state, so
// a user can start after progress shows stopped while the stored apply still
// says running.
func storedApplyMayHaveStoppedTasksForStart(storedApplyState string) bool {
	return state.IsState(storedApplyState, state.Apply.Stopped) ||
		state.IsState(storedApplyState, state.Apply.Running)
}

func validateStartRequestState(apply *storage.Apply) error {
	if !isStartRequestAllowedState(apply.State) {
		return startNotAllowedForState(apply)
	}
	if apply.GetOptions().DeferDeploy && !state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return controlConflictf("schema change is not ready for deploy (current state: %s)", apply.State)
	}
	return nil
}

// isStartRequestAllowedState is an allowlist for states where /start has a
// concrete action. New apply states must opt in here before they can reach the
// operator or Tern start paths.
func isStartRequestAllowedState(applyState string) bool {
	return state.IsState(
		applyState,
		state.Apply.WaitingForDeploy,
		state.Apply.Pending,
		state.Apply.Running,
		state.Apply.Stopped,
	)
}

func startNotAllowedForState(apply *storage.Apply) error {
	switch {
	case state.IsState(apply.State, state.Apply.Pending):
		return controlConflictf("schema change is pending and no start request is queued")
	case state.IsState(apply.State, state.Apply.Running):
		// Running applies may reach this helper after the handler checks for
		// stopped task rows. Without stopped task rows, there is no operator
		// start work to queue.
		return controlConflictf("schema change is still running; stop it before starting it again")
	case state.IsState(apply.State, state.Apply.WaitingForCutover):
		return controlConflictf("schema change is waiting for cutover; use cutover instead of start")
	case state.IsState(apply.State, state.Apply.CuttingOver):
		return controlConflictf("schema change is cutting over; start is not allowed")
	case state.IsState(apply.State, state.Apply.RevertWindow):
		return controlConflictf("schema change is in revert window; use revert or skip-revert instead of start")
	case state.IsState(apply.State, state.Apply.FailedRetryable):
		return controlConflictf("schema change is waiting for operator retry; start is not allowed")
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
		metrics.RecordControlOperation(r.Context(), "volume", apply.Database, apply.Deployment, apply.Environment, "error")
		s.writeControlError(w, "volume", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "volume", apply.Database, apply.Deployment, apply.Environment, controlStatus(resp.Accepted))

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
		metrics.RecordControlOperation(r.Context(), "revert", apply.Database, apply.Deployment, apply.Environment, "error")
		s.writeControlError(w, "revert", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "revert", apply.Database, apply.Deployment, apply.Environment, controlStatus(resp.Accepted))
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
		metrics.RecordControlOperation(r.Context(), "skip_revert", apply.Database, apply.Deployment, apply.Environment, "error")
		s.writeControlError(w, "skip-revert", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "skip_revert", apply.Database, apply.Deployment, apply.Environment, controlStatus(resp.Accepted))

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
		metrics.RecordControlOperation(r.Context(), "rollback_plan", apply.Database, apply.Deployment, apply.Environment, "error")
		s.writeControlError(w, "rollback plan", apply, err)
		return
	}
	metrics.RecordControlOperation(r.Context(), "rollback_plan", apply.Database, apply.Deployment, apply.Environment, "success")

	// Include database metadata so the caller doesn't need to look it up separately
	resp.Database = apply.Database
	resp.DatabaseType = apply.DatabaseType
	resp.Environment = apply.Environment

	s.writeJSON(w, http.StatusOK, resp)
}
