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

// ternApplyIDForStoredApply returns the identifier that a Tern RPC expects for
// an apply-scoped control or progress request.
//
// HTTP callers use SchemaBot's apply_identifier. In remote gRPC mode,
// SchemaBot queues work locally first; after the scheduler dispatches it, Tern
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
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Environment, "rejected")
		s.writeControlError(w, "start", apply, err)
		return
	}

	var resp *ternv1.StartResponse
	var err error
	responseStatus := ""
	queuedForScheduler := false
	switch {
	case state.IsState(apply.State, state.Apply.WaitingForDeploy):
		resp, err = client.Start(r.Context(), &ternv1.StartRequest{
			ApplyId:     applyID,
			Environment: apply.Environment,
		})
	case state.IsState(apply.State, state.Apply.Pending):
		resp, responseStatus, err = s.startResponseForPendingStartRequest(r.Context(), apply)
		queuedForScheduler = err == nil && resp.Accepted
	case storedApplyMayHaveStoppedTasksForStart(apply.State):
		// A queued or scheduler-claimed start can leave the durable request
		// pending while the stored apply is stopped or running. Treat retries in
		// either state as idempotent duplicates instead of revalidating remote
		// state or recording another request.
		var foundPendingStart bool
		resp, responseStatus, foundPendingStart, err = s.pendingStartResponseIfPresent(r.Context(), apply)
		queuedForScheduler = err == nil && foundPendingStart && resp.Accepted
		if err == nil && !foundPendingStart && client.IsRemote() && apply.ExternalID != "" {
			resp, responseStatus, err = s.queueRemoteStoppedApplyForScheduler(r.Context(), client, apply, req.Caller)
		} else if err == nil && !foundPendingStart {
			resp, responseStatus, err = s.queueStoppedApplyForScheduler(r.Context(), apply, req.Caller)
		}
		queuedForScheduler = queuedForScheduler || (err == nil && resp.Accepted)
	default:
		err = startNotAllowedForState(apply)
	}
	if err != nil {
		status := "error"
		if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
			status = "rejected"
		}
		metrics.RecordControlOperation(r.Context(), "start", apply.Database, apply.Environment, status)
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
		Status:       responseStatus,
	}

	httpStatus := http.StatusOK
	if responseStatus == startResponseStatusAlreadyRequested {
		httpStatus = http.StatusAccepted
	}
	s.writeJSON(w, httpStatus, httpResp)
}

// queueStoppedApplyForScheduler makes a user start request claimable by a
// scheduler worker. Resuming stopped table work can outlive the HTTP request,
// so the handler records intent, normalizes a lagging stored apply row to
// stopped, and wakes the scheduler.
func (s *Service) queueStoppedApplyForScheduler(ctx context.Context, apply *storage.Apply, caller string) (*ternv1.StartResponse, string, error) {
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
			"environment", apply.Environment,
			"stopped_count", startedCount,
			"terminal_count", skippedCount)
		if err := s.ensureStoredApplyStoppedForStartClaim(ctx, apply); err != nil {
			return nil, "", err
		}
	}
	return s.persistStartRequestForScheduler(ctx, apply, caller, startedCount, skippedCount)
}

// queueRemoteStoppedApplyForScheduler validates remote stopped state before
// recording scheduler work. In gRPC mode, progress can show the data plane as
// stopped before the control-plane task rows have synced to stopped, so the
// remote apply state is the start gate.
func (s *Service) queueRemoteStoppedApplyForScheduler(ctx context.Context, client tern.Client, apply *storage.Apply, caller string) (*ternv1.StartResponse, string, error) {
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
			"environment", apply.Environment,
			"remote_state", remoteState,
			"stopped_count", startedCount,
			"terminal_count", skippedCount)
		if err := s.ensureStoredApplyStoppedForStartClaim(ctx, apply); err != nil {
			return nil, "", err
		}
	}
	return s.persistStartRequestForScheduler(ctx, apply, caller, startedCount, skippedCount)
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
		return fmt.Errorf("mark stored apply %s stopped before scheduler start: %w", apply.ApplyIdentifier, err)
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

func (s *Service) persistStartRequestForScheduler(ctx context.Context, apply *storage.Apply, caller string, startedCount, skippedCount int64) (*ternv1.StartResponse, string, error) {
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
// scheduler or Tern start paths.
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
		// stopped task rows. Without stopped task rows, there is no scheduler
		// start work to queue.
		return controlConflictf("schema change is still running; stop it before starting it again")
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
