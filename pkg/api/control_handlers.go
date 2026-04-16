package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/tern"
)

// writeControlError logs and writes an HTTP error for a control operation.
func (s *Service) writeControlError(w http.ResponseWriter, opName, database string, err error) {
	s.logger.Error(opName+" failed", "database", database, "error", err)
	s.writeError(w, http.StatusInternalServerError, opName+" failed: "+err.Error())
}

// decodeControlRequest decodes a control request (stop/start/cutover/volume),
// resolves the apply ID, and returns a Tern client using the deployment stored
// on the apply record. Deployment is a plan-time decision — control operations
// always use the stored deployment, never a caller-provided one.
func (s *Service) decodeControlRequest(w http.ResponseWriter, r *http.Request, dest any,
	database, environment, applyID *string) (tern.Client, string, bool) {

	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return nil, "", false
	}
	if *database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return nil, "", false
	}
	if *environment == "" {
		*environment = "staging"
	}

	// Resolve the apply — either from explicit apply_id or by finding the active
	// apply for the database/environment. This ensures we always use the deployment
	// stored on the apply, even when the caller only provides database+environment.
	var resolvedApplyID string
	var deployment string
	applyIdentifier := *applyID
	if applyIdentifier == "" {
		// No explicit apply_id — find the active apply for this database/environment.
		_, activeApply, err := s.findActiveApplyID(r.Context(), *database, *environment)
		if err != nil {
			s.writeError(w, http.StatusNotFound, fmt.Sprintf("no active schema change for %s/%s: %v", *database, *environment, err))
			return nil, "", false
		}
		if activeApply != nil {
			applyIdentifier = activeApply.ApplyIdentifier
			deployment = activeApply.Deployment
		}
	}
	if applyIdentifier != "" {
		resolved, err := s.resolveApplyID(r.Context(), applyIdentifier)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve apply_id: %v", err))
			return nil, "", false
		}
		resolvedApplyID = resolved

		// If we didn't get the deployment from findActiveApplyID, look it up now.
		if deployment == "" {
			if applyStore := s.storage.Applies(); applyStore != nil {
				apply, err := applyStore.GetByApplyIdentifier(r.Context(), applyIdentifier)
				if err == nil && apply != nil {
					deployment = apply.Deployment
				}
			}
		}
	}
	deployment = s.resolveDeployment(*database, deployment)

	client, err := s.TernClient(deployment, *environment)
	if err != nil {
		s.logger.Error("failed to create Tern client",
			"deployment", deployment,
			"database", *database,
			"environment", *environment,
			"error", err)
		s.writeError(w, http.StatusNotFound, err.Error())
		return nil, "", false
	}

	return client, resolvedApplyID, true
}

// CutoverRequest is the HTTP request body for POST /api/cutover.
type CutoverRequest struct {
	Database    string `json:"database"`
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id,omitempty"`
}

// handleCutover handles POST /api/cutover requests.
func (s *Service) handleCutover(w http.ResponseWriter, r *http.Request) {
	var req CutoverRequest
	client, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Database, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.Cutover(r.Context(), &ternv1.CutoverRequest{
		ApplyId:     applyID,
		Database:    req.Database,
		Environment: req.Environment,
	})
	if err != nil {
		s.writeControlError(w, "cutover", req.Database, err)
		return
	}

	s.writeJSON(w, http.StatusOK, &apitypes.ControlResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
	})
}

// StopRequest is the HTTP request body for POST /api/stop.
type StopRequest struct {
	Database    string `json:"database"`
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id,omitempty"`
}

// handleStop handles POST /api/stop requests.
// Stops all non-terminal tasks for the database.
func (s *Service) handleStop(w http.ResponseWriter, r *http.Request) {
	var req StopRequest
	client, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Database, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.Stop(r.Context(), &ternv1.StopRequest{
		ApplyId:     applyID,
		Database:    req.Database,
		Environment: req.Environment,
	})
	if err != nil {
		s.writeControlError(w, "stop", req.Database, err)
		return
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
	Database    string `json:"database"`
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id,omitempty"`
}

// handleStart handles POST /api/start requests.
func (s *Service) handleStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	client, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Database, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.Start(r.Context(), &ternv1.StartRequest{
		ApplyId:     applyID,
		Database:    req.Database,
		Environment: req.Environment,
	})
	if err != nil {
		s.writeControlError(w, "start", req.Database, err)
		return
	}

	// For remote (gRPC) clients, update local apply state and restart the
	// background progress poller. Without this, the local applies.state stays
	// "stopped" permanently because the old poller exited when it saw the
	// terminal state.
	var syncErr string
	if resp.Accepted && client.IsRemote() && req.ApplyID != "" {
		apply, lookupErr := s.storage.Applies().GetByApplyIdentifier(r.Context(), req.ApplyID)
		if lookupErr != nil {
			s.logger.Error("failed to look up apply for post-start sync", "apply_id", req.ApplyID, "error", lookupErr)
			syncErr = "schema change was started successfully, but the status and progress endpoints may show stale state until the next recovery cycle"
		} else if apply != nil {
			// Mark running before ResumeApply so it doesn't re-issue a Start RPC.
			apply.State = state.Apply.Running
			if resumeErr := client.ResumeApply(r.Context(), apply); resumeErr != nil {
				s.logger.Error("failed to resume apply tracking after start", "apply_id", req.ApplyID, "error", resumeErr)
				syncErr = "schema change was started successfully, but the status and progress endpoints may show stale state until the next recovery cycle"
			}
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

// VolumeRequest is the HTTP request body for POST /api/volume.
type VolumeRequest struct {
	ApplyID     string `json:"apply_id,omitempty"`
	Database    string `json:"database"`
	Environment string `json:"environment"`
	Volume      int32  `json:"volume"` // 1-11 (1=conservative, 11=aggressive)
}

// handleVolume handles POST /api/volume requests.
func (s *Service) handleVolume(w http.ResponseWriter, r *http.Request) {
	var req VolumeRequest
	client, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Database, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	if req.Volume < 1 || req.Volume > 11 {
		s.writeError(w, http.StatusBadRequest, "volume must be between 1 and 11")
		return
	}

	resp, err := client.Volume(r.Context(), &ternv1.VolumeRequest{
		ApplyId:  applyID,
		Database: req.Database,
		Volume:   req.Volume,
	})
	if err != nil {
		s.writeControlError(w, "volume", req.Database, err)
		return
	}

	s.writeJSON(w, http.StatusOK, &apitypes.VolumeResponse{
		Accepted:       resp.Accepted,
		ErrorMessage:   resp.ErrorMessage,
		PreviousVolume: resp.PreviousVolume,
		NewVolume:      resp.NewVolume,
	})
}

// RevertRequest is the HTTP request body for POST /api/revert.
type RevertRequest struct {
	Database    string `json:"database"`
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id,omitempty"`
}

// handleRevert handles POST /api/revert requests.
func (s *Service) handleRevert(w http.ResponseWriter, r *http.Request) {
	var req RevertRequest
	client, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Database, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.Revert(r.Context(), &ternv1.RevertRequest{
		ApplyId:  applyID,
		Database: req.Database,
	})
	if err != nil {
		s.writeControlError(w, "revert", req.Database, err)
		return
	}

	s.writeJSON(w, http.StatusOK, &apitypes.ControlResponse{
		Accepted:     resp.Accepted,
		ErrorMessage: resp.ErrorMessage,
	})
}

// SkipRevertRequest is the HTTP request body for POST /api/skip-revert.
type SkipRevertRequest struct {
	Database    string `json:"database"`
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id,omitempty"`
}

// handleSkipRevert handles POST /api/skip-revert requests.
func (s *Service) handleSkipRevert(w http.ResponseWriter, r *http.Request) {
	var req SkipRevertRequest
	client, applyID, ok := s.decodeControlRequest(w, r, &req, &req.Database, &req.Environment, &req.ApplyID)
	if !ok {
		return
	}

	resp, err := client.SkipRevert(r.Context(), &ternv1.SkipRevertRequest{
		ApplyId:  applyID,
		Database: req.Database,
	})
	if err != nil {
		s.writeControlError(w, "skip-revert", req.Database, err)
		return
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		s.writeControlError(w, "rollback plan", apply.Database, err)
		return
	}

	// Include database metadata so the caller doesn't need to look it up separately
	resp.Database = apply.Database
	resp.DatabaseType = apply.DatabaseType
	resp.Environment = apply.Environment

	s.writeJSON(w, http.StatusOK, resp)
}
