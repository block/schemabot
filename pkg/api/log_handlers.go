package api

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handleLogs returns apply logs for a database or apply.
// GET /api/logs/{database}?environment=staging&limit=50
// GET /api/logs/{database}?apply_id=apply_abc123&limit=50
func (s *Service) handleLogs(w http.ResponseWriter, r *http.Request) {
	database := r.PathValue("database")
	environment := r.URL.Query().Get("environment")
	applyID := r.URL.Query().Get("apply_id")
	limitStr := r.URL.Query().Get("limit")
	deployment := r.URL.Query().Get("deployment")

	if database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}

	s.handleLogsCommon(w, r, database, environment, applyID, deployment, limitStr)
}

// handleLogsWithoutDatabase returns apply logs for a specific apply ID.
// GET /api/logs?apply_id=apply_abc123&limit=50
func (s *Service) handleLogsWithoutDatabase(w http.ResponseWriter, r *http.Request) {
	applyID := r.URL.Query().Get("apply_id")
	limitStr := r.URL.Query().Get("limit")
	deployment := r.URL.Query().Get("deployment")

	if applyID == "" {
		s.writeError(w, http.StatusBadRequest, "apply_id is required")
		return
	}

	s.handleLogsCommon(w, r, "", "", applyID, deployment, limitStr)
}

// handleLogsCommon is the shared implementation for log handlers.
func (s *Service) handleLogsCommon(w http.ResponseWriter, r *http.Request, database, environment, applyID, deployment, limitStr string) {

	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if deployment != "" && applyID == "" {
		s.writeError(w, http.StatusBadRequest, "deployment requires an explicit apply_id")
		return
	}

	var apply *storage.Apply

	if applyID != "" {
		// Get specific apply by ID
		var err error
		apply, err = s.storage.Applies().GetByApplyIdentifier(r.Context(), applyID)
		if err != nil {
			s.logger.Error("failed to get apply", "apply_id", applyID, "error", err)
			s.writeError(w, http.StatusInternalServerError, "failed to get apply")
			return
		}
		if apply == nil {
			s.writeError(w, http.StatusNotFound, "apply not found")
			return
		}
	} else {
		// Get the most recent apply for this database/environment
		if environment == "" {
			s.writeError(w, http.StatusBadRequest, "environment or apply_id is required")
			return
		}

		applies, err := s.storage.Applies().GetByDatabase(r.Context(), database, "", environment)
		if err != nil {
			s.logger.Error("failed to get applies", "database", database, "error", err)
			s.writeError(w, http.StatusInternalServerError, "failed to get applies")
			return
		}

		if len(applies) == 0 {
			s.writeJSON(w, http.StatusOK, map[string]any{
				"logs": []any{},
			})
			return
		}
		apply = applies[0]
	}
	if deployment != "" {
		s.handleDeploymentLogs(w, r, apply, deployment, limit)
		return
	}

	logs, err := s.storage.ApplyLogs().List(r.Context(), storage.ApplyLogFilter{
		ApplyID: apply.ID,
		Limit:   limit,
	})
	if err != nil {
		s.logger.Error("failed to get logs", "apply_id", apply.ID, "error", err)
		s.writeError(w, http.StatusInternalServerError, "failed to get logs")
		return
	}

	// Convert to response format
	logEntries := make([]map[string]any, len(logs))
	for i, log := range logs {
		entry := map[string]any{
			"id":         log.ID,
			"apply_id":   apply.ApplyIdentifier,
			"level":      log.Level,
			"event_type": log.EventType,
			"message":    log.Message,
			"created_at": log.CreatedAt,
		}
		if log.TaskID != nil {
			entry["task_id"] = *log.TaskID
		}
		if log.OldState != "" {
			entry["old_state"] = log.OldState
		}
		if log.NewState != "" {
			entry["new_state"] = log.NewState
		}
		logEntries[i] = entry
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"logs":     logEntries,
		"apply_id": apply.ApplyIdentifier,
	})
}

type deploymentLogFetch struct {
	target     string
	externalID string
	operations []*apitypes.LogOperationProvenance
}

func (s *Service) handleDeploymentLogs(w http.ResponseWriter, r *http.Request, apply *storage.Apply, deployment string, limit int) {
	ops, err := s.storage.ApplyOperations().ListByApply(r.Context(), apply.ID)
	if err != nil {
		s.logger.Error("failed to list apply operations for data-plane logs", append(apply.LogAttrs(), "operation", "read_deployment_logs", "operation_deployment", deployment, "error", err)...)
		s.writeError(w, http.StatusInternalServerError, "failed to list apply operations")
		return
	}
	fetches := make(map[string]*deploymentLogFetch)
	matched := false
	for _, op := range ops {
		if op.Deployment != deployment {
			continue
		}
		matched = true
		externalID := op.ExternalID
		if externalID == "" && len(ops) == 1 {
			externalID = apply.ExternalID
		}
		if externalID == "" {
			// An operation without a remote apply id ran on the control plane;
			// its logs live in control-plane storage, not behind this fan-out.
			s.logger.Debug("skipping operation without a remote apply id for data-plane logs", append(apply.LogAttrs(), "operation", "read_deployment_logs", "operation_deployment", deployment, "operation_key", op.OperationKey, "target", op.Target)...)
			continue
		}
		key := op.Target + "\x00" + externalID
		fetch := fetches[key]
		if fetch == nil {
			fetch = &deploymentLogFetch{target: op.Target, externalID: externalID}
			fetches[key] = fetch
		}
		fetch.operations = append(fetch.operations, &apitypes.LogOperationProvenance{OperationKey: op.OperationKey, Target: op.Target, OperationKind: op.OperationKind})
	}
	if !matched {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("deployment %q has no operations for apply %q", deployment, apply.ApplyIdentifier))
		return
	}
	if len(fetches) == 0 {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("deployment %q has no remote operation logs; omit --deployment to read control-plane logs", deployment))
		return
	}
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("failed to resolve deployment for data-plane logs", append(apply.LogAttrs(), "operation", "read_deployment_logs", "operation_deployment", deployment, "error", err)...)
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("cannot resolve deployment %q; check server logs", deployment))
		return
	}
	if !client.IsRemote() {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("deployment %q is local-only; omit --deployment to read control-plane logs", deployment))
		return
	}
	result := &apitypes.DeploymentLogsResponse{ApplyID: apply.ApplyIdentifier, Deployment: deployment, Sources: []*apitypes.DeploymentLogSource{}, Errors: []*apitypes.DeploymentLogError{}}
	keys := make([]string, 0, len(fetches))
	for key := range fetches {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	requestLimit := int32(min(int64(limit), math.MaxInt32))
	for _, key := range keys {
		fetch := fetches[key]
		resp, fetchErr := client.Logs(r.Context(), &ternv1.LogsRequest{ApplyId: fetch.externalID, Target: fetch.target, Database: apply.Database, Type: apply.DatabaseType, Environment: apply.Environment, Limit: requestLimit})
		if fetchErr != nil {
			s.recordDeploymentLogFailure(apply, deployment, fetch, fetchErr)
			result.Errors = append(result.Errors, deploymentLogError(fetch, fetchErr))
			continue
		}
		source := &apitypes.DeploymentLogSource{ExternalID: fetch.externalID, Operations: fetch.operations, Logs: []*apitypes.LogEntry{}}
		for _, log := range resp.Logs {
			entry, convertErr := deploymentLogEntry(fetch.externalID, log)
			if convertErr != nil {
				s.recordDeploymentLogFailure(apply, deployment, fetch, convertErr)
				result.Errors = append(result.Errors, deploymentLogRecordError(fetch))
				source = nil
				break
			}
			source.Logs = append(source.Logs, entry)
		}
		if source != nil {
			result.Sources = append(result.Sources, source)
		}
	}
	s.writeJSON(w, http.StatusOK, result)
}

func deploymentLogEntry(applyID string, log *ternv1.ApplyLog) (*apitypes.LogEntry, error) {
	createdAt, err := time.Parse(time.RFC3339Nano, log.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("parse remote log created_at: %w", err)
	}
	var metadata json.RawMessage
	if len(log.MetadataJson) > 0 {
		if !json.Valid(log.MetadataJson) {
			return nil, fmt.Errorf("remote log metadata is not valid JSON")
		}
		metadata = append(metadata, log.MetadataJson...)
	}
	return &apitypes.LogEntry{ID: log.Id, ApplyID: applyID, TaskID: log.TaskId, Level: log.Level, EventType: log.EventType, Source: log.Source, Message: log.Message, OldState: log.OldState, NewState: log.NewState, Metadata: metadata, CreatedAt: createdAt}, nil
}

func deploymentLogError(fetch *deploymentLogFetch, err error) *apitypes.DeploymentLogError {
	result := &apitypes.DeploymentLogError{ExternalID: fetch.externalID, Target: fetch.target, Operations: fetch.operations, Code: "RemoteLogReadFailed", Reason: "remote_log_read_failed", Message: "Data-plane logs could not be read; check server logs and retry."}
	if status.Code(err) == codes.Unimplemented {
		result.Code = "UnsupportedCapability"
		result.Reason = "upgrade_required"
		result.Message = "The selected data plane does not support log reads; upgrade it and retry."
	}
	return result
}

// deploymentLogRecordError reports a source whose log records could not be
// decoded. Unlike a transport failure, retrying cannot help — the data plane
// returned records the control plane does not understand.
func deploymentLogRecordError(fetch *deploymentLogFetch) *apitypes.DeploymentLogError {
	return &apitypes.DeploymentLogError{ExternalID: fetch.externalID, Target: fetch.target, Operations: fetch.operations, Code: "MalformedRemoteLog", Reason: "malformed_remote_log", Message: "The data plane returned log records SchemaBot could not decode; check server logs."}
}

func (s *Service) recordDeploymentLogFailure(apply *storage.Apply, deployment string, fetch *deploymentLogFetch, err error) {
	attrs := append(apply.LogAttrs(), "operation", "read_deployment_logs", "operation_deployment", deployment, "target", fetch.target, "external_id", fetch.externalID, "error", err)
	s.logger.Error("failed to read data-plane logs", attrs...)
}
