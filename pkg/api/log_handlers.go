package api

import (
	"net/http"
	"strconv"

	"github.com/block/schemabot/pkg/storage"
)

// handleLogs returns apply logs for a database or apply.
// GET /api/logs/{database}?environment=staging&limit=50
// GET /api/logs/{database}?apply_id=apply_abc123&limit=50
func (s *Service) handleLogs(w http.ResponseWriter, r *http.Request) {
	database := r.PathValue("database")
	environment := r.URL.Query().Get("environment")
	applyID := r.URL.Query().Get("apply_id")
	limitStr := r.URL.Query().Get("limit")

	if database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}

	s.handleLogsCommon(w, r, database, environment, applyID, limitStr)
}

// handleLogsWithoutDatabase returns apply logs for a specific apply ID.
// GET /api/logs?apply_id=apply_abc123&limit=50
func (s *Service) handleLogsWithoutDatabase(w http.ResponseWriter, r *http.Request) {
	applyID := r.URL.Query().Get("apply_id")
	limitStr := r.URL.Query().Get("limit")

	if applyID == "" {
		s.writeError(w, http.StatusBadRequest, "apply_id is required")
		return
	}

	s.handleLogsCommon(w, r, "", "", applyID, limitStr)
}

// handleLogsCommon is the shared implementation for log handlers.
func (s *Service) handleLogsCommon(w http.ResponseWriter, r *http.Request, database, environment, applyID, limitStr string) {

	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
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
