package api

import (
	"encoding/json"
	"net/http"

	"github.com/block/schemabot/pkg/apitypes"
)

func (s *Service) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.storage.Ping(r.Context()); err != nil {
		s.logger.Error("health check failed", "error", err)
		s.writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Service) handleTernHealth(w http.ResponseWriter, r *http.Request) {
	deployment := r.PathValue("deployment")
	environment := r.PathValue("environment")

	client, err := s.TernClient(deployment, environment)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := client.Health(r.Context()); err != nil {
		s.logger.Error("tern health check failed", "deployment", deployment, "environment", environment, "error", err)
		s.writeError(w, http.StatusServiceUnavailable, "tern unavailable")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"status":      "ok",
		"deployment":  deployment,
		"environment": environment,
	})
}

// writeJSON writes a JSON response with the given status code.
func (s *Service) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error("failed to write JSON response", "error", err)
	}
}

// writeError writes a JSON error response without an error code.
func (s *Service) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, apitypes.ErrorResponse{Error: message})
}

// writeErrorCode writes a JSON error response with an error code.
// Clients should match on error_code rather than parsing the error message.
func (s *Service) writeErrorCode(w http.ResponseWriter, status int, code, message string) {
	s.writeJSON(w, status, apitypes.ErrorResponse{Error: message, ErrorCode: code})
}
