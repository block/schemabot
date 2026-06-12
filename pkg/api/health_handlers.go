package api

import (
	"encoding/json"
	"errors"
	"fmt"
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
		s.logger.Error("tern health check failed", "deployment", deployment, "environment", environment, "endpoint", client.Endpoint(), "error", err)
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

// writeBodyDecodeError maps a request-body decode failure to the right client
// error. Bodies that exceed the enforced request body limit get a 413 that
// tells the caller the limit so they can shrink the payload; every other
// decode failure gets a 400 with the decoder's error.
func (s *Service) writeBodyDecodeError(w http.ResponseWriter, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		s.writeError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("request body exceeds the %d MiB limit; reduce the payload size", maxBytesErr.Limit>>20))
		return
	}
	s.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
}
