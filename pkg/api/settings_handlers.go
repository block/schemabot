package api

import (
	"encoding/json"
	"net/http"
)

// handleSettingsList returns all settings.
func (s *Service) handleSettingsList(w http.ResponseWriter, r *http.Request) {
	settings, err := s.storage.Settings().List(r.Context())
	if err != nil {
		s.logger.Error("list settings failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "list settings failed")
		return
	}

	// Convert to client-friendly format
	type settingDTO struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	result := make([]settingDTO, 0, len(settings))
	for _, setting := range settings {
		result = append(result, settingDTO{Key: setting.Key, Value: setting.Value})
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"settings": result,
	})
}

// handleSettingsGet returns a specific setting by key.
func (s *Service) handleSettingsGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	setting, err := s.storage.Settings().Get(r.Context(), key)
	if err != nil {
		s.logger.Error("get setting failed", "key", key, "error", err)
		s.writeError(w, http.StatusInternalServerError, "get setting failed")
		return
	}

	if setting == nil {
		s.writeError(w, http.StatusNotFound, "setting not found")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"key":   setting.Key,
		"value": setting.Value,
	})
}

// handleSettingsSet creates or updates a setting.
func (s *Service) handleSettingsSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	err := s.storage.Settings().Set(r.Context(), req.Key, req.Value)
	if err != nil {
		s.logger.Error("set setting failed", "key", req.Key, "error", err)
		s.writeError(w, http.StatusInternalServerError, "set setting failed")
		return
	}

	s.logger.Info("setting updated", "key", req.Key, "value", req.Value)

	s.writeJSON(w, http.StatusOK, map[string]any{
		"key":   req.Key,
		"value": req.Value,
	})
}
