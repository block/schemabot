package api

import (
	"cmp"
	"fmt"
	"net/http"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// handleSettingsList returns the operator settings. Rows that components keep
// for their own state share the settings table but are not settings an
// operator chooses, so the listing leaves them out; handleSettingsGet still
// returns one by key. The route matches a single path segment, so a caller
// percent-encodes a key that contains a slash.
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
		if storage.IsComponentStateSettingKey(setting.Key) {
			continue
		}
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
	// Settings are deployment-wide, so there is no database to scope a grant
	// to: admin-only. Nothing in the body decides that, so the check runs
	// before the body is read and a caller who may not write settings gets
	// the same refusal whatever they sent.
	if !s.authorizeDirectAdminWrite(w, r, "settings_set") {
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := decodeStrictJSONBody(r, &req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}

	if req.Key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// The key is measured before it is logged or checked further, so an
	// oversized key is never echoed into the logs or the response.
	if len(req.Key) > storage.MaxSettingKeyBytes {
		s.logger.Warn("refused settings write to a key longer than storage holds", "key_bytes", len(req.Key),
			"max_key_bytes", storage.MaxSettingKeyBytes, "caller", cmp.Or(resolveCaller(r.Context(), ""), "unknown"))
		s.writeError(w, http.StatusBadRequest,
			fmt.Sprintf("setting key is %d bytes; the limit is %d bytes", len(req.Key), storage.MaxSettingKeyBytes))
		return
	}

	// Storage can treat a key with an accented or full-width letter, or an
	// invisible character, as naming another key's row. Printable ASCII keys
	// differ to storage only by letter case, which the check below allows for.
	if !storage.IsPrintableASCIISettingKey(req.Key) {
		s.logger.Warn("refused settings write to a key with characters outside printable ASCII", "key", req.Key,
			"caller", cmp.Or(resolveCaller(r.Context(), ""), "unknown"))
		s.writeError(w, http.StatusBadRequest,
			fmt.Sprintf("setting key %q must contain only printable ASCII characters", req.Key))
		return
	}

	// Component-state rows share the settings table but belong to the
	// component that writes them; an operator value there would silently
	// steer that component, so the API reads them but never writes them.
	if storage.IsComponentStateSettingKey(req.Key) {
		s.logger.Warn("refused settings write to a component-state key", "key", req.Key,
			"caller", cmp.Or(resolveCaller(r.Context(), ""), "unknown"))
		s.writeError(w, http.StatusBadRequest,
			fmt.Sprintf("%q is kept by SchemaBot itself and cannot be set through the settings API", req.Key))
		return
	}

	// Storage rejects a value past its column, which would reach the caller as
	// a 500; refuse it here and say what the limit is.
	if len(req.Value) > storage.MaxSettingValueBytes {
		s.logger.Warn("refused settings write of a value larger than storage holds", "key", req.Key,
			"value_bytes", len(req.Value), "max_value_bytes", storage.MaxSettingValueBytes,
			"caller", cmp.Or(resolveCaller(r.Context(), ""), "unknown"))
		s.writeError(w, http.StatusBadRequest,
			fmt.Sprintf("setting value is %d bytes; the limit is %d bytes", len(req.Value), storage.MaxSettingValueBytes))
		return
	}

	// PostgreSQL text cannot hold a NUL character, so refuse it on every
	// dialect rather than answer a 500 on one of them.
	if strings.IndexByte(req.Value, 0) >= 0 {
		s.logger.Warn("refused settings write of a value with a NUL character", "key", req.Key,
			"caller", cmp.Or(resolveCaller(r.Context(), ""), "unknown"))
		s.writeError(w, http.StatusBadRequest, "setting value contains a NUL character, which storage cannot hold")
		return
	}

	err := s.storage.Settings().Set(r.Context(), req.Key, req.Value)
	if err != nil {
		s.logger.Error("set setting failed", "key", req.Key, "error", err)
		s.writeError(w, http.StatusInternalServerError, "set setting failed")
		return
	}

	// The value is not logged: a setting can hold anything an operator
	// stores, and the API response already returns it to the caller.
	s.logger.Info("setting updated", "key", req.Key, "value_bytes", len(req.Value),
		"caller", cmp.Or(resolveCaller(r.Context(), ""), "unknown"))

	s.writeJSON(w, http.StatusOK, map[string]any{
		"key":   req.Key,
		"value": req.Value,
	})
}
