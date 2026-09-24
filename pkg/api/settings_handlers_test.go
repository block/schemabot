package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// staticSettingsStore serves a fixed set of settings rows, keyed as the
// storage layer would list them.
type staticSettingsStore struct {
	settings []*storage.Setting
}

func (s *staticSettingsStore) Get(_ context.Context, key string) (*storage.Setting, error) {
	for _, setting := range s.settings {
		if setting.Key == key {
			return setting, nil
		}
	}
	return nil, nil
}

func (s *staticSettingsStore) Set(context.Context, string, string) error { return nil }

func (s *staticSettingsStore) CompareAndSet(context.Context, string, *storage.Setting, string) (bool, error) {
	return false, nil
}

func (s *staticSettingsStore) List(context.Context) ([]*storage.Setting, error) {
	return s.settings, nil
}

func (s *staticSettingsStore) Delete(context.Context, string) error { return nil }

type mockStorageWithSettings struct {
	mockStorage
	settings storage.SettingsStore
}

func (m *mockStorageWithSettings) Settings() storage.SettingsStore { return m.settings }

type settingsListResponse struct {
	Settings []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"settings"`
}

// The settings listing is what an operator reads to see what they can change.
// The webhook reconciler's per-repository scan cursors share the table but are
// state the reconciler derives for itself, so the listing leaves them out
// while every operator setting stays, in stored order.
func TestSettingsListOmitsComponentStateRows(t *testing.T) {
	service := New(&mockStorageWithSettings{settings: &staticSettingsStore{settings: []*storage.Setting{
		{Key: "spirit_debug_logs", Value: "true"},
		{Key: storage.WebhookReconcileScanCursorSettingKeyPrefix + "octo/payments", Value: `{"page":3}`},
		{Key: "webhook_reconcile_scan_cursor", Value: "an operator key that only resembles the prefix"},
	}}}, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/settings", nil)
	w := httptest.NewRecorder()
	service.handleSettingsList(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var response settingsListResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Settings, 2)
	assert.Equal(t, "spirit_debug_logs", response.Settings[0].Key)
	assert.Equal(t, "true", response.Settings[0].Value)
	assert.Equal(t, "webhook_reconcile_scan_cursor", response.Settings[1].Key, "only keys under the prefix are component state")
}

// A component-state row stays reachable by its exact key so an operator can
// inspect a repository's cursor when the reconciler's logs point at it.
func TestSettingsGetReturnsComponentStateRowByKey(t *testing.T) {
	key := storage.WebhookReconcileScanCursorSettingKeyPrefix + "octo/payments"
	service := New(&mockStorageWithSettings{settings: &staticSettingsStore{settings: []*storage.Setting{
		{Key: key, Value: `{"page":3}`},
	}}}, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/settings/"+key, nil)
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()
	service.handleSettingsGet(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var response struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, key, response.Key)
	assert.Equal(t, `{"page":3}`, response.Value)
}
