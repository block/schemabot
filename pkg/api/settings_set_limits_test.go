package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// valueRecordingSettingsStore keeps each written value so a test can tell what
// reached storage, not only which key.
type valueRecordingSettingsStore struct {
	staticSettingsStore
	written map[string]string
}

func (s *valueRecordingSettingsStore) Set(_ context.Context, key, value string) error {
	if s.written == nil {
		s.written = map[string]string{}
	}
	s.written[key] = value
	return nil
}

// settingsSetter sends a settings write as an admin and returns the response,
// the store it wrote to, and everything the handler logged.
func settingsSetter(t *testing.T) func(key, value string) (*httptest.ResponseRecorder, *valueRecordingSettingsStore, string) {
	t.Helper()
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	return func(key, value string) (*httptest.ResponseRecorder, *valueRecordingSettingsStore, string) {
		t.Helper()
		var logs bytes.Buffer
		store := &valueRecordingSettingsStore{}
		svc := New(&mockStorageWithSettings{settings: store}, scopedWriteConfig(), nil,
			slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		body, err := json.Marshal(map[string]string{"key": key, "value": value})
		require.NoError(t, err)
		rec := scopedDenialRequest(t, svc.handleSettingsSet, admin, http.MethodPost, "/api/settings", string(body))
		return rec, store, logs.String()
	}
}

// A settings write is refused when storage could not keep it whole, with the
// limit in the message, instead of reaching storage and coming back as a 500.
// A key or value exactly at its limit is still stored, and a refused write
// leaves the stored value as it was.
func TestSettingsSetBoundsKeyAndValueSizes(t *testing.T) {
	set := settingsSetter(t)

	t.Run("a value at the limit is stored", func(t *testing.T) {
		value := strings.Repeat("v", storage.MaxSettingValueBytes)
		rec, store, _ := set("pause_applies", value)

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, map[string]string{"pause_applies": value}, store.written)
	})

	t.Run("a key at the limit is stored", func(t *testing.T) {
		key := strings.Repeat("k", storage.MaxSettingKeyBytes)
		rec, store, _ := set(key, "true")

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, map[string]string{key: "true"}, store.written)
	})

	t.Run("a value over the limit is refused", func(t *testing.T) {
		rec, store, _ := set("pause_applies", strings.Repeat("v", storage.MaxSettingValueBytes+1))

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(),
			fmt.Sprintf("setting value is %d bytes; the limit is %d bytes",
				storage.MaxSettingValueBytes+1, storage.MaxSettingValueBytes))
		assert.Empty(t, store.written, "nothing is written")
	})

	t.Run("a key over the limit is refused", func(t *testing.T) {
		rec, store, _ := set(strings.Repeat("k", storage.MaxSettingKeyBytes+1), "true")

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(),
			fmt.Sprintf("setting key is %d bytes; the limit is %d bytes",
				storage.MaxSettingKeyBytes+1, storage.MaxSettingKeyBytes))
		assert.Empty(t, store.written, "nothing is written")
	})

	t.Run("a component-state key is still refused with the stored state unchanged", func(t *testing.T) {
		cursorKey := storage.WebhookReconcileScanCursorSettingKeyPrefix + "octo/payments"
		rec, store, _ := set(cursorKey, `{"page":1}`)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "cannot be set through the settings API")
		assert.Empty(t, store.written, "the reconciler's cursor is left alone")
	})
}

// A setting can hold anything an operator stores, so no log line carries the
// value. The key, the value's size and the caller are enough to trace a write,
// whether it is stored or refused.
func TestSettingsSetLogsNeverCarryTheValue(t *testing.T) {
	set := settingsSetter(t)
	secret := "s3cr3t-token-value"

	t.Run("a stored write", func(t *testing.T) {
		rec, _, logs := set("pause_applies", secret)

		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.NotContains(t, logs, secret)
		assert.Contains(t, logs, `"key":"pause_applies"`)
		assert.Contains(t, logs, fmt.Sprintf(`"value_bytes":%d`, len(secret)))
		assert.Contains(t, logs, `"caller":"alice"`)
	})

	t.Run("an oversized value", func(t *testing.T) {
		value := secret + strings.Repeat("v", storage.MaxSettingValueBytes)
		rec, _, logs := set("pause_applies", value)

		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.NotContains(t, logs, secret)
		assert.Contains(t, logs, fmt.Sprintf(`"value_bytes":%d`, len(value)))
	})

	t.Run("a component-state key", func(t *testing.T) {
		rec, _, logs := set(storage.WebhookReconcileScanCursorSettingKeyPrefix+"octo/payments", secret)

		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.NotContains(t, logs, secret)
	})

	t.Run("an oversized key is not echoed", func(t *testing.T) {
		key := strings.Repeat("k", storage.MaxSettingKeyBytes+1)
		rec, _, logs := set(key, secret)

		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.NotContains(t, logs, key)
		assert.NotContains(t, logs, secret)
		assert.NotContains(t, rec.Body.String(), key)
		assert.Contains(t, logs, fmt.Sprintf(`"key_bytes":%d`, len(key)))
	})
}

// The limits are the settings table's own column sizes. If the schema changes
// the column, this fails so the constants and the API's refusals change with it.
func TestSettingsLimitsMatchStorageColumns(t *testing.T) {
	mysqlSchema, err := schema.MySQLFS.ReadFile("mysql/settings.sql")
	require.NoError(t, err)
	assert.Contains(t, string(mysqlSchema), fmt.Sprintf("`setting_key` varchar(%d) NOT NULL", storage.MaxSettingKeyBytes))
	assert.Contains(t, string(mysqlSchema), "`setting_value` text NOT NULL")
	assert.Equal(t, 65535, storage.MaxSettingValueBytes, "a MySQL TEXT column holds 65,535 bytes")

	postgresSchema, err := schema.PostgresFS.ReadFile("postgres/settings.sql")
	require.NoError(t, err)
	assert.Contains(t, string(postgresSchema), fmt.Sprintf("setting_key varchar(%d) NOT NULL", storage.MaxSettingKeyBytes))
	assert.Contains(t, string(postgresSchema), "setting_value text NOT NULL")
}

// The value limit counts bytes, because that is how storage sizes the column,
// so a value of multibyte characters is refused at the byte count and stored
// at exactly the limit.
func TestSettingsSetValueLimitCountsBytesNotCharacters(t *testing.T) {
	set := settingsSetter(t)

	atLimit := strings.Repeat("€", storage.MaxSettingValueBytes/3)
	rec, store, _ := set("pause_applies", atLimit)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, map[string]string{"pause_applies": atLimit}, store.written)

	overLimit := atLimit + "€"
	rec, store, _ = set("pause_applies", overLimit)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "setting value is 65538 bytes; the limit is 65535 bytes")
	assert.Empty(t, store.written)
}

// PostgreSQL cannot store a NUL character in a text column, so a value that
// carries one is refused before storage on every dialect.
func TestSettingsSetRefusesNULInValue(t *testing.T) {
	set := settingsSetter(t)

	rec, store, _ := set("pause_applies", "true\x00")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "setting value contains a NUL character")
	assert.Empty(t, store.written)
}
