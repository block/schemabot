//go:build integration

package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// The webhook reconciler keeps each repository's scan cursor in the settings
// table. MySQL decides which row a setting key names without regard to letter
// case, accents, character width or invisible characters, so a key spelled
// differently from the cursor's can still name the cursor's row. An admin's
// settings write under any such spelling is refused: a cursor that exists
// keeps its value, and no row is created for the reconciler to later read as
// its own. An operator setting is still stored.
func TestSettingsSetLeavesTheReconcilerCursorAloneOnMySQL(t *testing.T) {
	ctx := t.Context()
	stor := mysqlstore.New(openMatrixStorage(t))
	svc := New(stor, scopedWriteConfig(), nil, slog.New(slog.DiscardHandler))
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}

	set := func(t *testing.T, key, value string) int {
		t.Helper()
		body, err := json.Marshal(map[string]string{"key": key, "value": value})
		require.NoError(t, err)
		rec := scopedDenialRequest(t, svc.handleSettingsSet, admin, http.MethodPost, "/api/settings", string(body))
		return rec.Code
	}

	cursorKey := storage.WebhookReconcileScanCursorSettingKeyPrefix + "octo/payments"
	const cursorValue = `{"page":3}`
	written, err := stor.Settings().CompareAndSet(ctx, cursorKey, nil, cursorValue)
	require.NoError(t, err)
	require.True(t, written, "the reconciler's cursor row is created")

	spellings := []struct {
		name   string
		prefix string
	}{
		{name: "upper case", prefix: "WEBHOOK_RECONCILE_SCAN_CURSOR:"},
		{name: "mixed case", prefix: "Webhook_Reconcile_Scan_Cursor:"},
		{name: "an accented letter", prefix: "wébhook_reconcile_scan_cursor:"},
		{name: "a full-width letter", prefix: "ｗebhook_reconcile_scan_cursor:"},
		{name: "an invisible soft hyphen", prefix: "web\u00adhook_reconcile_scan_cursor:"},
	}
	for _, spelling := range spellings {
		t.Run(spelling.name+" names the existing cursor and is refused", func(t *testing.T) {
			key := spelling.prefix + "octo/payments"
			named, err := stor.Settings().Get(ctx, key)
			require.NoError(t, err)
			require.NotNil(t, named, "MySQL must treat this spelling as the cursor's key for the case to prove anything")
			require.Equal(t, cursorKey, named.Key)

			assert.Equal(t, http.StatusBadRequest, set(t, key, `{"page":1}`))

			stored, err := stor.Settings().Get(ctx, cursorKey)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, cursorKey, stored.Key)
			assert.Equal(t, cursorValue, stored.Value, "the cursor keeps the reconciler's value")
		})

		t.Run(spelling.name+" creates no row for a cursor that does not exist yet", func(t *testing.T) {
			assert.Equal(t, http.StatusBadRequest, set(t, spelling.prefix+"octo/orders", `{"page":1}`))

			stored, err := stor.Settings().Get(ctx, storage.WebhookReconcileScanCursorSettingKeyPrefix+"octo/orders")
			require.NoError(t, err)
			assert.Nil(t, stored, "the reconciler finds no cursor for the repository")
		})
	}

	t.Run("an operator setting is stored", func(t *testing.T) {
		assert.Equal(t, http.StatusOK, set(t, "spirit_debug_logs", "true"))

		stored, err := stor.Settings().Get(ctx, "spirit_debug_logs")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "true", stored.Value)
	})
}
