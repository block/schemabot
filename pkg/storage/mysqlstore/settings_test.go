//go:build integration

package mysqlstore

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func TestSettingsStore_Get(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Get non-existent should return nil
	setting, err := store.Settings().Get(ctx, "test_key")
	require.NoError(t, err)
	require.Nil(t, setting)

	// Create a setting
	require.NoError(t, store.Settings().Set(ctx, "test_key", "test_value"))

	// Get should return the setting
	setting, err = store.Settings().Get(ctx, "test_key")
	require.NoError(t, err)
	require.NotNil(t, setting)
	require.Equal(t, "test_key", setting.Key)
	require.Equal(t, "test_value", setting.Value)
}

func TestSettingsStore_Set(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Set should create
	require.NoError(t, store.Settings().Set(ctx, "test_key", "test_value"))

	// Set again should update (upsert)
	require.NoError(t, store.Settings().Set(ctx, "test_key", "updated_value"))

	// Verify update
	setting, err := store.Settings().Get(ctx, "test_key")
	require.NoError(t, err)
	require.Equal(t, "updated_value", setting.Value)
}

func TestSettingsStore_List(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Empty should return empty slice
	settings, err := store.Settings().List(ctx)
	require.NoError(t, err)
	require.Empty(t, settings)

	// Create some settings
	keys := []string{"aaa", "bbb", "ccc"}
	for _, key := range keys {
		require.NoError(t, store.Settings().Set(ctx, key, "value-"+key))
	}

	// List should return all settings, sorted by key
	settings, err = store.Settings().List(ctx)
	require.NoError(t, err)
	require.Len(t, settings, 3)
	for i, key := range keys {
		require.Equal(t, key, settings[i].Key)
	}
}

func TestSettingsStore_Delete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create setting
	require.NoError(t, store.Settings().Set(ctx, "delete_me", "value"))

	// Delete should succeed
	require.NoError(t, store.Settings().Delete(ctx, "delete_me"))

	// Verify deleted
	setting, err := store.Settings().Get(ctx, "delete_me")
	require.NoError(t, err)
	require.Nil(t, setting)

	// Delete non-existent should fail
	require.ErrorIs(t, store.Settings().Delete(ctx, "nonexistent"), storage.ErrSettingNotFound)
}

func TestSettingsStore_JSONValues(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	jsonValue := `{"max_concurrent": 5, "timeout": 300}`
	require.NoError(t, store.Settings().Set(ctx, "rate_limits", jsonValue))

	setting, err := store.Settings().Get(ctx, "rate_limits")
	require.NoError(t, err)
	require.Equal(t, jsonValue, setting.Value)
}

func TestSettingsStore_Get_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Settings().Get(t.Context(), "key")
	require.Error(t, err)
}

func TestSettingsStore_List_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Settings().List(t.Context())
	require.Error(t, err)
}

func TestSettingsStore_Delete_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Settings().Delete(t.Context(), "key")
	require.Error(t, err)
}
