//go:build integration

package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// The behavioral suite for SettingsStore lives in pkg/storage/storagetest and
// runs against every dialect via parity_test.go. The test here covers only
// MySQL changed-rows behavior, which requires a connection configuration the
// storage interface cannot express.

// TestSettingsStore_CompareAndSetUnderChangedRows runs CompareAndSet over the
// changed-rows connection production uses, where rewriting a row with the
// value it already holds reports zero affected rows. Rewriting the matched
// value still reports success, and a stale previous still loses even when the
// value it would write is the one already stored.
func TestSettingsStore_CompareAndSetUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	settings := newChangedRowsStore(t).Settings()

	require.NoError(t, settings.Set(ctx, "cas_changed_rows", "v1"))
	current, err := settings.Get(ctx, "cas_changed_rows")
	require.NoError(t, err)
	require.NotNil(t, current)

	swapped, err := settings.CompareAndSet(ctx, "cas_changed_rows", current, "v1")
	require.NoError(t, err)
	require.True(t, swapped, "rewriting the stored value is a successful no-op")

	swapped, err = settings.CompareAndSet(ctx, "cas_changed_rows", &storage.Setting{Key: "cas_changed_rows", Value: "stale"}, "v1")
	require.NoError(t, err)
	require.False(t, swapped, "a previous that no longer matches loses even when the new value equals the stored one")

	swapped, err = settings.CompareAndSet(ctx, "cas_changed_rows", current, "v2")
	require.NoError(t, err)
	require.True(t, swapped, "a matching previous advances the value")
	updated, err := settings.Get(ctx, "cas_changed_rows")
	require.NoError(t, err)
	require.NotNil(t, updated)
	require.Equal(t, "v2", updated.Value)
}
