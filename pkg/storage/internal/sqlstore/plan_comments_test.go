//go:build integration

package sqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage/storagetest"
)

// TestPlanCommentStore_MarkMinimizedPreservesStamp reads the raw minimized_at
// column to prove a repeat mark does not move the original stamp. The row is
// backdated between marks because the column's whole-second precision would
// otherwise let a re-stamp within the same second pass unnoticed.
func TestPlanCommentStore_MarkMinimizedPreservesStamp(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	comment := storagetest.InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha1", 100)

	require.NoError(t, store.PlanComments().MarkMinimized(ctx, comment.ID))

	var minimizedAt *time.Time
	require.NoError(t, testDB.QueryRowContext(ctx,
		"SELECT minimized_at FROM plan_comments WHERE id = ?", comment.ID).Scan(&minimizedAt))
	require.NotNil(t, minimizedAt, "the row is stamped, not deleted")

	_, err := testDB.ExecContext(ctx,
		"UPDATE plan_comments SET minimized_at = DATE_SUB(NOW(), INTERVAL 1 HOUR) WHERE id = ?", comment.ID)
	require.NoError(t, err)
	var backdated *time.Time
	require.NoError(t, testDB.QueryRowContext(ctx,
		"SELECT minimized_at FROM plan_comments WHERE id = ?", comment.ID).Scan(&backdated))
	require.NotNil(t, backdated)

	require.NoError(t, store.PlanComments().MarkMinimized(ctx, comment.ID))

	var afterRepeat *time.Time
	require.NoError(t, testDB.QueryRowContext(ctx,
		"SELECT minimized_at FROM plan_comments WHERE id = ?", comment.ID).Scan(&afterRepeat))
	require.NotNil(t, afterRepeat)
	assert.Equal(t, *backdated, *afterRepeat, "a repeat mark must not move the stamp")
}
