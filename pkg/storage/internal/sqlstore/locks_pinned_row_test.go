package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

// A release pinned to lock row 7, held by org/repo#42 for plan-1, deletes the
// row only when the row it locked is that same row, and otherwise names what
// differs. Owner and pending plan compare byte-exact, so a row that differs
// only in case is a different intent.
func TestPinnedLockRowRefusal(t *testing.T) {
	held := &storage.Lock{ID: 7, Owner: "org/repo#42", PendingPlanID: "plan-1"}

	assert.NoError(t, pinnedLockRowRefusal(held, 7, "org/repo#42", "plan-1"))
	for _, tc := range []struct {
		name          string
		id            int64
		owner, planID string
		want          error
	}{
		{"different row id", 8, "org/repo#42", "plan-1", storage.ErrLockReplaced},
		{"different row id wins over a different owner", 8, "org/repo#43", "plan-2", storage.ErrLockReplaced},
		{"different owner", 7, "org/repo#43", "plan-1", storage.ErrLockNotOwned},
		{"owner is byte-exact", 7, "ORG/repo#42", "plan-1", storage.ErrLockNotOwned},
		{"different plan", 7, "org/repo#42", "plan-2", storage.ErrLockIntentChanged},
		{"plan is byte-exact", 7, "org/repo#42", "PLAN-1", storage.ErrLockIntentChanged},
		{"empty plan differs", 7, "org/repo#42", "", storage.ErrLockIntentChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, pinnedLockRowRefusal(held, tc.id, tc.owner, tc.planID), tc.want)
		})
	}
}
