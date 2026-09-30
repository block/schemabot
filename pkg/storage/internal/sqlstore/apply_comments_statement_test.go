package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// applyCommentUpsertStatement renders the comment upsert for each dialect with
// and without an apply lease. These tests pin the exact SQL: the five inserted
// columns bind first, the leased rendering then binds the apply ID and token,
// the conflict arm renews updated_at and clears superseded_at on both dialects,
// and the leased rendering locks the applies row it selects from, with a share
// lock on MySQL and an update lock on PostgreSQL, so the token check does not
// depend on the session's isolation level.
func TestApplyCommentUpsertStatement(t *testing.T) {
	const insert = "INSERT INTO apply_comments (apply_id, comment_state, github_comment_id, posted_phase, pending_freeze_github_comment_id) "
	const mysqlConflict = "ON DUPLICATE KEY UPDATE github_comment_id = VALUES(github_comment_id), posted_phase = VALUES(posted_phase), pending_freeze_github_comment_id = VALUES(pending_freeze_github_comment_id), superseded_at = NULL, updated_at = NOW()"
	const postgresConflict = "ON CONFLICT (apply_id, comment_state) DO UPDATE SET github_comment_id = EXCLUDED.github_comment_id, posted_phase = EXCLUDED.posted_phase, pending_freeze_github_comment_id = EXCLUDED.pending_freeze_github_comment_id, superseded_at = NULL, updated_at = NOW()"

	tests := []struct {
		name    string
		dialect Dialect
		leased  bool
		want    string
	}{
		{
			name:    "no lease inserts the values on MySQL",
			dialect: MySQLDialect{},
			want:    insert + "VALUES (?, ?, ?, ?, ?) " + mysqlConflict,
		},
		{
			name:    "no lease inserts the values on PostgreSQL",
			dialect: PostgresDialect{},
			want:    insert + "VALUES (?, ?, ?, ?, ?) " + postgresConflict,
		},
		{
			name:    "apply lease selects from the share-locked applies row on MySQL",
			dialect: MySQLDialect{},
			leased:  true,
			want:    insert + "SELECT ?, ?, ?, ?, ? FROM applies a WHERE a.id = ? AND a.id = (SELECT fence.id FROM applies fence WHERE fence.id = a.id AND fence.lease_token = ? FOR SHARE) " + mysqlConflict,
		},
		{
			name:    "apply lease selects from the update-locked applies row on PostgreSQL",
			dialect: PostgresDialect{},
			leased:  true,
			want:    insert + "SELECT ?, ?, ?, ?, ? FROM applies a WHERE a.id = ? AND a.id = (SELECT fence.id FROM applies fence WHERE fence.id = a.id AND fence.lease_token = ? FOR UPDATE) " + postgresConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, applyCommentUpsertStatement(tc.dialect, tc.leased))
		})
	}
}
