package mysqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLDialectExcludedValue(t *testing.T) {
	assert.Equal(t, "VALUES(setting_value)", MySQLDialect{}.ExcludedValue("setting_value"))
}

// UpsertClause must produce a MySQL ON DUPLICATE KEY UPDATE clause that matches
// the hand-written SQL the store used before the dialect seam, including the
// column set, ordering, defaulted excluded values, and custom expressions. The
// conflict columns are accepted but not rendered, since MySQL resolves the
// conflict against the table's unique keys.
func TestMySQLDialectUpsertClause(t *testing.T) {
	d := MySQLDialect{}

	tests := []struct {
		name        string
		conflict    []string
		assignments []UpsertAssignment
		want        string
	}{
		{
			name:        "single defaulted column",
			conflict:    []string{"setting_key"},
			assignments: []UpsertAssignment{{Column: "setting_value"}},
			want:        "ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)",
		},
		{
			name:     "defaulted columns with a literal expression",
			conflict: []string{"apply_id", "comment_state"},
			assignments: []UpsertAssignment{
				{Column: "github_comment_id"},
				{Column: "posted_volume"},
				{Column: "pending_freeze_github_comment_id"},
				{Column: "superseded_at", Expr: "NULL"},
			},
			want: "ON DUPLICATE KEY UPDATE github_comment_id = VALUES(github_comment_id), " +
				"posted_volume = VALUES(posted_volume), " +
				"pending_freeze_github_comment_id = VALUES(pending_freeze_github_comment_id), " +
				"superseded_at = NULL",
		},
		{
			name:     "custom expression referencing the excluded value",
			conflict: []string{"repository", "pull_request", "environment", "database_type", "database_name"},
			assignments: []UpsertAssignment{
				{Column: "head_sha"},
				{Column: "change_summary", Expr: "COALESCE(NULLIF(" + d.ExcludedValue("change_summary") + ", ''), change_summary)"},
			},
			want: "ON DUPLICATE KEY UPDATE head_sha = VALUES(head_sha), " +
				"change_summary = COALESCE(NULLIF(VALUES(change_summary), ''), change_summary)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, d.UpsertClause(tc.conflict, tc.assignments))
		})
	}
}
