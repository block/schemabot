package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLDialectExcludedValue(t *testing.T) {
	assert.Equal(t, "VALUES(setting_value)", MySQLDialect{}.ExcludedValue("setting_value"))
}

func TestMySQLDialectInsertIfAbsent(t *testing.T) {
	assert.Equal(t, InsertIfAbsentSyntax{Modifier: " IGNORE"}, MySQLDialect{}.InsertIfAbsent([]string{"apply_id", "comment_state"}))
}

func TestMySQLDialectJSONBooleanIsTrue(t *testing.T) {
	assert.Equal(t,
		`(JSON_EXTRACT(a.options, '$."defer_cutover"') <=> CAST('true' AS JSON))`,
		MySQLDialect{}.JSONBooleanIsTrue("a.options", []string{"defer_cutover"}),
	)
}

func TestMySQLDialectJSONBooleanIsTrueRejectsNonIdentifierKey(t *testing.T) {
	require.PanicsWithValue(t,
		`sqlstore: JSON path key "defer-cutover" is not a plain identifier`,
		func() { MySQLDialect{}.JSONBooleanIsTrue("a.options", []string{"defer-cutover"}) },
	)
}

func TestMySQLDialectIndexHint(t *testing.T) {
	assert.Equal(t, " FORCE INDEX (`idx_database_env_deployment`)", MySQLDialect{}.IndexHint("idx_database_env_deployment"))
}

func TestMySQLDialectJoinedUpdate(t *testing.T) {
	assert.Equal(t,
		"UPDATE apply_comments c JOIN applies a ON a.id = c.apply_id SET c.edit_count = c.edit_count + 1, c.updated_at = NOW() WHERE c.apply_id = ? AND a.lease_token = ?",
		MySQLDialect{}.JoinedUpdate(
			"apply_comments", "c", "applies", "a", "a.id = c.apply_id",
			[]JoinedUpdateAssignment{
				{Column: "edit_count", Expr: "c.edit_count + 1"},
				{Column: "updated_at", Expr: "NOW()"},
			},
			"c.apply_id = ? AND a.lease_token = ?",
		),
	)
	// A placeholder assignment expression must render ahead of the predicate's
	// placeholders, pinning the interface's assignments-then-predicate argument
	// order at the rendering level.
	assert.Equal(t,
		"UPDATE apply_control_requests cr JOIN applies a ON a.id = cr.apply_id SET cr.status = ?, cr.completed_at = COALESCE(cr.completed_at, NOW()) WHERE cr.apply_id = ? AND a.lease_token = ?",
		MySQLDialect{}.JoinedUpdate(
			"apply_control_requests", "cr", "applies", "a", "a.id = cr.apply_id",
			[]JoinedUpdateAssignment{
				{Column: "status", Expr: "?"},
				{Column: "completed_at", Expr: "COALESCE(cr.completed_at, NOW())"},
			},
			"cr.apply_id = ? AND a.lease_token = ?",
		),
	)
}

func TestMySQLDialectJoinedUpdateRequiresAssignments(t *testing.T) {
	require.PanicsWithValue(t, "sqlstore: JoinedUpdate requires at least one assignment", func() {
		MySQLDialect{}.JoinedUpdate("apply_comments", "c", "applies", "a", "a.id = c.apply_id", nil, "c.apply_id = ?")
	})
}

// A placeholder in the join condition would bind its argument into a
// dialect-dependent position, silently shifting every subsequent binding, so
// the seam rejects it outright.
func TestMySQLDialectJoinedUpdateRejectsJoinConditionPlaceholders(t *testing.T) {
	require.PanicsWithValue(t, "sqlstore: JoinedUpdate joinCondition must not contain bind placeholders", func() {
		MySQLDialect{}.JoinedUpdate(
			"apply_comments", "c", "applies", "a", "a.id = c.apply_id AND a.state = ?",
			[]JoinedUpdateAssignment{{Column: "state", Expr: "?"}},
			"c.apply_id = ?",
		)
	})
}

// The dialect qualifies assignment columns with the target alias itself, so a
// pre-qualified column would render an invalid double-qualified reference and
// break on dialects with different assignment syntax.
func TestMySQLDialectJoinedUpdateRejectsQualifiedAssignmentColumns(t *testing.T) {
	require.PanicsWithValue(t, "sqlstore: JoinedUpdate assignment columns must be unqualified", func() {
		MySQLDialect{}.JoinedUpdate(
			"apply_comments", "c", "applies", "a", "a.id = c.apply_id",
			[]JoinedUpdateAssignment{{Column: "c.state", Expr: "?"}},
			"c.apply_id = ?",
		)
	})
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

func TestMySQLDialectCurrentTimestamp(t *testing.T) {
	d := MySQLDialect{}
	assert.Equal(t, "NOW()", d.CurrentTimestamp(TimestampPrecisionDefault))
	assert.Equal(t, "NOW(6)", d.CurrentTimestamp(TimestampPrecisionMicrosecond))
}

// RelativeTime must render the exact MySQL expressions the store used before the
// dialect seam, including the DATE_ADD form for additions and a single "?"
// placeholder for parameterized magnitudes.
func TestMySQLDialectRelativeTime(t *testing.T) {
	d := MySQLDialect{}

	tests := []struct {
		name      string
		precision TimestampPrecision
		direction RelativeTimeDirection
		amount    IntervalAmount
		unit      IntervalUnit
		want      string
	}{
		{
			name:      "literal minute before",
			precision: TimestampPrecisionDefault,
			direction: BeforeCurrentTime,
			amount:    LiteralIntervalAmount(1),
			unit:      IntervalMinute,
			want:      "NOW() - INTERVAL 1 MINUTE",
		},
		{
			name:      "literal hour before",
			precision: TimestampPrecisionDefault,
			direction: BeforeCurrentTime,
			amount:    LiteralIntervalAmount(1),
			unit:      IntervalHour,
			want:      "NOW() - INTERVAL 1 HOUR",
		},
		{
			name:      "parameterized second before",
			precision: TimestampPrecisionDefault,
			direction: BeforeCurrentTime,
			amount:    ParameterIntervalAmount(),
			unit:      IntervalSecond,
			want:      "NOW() - INTERVAL ? SECOND",
		},
		{
			name:      "parameterized day before",
			precision: TimestampPrecisionDefault,
			direction: BeforeCurrentTime,
			amount:    ParameterIntervalAmount(),
			unit:      IntervalDay,
			want:      "NOW() - INTERVAL ? DAY",
		},
		{
			name:      "parameterized microsecond after at microsecond precision",
			precision: TimestampPrecisionMicrosecond,
			direction: AfterCurrentTime,
			amount:    ParameterIntervalAmount(),
			unit:      IntervalMicrosecond,
			want:      "DATE_ADD(NOW(6), INTERVAL ? MICROSECOND)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := d.RelativeTime(tc.precision, tc.direction, tc.amount, tc.unit)
			assert.Equal(t, tc.want, got)
			if tc.amount.parameterized {
				assert.Equal(t, 1, countPlaceholders(got), "parameterized amount must emit exactly one ?")
			} else {
				assert.Equal(t, 0, countPlaceholders(got), "literal amount must emit no ?")
			}
		})
	}
}

func countPlaceholders(s string) int {
	n := 0
	for _, r := range s {
		if r == '?' {
			n++
		}
	}
	return n
}
