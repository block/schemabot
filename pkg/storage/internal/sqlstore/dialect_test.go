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

func TestPostgresDialectRebind(t *testing.T) {
	d := PostgresDialect{}
	assert.Equal(t, "SELECT $1, '$2?', 'it''s ?', \"why?\", $$body ?$$, $tag$also ?$tag$ -- ?\nWHERE id = $2",
		d.Rebind("SELECT ?, '$2?', 'it''s ?', \"why?\", $$body ?$$, $tag$also ?$tag$ -- ?\nWHERE id = ?"))
	// Question marks inside block comments (including nested ones) are prose,
	// not placeholders, so they must not consume ordinals.
	assert.Equal(t, "SELECT 1 /* why? /* nested? */ still? */ WHERE id = $1",
		d.Rebind("SELECT 1 /* why? /* nested? */ still? */ WHERE id = ?"))
	// An unterminated block comment consumes the rest of the query, matching
	// the server's tokenization.
	assert.Equal(t, "SELECT 1 /* dangling ? WHERE id = ?",
		d.Rebind("SELECT 1 /* dangling ? WHERE id = ?"))
	// A dollar sign continuing an identifier does not open a dollar-quoted
	// string, so later placeholders still rebind.
	assert.Equal(t, "SELECT abc$x$ FROM t WHERE id = $1",
		d.Rebind("SELECT abc$x$ FROM t WHERE id = ?"))
}

func TestPostgresDialect(t *testing.T) {
	d := PostgresDialect{}
	assert.Equal(t, "EXCLUDED.setting_value", d.ExcludedValue("setting_value"))
	assert.Equal(t, "ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value",
		d.UpsertClause([]string{"setting_key"}, []UpsertAssignment{{Column: "setting_value"}}))
	assert.Equal(t, "now()", d.CurrentTimestamp(TimestampPrecisionDefault))
	assert.Equal(t, "now()", d.CurrentTimestamp(TimestampPrecisionMicrosecond))
	assert.Equal(t, "now() - ? * interval '1 second'",
		d.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalSecond))
	assert.Equal(t, "now() + 2 * interval '1 day'",
		d.RelativeTime(TimestampPrecisionDefault, AfterCurrentTime, LiteralIntervalAmount(2), IntervalDay))
}

func TestPostgresDialectInsertIfAbsent(t *testing.T) {
	assert.Equal(t,
		InsertIfAbsentSyntax{Suffix: " ON CONFLICT (apply_id, comment_state) DO NOTHING"},
		PostgresDialect{}.InsertIfAbsent([]string{"apply_id", "comment_state"}),
	)
}

func TestPostgresDialectJSONBooleanIsTrue(t *testing.T) {
	assert.Equal(t,
		`(jsonb_extract_path(a.options, 'defer_cutover') IS NOT DISTINCT FROM 'true'::jsonb)`,
		PostgresDialect{}.JSONBooleanIsTrue("a.options", []string{"defer_cutover"}),
	)
	assert.Equal(t,
		`(jsonb_extract_path(a.options, 'outer', 'inner_key') IS NOT DISTINCT FROM 'true'::jsonb)`,
		PostgresDialect{}.JSONBooleanIsTrue("a.options", []string{"outer", "inner_key"}),
	)
}

func TestPostgresDialectJSONBooleanIsTrueRejectsNonIdentifierKey(t *testing.T) {
	require.PanicsWithValue(t,
		`sqlstore: JSON path key "it's" is not a plain identifier`,
		func() { PostgresDialect{}.JSONBooleanIsTrue("a.options", []string{"outer", "it's"}) },
	)
}

func TestPostgresDialectIndexHint(t *testing.T) {
	assert.Empty(t, PostgresDialect{}.IndexHint("idx_database_env_deployment"))
}

// The PostgreSQL joined UPDATE moves the join condition into the WHERE clause
// (UPDATE … FROM has no ON clause) while keeping SET assignments before the
// predicate, so placeholder ordinals line up with the MySQL rendering's
// argument order.
func TestPostgresDialectJoinedUpdate(t *testing.T) {
	assert.Equal(t,
		"UPDATE apply_comments c SET edit_count = c.edit_count + 1, updated_at = NOW() FROM applies a WHERE (a.id = c.apply_id) AND (c.apply_id = ? AND a.lease_token = ?)",
		PostgresDialect{}.JoinedUpdate(
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
		"UPDATE apply_control_requests cr SET status = ?, completed_at = COALESCE(cr.completed_at, NOW()) FROM applies a WHERE (a.id = cr.apply_id) AND (cr.apply_id = ? AND a.lease_token = ?)",
		PostgresDialect{}.JoinedUpdate(
			"apply_control_requests", "cr", "applies", "a", "a.id = cr.apply_id",
			[]JoinedUpdateAssignment{
				{Column: "status", Expr: "?"},
				{Column: "completed_at", Expr: "COALESCE(cr.completed_at, NOW())"},
			},
			"cr.apply_id = ? AND a.lease_token = ?",
		),
	)
}

func TestPostgresDialectJoinedUpdateRequiresAssignments(t *testing.T) {
	require.PanicsWithValue(t, "sqlstore: JoinedUpdate requires at least one assignment", func() {
		PostgresDialect{}.JoinedUpdate("apply_comments", "c", "applies", "a", "a.id = c.apply_id", nil, "c.apply_id = ?")
	})
}
