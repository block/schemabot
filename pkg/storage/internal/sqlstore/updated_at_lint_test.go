package sqlstore

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// timestampedTableUpdateRe matches UPDATE statements against storage tables
// whose schema has an updated_at column.
var timestampedTableUpdateRe = regexp.MustCompile(`UPDATE\s+(?:applies|apply_comments|apply_control_requests|apply_operations|apply_target_locks|checks|locks|plan_comments|settings|tasks|webhook_events)\b`)

var upsertClauseRe = regexp.MustCompile(`\.UpsertClause\(`)

// TestTimestampedTableWritesStampUpdatedAt asserts that every UPDATE and upsert
// in this package against a table with updated_at assigns the column explicitly.
// Stamping these columns is the application's responsibility on every dialect.
// MySQL's ON UPDATE CURRENT_TIMESTAMP masks a missing stamp for any
// value-changing write, so behavioral integration tests cannot catch a dropped
// assignment there; this source scan can.
//
// A deliberate non-NOW() assignment (such as a release-time stale backdate)
// still satisfies the invariant: the statement owns the column's value rather
// than leaving it to schema defaults.
//
// Some statements assemble their SET clause in a variable above the SQL
// template (per-guard qualified/unqualified variants), so the scan window
// runs from the start of the enclosing function to the statement's WHERE
// clause rather than covering the SQL literal alone.
func TestTimestampedTableWritesStampUpdatedAt(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		content, err := os.ReadFile(name)
		require.NoError(t, err)
		src := string(content)

		for _, loc := range timestampedTableUpdateRe.FindAllStringIndex(src, -1) {
			checked++
			line := 1 + strings.Count(src[:loc[0]], "\n")
			funcIdx := strings.LastIndex(src[:loc[0]], "\nfunc ")
			require.GreaterOrEqual(t, funcIdx, 0,
				"%s:%d: UPDATE on a timestamped table outside a function body", name, line)
			rest := src[loc[1]:]
			whereIdx := strings.Index(rest, "WHERE")
			require.GreaterOrEqual(t, whereIdx, 0,
				"%s:%d: UPDATE on a heartbeat table without a WHERE clause", name, line)
			window := src[funcIdx:loc[1]] + rest[:whereIdx]
			assert.Contains(t, window, "updated_at",
				"%s:%d: UPDATE on a timestamped table must assign updated_at in its SET clause; "+
					"no dialect-level default may be relied on", name, line)
		}

		for _, loc := range upsertClauseRe.FindAllStringIndex(src, -1) {
			checked++
			line := 1 + strings.Count(src[:loc[0]], "\n")
			funcIdx := strings.LastIndex(src[:loc[0]], "\nfunc ")
			require.GreaterOrEqual(t, funcIdx, 0,
				"%s:%d: upsert clause outside a function body", name, line)
			execIdx := strings.Index(src[loc[1]:], "ExecContext")
			require.GreaterOrEqual(t, execIdx, 0,
				"%s:%d: upsert clause without an ExecContext call", name, line)
			window := src[funcIdx : loc[1]+execIdx]
			assert.Contains(t, window, "updated_at",
				"%s:%d: upsert must assign updated_at in its conflict branch", name, line)
		}
	}
	require.NotZero(t, checked, "expected UPDATE or upsert statements on timestamped tables in this package")
}
