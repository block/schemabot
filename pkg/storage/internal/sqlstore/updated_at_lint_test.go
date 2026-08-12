package sqlstore

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heartbeatTableUpdateRe matches UPDATE statements against the two tables
// whose updated_at column is a liveness signal: apply_operations (operation
// lease heartbeat) and apply_comments (summary-claim freshness).
var heartbeatTableUpdateRe = regexp.MustCompile(`UPDATE\s+apply_(?:operations|comments)\b`)

// TestHeartbeatTableUpdatesStampUpdatedAt asserts that every UPDATE statement
// in this package against apply_operations or apply_comments assigns
// updated_at in its SET clause. Stamping these columns is the application's
// responsibility on every dialect: the staleness predicates
// (FindNextApplyOperation's stale-active arm, ReclaimStaleSummaryClaim) read
// updated_at as the liveness signal, and a write that skips the stamp freezes
// the heartbeat on dialects without automatic renewal — making an actively
// driven claim look abandoned. MySQL's ON UPDATE CURRENT_TIMESTAMP masks a
// missing stamp for any value-changing write, so the behavioral integration
// tests cannot catch a dropped assignment there; this source scan can.
//
// A deliberate non-NOW() assignment (such as a release-time stale backdate)
// still satisfies the invariant: the statement owns the column's value rather
// than leaving it to schema defaults.
//
// Some statements assemble their SET clause in a variable above the SQL
// template (per-guard qualified/unqualified variants), so the scan window
// runs from the start of the enclosing function to the statement's WHERE
// clause rather than covering the SQL literal alone.
func TestHeartbeatTableUpdatesStampUpdatedAt(t *testing.T) {
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

		for _, loc := range heartbeatTableUpdateRe.FindAllStringIndex(src, -1) {
			checked++
			line := 1 + strings.Count(src[:loc[0]], "\n")
			funcIdx := strings.LastIndex(src[:loc[0]], "\nfunc ")
			require.GreaterOrEqual(t, funcIdx, 0,
				"%s:%d: UPDATE on a heartbeat table outside a function body", name, line)
			rest := src[loc[1]:]
			whereIdx := strings.Index(rest, "WHERE")
			require.GreaterOrEqual(t, whereIdx, 0,
				"%s:%d: UPDATE on a heartbeat table without a WHERE clause", name, line)
			window := src[funcIdx:loc[1]] + rest[:whereIdx]
			assert.Contains(t, window, "updated_at",
				"%s:%d: UPDATE on a heartbeat table must assign updated_at in its SET clause; "+
					"the staleness predicates read it as the liveness signal and no dialect-level "+
					"default may be relied on", name, line)
		}
	}
	require.NotZero(t, checked, "expected UPDATE statements on apply_operations/apply_comments in this package")
}
