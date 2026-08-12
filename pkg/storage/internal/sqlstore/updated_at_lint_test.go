package sqlstore

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var upsertClauseRe = regexp.MustCompile(`\.UpsertClause\(`)
var createTableNameRe = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+` + "`" + `([^` + "`" + `]+)` + "`")
var updatedAtColumnRe = regexp.MustCompile(`(?m)^\s*` + "`updated_at`" + `\s+`)

type timestampedWrite struct {
	loc    []int
	upsert bool
}

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
// starts after the preceding timestamped write in the enclosing function and
// ends at the statement's WHERE clause or ExecContext call. This keeps nearby
// variable-assembled clauses in scope without letting one write's stamp satisfy
// a later write.
func TestTimestampedTableWritesStampUpdatedAt(t *testing.T) {
	timestampedTables := timestampedMySQLTables(t)
	require.NotEmpty(t, timestampedTables, "expected timestamped tables in the MySQL schema")
	quotedTables := make([]string, 0, len(timestampedTables))
	for _, table := range timestampedTables {
		quotedTables = append(quotedTables, regexp.QuoteMeta(table))
	}
	timestampedTableUpdateRe := regexp.MustCompile(`UPDATE\s+(?:` + strings.Join(quotedTables, "|") + `)\b`)

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

		var writes []timestampedWrite
		for _, loc := range timestampedTableUpdateRe.FindAllStringIndex(src, -1) {
			writes = append(writes, timestampedWrite{loc: loc})
		}
		for _, loc := range upsertClauseRe.FindAllStringIndex(src, -1) {
			writes = append(writes, timestampedWrite{loc: loc, upsert: true})
		}
		sort.Slice(writes, func(i, j int) bool { return writes[i].loc[0] < writes[j].loc[0] })

		previousWindowEnd := 0
		for _, write := range writes {
			checked++
			loc := write.loc
			line := 1 + strings.Count(src[:loc[0]], "\n")
			funcIdx := strings.LastIndex(src[:loc[0]], "\nfunc ")
			require.GreaterOrEqual(t, funcIdx, 0,
				"%s:%d: timestamped write outside a function body", name, line)
			windowStart := max(funcIdx, previousWindowEnd)

			if write.upsert {
				execIdx := strings.Index(src[loc[1]:], "ExecContext")
				require.GreaterOrEqual(t, execIdx, 0,
					"%s:%d: upsert clause without an ExecContext call", name, line)
				previousWindowEnd = loc[1] + execIdx
				assert.Contains(t, src[windowStart:previousWindowEnd], "updated_at",
					"%s:%d: upsert must assign updated_at in its conflict branch", name, line)
				continue
			}

			whereIdx := strings.Index(src[loc[1]:], "WHERE")
			require.GreaterOrEqual(t, whereIdx, 0,
				"%s:%d: UPDATE on a timestamped table without a WHERE clause", name, line)
			previousWindowEnd = loc[1] + whereIdx
			assert.Contains(t, src[windowStart:previousWindowEnd], "updated_at",
				"%s:%d: UPDATE on a timestamped table must assign updated_at in its SET clause; "+
					"no dialect-level default may be relied on", name, line)
		}
	}
	require.NotZero(t, checked, "expected UPDATE or upsert statements on timestamped tables in this package")
}

func timestampedMySQLTables(t *testing.T) []string {
	t.Helper()
	entries, err := schema.MySQLFS.ReadDir("mysql")
	require.NoError(t, err)

	var tables []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		content, err := schema.MySQLFS.ReadFile("mysql/" + entry.Name())
		require.NoError(t, err)
		if !updatedAtColumnRe.Match(content) {
			continue
		}
		match := createTableNameRe.FindSubmatch(content)
		require.Len(t, match, 2, "%s must contain one backtick-quoted CREATE TABLE name", entry.Name())
		tables = append(tables, string(match[1]))
	}
	sort.Strings(tables)
	return tables
}
