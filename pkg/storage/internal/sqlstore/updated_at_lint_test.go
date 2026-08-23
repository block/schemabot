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
	joined bool
}

// TestTimestampedTableWritesStampUpdatedAt asserts that every UPDATE, upsert,
// and dialect-built joined UPDATE in this package against a table with
// updated_at assigns the column explicitly. Stamping these columns is the
// application's responsibility on every dialect. MySQL's ON UPDATE
// CURRENT_TIMESTAMP masks a missing stamp for any value-changing write, so
// behavioral integration tests cannot catch a dropped assignment there; this
// source scan can.
//
// A deliberate non-NOW() assignment (such as a release-time stale backdate)
// still satisfies the invariant: the statement owns the column's value rather
// than leaving it to schema defaults.
//
// Some UPDATE statements assemble their SET clause in a variable above the SQL
// template (per-guard qualified/unqualified variants), so their scan window
// starts after the preceding timestamped write in the enclosing function and
// ends at the statement's WHERE clause. This keeps nearby variable-assembled
// clauses in scope without letting one write's stamp satisfy a later write.
// Upserts are scanned more tightly: the assignment must appear inside the
// UpsertClause call's argument list, so unrelated mentions of the column
// elsewhere in the function cannot satisfy the check.
//
// Joined UPDATEs rendered through Dialect.JoinedUpdate never contain a literal
// "UPDATE <table>" in source, so they get their own scan. A call that writes
// its assignments inline must include the stamp among them, so an unrelated
// updated_at literal earlier in a long function cannot mask a dropped stamp;
// only a call that assembles its assignments in a variable above widens the
// scan to the enclosing function, bounded by the preceding timestamped write
// like the UPDATE scan. Such an assembled clause is shared with sibling
// renderings in the same function, so unlike other writes it does not advance
// the window boundary past itself.
func TestTimestampedTableWritesStampUpdatedAt(t *testing.T) {
	timestampedTables := timestampedMySQLTables(t)
	require.NotEmpty(t, timestampedTables, "expected timestamped tables in the MySQL schema")
	quotedTables := make([]string, 0, len(timestampedTables))
	for _, table := range timestampedTables {
		quotedTables = append(quotedTables, regexp.QuoteMeta(table))
	}
	timestampedTableUpdateRe := regexp.MustCompile(`UPDATE\s+(?:` + strings.Join(quotedTables, "|") + `)\b`)
	joinedUpdateRe := regexp.MustCompile(`\.JoinedUpdate\(\s*"(?:` + strings.Join(quotedTables, "|") + `)"`)

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
		for _, loc := range joinedUpdateRe.FindAllStringIndex(src, -1) {
			writes = append(writes, timestampedWrite{loc: loc, joined: true})
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
				argsEnd := balancedParenEnd(src, loc[1])
				require.Greater(t, argsEnd, loc[1],
					"%s:%d: unterminated UpsertClause call", name, line)
				previousWindowEnd = argsEnd
				assert.Contains(t, src[loc[1]:argsEnd], "updated_at",
					"%s:%d: upsert must assign updated_at in its conflict assignments", name, line)
				continue
			}

			if write.joined {
				argsEnd := balancedParenEnd(src, loc[1])
				require.Greater(t, argsEnd, loc[1],
					"%s:%d: unterminated JoinedUpdate call", name, line)
				window := src[loc[0]:argsEnd]
				if strings.Contains(window, "JoinedUpdateAssignment{") {
					previousWindowEnd = argsEnd
				} else {
					// A variable-assembled clause is shared with sibling
					// renderings in the same function (per-guard variants), so
					// it stays visible to the writes that follow.
					window = src[windowStart:argsEnd]
				}
				assert.Contains(t, window, "updated_at",
					"%s:%d: joined UPDATE on a timestamped table must include an updated_at assignment; "+
						"no dialect-level default may be relied on", name, line)
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

// balancedParenEnd returns the index just past the parenthesis that closes the
// call whose opening parenthesis ends at start, or -1 when the call is never
// closed. Parentheses inside string literals are counted too, which is correct
// for SQL expression fragments: a valid SQL expression's parentheses balance
// across the argument list even when individual fragments are unbalanced.
func balancedParenEnd(src string, start int) int {
	depth := 1
	for i := start; i < len(src); i++ {
		switch src[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
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
