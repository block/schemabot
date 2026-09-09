package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
)

func TestWriteSQLFencedBlock(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected string
	}{
		{
			name:     "plain DDL",
			content:  "CREATE TABLE t (id int);",
			expected: "```sql\nCREATE TABLE t (id int);\n```\n",
		},
		{
			name:     "three backticks",
			content:  "CREATE TABLE \"x\n```\n# injected\" (id int);",
			expected: "````sql\nCREATE TABLE \"x\n```\n# injected\" (id int);\n````\n",
		},
		{
			name:     "seven backticks",
			content:  "SELECT '```````';",
			expected: "````````sql\nSELECT '```````';\n````````\n",
		},
		{
			name:     "trailing newline is not doubled",
			content:  "CREATE TABLE t (id int);\n",
			expected: "```sql\nCREATE TABLE t (id int);\n```\n",
		},
		{
			name:     "empty content",
			content:  "",
			expected: "```sql\n```\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out strings.Builder
			writeSQLFencedBlock(&out, tt.content)

			assert.Equal(t, tt.expected, out.String())
			lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
			openingLength := maxBacktickRun(lines[0])
			assert.Equal(t, strings.Repeat("`", openingLength), lines[len(lines)-1])
			assert.Greater(t, openingLength, maxBacktickRun(tt.content))
		})
	}
}

func TestWriteSQLFencedBlockTruncatesToBudget(t *testing.T) {
	statement := "CREATE TABLE t (id int);\n"
	content := statement + strings.Repeat("a", maxCommentDDLLen)

	var out strings.Builder
	writeSQLFencedBlock(&out, content)
	rendered := out.String()

	block := strings.TrimSuffix(rendered, ddlTruncatedMarker)
	assert.NotEqual(t, rendered, block, "truncated block carries the marker")
	assert.LessOrEqual(t, len(block), maxCommentDDLLen)
	assert.True(t, strings.HasPrefix(block, "```sql\n"+statement))
	assert.True(t, strings.HasSuffix(block, "\n```\n"))
	assert.Equal(t, maxCommentDDLLen, len(block), "the cut keeps every byte the budget allows")
}

func TestFitSQLBlock(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		budget    int
		expected  string
		truncated bool
	}{
		{
			name:     "block that exactly meets the budget is untouched",
			content:  "abc",
			budget:   len("```sql\nabc\n```\n"),
			expected: "abc",
		},
		{
			name:      "one byte over the budget drops one byte of content",
			content:   "abc",
			budget:    len("```sql\nabc\n```\n") - 1,
			expected:  "ab",
			truncated: true,
		},
		{
			name:      "a backtick run at the tail shrinks with the fence rather than erasing the statement",
			content:   "CREATE;\n" + strings.Repeat("`", 20),
			budget:    40,
			expected:  "CREATE;\n" + strings.Repeat("`", 8),
			truncated: true,
		},
		{
			name:      "cut lands on a rune boundary",
			content:   "éééé",
			budget:    len("```sql\nééé\n```\n") + 1,
			expected:  "ééé",
			truncated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, truncated := fitSQLBlock(tt.content, tt.budget)

			assert.Equal(t, tt.expected, got)
			assert.Equal(t, tt.truncated, truncated)
			var out strings.Builder
			writeSQLFencedBlock(&out, got)
			assert.LessOrEqual(t, len(out.String()), tt.budget)
		})
	}
}

func TestInlineCode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "plain identifier", input: "users", expected: "`users`"},
		{name: "backtick inside widens and pads the delimiter", input: "a`b", expected: "`` a`b ``"},
		{name: "fence on its own line is flattened and out-delimited", input: "x\n```\n# injected", expected: "```` x ``` # injected ````"},
		{name: "bidi override is removed", input: "x\u202ey", expected: "`xy`"},
		{name: "whitespace runs collapse", input: "a \t b", expected: "`a b`"},
		{name: "leading backtick cannot merge with the delimiter", input: "`x", expected: "`` `x ``"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, inlineCode(tt.input))
		})
	}

	t.Run("cell escapes the column separator", func(t *testing.T) {
		assert.Equal(t, "`a\\|b`", inlineCodeCell("a|b"))
	})
}

func TestDDLBlocksContainHostileIdentifier(t *testing.T) {
	const hostileDDL = "CREATE TABLE \"x\n```\n# injected\" (id int)"
	const expectedBlock = "````sql\nCREATE TABLE \"x\n```\n# injected\" (id int);\n````\n"

	t.Run("plan", func(t *testing.T) {
		var out strings.Builder
		writePlanDDLBlock(&out, []string{hostileDDL}, schema.DialectPostgres)

		assert.Equal(t, expectedBlock+"\n", out.String())
	})

	t.Run("apply row", func(t *testing.T) {
		var out strings.Builder
		writeDDLLine(&out, schema.DialectPostgres, hostileDDL)

		assert.Equal(t, "\n"+expectedBlock, out.String())
	})

	t.Run("apply summary", func(t *testing.T) {
		var out strings.Builder
		writeSummaryTableEntry(&out, schema.DialectPostgres, TableProgressData{
			TableName: "x\n```\n# injected",
			Status:    state.Task.Completed,
			DDL:       hostileDDL,
		}, false)

		assert.Equal(t, "**```` x ``` # injected ````**\n"+expectedBlock+"\n", out.String())
	})
}
