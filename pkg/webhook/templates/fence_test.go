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
			name:     "fence outruns the longest run, not the first or the last",
			content:  "SELECT 'a``b`````c```';",
			expected: "``````sql\nSELECT 'a``b`````c```';\n``````\n",
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
			writeSQLFencedBlock(&out, tt.content, newDDLBlockBudget(1))

			assert.Equal(t, tt.expected, out.String())
			lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
			openingLength := maxBacktickRun(lines[0])
			assert.Equal(t, strings.Repeat("`", openingLength), lines[len(lines)-1])
			assert.Greater(t, openingLength, maxBacktickRun(tt.content))
		})
	}
}

func TestWriteSQLFencedBlockTruncatesToBudget(t *testing.T) {
	const budget = 64
	statement := "CREATE TABLE t (id int);\n"
	content := statement + strings.Repeat("a", budget)

	var out strings.Builder
	writeSQLFencedBlock(&out, content, &ddlBlockBudget{remaining: budget, blocksLeft: 1})
	rendered := out.String()

	block := strings.TrimSuffix(rendered, ddlTruncatedMarker)
	assert.NotEqual(t, rendered, block, "truncated block carries the marker")
	assert.True(t, strings.HasPrefix(block, "```sql\n"+statement))
	assert.True(t, strings.HasSuffix(block, "\n```\n"))
	assert.Equal(t, budget, len(block), "the cut keeps every byte the budget allows")
}

// A comment's DDL budget is shared by the blocks it renders: each block may
// use an equal share of what is left, a short block hands its unused share on,
// and the blocks together never render past the budget.
func TestDDLBlockBudgetIsSharedAcrossBlocks(t *testing.T) {
	const total = 200
	short := "SELECT 1;"
	long := strings.Repeat("a", total)

	t.Run("a short block leaves its share to the blocks after it", func(t *testing.T) {
		budget := &ddlBlockBudget{remaining: total, blocksLeft: 2}
		var out strings.Builder
		writeSQLFencedBlock(&out, short, budget)
		writeSQLFencedBlock(&out, long, budget)

		rendered := strings.Replace(out.String(), ddlTruncatedMarker, "", 1)
		assert.Equal(t, total, len(rendered), "the second block fills what the first left")
		assert.Greater(t, strings.Count(rendered, "a"), total/2, "the second block got more than an even split")
	})

	t.Run("long blocks share the budget evenly", func(t *testing.T) {
		budget := &ddlBlockBudget{remaining: total, blocksLeft: 2}
		var first, second strings.Builder
		writeSQLFencedBlock(&first, long, budget)
		writeSQLFencedBlock(&second, long, budget)

		firstBlock := strings.TrimSuffix(first.String(), ddlTruncatedMarker)
		secondBlock := strings.TrimSuffix(second.String(), ddlTruncatedMarker)
		assert.Equal(t, total/2, len(firstBlock))
		assert.Equal(t, total/2, len(secondBlock))
	})

	t.Run("a block past the announced count draws on what remains", func(t *testing.T) {
		budget := &ddlBlockBudget{remaining: total, blocksLeft: 1}
		var out strings.Builder
		writeSQLFencedBlock(&out, long, budget)
		writeSQLFencedBlock(&out, long, budget)

		assert.Equal(t, 0, budget.remaining)
		rendered := strings.ReplaceAll(out.String(), ddlTruncatedMarker, "")
		assert.LessOrEqual(t, len(rendered), total+len("```sql\n```\n"), "an exhausted budget renders an empty block, never more content")
	})
}

func TestWriteVSchemaDiffFenceSizesFenceToContent(t *testing.T) {
	var out strings.Builder
	writeVSchemaDiffFence(&out, "-a\n+```\n+# injected", maxCommentVSchemaDiffLen)

	assert.Equal(t, "````diff\n-a\n+```\n+# injected\n````\n\n", out.String())
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
			writeSQLFencedBlock(&out, got, newDDLBlockBudget(1))
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
		{name: "empty name renders a closed span, not an unmatched run", input: "", expected: "` `"},
		{name: "name that sanitizes away renders a closed span", input: "\u202e \t", expected: "` `"},
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
		writePlanDDLBlock(&out, []string{hostileDDL}, schema.DialectPostgres, newDDLBlockBudget(1))

		assert.Equal(t, expectedBlock+"\n", out.String())
	})

	t.Run("apply row", func(t *testing.T) {
		var out strings.Builder
		writeDDLLine(&out, schema.DialectPostgres, hostileDDL, newDDLBlockBudget(1))

		assert.Equal(t, "\n"+expectedBlock, out.String())
	})

	t.Run("apply summary", func(t *testing.T) {
		var out strings.Builder
		writeSummaryTableEntry(&out, schema.DialectPostgres, TableProgressData{
			TableName: "x\n```\n# injected",
			Status:    state.Task.Completed,
			DDL:       hostileDDL,
		}, false, newDDLBlockBudget(1))

		assert.Equal(t, "**```` x ``` # injected ````**\n"+expectedBlock+"\n", out.String())
	})
}
