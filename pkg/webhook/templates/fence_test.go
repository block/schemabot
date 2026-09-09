package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/schema"
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
}
