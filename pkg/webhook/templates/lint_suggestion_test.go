package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A finding carries the linter's own advice on the line below it, indented
// into the numbered item so it reads as part of that finding. A finding whose
// linter offers no advice renders as it always has.
func TestWriteLintViolations_SuggestionRendersUnderItsFinding(t *testing.T) {
	var sb strings.Builder
	writeLintViolations(&sb, []LintViolationData{
		{Table: "users", Message: `Primary key column "id" has type "int"`, LinterName: "primary_key",
			Suggestion: `Change column "id" to a supported column type ("BIGINT")`},
		{Table: "orders", Message: `Column "amt" uses "float" data type`, LinterName: "has_float"},
	})

	lines := strings.Split(strings.TrimSpace(sb.String()), "\n")
	require.Len(t, lines, 4, "header, finding, its advice, then the finding with none")
	assert.Equal(t, "1. `users`: Primary key column `id` has type `int`", lines[1])
	assert.Equal(t, "   *Change column `id` to a supported column type (`BIGINT`)*", lines[2],
		"the advice is indented to the width of the list marker and set apart from the message")
	assert.Equal(t, "2. `orders`: Column `amt` uses `float` data type", lines[3])
}

// The numbering counts findings, not lines, so advice never consumes a number
// an operator could cite.
func TestWriteLintViolations_SuggestionDoesNotTakeANumber(t *testing.T) {
	var sb strings.Builder
	writeLintViolations(&sb, []LintViolationData{
		{Table: "users", Message: "first", Suggestion: "do this"},
		{Table: "users", Message: "second", Suggestion: "do that"},
	})

	out := sb.String()
	assert.Contains(t, out, "1. `users`: first")
	assert.Contains(t, out, "2. `users`: second")
}
