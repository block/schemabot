package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeCheckRunErrorSummary(t *testing.T) {
	t.Run("untrusted error text is redacted, collapsed, clamped, and markup-escaped", func(t *testing.T) {
		input := "`plan failed`\n\x1b[31mdial tcp db-primary.internal:3306: connection refused <retry> " + strings.Repeat("x", 600)

		got := sanitizeCheckRunErrorSummary(input)

		assert.Len(t, []rune(got), 506, "sanitization clamps before markup escaping expands entities")
		assert.True(t, strings.HasSuffix(got, "…"), "oversize summary ends with a truncation marker")
		assert.Contains(t, got, "`plan failed` dial tcp [endpoint redacted]: connection refused &lt;retry&gt;")
		assert.NotContains(t, got, "\n")
		assert.NotContains(t, got, "\x1b")
		assert.NotContains(t, got, "db-primary.internal")
	})

	t.Run("quotes and apostrophes in operator-facing prose pass through verbatim", func(t *testing.T) {
		input := "Check SchemaBot's access to this \"repository\", then retry the check."

		assert.Equal(t, input, sanitizeCheckRunErrorSummary(input))
	})

	t.Run("text that sanitizes to nothing falls back to a fixed summary", func(t *testing.T) {
		assert.Equal(t, "Plan failed", sanitizeCheckRunErrorSummary(" \n\t \x1b[31m "))
		assert.Equal(t, "Plan failed", sanitizeCheckRunErrorSummary(""))
	})
}
