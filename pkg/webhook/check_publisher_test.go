package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeCheckRunErrorSummary(t *testing.T) {
	input := "`plan failed`\n\x1b[31mdial tcp db-primary.internal:3306: connection refused <retry> " + strings.Repeat("x", 600)

	got := sanitizeCheckRunErrorSummary(input)

	assert.Len(t, []rune(got), 506, "sanitization clamps before HTML escaping expands entities")
	assert.True(t, strings.HasSuffix(got, "…"), "oversize summary ends with a truncation marker")
	assert.Contains(t, got, "`plan failed` dial tcp [endpoint redacted]: connection refused &lt;retry&gt;")
	assert.NotContains(t, got, "\n")
	assert.NotContains(t, got, "\x1b")
	assert.NotContains(t, got, "db-primary.internal")
}
