package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestSanitizeCheckRunErrorSummary(t *testing.T) {
	t.Run("untrusted error text is redacted, collapsed, clamped, and markup-escaped", func(t *testing.T) {
		input := "`plan failed`\n\x1b[31mdial tcp db-primary.internal:3306: connection refused <retry> " + strings.Repeat("x", 600)

		got := sanitizeCheckRunErrorSummary(input)

		assert.Greater(t, len([]rune(got)), 500,
			"the summary is clamped to 500 runes before escaping, so entity expansion pushes it past the clamp; the clamp bounds the payload, not the escaped length")
		assert.True(t, strings.HasSuffix(got, "…"), "oversize summary ends with a truncation marker")
		assert.Contains(t, got, "&#96;plan failed&#96; dial tcp &#91;endpoint redacted&#93;: connection refused &lt;retry&gt;")
		assert.NotContains(t, got, "\n")
		assert.NotContains(t, got, "\x1b")
		assert.NotContains(t, got, "db-primary.internal")
	})

	t.Run("ampersands escape exactly once, including around escaped angle brackets", func(t *testing.T) {
		assert.Equal(t, "retry &amp; wait &lt;5s&gt;", sanitizeCheckRunErrorSummary("retry & wait <5s>"))
	})

	t.Run("markdown links, images, and code spans are neutralized without changing the displayed text", func(t *testing.T) {
		got := sanitizeCheckRunErrorSummary("plan failed: ![](https://attacker.example/p.png) [click here](https://attacker.example) `near \"x\"`")

		assert.Equal(t, "plan failed: !&#91;&#93;(https://attacker.example/p.png) &#91;click here&#93;(https://attacker.example) &#96;near \"x\"&#96;", got)
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

// A passing aggregate says "No schema files changed" only when the PR changes
// no schema. When it changes schema this deployment does not manage, the
// environment-scoped check names its environment, and the check of a
// deployment serving every environment speaks for all of them.
func TestPassingAggregateOutput(t *testing.T) {
	unmanaged := []templates.UnmanagedSchemaConfigNoticeData{{Database: "merchants", SchemaPath: "services/merchants/schema"}}

	t.Run("no schema changes", func(t *testing.T) {
		title, summary := passingAggregateOutput("production", nil)
		assert.Equal(t, "No schema files changed", title)
		assert.Equal(t, "SchemaBot found no changes to managed schema files in this PR.", summary)
	})

	t.Run("environment-scoped check names its environment", func(t *testing.T) {
		title, summary := passingAggregateOutput("production", unmanaged)
		assert.Equal(t, "No schema changes managed in production", title)
		assert.Contains(t, summary, "SchemaBot does not manage in `production`")
		assert.Contains(t, summary, "- `services/merchants/schema` declares database `merchants`")
	})

	t.Run("check of a deployment serving every environment", func(t *testing.T) {
		title, summary := passingAggregateOutput(aggregateSentinel, unmanaged)
		assert.Equal(t, "No schema changes managed by SchemaBot", title)
		assert.Contains(t, summary, "SchemaBot does not manage in any environment")
	})
}
