package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderUnknownEnv(t *testing.T) {
	t.Run("lists the configured environments", func(t *testing.T) {
		body := RenderUnknownEnv("apply", "prodction", []string{"production", "staging"})
		assert.Contains(t, body, "Unknown Environment")
		assert.Contains(t, body, "`prodction` isn't a configured environment")
		assert.Contains(t, body, "**Available environments**: `production`, `staging`")
		assert.Contains(t, body, "`schemabot apply -e <environment>`")
	})

	t.Run("normalizes names that would break markdown code spans", func(t *testing.T) {
		body := RenderUnknownEnv("apply", "we`ird", []string{"pro`duction", "sta\nging"})
		assert.Contains(t, body, "`weird` isn't a configured environment")
		assert.Contains(t, body, "`production`")
		assert.Contains(t, body, "`sta ging`")
		assert.NotContains(t, body, "``")
	})
}
