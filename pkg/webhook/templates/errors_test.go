package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderInvalidEnv(t *testing.T) {
	t.Run("lists the configured environments", func(t *testing.T) {
		body := RenderInvalidEnv("apply", []string{"production", "staging"})
		assert.Contains(t, body, "Invalid Environment")
		assert.Contains(t, body, "must name one of the configured environments")
		assert.Contains(t, body, "**Available environments**: `production`, `staging`")
		assert.Contains(t, body, "`schemabot apply -e <environment> [flags]`")
	})

	t.Run("omits the available line when no environments are configured", func(t *testing.T) {
		body := RenderInvalidEnv("apply", nil)
		assert.Contains(t, body, "Invalid Environment")
		assert.NotContains(t, body, "Available environments")
	})

	t.Run("normalizes names that would break markdown code spans", func(t *testing.T) {
		body := RenderInvalidEnv("apply", []string{"pro`duction", "sta\nging"})
		assert.Contains(t, body, "`production`")
		assert.Contains(t, body, "`sta ging`")
		assert.NotContains(t, body, "``")
	})
}
