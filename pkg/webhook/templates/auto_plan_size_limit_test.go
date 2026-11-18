package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderAutoPlanSizeLimitNamesTheLimitAndTheWayForward(t *testing.T) {
	body := PreviewAutoPlanSizeLimitDatabases()
	assert.Contains(t, body, "## ⚠️ This PR is too large to plan automatically")
	assert.Contains(t, body, "It changes schema in 28 databases, and SchemaBot plans at most 10 databases in one PR.")
	assert.Contains(t, body, "split this PR into smaller PRs")
	assert.Contains(t, body, "```\nschemabot plan -d billing\n```")
	assert.Contains(t, body, "`billing`, `catalog`")
	assert.Contains(t, body, "`orders`, and 18 more")
	assert.NotContains(t, body, "`payments`", "only the first databases are listed")
}

func TestRenderAutoPlanSizeLimitForTooManySchemaFiles(t *testing.T) {
	body := PreviewAutoPlanSizeLimitSchemaFiles()
	assert.Contains(t, body, "It changes 240 schema files across 3 databases, and SchemaBot plans at most 100 schema files in one PR that changes more than one database.")
	assert.Contains(t, body, "Databases this PR changes: `orders`, `payments`, `shipping`\n")
	assert.NotContains(t, body, "more\n")
}

func TestRenderAutoPlanSizeLimitCarriesTheTenant(t *testing.T) {
	body := RenderAutoPlanSizeLimit(AutoPlanSizeLimitData{
		Databases:        []string{"orders", "payments"},
		SchemaFiles:      2,
		MaxDatabases:     1,
		MaxSchemaFiles:   100,
		TooManyDatabases: true,
		Tenant:           "retail",
	})
	assert.Contains(t, body, "schemabot plan -d orders --tenant retail")
}
