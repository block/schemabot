package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRenderHelpCommentListsControlCommands verifies the help table advertises
// every apply-scoped control command with its full syntax, so an operator can
// copy a working command straight from the PR without consulting the docs.
func TestRenderHelpCommentListsControlCommands(t *testing.T) {
	rendered := RenderHelpComment()
	assert.Contains(t, rendered, "SchemaBot Help")
	assert.Contains(t, rendered, "`schemabot stop <apply-id> -e <env>`")
	assert.Contains(t, rendered, "`schemabot cancel <apply-id> -e <env>`")
	assert.Contains(t, rendered, "`schemabot start <apply-id> -e <env>`")
	assert.Contains(t, rendered, "`schemabot cutover <apply-id> -e <env>`")
	assert.Contains(t, rendered, "`schemabot volume <apply-id> -e <env> -v <level>`")
	assert.Contains(t, rendered, "Adjust schema change speed (1=slowest, 11=fastest)")
}
