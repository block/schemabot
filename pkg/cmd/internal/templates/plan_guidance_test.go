package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A CLI plan links each relevant guide once, with the same dialect scope as PRs.
func TestWriteRelatedGuidance(t *testing.T) {
	out := captureStdout(t, func() { WriteRelatedGuidance([]string{"primary_key", "primary_key", "rename_column"}, true) })
	assert.Equal(t, 1, strings.Count(out, "#choosing-a-primary-key"))
	assert.Contains(t, out, "Renaming a column or table: https://")
	vitess := captureStdout(t, func() { WriteRelatedGuidance([]string{"primary_key", "rename_column"}, false) })
	assert.NotContains(t, vitess, "#choosing-a-primary-key")
	assert.Contains(t, vitess, "#renaming-a-column-or-table")
	assert.Empty(t, captureStdout(t, func() { WriteRelatedGuidance([]string{"unknown"}, true) }))
}
