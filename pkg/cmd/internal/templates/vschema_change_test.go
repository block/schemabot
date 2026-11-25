package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// The terminal reads a VSchema change the way the PR comment does: one line
// per table, vindex, or keyspace setting, then the diff. A terminal has no
// collapsed block, so the diff follows the description, and the markdown code
// fences are dropped.
func TestFormatVSchemaStatusReadsTableByTable(t *testing.T) {
	out := ansiEscape.ReplaceAllString(FormatVSchemaStatus([]apitypes.VSchemaChange{{
		Namespace: "commerce",
		Status:    "applying",
		Diff:      "+    \"orders_archive\": {}",
		Summary:   []string{"adds table `orders_archive`: vindex `hash` on `id`", "adds vindex `odd``name`: type `hash`"},
	}}), "")

	assert.Contains(t, out, "    ~ VSchema (commerce): Applying...\n")
	assert.Contains(t, out, indentContent+"- adds table orders_archive: vindex hash on id\n")
	assert.Contains(t, out, "+    \"orders_archive\": {}", "the diff follows the description")
	assert.Less(t, strings.Index(out, "- adds table"), strings.Index(out, "orders_archive\": {}"))
}

// A change with a description and no diff still renders: the description is
// the account of the change.
func TestFormatVSchemaStatusRendersADescriptionWithoutADiff(t *testing.T) {
	out := ansiEscape.ReplaceAllString(FormatVSchemaStatus([]apitypes.VSchemaChange{{
		Namespace: "commerce", Status: "applied", Summary: []string{"removes table `orders_archive`"},
	}}), "")

	assert.Contains(t, out, "    ~ VSchema (commerce): Applied\n"+indentContent+"- removes table orders_archive\n")
}

// A long description is cut at the same line on both surfaces, and both say
// how many changes they left for the diff (UX-5).
func TestVSchemaDescriptionIsCappedTheSameOnEverySurface(t *testing.T) {
	summary := make([]string, 25)
	for i := range summary {
		summary[i] = fmt.Sprintf("adds table `t%02d`", i)
	}
	change := apitypes.VSchemaChange{Namespace: "commerce", Status: "applying", Diff: "+ x", Summary: summary}

	cli := ansiEscape.ReplaceAllString(FormatVSchemaStatus([]apitypes.VSchemaChange{change}), "")
	pr := webhooktemplates.RenderShardedApplyComment(webhooktemplates.ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "shop", ApplyID: "apply-x",
		VSchemaChanges: []apitypes.VSchemaChange{change},
	})

	for _, out := range []string{cli, pr} {
		assert.Contains(t, out, "t18")
		assert.NotContains(t, out, "t19", "the twentieth line is the count of what was left out")
		assert.Contains(t, out, "- and 6 more changes, shown in the diff")
	}
}
