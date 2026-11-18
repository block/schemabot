package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sandboxUnmanagedSchema = []UnmanagedSchemaConfigNoticeData{
	{Database: "ledger_sandbox", SchemaPath: "services/ledger/schema_sandbox"},
}

const sandboxUnmanagedNote = "ℹ️ This PR also changes schema under paths SchemaBot does not manage in `staging`, so this plan does not cover them:\n\n" +
	"- `services/ledger/schema_sandbox` declares database `ledger_sandbox`\n"

func TestRenderUnmanagedSchemaPlanNote(t *testing.T) {
	t.Run("names each config and the environments the deployment serves", func(t *testing.T) {
		note := RenderUnmanagedSchemaPlanNote([]string{"staging", "production"}, []UnmanagedSchemaConfigNoticeData{
			{Database: "ledger_sandbox", SchemaPath: "services/ledger/schema_sandbox"},
			{Database: "merchants", SchemaPath: "services/merchants/schema"},
		})
		assert.Equal(t, "ℹ️ This PR also changes schema under paths SchemaBot does not manage in `staging` and `production`, so this plan does not cover them:\n\n"+
			"- `services/ledger/schema_sandbox` declares database `ledger_sandbox`\n"+
			"- `services/merchants/schema` declares database `merchants`\n", note)
	})

	t.Run("renders nothing with no unmanaged configs", func(t *testing.T) {
		assert.Empty(t, RenderUnmanagedSchemaPlanNote([]string{"staging"}, nil))
	})

	t.Run("keeps PR-supplied values inside their code spans", func(t *testing.T) {
		note := RenderUnmanagedSchemaPlanNote([]string{"staging"}, []UnmanagedSchemaConfigNoticeData{
			{Database: "ledger`sandbox", SchemaPath: "services/ledger\nschema"},
		})
		assert.Contains(t, note, "- `services/ledger schema` declares database `` ledger`sandbox ``\n")
	})
}

// An environment-scoped deployment posts no separate notice for schema it does
// not manage, so its plan comment is where the PR shows what it left out. The
// note follows the plan, whichever renderer the comment takes and whether or
// not the plan has changes, and the agent hint stays the comment's last line.
func TestPlanCommentNamesUnmanagedSchema(t *testing.T) {
	staging := planData()
	production := planData()
	production.Environment = "production"
	noChanges := planData()
	noChanges.Changes = nil
	// Each case renders its own copy, so no case sees another's plan.
	copyOf := func(plan PlanCommentData) *PlanCommentData { return &plan }

	cases := []struct {
		name  string
		plans map[string]*PlanCommentData
		plan  string
	}{
		{name: "one environment with changes", plans: map[string]*PlanCommentData{"staging": copyOf(staging)}, plan: "ADD COLUMN `email`"},
		{name: "one environment with no changes", plans: map[string]*PlanCommentData{"staging": copyOf(noChanges)}, plan: "No schema changes detected"},
		{name: "several environments with changes", plans: map[string]*PlanCommentData{"staging": copyOf(staging), "production": copyOf(production)}, plan: "ADD COLUMN `email`"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := multiEnvPlanData(tc.plans)
			data.UnmanagedSchema = sandboxUnmanagedSchema
			data.UnmanagedEnvironments = []string{"staging"}

			rendered := RenderMultiEnvPlanComment(data)

			assert.Contains(t, rendered, tc.plan)
			assert.True(t, strings.HasSuffix(rendered, "\n\n"+strings.TrimSuffix(sandboxUnmanagedNote, "\n")+"\n\n"+testAgentHintFooter), "the note closes the plan, before the agent hint:\n%s", rendered)
			assert.Equal(t, 1, strings.Count(rendered, testAgentHintFooter), "the hint renders once")
			planIndex := strings.Index(rendered, tc.plan)
			noteIndex := strings.Index(rendered, sandboxUnmanagedNote)
			require.NotEqual(t, -1, noteIndex)
			assert.Less(t, planIndex, noteIndex, "the note follows the plan")
		})
	}

	t.Run("a plan with no unmanaged schema renders no note", func(t *testing.T) {
		rendered := RenderMultiEnvPlanComment(multiEnvPlanData(map[string]*PlanCommentData{"staging": copyOf(staging)}))

		assert.NotContains(t, rendered, "does not manage")
		assert.True(t, strings.HasSuffix(rendered, testAgentHintFooter))
	})
}

// The note is appended after the DDL is fitted, so the fit leaves room for it:
// a plan whose DDL alone would fill the comment still lands under GitHub's
// size limit with the note intact.
func TestPlanCommentWithUnmanagedSchemaStaysUnderTheCommentLimit(t *testing.T) {
	plan := planData()
	plan.Changes = []KeyspaceChangeData{{
		Keyspace:   "orders",
		Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255) COMMENT '" + strings.Repeat("x", commentBodyLimit) + "';"},
	}}
	data := multiEnvPlanData(map[string]*PlanCommentData{"staging": &plan})
	data.UnmanagedSchema = sandboxUnmanagedSchema
	data.UnmanagedEnvironments = []string{"staging"}

	rendered := RenderMultiEnvPlanComment(data)

	assert.LessOrEqual(t, len(rendered), commentBodyLimit)
	assert.Contains(t, rendered, sandboxUnmanagedNote)
	assert.True(t, strings.HasSuffix(rendered, testAgentHintFooter))
}
