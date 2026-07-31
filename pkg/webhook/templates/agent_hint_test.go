package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testAgentHint = "Agents: comment `schemabot help` for the command reference."

const testAgentHintFooter = "<!-- 💡 Agents: comment `schemabot help` for the command reference. -->"

func planData() PlanCommentData {
	return PlanCommentData{
		Database:    "orders",
		SchemaName:  "orders",
		Environment: "staging",
		IsMySQL:     true,
		AgentHint:   testAgentHint,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "orders",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255);"},
		}},
	}
}

// Plan comments — the entry point where a PR author first meets SchemaBot —
// carry the configured agent hint. It rides on the plan data, so a comment
// that is not a plan has no way to acquire one.
func TestPlanCommentsCarryTheAgentHint(t *testing.T) {
	assert.Contains(t, RenderPlanComment(planData()), testAgentHintFooter, "plan comment")
	assert.Contains(t, RenderRollbackPlanComment(planData()), testAgentHintFooter, "rollback plan comment")
	assert.Contains(t, RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "orders",
		SchemaName:   "orders",
		IsMySQL:      true,
		AgentHint:    testAgentHint,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Environment: "staging"},
			"production": {Environment: "production"},
		},
	}), testAgentHintFooter, "multi-env plan comment")
}

// A multi-env plan for a single environment renders through the plan renderer,
// so the hint configured on the multi-env data has to reach it.
func TestMultiEnvPlanCommentCarriesTheAgentHintForOneEnvironment(t *testing.T) {
	rendered := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "orders",
		SchemaName:   "orders",
		IsMySQL:      true,
		AgentHint:    testAgentHint,
		Environments: []string{"staging"},
		Plans:        map[string]*PlanCommentData{"staging": {Environment: "staging"}},
	})

	assert.Contains(t, rendered, testAgentHintFooter)
}

// A plan with no detected changes is still a plan comment and keeps the hint.
func TestPlanCommentCarriesTheAgentHintWithNoChanges(t *testing.T) {
	data := planData()
	data.Changes = nil

	assert.Contains(t, RenderPlanComment(data), testAgentHintFooter)
}

// The locked apply comment is the same plan renderer under a held lock, and it
// carries the apply-confirm command an agent acts on next, so it keeps the
// hint too.
func TestLockedApplyPlanCarriesTheAgentHint(t *testing.T) {
	data := planData()
	data.IsLocked = true
	data.LockOwner = "octocat"
	data.AutoConfirmDowngradeReason = "the plan changed since it was approved"

	rendered := RenderPlanComment(data)

	assert.Contains(t, rendered, "schemabot apply-confirm -e staging")
	assert.Contains(t, rendered, testAgentHintFooter)
}

// A deployment that configures no hint sends the comment it always sent — the
// hint adds nothing to the body, not even an invisible marker.
func TestPlanCommentsAreUnchangedWithoutAnAgentHint(t *testing.T) {
	data := planData()
	data.AgentHint = ""

	assert.NotContains(t, RenderPlanComment(data), "<!-- 💡 ")
	assert.NotContains(t, RenderRollbackPlanComment(data), "<!-- 💡 ")
	assert.NotContains(t, RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "orders",
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Environment: "staging"},
			"production": {Environment: "production"},
		},
	}), "<!-- 💡 ")
}

// The hint is delivered inside an HTML comment, which GitHub never renders, so
// the PR page is unchanged for human readers while agents reading the raw
// markdown receive it verbatim.
func TestAgentHintIsDeliveredAsATrailingHTMLComment(t *testing.T) {
	rendered := RenderPlanComment(planData())

	assert.True(t, strings.HasSuffix(rendered, "\n\n"+testAgentHintFooter),
		"the hint must end the comment: %q", rendered)
}
