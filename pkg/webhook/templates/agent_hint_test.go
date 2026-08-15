package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testAgentHint = "Agents: comment `schemabot help` for the command reference."

const testAgentHintFooter = "<!-- 💡 Agents: comment `schemabot help` for the command reference. -->"

func multiEnvPlanData(plans map[string]*PlanCommentData) MultiEnvPlanCommentData {
	environments := make([]string, 0, len(plans))
	for _, environment := range []string{"staging", "production"} {
		if _, ok := plans[environment]; ok {
			environments = append(environments, environment)
		}
	}
	return MultiEnvPlanCommentData{
		Database:     "orders",
		SchemaName:   "orders",
		IsMySQL:      true,
		AgentHint:    testAgentHint,
		Environments: environments,
		Plans:        plans,
	}
}

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
	assert.Contains(t, RenderMultiEnvPlanComment(multiEnvPlanData(map[string]*PlanCommentData{
		"staging":    {Environment: "staging"},
		"production": {Environment: "production"},
	})), testAgentHintFooter, "multi-env plan comment")
}

// The everyday multi-env shape — several environments each with pending
// changes — renders a different exit path from the no-changes shortcut, and
// carries the hint down both.
func TestMultiEnvPlanCommentCarriesTheAgentHintWithChanges(t *testing.T) {
	staging := planData()
	production := planData()
	production.Environment = "production"

	rendered := RenderMultiEnvPlanComment(multiEnvPlanData(map[string]*PlanCommentData{
		"staging":    &staging,
		"production": &production,
	}))

	assert.Contains(t, rendered, "ADD COLUMN `email`", "the plan sections must render")
	assert.Contains(t, rendered, testAgentHintFooter)
}

// A multi-env plan for a single environment renders through the plan renderer,
// so the hint configured on the multi-env data has to reach it.
func TestMultiEnvPlanCommentCarriesTheAgentHintForOneEnvironment(t *testing.T) {
	rendered := RenderMultiEnvPlanComment(multiEnvPlanData(map[string]*PlanCommentData{
		"staging": {Environment: "staging"},
	}))

	assert.Contains(t, rendered, testAgentHintFooter)
}

// A plan with no detected changes is still a plan comment and keeps the hint.
func TestPlanCommentCarriesTheAgentHintWithNoChanges(t *testing.T) {
	data := planData()
	data.Changes = nil

	assert.Contains(t, RenderPlanComment(data), testAgentHintFooter)
}

// A rollback plan that finds the database already matching the original schema
// is still a plan comment an agent acts on, and keeps the hint.
func TestRollbackPlanCommentCarriesTheAgentHintWithNoChanges(t *testing.T) {
	data := planData()
	data.Changes = nil

	assert.Contains(t, RenderRollbackPlanComment(data), testAgentHintFooter)
}

// An apply the engine refuses — destructive changes needing --allow-unsafe, or
// statements it cannot run at all — answers a command an agent just issued and
// coaches the next one, so it carries the hint like any other plan comment.
func TestBlockedApplyCommentsCarryTheAgentHint(t *testing.T) {
	unsafe := planData()
	unsafe.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "DROP COLUMN"}}
	assert.Contains(t, RenderUnsafeChangesBlocked(unsafe), testAgentHintFooter, "unsafe changes blocked")

	blocked := planData()
	blocked.BlockedChanges = []BlockedChangeData{{Table: "users", Reason: "unsupported statement"}}
	assert.Contains(t, RenderBlockedChangesApplyRejected(blocked), testAgentHintFooter, "apply rejected")
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
	noHint := multiEnvPlanData(map[string]*PlanCommentData{
		"staging":    {Environment: "staging"},
		"production": {Environment: "production"},
	})
	noHint.AgentHint = ""
	assert.NotContains(t, RenderMultiEnvPlanComment(noHint), "<!-- 💡 ")
	assert.NotContains(t, RenderUnsafeChangesBlocked(data), "<!-- 💡 ")
	assert.NotContains(t, RenderBlockedChangesApplyRejected(data), "<!-- 💡 ")
}

// The hint is delivered inside an HTML comment, which GitHub never renders, so
// the PR page is unchanged for human readers while agents reading the raw
// markdown receive it verbatim.
func TestAgentHintIsDeliveredAsATrailingHTMLComment(t *testing.T) {
	rendered := RenderPlanComment(planData())

	assert.True(t, strings.HasSuffix(rendered, "\n\n"+testAgentHintFooter),
		"the hint must end the comment: %q", rendered)
}
