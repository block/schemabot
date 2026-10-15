package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// An operator comments `schemabot cutover -e production` without the apply ID.
// The reply names the one production apply on the PR and the exact command to
// comment, and never sends the operator off to look the ID up.
func TestRenderControlMissingApplyIDNamesTheApply(t *testing.T) {
	rendered := RenderControlMissingApplyID(MissingApplyIDData{
		Command:     "cutover",
		Environment: "production",
		Candidates:  []ApplyIDCandidate{{ApplyID: "apply-31eaaa9288e74d87", Database: "orders", Environment: "production"}},
		Complete:    true,
	})
	assert.Contains(t, rendered, "## Missing Apply ID\n\nComment this to `cutover` the `orders` apply in `production`:\n"+
		"```\nschemabot cutover apply-31eaaa9288e74d87 -e production\n```")
	assert.NotContains(t, rendered, "status")
	assert.NotContains(t, rendered, "<apply-id>")
	assert.NotContains(t, rendered, "another deployment")
}

func TestRenderControlMissingApplyIDNamesEachApply(t *testing.T) {
	rendered := RenderControlMissingApplyID(MissingApplyIDData{
		Command: "stop",
		Tenant:  "acme",
		Candidates: []ApplyIDCandidate{
			{ApplyID: "apply-aaa", Database: "orders", Environment: "staging"},
			{ApplyID: "apply-bbb", Database: "users", Environment: "production"},
		},
		Complete: true,
	})
	assert.Contains(t, rendered, "Comment the command for the apply you mean:\n\n"+
		"`orders` in `staging`:\n```\nschemabot stop apply-aaa -e staging --tenant acme\n```\n\n"+
		"`users` in `production`:\n```\nschemabot stop apply-bbb -e production --tenant acme\n```")
}

func TestRenderControlMissingApplyIDWithNothingToActOn(t *testing.T) {
	rendered := RenderControlMissingApplyID(MissingApplyIDData{Command: "start", Environment: "staging", Complete: true})
	assert.Contains(t, rendered, "## Missing Apply ID\n\nThis PR has no apply in `staging` that `start` can act on.")
	assert.NotContains(t, rendered, "progress comment")
}

// A failed lookup must not claim the PR has no apply: the reply points at the
// progress comment, which carries the command with its apply ID.
func TestRenderControlMissingApplyIDWhenTheLookupFailed(t *testing.T) {
	rendered := RenderControlMissingApplyID(MissingApplyIDData{Command: "cancel", Environment: "staging"})
	assert.Contains(t, rendered, "## Missing Apply ID\n\nCopy the command from the apply's progress comment. It carries the apply ID.")
	assert.NotContains(t, rendered, "no apply")
}

// On an aggregate repo the replying deployment sees only its own applies, so
// the reply names those and says where the rest are.
func TestRenderControlMissingApplyIDOnlyThisDeploymentsApplies(t *testing.T) {
	rendered := RenderControlMissingApplyID(MissingApplyIDData{
		Command:     "cutover",
		Environment: "production",
		Candidates:  []ApplyIDCandidate{{ApplyID: "apply-aaa", Database: "orders", Environment: "production"}},
	})
	assert.Contains(t, rendered, "```\nschemabot cutover apply-aaa -e production\n```\n\n"+
		"Applies run by another deployment carry their command on their progress comment.")
}

func TestRenderRollbackMissingApplyIDNamesTheCompletedApply(t *testing.T) {
	rendered := RenderRollbackMissingApplyID(MissingApplyIDData{
		Command:     "rollback",
		Environment: "staging",
		Tenant:      "acme",
		Candidates:  []ApplyIDCandidate{{ApplyID: "apply-aaa", Database: "orders", Environment: "staging"}},
		Complete:    true,
	})
	assert.Contains(t, rendered, "Comment this to `rollback` the `orders` apply in `staging`:\n"+
		"```\nschemabot rollback apply-aaa -e staging --tenant acme\n```\n\n"+
		"To confirm a generated rollback, comment `schemabot rollback-confirm -e staging --tenant acme`.")
	assert.NotContains(t, rendered, "status")
}

func TestRenderRollbackMissingApplyIDWhenTheLookupFailed(t *testing.T) {
	rendered := RenderRollbackMissingApplyID(MissingApplyIDData{Command: "rollback"})
	assert.Contains(t, rendered, "Copy the command from the summary comment of the completed apply. It carries the apply ID.\n\n"+
		"To confirm a generated rollback, comment `schemabot rollback-confirm -e <environment>`.")
}
