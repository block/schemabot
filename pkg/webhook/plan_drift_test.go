package webhook

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
)

// The drift summary names diverged deployments so the check's Change column
// tells an operator which deployment to reconcile.
func TestSummarizeReviewDrift_NamesDivergedDeployments(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentDiverged},
		},
	}
	summary := summarizeReviewDrift(rollup)
	assert.Contains(t, summary, "diverged: au")
	assert.NotContains(t, summary, "eu")
}

// A deployment that could not be diffed or compared is reported as unverifiable,
// separately from divergence, so the two failure modes are distinguishable.
func TestSummarizeReviewDrift_SeparatesDivergedAndErrored(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentDiverged},
			{Deployment: "us", Class: api.DeploymentErrored},
		},
	}
	summary := summarizeReviewDrift(rollup)
	assert.Contains(t, summary, "diverged: au")
	assert.Contains(t, summary, "could not verify: us")
}

// The stored drift summary is bounded to the change_summary column width and is
// kept on a single line with no markdown table separators, so a database with
// many drifted deployments cannot overflow the column or break the aggregate
// table rendering.
func TestSummarizeReviewDrift_BoundedAndSanitized(t *testing.T) {
	var entries []api.DeploymentRollupEntry
	entries = append(entries, api.DeploymentRollupEntry{Deployment: "eu", Class: api.DeploymentMatch})
	for range 200 {
		entries = append(entries, api.DeploymentRollupEntry{
			Deployment: "deployment-with-a-fairly-long-name",
			Class:      api.DeploymentDiverged,
		})
	}
	summary := summarizeReviewDrift(api.PlanRollup{Entries: entries})

	assert.LessOrEqual(t, utf8.RuneCountInString(summary), maxDriftSummaryLen)
	assert.NotContains(t, summary, "\n")
	assert.NotContains(t, summary, "|")
}

// Review-time drift fails the plan check closed even when the reviewed primary
// plan is a clean no-op, taking precedence over the plan's own outcome.
func TestPlanCheckConclusion_DriftFailsClosed(t *testing.T) {
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(false, false, true),
		"drift must block even a clean no-op primary plan")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(true, false, true),
		"drift must block a plan that also has changes")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(false, true, false),
		"a primary plan with errors fails closed")
	assert.Equal(t, checkConclusionActionRequired, planCheckConclusion(true, false, false),
		"changes without drift require an apply")
	assert.Equal(t, checkConclusionSuccess, planCheckConclusion(false, false, false),
		"a clean no-op plan with no drift passes")
}
