package webhook

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/tern"
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
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(false, false, false, true),
		"drift must block even a clean no-op primary plan")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(true, false, false, true),
		"drift must block a plan that also has changes")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(false, true, false, false),
		"a primary plan with errors fails closed")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(true, false, true, false),
		"a PostgreSQL engine refusal fails the plan check")
	assert.Equal(t, checkConclusionActionRequired, planCheckConclusion(true, false, false, false),
		"non-PostgreSQL changes retain the existing action-required policy")
	assert.Equal(t, checkConclusionSuccess, planCheckConclusion(false, false, false, false),
		"a clean no-op plan with no drift passes")
}

// A single-deployment database has nothing to compare, so the preview is nil and
// no drift section is rendered on its plan comment.
func TestDeploymentDriftPreview_NilForSingleDeployment(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{{Deployment: "primary", Class: api.DeploymentMatch}},
		Clean:   true,
	}
	assert.Nil(t, deploymentDriftPreview(rollup))
}

// A clean multi-deployment rollup becomes preview data flagged clean and
// computed, with the primary marked and every deployment classified as a match.
func TestDeploymentDriftPreview_CleanMultiDeployment(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentMatch},
		},
		Clean: true,
	}
	preview := deploymentDriftPreview(rollup)
	assert.NotNil(t, preview)
	assert.True(t, preview.Computed)
	assert.True(t, preview.Clean)
	assert.Len(t, preview.Deployments, 2)
	assert.True(t, preview.Deployments[0].Primary)
	assert.False(t, preview.Deployments[1].Primary)
	assert.Equal(t, "match", preview.Deployments[1].Class)
}

// A diverged deployment carries a compact change-count detail; an errored
// deployment carries a sanitized error detail so the preview names why each
// deployment is blocking.
func TestDeploymentDriftPreview_DivergedAndErroredDetails(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentDiverged, Diff: tern.ChangeSetDiff{
				UnexpectedInCandidate: []tern.ChangeSetDiffItem{{Table: "users"}},
				MissingFromCandidate:  []tern.ChangeSetDiffItem{{Table: "orders"}, {Table: "items"}},
			}},
			{Deployment: "us", Class: api.DeploymentErrored, Err: assert.AnError},
		},
		Clean: false,
	}
	preview := deploymentDriftPreview(rollup)
	assert.False(t, preview.Clean)
	assert.Equal(t, "diverged", preview.Deployments[1].Class)
	assert.Contains(t, preview.Deployments[1].Detail, "1 unexpected")
	assert.Contains(t, preview.Deployments[1].Detail, "2 missing")
	assert.Equal(t, "errored", preview.Deployments[2].Class)
	// The raw diff error stays out of the PR markdown: the preview carries only
	// the sanitized detail, and the underlying error text is not leaked.
	assert.Equal(t, erroredDriftDetail, preview.Deployments[2].Detail)
	assert.NotContains(t, preview.Deployments[2].Detail, assert.AnError.Error())
}
