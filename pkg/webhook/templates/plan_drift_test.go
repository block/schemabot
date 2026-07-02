package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A clean multi-deployment rollup renders one uniform line confirming every
// deployment would plan the reviewed change, so a reviewer sees the change is
// safe to apply everywhere at a glance.
func TestRenderPlanComment_DriftCleanShowsUniformLine(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "match"},
				{Deployment: "us", Class: "match"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Same plan on all 3 deployments")
	assert.Contains(t, out, "eu, au, us")
}

// A diverged deployment is named with a compact change summary, and an errored
// deployment is called out as unverifiable, so a reviewer knows exactly which
// deployment to reconcile before approving.
func TestRenderPlanComment_DriftNotCleanListsDeployments(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "1 unexpected change(s) vs the reviewed plan"},
				{Deployment: "us", Class: "errored", Detail: "connection refused"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Deployment drift detected")
	assert.Contains(t, out, "`eu` (primary)")
	assert.Contains(t, out, "`au`")
	assert.Contains(t, out, "diverged")
	assert.Contains(t, out, "1 unexpected change(s) vs the reviewed plan")
	assert.Contains(t, out, "`us`")
	assert.Contains(t, out, "could not verify")
	assert.Contains(t, out, "connection refused")
}

// Drift on a non-primary deployment must surface even when the reviewed primary
// plan is a clean no-op: the change is a no-op only on the primary, so hiding
// the drift behind the no-changes short-circuit would let a diverged deployment
// pass review unnoticed.
func TestRenderPlanComment_DriftShownWhenPlanIsNoOp(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: nil, // no-op primary plan
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "2 missing change(s) vs the reviewed plan"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Deployment drift detected")
	assert.Contains(t, out, "`au`")
	assert.Contains(t, out, "2 missing change(s) vs the reviewed plan")
}

// When the rollup itself could not be computed, the preview says drift is
// unverified and the check is failing closed, so a reviewer understands why the
// gate is blocking rather than seeing a silent pass.
func TestRenderPlanComment_DriftNotComputedWarns(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{Computed: false},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Could not verify deployment drift")
	assert.Contains(t, out, "failing closed")
}

// A nil rollup (single-deployment database, or drift not evaluated) renders no
// drift section at all.
func TestRenderPlanComment_NoDriftSectionWhenNil(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}

	out := RenderPlanComment(data)
	assert.NotContains(t, out, "deployment")
	assert.NotContains(t, out, "drift")
}

// The multi-env no-changes short-circuit must not hide drift: when every
// environment's primary plan is a no-op but a deployment diverged, the comment
// still renders the per-environment drift breakdown instead of the simple
// "no schema changes" message.
func TestRenderMultiEnvPlanComment_NoChangesShortCircuitDoesNotHideDrift(t *testing.T) {
	prod := &PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: nil,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "1 unexpected change(s) vs the reviewed plan"},
			},
		},
	}
	data := MultiEnvPlanCommentData{
		Database:     "testapp",
		Environments: []string{"production"},
		Plans:        map[string]*PlanCommentData{"production": prod},
	}

	out := RenderMultiEnvPlanComment(data)
	// The top-level "no changes for any environment" short-circuit must not fire,
	// because it would return before rendering any drift.
	assert.NotContains(t, out, "No schema changes detected** for any environment")
	assert.Contains(t, out, "Deployment drift detected")
	assert.Contains(t, out, "`au`")
}

// Environments are only deduplicated when both their plans and their drift
// rollups render identically. Two environments with the same no-op plan but
// different drift must not collapse into one, or a diverged deployment in one
// environment would be hidden.
func TestPlansIdentical_DifferentDriftPreventsDedup(t *testing.T) {
	clean := &PlanCommentData{
		Database: "testapp", Changes: nil,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "match"},
			},
		},
	}
	drifted := &PlanCommentData{
		Database: "testapp", Changes: nil,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "1 unexpected change(s) vs the reviewed plan"},
			},
		},
	}

	assert.False(t, plansIdentical(clean, drifted), "differing drift must block dedup")
	assert.True(t, plansIdentical(clean, clean), "identical plans and drift dedup")
}

// A missing '·' — the drift line ordering is stable so the uniform clean line is
// emitted before the change list, keeping the safety signal at the top.
func TestRenderPlanComment_DriftBeforeChangeList(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "match"},
			},
		},
	}

	out := RenderPlanComment(data)
	driftIdx := strings.Index(out, "Same plan on all")
	changeIdx := strings.Index(out, "ADD COLUMN")
	assert.Positive(t, driftIdx)
	assert.Positive(t, changeIdx)
	assert.Less(t, driftIdx, changeIdx, "drift line appears before the change list")
}
