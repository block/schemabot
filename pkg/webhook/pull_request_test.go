package webhook

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

// TestAutoPlanPullRequestPredicate pins which deliveries trigger auto-plan.
// A retarget changes the diff the PR proposes and so must re-plan; the title
// and body edits that arrive under the same action must not.
func TestAutoPlanPullRequestPredicate(t *testing.T) {
	retarget := func(from string) pullRequestPayload {
		var p pullRequestPayload
		p.Action = "edited"
		p.Changes.Base.Ref.From = from
		return p
	}

	tests := []struct {
		name    string
		payload pullRequestPayload
		want    bool
	}{
		{"opened", pullRequestPayload{Action: "opened"}, true},
		{"synchronize", pullRequestPayload{Action: "synchronize"}, true},
		{"reopened", pullRequestPayload{Action: "reopened"}, true},
		{"edited after a retarget", retarget("stacked-parent"), true},
		{"edited without a base change", pullRequestPayload{Action: "edited"}, false},
		{"closed", pullRequestPayload{Action: "closed"}, false},
		{"labeled", pullRequestPayload{Action: "labeled"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isAutoPlannablePullRequest(tc.payload))
		})
	}
}

// TestAutoPlanCoverageActionsAreUnconditional pins that every action the inbox
// coverage query treats as proof a head was planned is one that plans its head
// on the action alone. HasEventForHead matches on the action column, so an
// action that only sometimes plans — a retarget shares "edited" with title and
// body edits — would let an unrelated delivery stand in as coverage and mask
// the lost delivery the reconciler exists to recover.
func TestAutoPlanCoverageActionsAreUnconditional(t *testing.T) {
	for _, action := range storage.AutoPlanPullRequestActions {
		assert.True(t, isAutoPlannablePullRequest(pullRequestPayload{Action: action}),
			"coverage action %q must plan its head on the action alone", action)
	}
	assert.False(t, slices.Contains(storage.AutoPlanPullRequestActions, "edited"),
		"a retarget plans conditionally, so it cannot be counted as coverage")
}
