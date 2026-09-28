package webhook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func TestAssertPlanStillCurrent_MatchProceeds(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	plan := &storage.Plan{
		PlanIdentifier: "plan_match",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		Database:       "testdb",
		DatabaseType:   "mysql",
		HeadSHA:        "sha-abc",
	}

	rejected := h.assertPlanStillCurrent(
		t.Context(),
		"octocat/hello-world", 1, int64(12345),
		plan,
		"sha-abc",
		"staging",
		"alice",
	)

	assert.False(t, rejected, "matching SHAs must not reject")

	select {
	case body := <-comments:
		t.Fatalf("no comment should be posted on a match; got: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAssertPlanStillCurrent_EmptyHeadSHASkips(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	plan := &storage.Plan{
		PlanIdentifier: "plan_legacy",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		Database:       "testdb",
		DatabaseType:   "mysql",
		// HeadSHA intentionally empty — legacy row created before the column existed.
	}

	rejected := h.assertPlanStillCurrent(
		t.Context(),
		"octocat/hello-world", 1, int64(12345),
		plan,
		"current-sha",
		"staging",
		"alice",
	)

	assert.False(t, rejected, "legacy plan (empty HeadSHA) must skip rather than fail closed")

	select {
	case body := <-comments:
		t.Fatalf("no comment should be posted for legacy plan rows; got: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAssertPlanStillCurrent_MismatchRejectsAndPostsComment(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	plan := &storage.Plan{
		PlanIdentifier: "plan_stale_abc",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		Database:       "testdb",
		DatabaseType:   "mysql",
		HeadSHA:        "plan-sha-abc",
	}

	rejected := h.assertPlanStillCurrent(
		t.Context(),
		"octocat/hello-world", 1, int64(12345),
		plan,
		"current-sha-def",
		"staging",
		"alice",
	)

	require.True(t, rejected, "stored plan SHA differing from fresh HEAD must reject")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Rejected", "title should call out rejection")
		assert.Contains(t, body, "the plan you confirmed is stale",
			"comment must explain the cross-delivery failure mode, not just generic discovery staleness")
		assert.Contains(t, body, "plan-sha-abc", "must include the stored plan SHA")
		assert.Contains(t, body, "current-sha-def", "must include the current PR HEAD")
		assert.Contains(t, body, "testdb", "must include database")
		assert.Contains(t, body, "staging", "must include environment")
		assert.Contains(t, body, "schemabot apply -e staging",
			"retry hint must direct the user to re-run apply (a fresh plan is needed)")
		assert.Contains(t, body, "alice", "must attribute the requester")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rejection comment")
	}
}
