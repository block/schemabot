package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validOverrideRequest() *FreezeOverrideRequest {
	return &FreezeOverrideRequest{
		Identifier:  "ovr-0123456789abcdef",
		Environment: "production",
		Action:      FreezeOverrideActionApply,
		TargetID:    "plan-1",
		Database:    "orders",
		Repository:  "block/orders",
		PullRequest: 42,
		PRAuthor:    "carol",
		Blockers:    []string{"freeze frz-1"},
		Reason:      "SEV-1234 hotfix index",
		RequestedBy: "alice",
		ExpiresAt:   time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC),
	}
}

func TestFreezeOverrideRequestValidate(t *testing.T) {
	require.NoError(t, validOverrideRequest().Validate())
	for _, action := range []string{FreezeOverrideActionCutover, FreezeOverrideActionStart} {
		request := validOverrideRequest()
		request.Action = action
		require.NoError(t, request.Validate(), action)
	}
	atLimit := validOverrideRequest()
	atLimit.Reason = "  " + strings.Repeat("é", FreezeOverrideReasonMax) + "  "
	require.NoError(t, atLimit.Validate(), "the limit counts characters of the trimmed reason")
	notFromAPR := validOverrideRequest()
	notFromAPR.Repository, notFromAPR.PullRequest, notFromAPR.PRAuthor = "", 0, ""
	require.NoError(t, notFromAPR.Validate(), "a request with no pull request needs no author")

	tests := []struct {
		name   string
		modify func(*FreezeOverrideRequest)
		want   string
	}{
		{"no identifier", func(r *FreezeOverrideRequest) { r.Identifier = " " }, "identifier is required"},
		{"no environment", func(r *FreezeOverrideRequest) { r.Environment = " " }, "environment is required"},
		{"unknown action", func(r *FreezeOverrideRequest) { r.Action = "stop" }, `action "stop"`},
		{"no target", func(r *FreezeOverrideRequest) { r.TargetID = "" }, "target is required"},
		{"no database", func(r *FreezeOverrideRequest) { r.Database = "" }, "database is required"},
		{"negative pull request", func(r *FreezeOverrideRequest) { r.PullRequest = -1 }, "not a PR number"},
		{"pull request without repository", func(r *FreezeOverrideRequest) { r.Repository = " " }, "needs its repository"},
		{"pull request without author", func(r *FreezeOverrideRequest) { r.PRAuthor = " " }, "needs its author"},
		{"no blockers", func(r *FreezeOverrideRequest) { r.Blockers = nil }, "at least one blocker"},
		{"blank blocker", func(r *FreezeOverrideRequest) { r.Blockers = []string{"freeze frz-1", " "} }, "empty entry"},
		{"blank reason", func(r *FreezeOverrideRequest) { r.Reason = "  " }, "reason is required"},
		{"long reason", func(r *FreezeOverrideRequest) { r.Reason = strings.Repeat("x", FreezeOverrideReasonMax+1) }, "longer than 500"},
		{"no requester", func(r *FreezeOverrideRequest) { r.RequestedBy = "" }, "requester is required"},
		{"no expiry", func(r *FreezeOverrideRequest) { r.ExpiresAt = time.Time{} }, "expiry is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := validOverrideRequest()
			tt.modify(request)
			require.ErrorContains(t, request.Validate(), tt.want)
		})
	}
}

func TestFreezeOverrideRequestCovers(t *testing.T) {
	request := validOverrideRequest()
	request.Blockers = []string{"freeze frz-1", "freeze frz-2"}

	assert.True(t, request.Covers([]string{"freeze frz-1"}))
	assert.True(t, request.Covers([]string{"freeze frz-2", "freeze frz-1"}))
	assert.False(t, request.Covers([]string{"freeze frz-1", "freeze frz-3"}), "a new blocker needs a new request")
}

func TestFreezeOverrideRequestIsPersonOf(t *testing.T) {
	request := validOverrideRequest()
	assert.True(t, request.IsPersonOf("Alice"))
	assert.True(t, request.IsPersonOf(" carol "))
	assert.False(t, request.IsPersonOf("bob"))

	request.PRAuthor = ""
	assert.False(t, request.IsPersonOf(""), "no author is nobody")
}

// A request belongs to the pull request domain by either marker, so a request
// that names a pull request or a GitHub requester is approved only there.
func TestFreezeOverrideRequestFromPullRequest(t *testing.T) {
	assert.Equal(t, "github:carol", GitHubIdentity("carol"))
	assert.True(t, IsGitHubIdentity("github:carol"))
	assert.True(t, IsGitHubIdentity(" github:carol"))
	assert.True(t, IsGitHubIdentity("GitHub:carol"))
	assert.False(t, IsGitHubIdentity("carol@example.com"))
	assert.False(t, IsGitHubIdentity("githubcarol"))

	assert.True(t, (&FreezeOverrideRequest{RequestedBy: "github:carol"}).FromPullRequest())
	assert.True(t, (&FreezeOverrideRequest{RequestedBy: "carol", PullRequest: 7}).FromPullRequest())
	assert.False(t, (&FreezeOverrideRequest{RequestedBy: "carol@example.com"}).FromPullRequest())
}
