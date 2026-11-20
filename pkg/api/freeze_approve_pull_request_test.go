package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func allowApprover(context.Context) (bool, error) { return true, nil }

// pullRequestOverride is a request from carol on alice's octocat/hello-world#7.
func pullRequestOverride(mutate func(*storage.FreezeOverrideRequest)) *storage.FreezeOverrideRequest {
	request := &storage.FreezeOverrideRequest{
		Identifier: "ovr-0123456789abcdef", Environment: "staging", Action: storage.FreezeOverrideActionApply, TargetID: "orders/x@1",
		Database: "orders", Blockers: []string{"freeze freeze-incident"}, Reason: "fix the index",
		RequestedBy: "github:carol", PRAuthor: "github:alice", Repository: "octocat/hello-world", PullRequest: 7,
		ExpiresAt: freezeTestNow.Add(time.Hour),
	}
	if mutate != nil {
		mutate(request)
	}
	return request
}

// A comment approves a request only as the rules say: a well-formed ID, an
// approver, a request from a pull request, on this pull request, by someone
// other than its requester and author. Every refusal leaves the request
// unapproved; the one approval is recorded in the GitHub identity domain.
func TestApprovePullRequestOverride(t *testing.T) {
	approved := func(*testing.T, *storage.FreezeOverrideRequest) {}
	tests := []struct {
		name    string
		request *storage.FreezeOverrideRequest
		input   PullRequestApproval
		// wantRefusal is a fragment of the refusal's message; wantCause is
		// the sentinel it unwraps to.
		wantRefusal string
		wantCause   error
		check       func(*testing.T, *storage.FreezeOverrideRequest)
	}{
		{name: "an approver on the same pull request approves", request: pullRequestOverride(nil),
			input: PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			check: func(t *testing.T, got *storage.FreezeOverrideRequest) {
				assert.Equal(t, "github:dave", got.ApprovedBy, "approved in the GitHub identity domain, never as the bare login")
				assert.NotNil(t, got.ApprovedAt)
			}},
		{name: "the repository compares without case", request: pullRequestOverride(nil),
			input: PullRequestApproval{Login: "dave", Repository: "OctoCat/Hello-World", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			check: approved},

		// Rule 1: the pull request.
		{name: "another pull request", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 8, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "belongs to octocat/hello-world#7"},
		{name: "another repository", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/other", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "belongs to octocat/hello-world#7"},

		// Rule 2: only a request from a pull request.
		{name: "a request from the API or CLI", request: pullRequestOverride(func(r *storage.FreezeOverrideRequest) {
			r.RequestedBy, r.PRAuthor, r.Repository, r.PullRequest = "rita", "", "", 0
		}),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "schemabot freeze approve ovr-0123456789abcdef` in the CLI"},

		// Rule 3: the approver.
		{name: "not an approver", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: func(context.Context) (bool, error) { return false, nil }},
			wantRefusal: "may not approve", wantCause: ErrNotPullRequestApprover},
		{name: "no approver check", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef"},
			wantRefusal: "may not approve", wantCause: ErrNotPullRequestApprover},

		// Rule 4: storage's refusals, as user replies.
		{name: "the requester", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "Carol", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "other than the person who asked for it and the pull request's author"},
		{name: "the pull request's author", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "ALICE", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "other than the person who asked for it and the pull request's author"},
		{name: "already approved", request: pullRequestOverride(func(r *storage.FreezeOverrideRequest) {
			at := freezeTestNow
			r.ApprovedBy, r.ApprovedAt = "github:erin", &at
		}),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "is already approved"},
		{name: "expired", request: pullRequestOverride(func(r *storage.FreezeOverrideRequest) { r.ExpiresAt = freezeTestNow.Add(-time.Minute) }),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "expired"},
		{name: "no such request", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-fedcba9876543210", IsApprover: allowApprover},
			wantRefusal: "was not found", wantCause: storage.ErrFreezeOverrideNotFound},

		// Rule 6: the ID's shape, before any storage call.
		{name: "not an override ID", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "the", IsApprover: allowApprover},
			wantRefusal: "not an override request ID", wantCause: ErrInvalidOverrideRequestID},
		{name: "an ID with a wildcard", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-%", IsApprover: allowApprover},
			wantRefusal: "not an override request ID", wantCause: ErrInvalidOverrideRequestID},
		{name: "an apply ID", request: pullRequestOverride(nil),
			input:       PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "apply-0123456789abcdef", IsApprover: allowApprover},
			wantRefusal: "not an override request ID", wantCause: ErrInvalidOverrideRequestID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, store, fake := newOverrideApproveService(t, freezeConfig())
			require.Equal(t, freezeTestNow, fake.Now())
			// A request from the past is stored as it would be before it expired.
			store.requests = append(store.requests, tt.request)

			got, err := svc.ApprovePullRequestOverride(t.Context(), tt.input)

			if tt.wantRefusal == "" {
				require.NoError(t, err)
				tt.check(t, got)
				return
			}
			refused, ok := errors.AsType[*PullRequestApprovalRefusedError](err)
			require.True(t, ok, "got %v", err)
			assert.Contains(t, refused.Message, tt.wantRefusal)
			if tt.wantCause != nil {
				assert.ErrorIs(t, err, tt.wantCause)
			}
			assert.Nil(t, got)
			stored, getErr := store.Get(t.Context(), tt.request.Identifier)
			require.NoError(t, getErr)
			assert.Equal(t, tt.request.ApprovedBy, stored.ApprovedBy, "a refusal approves nothing")
		})
	}
}

// An approver check that cannot be answered is not a refusal and not a pass:
// nothing is approved and the error says the check failed.
func TestApprovePullRequestOverrideFailsClosedWhenTheApproverCheckFails(t *testing.T) {
	svc, _, store, _ := newOverrideApproveService(t, freezeConfig())
	store.requests = append(store.requests, pullRequestOverride(nil))

	_, err := svc.ApprovePullRequestOverride(t.Context(), PullRequestApproval{
		Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef",
		IsApprover: func(context.Context) (bool, error) { return true, errors.New("team unreadable") },
	})

	require.ErrorIs(t, err, ErrPullRequestApproverUnchecked)
	_, refused := errors.AsType[*PullRequestApprovalRefusedError](err)
	assert.False(t, refused)
	stored, getErr := store.Get(t.Context(), "ovr-0123456789abcdef")
	require.NoError(t, getErr)
	assert.Nil(t, stored.ApprovedAt)
}

// A commenter with no login, or one that is not trimmed, approves nothing.
func TestApprovePullRequestOverrideNeedsACommenter(t *testing.T) {
	for _, login := range []string{"", "  ", " dave"} {
		svc, _, store, _ := newOverrideApproveService(t, freezeConfig())
		store.requests = append(store.requests, pullRequestOverride(nil))

		_, err := svc.ApprovePullRequestOverride(t.Context(), PullRequestApproval{
			Login: login, Repository: "octocat/hello-world", PullRequest: 7, ID: "ovr-0123456789abcdef", IsApprover: allowApprover,
		})

		require.Error(t, err, login)
		stored, getErr := store.Get(t.Context(), "ovr-0123456789abcdef")
		require.NoError(t, getErr)
		assert.Nil(t, stored.ApprovedAt, login)
	}
}

func TestServerConfig_ValidateFreezeGitHubTeams(t *testing.T) {
	config := testServerConfig()
	config.Freeze.GitHubTeams = []string{"acme/incident-commanders", "acme/release-managers"}
	require.NoError(t, config.Validate(), "the teams need no forward_auth")

	for name, teams := range map[string][]string{
		"malformed":  {"incident-commanders"},
		"no slug":    {"acme/"},
		"padded":     {" acme/incident-commanders"},
		"duplicate":  {"acme/incident-commanders", "acme/incident-commanders"},
		"cased twin": {"acme/incident-commanders", "Acme/Incident-Commanders"},
	} {
		config := testServerConfig()
		config.Freeze.GitHubTeams = teams
		require.ErrorContains(t, config.Validate(), "freeze.github_teams", name)
	}
}

// The refusal of a malformed ID does not repeat the commenter's word, which
// could name a team and notify it.
func TestApprovePullRequestOverrideDoesNotEchoAMalformedID(t *testing.T) {
	svc := &Service{}
	_, err := svc.ApprovePullRequestOverride(t.Context(), PullRequestApproval{Login: "dave", Repository: "octocat/hello-world", PullRequest: 7, ID: "@acme/db-admins"})
	refused, ok := errors.AsType[*PullRequestApprovalRefusedError](err)
	require.True(t, ok, "want a refusal, got %v", err)
	assert.False(t, strings.Contains(refused.Message, "@acme"), "refusal repeats the ID: %q", refused.Message)
}
