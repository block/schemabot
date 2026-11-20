package api

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage"
)

var pullRequestRequester = PullRequestRequester{Login: "Carol", Repository: "octocat/hello-world", PullRequest: 7, Author: "alice"}

// A request from a pull request command names the commenter and the author in
// the "github:" domain, the pull request, and the apply's target. Another
// GitHub login approves it; neither the commenter nor the author can, however
// the login is cased, and the repeated command is admitted.
func TestFreezeOverrideFromAPullRequestIsInTheGitHubDomain(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	ctx := WithPullRequestOverrideRequester(t.Context(), pullRequestRequester)
	request := ApplyRequest{PlanID: "plan-1", Environment: "staging", Override: "fix"}

	_, _, err := svc.ExecuteApply(ctx, request)

	refused, ok := errors.AsType[*ChangeGateRefusedError](err)
	require.True(t, ok, "got %v", err)
	require.NotNil(t, refused.OverrideRequest)
	assert.False(t, refused.OverrideUnrecorded)
	stored, err := store.Get(t.Context(), refused.OverrideRequest.Identifier)
	require.NoError(t, err)
	assert.Equal(t, "github:Carol", stored.RequestedBy)
	assert.Equal(t, "github:alice", stored.PRAuthor)
	assert.Equal(t, "octocat/hello-world", stored.Repository)
	assert.Equal(t, 7, stored.PullRequest)
	assert.Equal(t, gateApplyTarget(t, svc), stored.TargetID)
	assert.Equal(t, []string{"freeze freeze-incident"}, stored.Blockers)
	assert.True(t, stored.FromPullRequest())
	assert.Nil(t, applies.apply)

	for _, who := range []string{"github:carol", "github:ALICE"} {
		_, err = store.Approve(t.Context(), stored.Identifier, who)
		require.ErrorIs(t, err, storage.ErrFreezeOverrideSelfApproval, who)
	}
	_, err = store.Approve(t.Context(), stored.Identifier, storage.GitHubIdentity("dave"))
	require.NoError(t, err)

	_, _, err = svc.ExecuteApply(ctx, request)

	require.NoError(t, err)
	require.NotNil(t, applies.apply)
	assert.Equal(t, stored.Identifier, applies.apply.GetOptions().FreezeOverride)
	assert.Equal(t, 1, store.count())
}

// A pull request request is never recorded without its author or commenter,
// and a verified API subject spelled like a GitHub login records nothing: it
// would pass for that login.
func TestFreezeOverrideRecordsNothingWithoutAnAccountableRequester(t *testing.T) {
	incomplete := func(mutate func(*PullRequestRequester)) PullRequestRequester {
		requester := pullRequestRequester
		mutate(&requester)
		return requester
	}
	for name, requester := range map[string]PullRequestRequester{
		"no author":        incomplete(func(r *PullRequestRequester) { r.Author = " " }),
		"no commenter":     incomplete(func(r *PullRequestRequester) { r.Login = "" }),
		"no repository":    incomplete(func(r *PullRequestRequester) { r.Repository = "" }),
		"no pull request":  incomplete(func(r *PullRequestRequester) { r.PullRequest = 0 }),
		"nothing at all":   {},
		"author but alone": {Author: "alice"},
	} {
		t.Run(name, func(t *testing.T) {
			svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
			store := withOverrides(svc)
			// A verified subject alongside must not stand in for the login.
			ctx := WithPullRequestOverrideRequester(auth.WithVerifiedUser(t.Context(), overrideRequester), requester)

			_, _, err := svc.ExecuteApply(ctx, ApplyRequest{PlanID: "plan-1", Environment: "staging", Override: "fix"})

			refused, ok := errors.AsType[*ChangeGateRefusedError](err)
			require.True(t, ok, "got %v", err)
			assert.Nil(t, refused.OverrideRequest)
			assert.True(t, refused.OverrideUnrecorded)
			assert.Zero(t, store.count())
			assert.Nil(t, applies.apply)
		})
	}

	t.Run("a subject in the github domain", func(t *testing.T) {
		svc, _ := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
		store := withOverrides(svc)
		ctx := auth.WithVerifiedUser(t.Context(), &auth.User{Subject: "github:bob"})

		_, _, err := svc.ExecuteApply(ctx, ApplyRequest{PlanID: "plan-1", Environment: "staging", Override: "fix"})

		refused, ok := errors.AsType[*ChangeGateRefusedError](err)
		require.True(t, ok, "got %v", err)
		assert.True(t, refused.OverrideUnrecorded)
		assert.Zero(t, store.count())
	})
}

// A start or cutover from a pull request records its request on the apply, as
// an apply does.
func TestFreezeOverrideFromAPullRequestCoversAStartAndACutover(t *testing.T) {
	for _, tc := range overrideControlCases() {
		t.Run(tc.name, func(t *testing.T) {
			client, apply, tasks := tc.build()
			svc := newControlTestServiceWithTasks(client, apply, tasks)
			svc.storage.(*mockStorageWithApplyStores).freezes = newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
			store := withOverrides(svc)
			ctx := WithPullRequestOverrideRequester(t.Context(), pullRequestRequester)
			control := apitypes.ControlRequest{Environment: "staging", ApplyID: "apply-stopped", Caller: "github:Carol", Override: " fix "}

			var err error
			if tc.path == "/api/start" {
				_, err = svc.ExecuteStart(ctx, control)
			} else {
				_, err = svc.ExecuteCutover(ctx, control)
			}

			refused, ok := errors.AsType[*ChangeGateRefusedError](err)
			require.True(t, ok, "got %v", err)
			require.NotNil(t, refused.OverrideRequest)
			stored, err := store.Get(t.Context(), refused.OverrideRequest.Identifier)
			require.NoError(t, err)
			assert.Equal(t, strings.TrimPrefix(tc.path, "/api/"), stored.Action)
			assert.Equal(t, "apply-stopped", stored.TargetID)
			assert.Equal(t, "fix", stored.Reason)
			assert.Equal(t, "github:Carol", stored.RequestedBy)
			assert.Equal(t, "github:alice", stored.PRAuthor)
			assert.Equal(t, 7, stored.PullRequest)
		})
	}
}

// newPullRequestOverrideService is a freeze service holding request ovr-pr,
// asked for on a pull request.
func newPullRequestOverrideService(t *testing.T, requestedBy string, pullRequest int) (*http.ServeMux, *memoryFreezeOverrideStore) {
	t.Helper()
	_, mux, store, _ := newOverrideApproveService(t, freezeConfig())
	request := &storage.FreezeOverrideRequest{
		Identifier: "ovr-pr", Environment: "staging", Action: storage.FreezeOverrideActionApply, TargetID: "plan-1",
		Database: "testdb", Blockers: []string{"freeze freeze-incident"}, Reason: "fix", RequestedBy: requestedBy,
		ExpiresAt: freezeTestNow.Add(time.Hour),
	}
	if pullRequest > 0 {
		request.Repository, request.PullRequest, request.PRAuthor = "octocat/hello-world", pullRequest, storage.GitHubIdentity("alice")
	}
	_, err := store.Create(t.Context(), request)
	require.NoError(t, err)
	return mux, store
}

// The API cannot compare a GitHub login with its approver's subject, so it
// refuses a request asked for on a pull request, whoever the approver is, and
// storage is never asked to approve it.
func TestFreezeOverrideApproveRefusesAPullRequestRequest(t *testing.T) {
	for name, tc := range map[string]struct {
		requestedBy string
		pullRequest int
	}{
		"from a pull request":    {storage.GitHubIdentity("carol"), 7},
		"github requester alone": {storage.GitHubIdentity("carol"), 0},
		"pull request only":      {"carol", 7},
	} {
		t.Run(name, func(t *testing.T) {
			mux, store := newPullRequestOverrideService(t, tc.requestedBy, tc.pullRequest)

			rec := approveRoute(t, mux, freezeMember, "ovr-pr")

			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), `override request ovr-pr was asked for on a pull request; approve it there with \"schemabot freeze approve ovr-pr\"`)
			stored, err := store.Get(t.Context(), "ovr-pr")
			require.NoError(t, err)
			assert.Nil(t, stored.ApprovedAt, "storage must not approve it")
			assert.Empty(t, stored.ApprovedBy)
		})
	}

	// A request that does not exist is still not found, not conflicting.
	mux, _ := newPullRequestOverrideService(t, storage.GitHubIdentity("carol"), 7)
	assert.Equal(t, http.StatusNotFound, approveRoute(t, mux, freezeMember, "ovr-nope").Code)
}

// A forward-auth subject that starts with "github:" cannot approve anything
// through the API: it would pass for the GitHub login it spells.
func TestFreezeOverrideApproveRefusesASubjectInTheGitHubDomain(t *testing.T) {
	_, mux, store, _ := newOverrideApproveService(t, freezeConfig())
	posing := &auth.User{Subject: "github:bob", Groups: freezeMember.Groups}

	rec := approveRoute(t, mux, posing, "ovr-1")

	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `starts with \"github:\"`)
	stored, err := store.Get(t.Context(), "ovr-1")
	require.NoError(t, err)
	assert.Nil(t, stored.ApprovedAt)

	result := freezeConfig().AuthorizeFreezeApprove(posing)
	assert.False(t, result.Allowed)
	assert.Equal(t, DirectWriteReasonPullRequestIdentity, result.Reason)
	assert.True(t, freezeConfig().AuthorizeFreezeApprove(freezeMember).Allowed, "the same groups under an ordinary subject approve")
}

func TestValidateOverrideReason(t *testing.T) {
	reason, err := ValidateOverrideReason("  fix the index \n")
	require.NoError(t, err)
	assert.Equal(t, "fix the index", reason)

	_, err = ValidateOverrideReason(" \t ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")

	_, err = ValidateOverrideReason(strings.Repeat("é", storage.FreezeOverrideReasonMax))
	require.NoError(t, err, "the limit counts characters, not bytes")
	_, err = ValidateOverrideReason(strings.Repeat("é", storage.FreezeOverrideReasonMax+1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "501 characters")
}
