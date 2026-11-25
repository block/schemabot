package api

import (
	"context"
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

// The target names only the schema, so the same change on two pull requests
// shares it. A request belongs to the pull request it was asked for on:
// asking again on another records a new one, and an approval there, even by
// this pull request's author, does not admit it. Otherwise Bob could approve
// Carol's change on Alice's PR and so admit it on his own (AZ-10).
func TestFreezeOverrideBelongsToItsPullRequest(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	onAlices := WithPullRequestOverrideRequester(t.Context(), pullRequestRequester)
	bobsPR := pullRequestRequester
	bobsPR.PullRequest, bobsPR.Author = 8, "bob"
	onBobs := WithPullRequestOverrideRequester(t.Context(), bobsPR)
	request := ApplyRequest{PlanID: "plan-1", Environment: "staging", Override: "fix"}

	_, _, err := svc.ExecuteApply(onAlices, request)
	first, ok := errors.AsType[*ChangeGateRefusedError](err)
	require.True(t, ok, "got %v", err)
	require.NotNil(t, first.OverrideRequest)

	_, _, err = svc.ExecuteApply(onBobs, request)
	second, ok := errors.AsType[*ChangeGateRefusedError](err)
	require.True(t, ok, "got %v", err)
	require.NotNil(t, second.OverrideRequest)
	assert.NotEqual(t, first.OverrideRequest.Identifier, second.OverrideRequest.Identifier, "PR #8 records its own request")
	assert.Equal(t, 2, store.count())

	// Bob is not PR #7's author, so storage lets him approve its request.
	_, err = store.Approve(t.Context(), first.OverrideRequest.Identifier, storage.GitHubIdentity("bob"))
	require.NoError(t, err)

	_, _, err = svc.ExecuteApply(onBobs, ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	_, refused := errors.AsType[*ChangeGateRefusedError](err)
	assert.True(t, refused, "an approval on PR #7 does not admit PR #8: got %v", err)
	assert.Nil(t, applies.apply)

	_, _, err = svc.ExecuteApply(onAlices, ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.NoError(t, err, "it still admits the pull request it was asked for on")
	require.NotNil(t, applies.apply)
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
	onThePullRequest := `override request ovr-pr was asked for on pull request octocat/hello-world#7; approve it there with \"schemabot freeze approve ovr-pr\"`
	for name, tc := range map[string]struct {
		requestedBy string
		pullRequest int
		message     string
	}{
		"from a pull request": {storage.GitHubIdentity("carol"), 7, onThePullRequest},
		// No pull request to send the approver to, so the requester asks again.
		"github requester alone": {storage.GitHubIdentity("carol"), 0, "override request ovr-pr has a GitHub requester but no pull request, so nothing can approve it; ask for a new override"},
		"pull request only":      {"carol", 7, onThePullRequest},
	} {
		t.Run(name, func(t *testing.T) {
			mux, store := newPullRequestOverrideService(t, tc.requestedBy, tc.pullRequest)

			rec := approveRoute(t, mux, freezeMember, "ovr-pr")

			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.message)
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

// lookupFailingOverrides fails Get and counts Approve calls.
type lookupFailingOverrides struct {
	*memoryFreezeOverrideStore
	approves int
}

func (l *lookupFailingOverrides) Get(context.Context, string) (*storage.FreezeOverrideRequest, error) {
	return nil, errors.New("connection refused")
}

func (l *lookupFailingOverrides) Approve(ctx context.Context, id, approver string) (*storage.FreezeOverrideRequest, error) {
	l.approves++
	return l.memoryFreezeOverrideStore.Approve(ctx, id, approver)
}

// A lookup that fails leaves the request's domain unknown, so the route
// answers 500 and never asks storage to approve it.
func TestFreezeOverrideApproveLookupFailureApprovesNothing(t *testing.T) {
	svc, mux, store, _ := newOverrideApproveService(t, freezeConfig())
	failing := &lookupFailingOverrides{memoryFreezeOverrideStore: store}
	svc.storage.(*mockStorageWithFreezes).overrides = failing

	rec := approveRoute(t, mux, freezeMember, "ovr-1")

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "approve override request failed")
	assert.NotContains(t, rec.Body.String(), "connection refused", "the storage error stays in the logs")
	assert.Zero(t, failing.approves, "storage must not be asked to approve")
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

	// A command SchemaBot offers carries a placeholder; pasted unedited, it
	// gives no reason.
	for _, placeholder := range []string{"<why it can't wait>", " <reason> "} {
		_, err = ValidateOverrideReason(placeholder)
		require.Error(t, err, placeholder)
		assert.Contains(t, err.Error(), "placeholder")
	}
	_, err = ValidateOverrideReason("<INC-42> checkout is down")
	require.NoError(t, err, "a reason that only starts with a bracket is a reason")

	_, err = ValidateOverrideReason(strings.Repeat("é", storage.FreezeOverrideReasonMax))
	require.NoError(t, err, "the limit counts characters, not bytes")
	_, err = ValidateOverrideReason(strings.Repeat("é", storage.FreezeOverrideReasonMax+1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "501 characters")
}
