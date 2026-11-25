//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	mysqlstore "github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/spirit/pkg/utils"
)

var overrideIDPattern = regexp.MustCompile(`ovr-[0-9a-f]{16}`)

// freezeForOverride freezes database in environment until the test ends, under
// a fresh identifier so an override approved by an earlier run never covers it.
func freezeForOverride(t *testing.T, st storage.Storage, environment, database string) string {
	t.Helper()
	freezeID := engine.NewFreezeID()
	_, err := st.Freezes().Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier:  freezeID,
		Environment: environment,
		Databases:   []string{database},
		Kind:        storage.FreezeKindOnDemand,
		Running:     storage.FreezeRunningContinue,
		StartsAt:    time.Now().Add(-time.Hour),
		// A long reason: the refusal must still end with the approval
		// instructions, which the default error bound would cut off.
		Reason:    "incident 44: " + strings.Repeat("payments latency ", 30),
		CreatedBy: "alice",
		Source:    storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = st.Freezes().Lift(context.WithoutCancel(t.Context()), freezeID, "alice", "test done")
	})
	return freezeID
}

// requestNamedIn returns the stored override request a refusal comment names.
func requestNamedIn(t *testing.T, st storage.Storage, comment string) *storage.FreezeOverrideRequest {
	t.Helper()
	id := overrideIDPattern.FindString(comment)
	require.NotEmpty(t, id, "the comment names an override request: %s", comment)
	request, err := st.FreezeOverrides().Get(t.Context(), id)
	require.NoError(t, err)
	return request
}

func commentRequest(t *testing.T, login, comment string) *http.Request {
	t.Helper()
	return buildWebhookRequest(t, webhookPayloadOpts{comment: comment, userLogin: login, isPR: true}, nil)
}

// A PR author asks for an override of a freeze. The refusal names a request
// recorded in the GitHub identity domain with the pull request, its author and
// the apply's target. The commenter and the author cannot approve it, another
// GitHub login can, and the repeated command is admitted.
func TestE2EApplyOverrideFromAPullRequestNeedsAnotherGitHubApprover(t *testing.T) {
	dbName := "webhook_override_apply"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	freezeID := freezeForOverride(t, svc.Storage(), driftEnv, dbName)
	svc.Config().Freeze.GitHubTeams = []string{approveTeam}
	svc.Config().Freeze.OverridePolicy = api.FreezeOverridePolicyApproval

	refused := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv+` --override "fix the index"`))
	comment := awaitCommentContaining(t, refused, "| `"+freezeID+"` | until lifted |")
	request := requestNamedIn(t, svc.Storage(), comment)
	assert.Contains(t, comment, "Override request `"+request.Identifier+"` is waiting for a freeze approver.")
	assert.Contains(t, comment, "**Freeze approvers**:\n- @"+approveTeam+"\n")
	assert.Contains(t, comment, "other than @carol and other than the PR author (@testuser)")
	assert.Contains(t, comment, "   schemabot freeze approve "+request.Identifier+"\n   ```\n2. Then @carol runs this command again.")
	requireNoApplies(t, svc, dbName)

	assert.Equal(t, "github:carol", request.RequestedBy)
	assert.Equal(t, "github:testuser", request.PRAuthor)
	assert.Equal(t, "octocat/hello-world", request.Repository)
	assert.Equal(t, 1, request.PullRequest)
	assert.Equal(t, storage.FreezeOverrideActionApply, request.Action)
	assert.Equal(t, driftEnv, request.Environment)
	assert.Equal(t, dbName, request.Database)
	assert.Regexp(t, "^"+dbName+"/.*@[0-9a-f]{16}$", request.TargetID)
	assert.Equal(t, []string{"freeze " + freezeID}, request.Blockers)
	assert.Equal(t, "fix the index", request.Reason)
	assert.Nil(t, request.ApprovedAt)

	// Without an approval the same command, with or without the reason, is
	// still refused.
	again := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv))
	awaitCommentContaining(t, again, "is frozen, so this command did not run.")
	requireNoApplies(t, svc, dbName)

	for _, who := range []string{"github:carol", "github:testuser", "github:TestUser"} {
		_, err := svc.Storage().FreezeOverrides().Approve(t.Context(), request.Identifier, who)
		require.ErrorIs(t, err, storage.ErrFreezeOverrideSelfApproval, who)
	}
	_, err := svc.Storage().FreezeOverrides().Approve(t.Context(), request.Identifier, storage.GitHubIdentity("dave"))
	require.NoError(t, err)

	admitted := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv))
	created := awaitRolloutApply(t, svc, dbName, admitted)
	assert.Equal(t, request.Identifier, created.GetOptions().FreezeOverride)
}

// Under the default reason override policy, a commenter's own reason admits
// the apply from the pull request at once. The override is stored in the
// GitHub identity domain, approved by its requester, with the reason, and
// the apply records it. No approver team is needed.
func TestE2EApplyOverrideFromAPullRequestOnReason(t *testing.T) {
	dbName := "webhook_override_reason"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	freezeID := freezeForOverride(t, svc.Storage(), driftEnv, dbName)
	require.True(t, svc.Config().Freeze.OverrideOnReason(), "reason is the default")

	refused := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv))
	comment := awaitCommentContaining(t, refused, "| `"+freezeID+"` | until lifted |")
	assert.Contains(t, comment, "- **Go ahead with a reason** if this change can't wait.")
	assert.NotContains(t, comment, "approver")
	requireNoApplies(t, svc, dbName)

	admitted := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv+` --override "fix the index"`))
	created := awaitRolloutApply(t, svc, dbName, admitted)
	request, err := svc.Storage().FreezeOverrides().Get(t.Context(), created.GetOptions().FreezeOverride)
	require.NoError(t, err)
	assert.True(t, request.OnReason())
	assert.Equal(t, "github:carol", request.RequestedBy)
	assert.Equal(t, "github:carol", request.ApprovedBy)
	assert.Equal(t, "github:testuser", request.PRAuthor)
	assert.Equal(t, 1, request.PullRequest)
	assert.Equal(t, []string{"freeze " + freezeID}, request.Blockers)
	assert.Equal(t, "fix the index", request.Reason)
}

// A frozen change that is also unsafe needs both consents. The plan comment
// offers one command that carries both, and that command is admitted: the
// unsafe consent lets the apply run without a confirmation step, and the
// freeze lets it through on its reason. Without --allow-unsafe nothing runs.
func TestE2EApplyOverrideOfAnUnsafeChange(t *testing.T) {
	dbName := "webhook_override_unsafe"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersWithEmailSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	freezeID := freezeForOverride(t, svc.Storage(), driftEnv, dbName)
	dropEmail := map[string]string{"users.sql": usersBaseSchema}

	planned := runRolloutWebhookWithFiles(t, svc, dbName, commentRequest(t, "carol", `schemabot plan -e `+driftEnv), dropEmail)
	plan := awaitCommentContaining(t, planned, "| `"+freezeID+"` | until lifted |")
	assert.Contains(t, plan, "Add `--allow-unsafe` to confirm 1 unsafe change on `users`")
	assert.Contains(t, plan, "schemabot apply -e "+driftEnv+` --override "<why it can't wait>"`)

	withoutConsent := runRolloutWebhookWithFiles(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv+` --override "fix the index"`), dropEmail)
	refusal := awaitCommentContaining(t, withoutConsent, "Apply rejected")
	assert.Contains(t, refusal, "| `"+freezeID+"` | until lifted |", "the refusal names the freeze the retry meets next")
	assert.Contains(t, refusal, "schemabot apply -e "+driftEnv+` --allow-unsafe --override "<why it can't wait>"`, "the retry carries the override")
	requireNoApplies(t, svc, dbName)

	// The retry pasted unedited still carries the placeholder, which is no
	// reason: it is answered, and nothing runs.
	pasted := runRolloutWebhookWithFiles(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv+` --allow-unsafe --override "<why it can't wait>"`), dropEmail)
	assert.Contains(t, awaitCommentContaining(t, pasted, "placeholder"), "replace it with why this change can't wait")
	requireNoApplies(t, svc, dbName)

	admitted := runRolloutWebhookWithFiles(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv+` --allow-unsafe --override "fix the index"`), dropEmail)
	created := awaitRolloutApply(t, svc, dbName, admitted)
	require.NotEmpty(t, created.GetOptions().FreezeOverride, "the freeze let the apply through on its reason")
	request, err := svc.Storage().FreezeOverrides().Get(t.Context(), created.GetOptions().FreezeOverride)
	require.NoError(t, err)
	assert.Equal(t, "fix the index", request.Reason)
}

// approveTeam is the one freeze approver team the approval tests configure.
const approveTeam = "acme/freeze-approvers"

// approveFromPullRequest comments `schemabot freeze approve id` as login on the
// pull request. The team holds dave, carol and testuser, and reads as
// teamStatus says.
func approveFromPullRequest(t *testing.T, svc *api.Service, dbName, login, id string, teamStatus int) *planFlowResult {
	t.Helper()
	return runRolloutWebhookWithFiles(t, svc, dbName, commentRequest(t, login, "schemabot freeze approve "+id),
		map[string]string{"users.sql": usersWithEmailSchema},
		func(mux *http.ServeMux, _ *planFlowResult) {
			handleTeamMembers(t, mux, "acme", "freeze-approvers", teamStatus, "dave", "carol", "testuser")
		})
}

// storePullRequestOverride records a pending request from carol on the
// pull request, for a test that needs one the command did not make.
func storePullRequestOverride(t *testing.T, st storage.Storage, dbName string, pullRequest int) *storage.FreezeOverrideRequest {
	t.Helper()
	request, err := st.FreezeOverrides().Create(t.Context(), &storage.FreezeOverrideRequest{
		Identifier: engine.NewFreezeOverrideID(), Environment: driftEnv, Action: storage.FreezeOverrideActionApply,
		TargetID: dbName + "/x@0123456789abcdef", Database: dbName, Blockers: []string{"freeze freeze-x"}, Reason: "fix",
		RequestedBy: "github:carol", PRAuthor: "github:testuser", Repository: "octocat/hello-world", PullRequest: pullRequest,
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)
	return request
}

// A freeze approver comments `schemabot freeze approve` on the pull request
// the request was made on. SchemaBot approves it as that GitHub login and says
// who approved what; the requester runs the command again and it is admitted.
func TestE2EFreezeApproveFromTheSamePullRequest(t *testing.T) {
	dbName := "webhook_override_approve"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	svc.Config().Freeze.GitHubTeams = []string{approveTeam}
	svc.Config().Freeze.OverridePolicy = api.FreezeOverridePolicyApproval
	schemabotDB := openDriftDB(t, e2eSchemabotDSN)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
		_, err := schemabotDB.ExecContext(context.WithoutCancel(t.Context()), "DELETE FROM `freeze_override_requests` WHERE `database_name` = ?", dbName)
		assert.NoError(t, err)
	})
	freezeID := freezeForOverride(t, svc.Storage(), driftEnv, dbName)
	refused := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv+` --override "fix"`))
	request := requestNamedIn(t, svc.Storage(), awaitCommentContaining(t, refused, "| `"+freezeID+"` | until lifted |"))

	t.Run("the requester and the author, though in the team, and a stranger approve nothing", func(t *testing.T) {
		for login, want := range map[string]string{
			"carol":    "other than the person who asked for it and the pull request's author",
			"testuser": "other than the person who asked for it and the pull request's author",
			"mallory":  "is not a member of a team that may approve",
		} {
			comment := awaitCommentContaining(t, approveFromPullRequest(t, svc, dbName, login, request.Identifier, http.StatusOK), "Nothing was approved")
			assert.Contains(t, comment, want, login)
		}
		stored, err := svc.Storage().FreezeOverrides().Get(t.Context(), request.Identifier)
		require.NoError(t, err)
		assert.Nil(t, stored.ApprovedAt, "the request stays pending")
	})

	t.Run("a team that cannot be read refuses", func(t *testing.T) {
		comment := awaitCommentContaining(t, approveFromPullRequest(t, svc, dbName, "dave", request.Identifier, http.StatusInternalServerError), "could not check whether @dave is a freeze approver")
		assert.Contains(t, comment, "Nothing was approved")
		stored, err := svc.Storage().FreezeOverrides().Get(t.Context(), request.Identifier)
		require.NoError(t, err)
		assert.Nil(t, stored.ApprovedAt)
	})

	t.Run("a request from another pull request is not approved here", func(t *testing.T) {
		other := storePullRequestOverride(t, svc.Storage(), dbName, 2)
		comment := awaitCommentContaining(t, approveFromPullRequest(t, svc, dbName, "dave", other.Identifier, http.StatusOK), "belongs to octocat/hello-world#2")
		assert.Contains(t, comment, "Nothing was approved")
		stored, err := svc.Storage().FreezeOverrides().Get(t.Context(), other.Identifier)
		require.NoError(t, err)
		assert.Nil(t, stored.ApprovedAt)
	})

	t.Run("an approver approves, and the requester is admitted", func(t *testing.T) {
		comment := awaitCommentContaining(t, approveFromPullRequest(t, svc, dbName, "dave", request.Identifier, http.StatusOK), "approved `"+request.Identifier+"`")
		assert.Contains(t, comment, "@dave approved `"+request.Identifier+"` for apply on `"+dbName+"` in "+driftEnv)
		assert.Contains(t, comment, "Requested by @carol: fix.")
		assert.Contains(t, comment, "@carol can now re-run the command.")
		stored, err := svc.Storage().FreezeOverrides().Get(t.Context(), request.Identifier)
		require.NoError(t, err)
		assert.Equal(t, "github:dave", stored.ApprovedBy)

		admitted := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv))
		created := awaitRolloutApply(t, svc, dbName, admitted)
		assert.Equal(t, request.Identifier, created.GetOptions().FreezeOverride)
	})
}

// With no freeze.github_teams nobody approves from a pull request, and a
// request made through the API or CLI is not approved there.
func TestE2EFreezeApproveNeedsConfiguredTeamsAndAPullRequestRequest(t *testing.T) {
	dbName := "webhook_override_approve_cfg"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	schemabotDB := openDriftDB(t, e2eSchemabotDSN)
	t.Cleanup(func() {
		_, err := schemabotDB.ExecContext(context.WithoutCancel(t.Context()), "DELETE FROM `freeze_override_requests` WHERE `database_name` = ?", dbName)
		assert.NoError(t, err)
	})
	request := storePullRequestOverride(t, svc.Storage(), dbName, 1)

	comment := awaitCommentContaining(t, approveFromPullRequest(t, svc, dbName, "dave", request.Identifier, http.StatusOK), "Nothing was approved")
	assert.Contains(t, comment, "not configured")

	svc.Config().Freeze.GitHubTeams = []string{approveTeam}
	svc.Config().Freeze.OverridePolicy = api.FreezeOverridePolicyApproval
	viaAPI, err := svc.Storage().FreezeOverrides().Create(t.Context(), &storage.FreezeOverrideRequest{
		Identifier: engine.NewFreezeOverrideID(), Environment: driftEnv, Action: storage.FreezeOverrideActionApply,
		TargetID: dbName + "/x@0123456789abcdef", Database: dbName, Blockers: []string{"freeze freeze-x"}, Reason: "fix",
		RequestedBy: "rita", ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)
	comment = awaitCommentContaining(t, approveFromPullRequest(t, svc, dbName, "dave", viaAPI.Identifier, http.StatusOK), "Nothing was approved")
	assert.Contains(t, comment, "schemabot freeze approve "+viaAPI.Identifier+"` in the CLI")
	stored, err := svc.Storage().FreezeOverrides().Get(t.Context(), viaAPI.Identifier)
	require.NoError(t, err)
	assert.Nil(t, stored.ApprovedAt)
}

// A blank reason, or --override on a command no freeze refuses, is answered
// before any work starts.
func TestE2EApplyOverrideFlagMisuseIsAnswered(t *testing.T) {
	dbName := "webhook_override_misuse"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)

	blank := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot apply -e `+driftEnv+` --override "  "`))
	awaitCommentContaining(t, blank, "override reason is empty")
	unsupported := runRolloutWebhook(t, svc, dbName, commentRequest(t, "carol", `schemabot rollback apply_abc123 -e `+driftEnv+` --override "fix"`))
	awaitCommentContaining(t, unsupported, "The `--override` flag is not supported for `rollback`")
	requireNoApplies(t, svc, dbName)
}

// controlOverrideFixture is a PR whose apply sits where a start or cutover can
// act on it, behind a freeze.
type controlOverrideFixture struct {
	handler  *Handler
	store    storage.Storage
	comments chan string
	freezeID string
	applyID  string
}

// newControlOverrideFixture seeds a stopped apply, or one waiting for its
// cutover, for the PR. authorLogin is the PR's author as GitHub reports it; an
// empty one leaves the pull request unreadable.
func newControlOverrideFixture(t *testing.T, applyIdentifier, database string, cutover bool, authorLogin string) *controlOverrideFixture {
	t.Helper()
	ctx := t.Context()
	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	store := mysqlstore.New(schemabotDB)
	cleanup := func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		_, err := schemabotDB.ExecContext(context.WithoutCancel(t.Context()), "DELETE FROM `freeze_override_requests` WHERE `database_name` = ?", database)
		require.NoError(t, err)
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		utils.CloseAndLog(schemabotDB)
	})

	applyID := createStopCommandApply(t, store, applyIdentifier, database)
	apply, err := store.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	tasks, err := store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	apply.State, tasks[0].State = state.Apply.Stopped, state.Task.Stopped
	if cutover {
		apply.State, tasks[0].State = state.Apply.WaitingForCutover, state.Task.WaitingForCutover
		apply.SetOptions(storage.ApplyOptions{DeferCutover: true})
	}
	apply.UpdatedAt, tasks[0].UpdatedAt = time.Now().UTC(), time.Now().UTC()
	require.NoError(t, store.Applies().Update(ctx, apply))
	require.NoError(t, store.Tasks().Update(ctx, tasks[0]))

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	if authorLogin != "" {
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(gh.PullRequest{State: new("open"), User: &gh.User{Login: new(authorLogin)}})
		})
	}

	service := apiServiceForStopCommandTest(t, store, database)
	service.RegisterTernClient(database, "staging", &stopCommandTernClient{remote: cutover})
	service.Config().Freeze.GitHubTeams = []string{approveTeam}
	service.Config().Freeze.OverridePolicy = api.FreezeOverridePolicyApproval
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}
	drainWebhookWorkOnCleanup(t, h)
	return &controlOverrideFixture{handler: h, store: store, comments: comments, applyID: applyIdentifier,
		freezeID: freezeForOverride(t, store, "staging", database)}
}

func (f *controlOverrideFixture) comment(t *testing.T, login, command string) {
	t.Helper()
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, commentRequest(t, login, command))
	require.Equal(t, http.StatusOK, rr.Code)
}

// A start or cutover from a pull request asks for an override the way an
// apply does: the request covers the apply, in the GitHub identity domain,
// with the pull request's author. When the author cannot be read no request is
// recorded, and the refusal still shows.
func TestE2EStartAndCutoverOverrideFromAPullRequest(t *testing.T) {
	for _, tc := range []struct {
		name, command string
		cutover       bool
		identifier    string
	}{
		{"start", action.Start, false, "apply_ce0f1001"},
		{"cutover", action.Cutover, true, "apply_ce0f1002"},
	} {
		t.Run(tc.name+" records the request", func(t *testing.T) {
			f := newControlOverrideFixture(t, tc.identifier, "override_"+tc.name+"_db", tc.cutover, "alice")

			f.comment(t, "carol", "schemabot "+tc.command+" "+tc.identifier+` -e staging --override "fix the index"`)

			body := readComment(t, f.comments)
			request := requestNamedIn(t, f.store, body)
			assert.Contains(t, body, "| `"+f.freezeID+"` |")
			assert.Contains(t, body, "other than @carol and other than the PR author (@alice)")
			assert.Equal(t, tc.command, request.Action)
			assert.Equal(t, tc.identifier, request.TargetID)
			assert.Equal(t, "github:carol", request.RequestedBy)
			assert.Equal(t, "github:alice", request.PRAuthor)
			assert.Equal(t, "octocat/hello-world", request.Repository)
			assert.Equal(t, 1, request.PullRequest)
			assert.Equal(t, []string{"freeze " + f.freezeID}, request.Blockers)
			operation := storage.ControlOperationStart
			if tc.cutover {
				operation = storage.ControlOperationCutover
			}
			apply, err := f.store.Applies().GetByApplyIdentifier(t.Context(), tc.identifier)
			require.NoError(t, err)
			pending, err := f.store.ControlRequests().GetPending(t.Context(), apply.ID, operation)
			require.NoError(t, err)
			assert.Nil(t, pending, "a refused command queues nothing")

			_, err = f.store.FreezeOverrides().Approve(t.Context(), request.Identifier, "github:dave")
			require.NoError(t, err)
			f.comment(t, "carol", "schemabot "+tc.command+" "+tc.identifier+" -e staging")
			assert.Contains(t, readComment(t, f.comments), "Request Accepted")
		})

		t.Run(tc.name+" without a readable author records nothing", func(t *testing.T) {
			f := newControlOverrideFixture(t, tc.identifier, "override_"+tc.name+"_nopr_db", tc.cutover, "")

			f.comment(t, "carol", "schemabot "+tc.command+" "+tc.identifier+` -e staging --override "fix the index"`)

			body := readComment(t, f.comments)
			assert.Contains(t, body, "| `"+f.freezeID+"` |")
			assert.Contains(t, body, "so it recorded no override request")
			assert.NotRegexp(t, overrideIDPattern, body)
			var count int
			require.NoError(t, e2eCount(t, "SELECT COUNT(*) FROM freeze_override_requests WHERE database_name = ?", "override_"+tc.name+"_nopr_db").Scan(&count))
			assert.Zero(t, count)
		})
	}
}

// e2eCount runs a counting query against the SchemaBot storage database.
func e2eCount(t *testing.T, query string, args ...any) *sql.Row {
	t.Helper()
	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db.QueryRowContext(t.Context(), query, args...)
}
