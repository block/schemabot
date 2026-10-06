//go:build integration

// Rollout member work webhook integration tests. A database that fans out to
// several targets is planned against its primary, and the primary already being
// at the desired schema says nothing about the other targets. These tests pin
// that neither the plan check nor the apply command treats a converged primary
// as a converged rollout.

package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/utils"
)

// runRolloutCommand drives one PR comment command against the rollout fixture
// and returns the capture of what SchemaBot posts in response.
func runRolloutCommand(t *testing.T, svc *api.Service, dbName, command string) *planFlowResult {
	t.Helper()
	return runRolloutWebhook(t, svc, dbName, buildWebhookRequest(t, webhookPayloadOpts{comment: command, isPR: true}, nil))
}

// runRolloutWebhook delivers one webhook to the rollout fixture and returns the
// capture of what SchemaBot posts in response.
func runRolloutWebhook(t *testing.T, svc *api.Service, dbName string, req *http.Request) *planFlowResult {
	t.Helper()
	return runRolloutWebhookWithFiles(t, svc, dbName, req, map[string]string{"users.sql": usersWithEmailSchema})
}

// runRolloutCommandWithFiles is runRolloutCommand for a PR whose schema files
// are files.
func runRolloutCommandWithFiles(t *testing.T, svc *api.Service, dbName, command string, files map[string]string) *planFlowResult {
	t.Helper()
	return runRolloutWebhookWithFiles(t, svc, dbName, buildWebhookRequest(t, webhookPayloadOpts{comment: command, isPR: true}, nil), files)
}

// register serves anything else the command reads from GitHub, such as the
// state of another pull request.
func runRolloutWebhookWithFiles(t *testing.T, svc *api.Service, dbName string, req *http.Request, files map[string]string, register ...func(*http.ServeMux)) *planFlowResult {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, files, schemabotConfig, dbName)
	for _, r := range register {
		r(mux)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	return result
}

// awaitConfirmedApply waits for the apply an apply-confirm creates on the
// rollout fixture's database, failing if the confirmation reports that it could
// not create one.
func awaitConfirmedApply(t *testing.T, svc *api.Service, dbName string, confirm *planFlowResult) *storage.Apply {
	t.Helper()
	var created *storage.Apply
	require.Eventually(t, func() bool {
		applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
		if err != nil {
			return false
		}
		for _, a := range applies {
			if a.Database == dbName {
				created = a
				return true
			}
		}
		select {
		case posted := <-confirm.comments:
			require.NotContains(t, posted, "Failed to execute apply", "the confirmation must create the apply")
		default:
		}
		return false
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "the confirmation creates an apply")
	return created
}

// awaitCapture returns the first value the command publishes on ch that
// matches, failing the test if none arrives before the deadline. Values
// published before it are skipped: a command can post an acknowledgment, or
// republish a stale check, first.
func awaitCapture[T any](t *testing.T, ch <-chan T, what string, match func(T) bool) T {
	t.Helper()
	deadline := time.After(webhookIntegrationPollDeadline)
	var seen []T
	for {
		select {
		case v := <-ch:
			if match(v) {
				return v
			}
			seen = append(seen, v)
		case <-deadline:
			require.FailNowf(t, "timed out waiting for "+what, "saw %d: %+v", len(seen), seen)
			var zero T
			return zero
		}
	}
}

func awaitCommentContaining(t *testing.T, result *planFlowResult, want string) string {
	t.Helper()
	return awaitCapture(t, result.comments, "a comment containing "+want, func(body string) bool { return strings.Contains(body, want) })
}

func rolloutCheck(t *testing.T, svc *api.Service, dbName string) *storage.Check {
	t.Helper()
	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, driftEnv, "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a stored check record")
	return check
}

func requireNoApplies(t *testing.T, svc *api.Service, dbName string) {
	t.Helper()
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, a := range applies {
		assert.NotEqual(t, dbName, a.Database, "no apply may be created for %s, got %s in state %s", dbName, a.ApplyIdentifier, a.State)
	}
}

// Two targets planned against schemas of their own, where the primary target
// (eu) already has the column the PR adds and us does not. The plan check must
// not pass on the primary's empty plan: it records the pending work on us. The
// apply command neither reports "no changes" nor runs anything yet: it pauses
// for confirmation on a comment that shows us's own plan, with the check still
// pending. Confirming runs us's own plan and settles eu, which has nothing to do.
func TestE2EConvergedPrimaryWithPendingTargetAppliesAfterConfirmation(t *testing.T) {
	dbName := "webhook_rollout_pending"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)

	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "a target that still needs the change keeps the check from passing")
	assert.True(t, check.HasChanges, "work on a non-primary target is work on the rollout")
	assert.Equal(t, "1 of 2 targets need this change", check.ChangeSummary)
	assert.Empty(t, check.BlockingReason, "independent targets differing is not drift")

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Confirmation required")
	assert.Contains(t, body, "The primary target already has this schema")
	assert.Contains(t, body, "ADD COLUMN `email`", "the comment shows the plan us would run")
	assert.Contains(t, body, "schemabot apply-confirm -e "+driftEnv)
	assert.Contains(t, body, "### Target `eu`\n\nNo schema changes detected\n\n", "eu is shown already at the desired schema")
	assert.NotContains(t, body, "✅ **No schema changes detected**", "a target still has work, so the comment never closes as a no-op")

	requireNoApplies(t, svc, dbName)
	check = rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "the paused apply leaves the check pending")
	assert.True(t, check.HasChanges)

	runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)

	var created *storage.Apply
	require.Eventually(t, func() bool {
		applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
		if err != nil {
			return false
		}
		for _, a := range applies {
			if a.Database == dbName {
				created = a
				return true
			}
		}
		return false
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "the confirmation creates an apply")

	operations, err := svc.Storage().ApplyOperations().ListByApply(t.Context(), created.ID)
	require.NoError(t, err)
	byDeployment := map[string]*storage.ApplyOperation{}
	for _, op := range operations {
		byDeployment[op.Deployment] = op
	}
	require.Contains(t, byDeployment, "eu")
	require.Contains(t, byDeployment, "us")
	assert.Equal(t, state.ApplyOperation.Completed, byDeployment["eu"].State, "eu already has the column, so it runs nothing")

	tasks, err := svc.Storage().Tasks().GetByApplyID(t.Context(), created.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "only us runs a statement")
	require.NotNil(t, tasks[0].ApplyOperationID)
	assert.Equal(t, byDeployment["us"].ID, *tasks[0].ApplyOperationID)
	assert.Equal(t, "users", tasks[0].TableName)
	assert.Contains(t, tasks[0].DDL, "ADD COLUMN `email`")
}

// Two targets planned against schemas of their own, where both the reviewed
// primary (eu) and us still need the column. The apply runs each target's own
// plan: the apply command pauses for apply-confirm on a comment that renders
// every target's plan, since the one-step gates read only the primary plan,
// and confirming creates one apply that drives the column onto both targets.
func TestE2EIndependentRolloutWithWorkOnTheReviewedTargetAppliesEveryTarget(t *testing.T) {
	dbName := "webhook_rollout_both"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)
	assert.Equal(t, "action_required", rolloutCheck(t, svc, dbName).Conclusion)

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCapture(t, apply.comments, "the apply command's answer", func(body string) bool {
		return strings.Contains(body, "Confirmation required") || strings.Contains(body, "Failed to execute apply")
	})
	require.NotContains(t, body, "Failed to execute apply")
	assert.Contains(t, body, "Each target runs its own plan")
	assert.Contains(t, body, "`eu`, `us`", "the comment names every target the plan runs on")
	assert.Contains(t, body, "ADD COLUMN `email`")
	assert.Contains(t, body, "schemabot apply-confirm -e "+driftEnv)
	requireNoApplies(t, svc, dbName)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)

	created := awaitConfirmedApply(t, svc, dbName, confirm)

	operations, err := svc.Storage().ApplyOperations().ListByApply(t.Context(), created.ID)
	require.NoError(t, err)
	byDeployment := map[string]*storage.ApplyOperation{}
	for _, op := range operations {
		byDeployment[op.Deployment] = op
	}
	require.Contains(t, byDeployment, "eu")
	require.Contains(t, byDeployment, "us")

	tasks, err := svc.Storage().Tasks().GetByApplyID(t.Context(), created.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "each target runs its own statement")
	byOperation := map[int64]*storage.Task{}
	for _, task := range tasks {
		require.NotNil(t, task.ApplyOperationID)
		byOperation[*task.ApplyOperationID] = task
		assert.Contains(t, task.DDL, "ADD COLUMN `email`")
	}
	assert.Contains(t, byOperation, byDeployment["eu"].ID)
	assert.Contains(t, byOperation, byDeployment["us"].ID)
}

// Two targets planned against schemas of their own, both on the direct
// execution policy, both reshaping the users primary key: the schema change
// engine refuses the reshape and the policy routes it to native DDL on each
// target. The comment the operator confirms discloses that direct change under
// the targets that run it, so apply-confirm creates one apply in which us's
// task carries us's own direct verdict.
func TestE2EApplyConfirmRunsAnotherTargetsDisclosedDirectChange(t *testing.T) {
	dbName := "webhook_rollout_direct"
	preReshape := "CREATE TABLE `users` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `tenant_id` bigint unsigned NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: preReshape, engineMetadata: directPolicyMetadata},
		{name: "us", liveSchema: preReshape, engineMetadata: directPolicyMetadata},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	files := map[string]string{"users.sql": pkSwapSchema}

	runRolloutCommandWithFiles(t, svc, dbName, "schemabot plan -e "+driftEnv, files)

	apply := runRolloutCommandWithFiles(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe", files)
	body := awaitCapture(t, apply.comments, "the apply command's answer", func(body string) bool {
		return strings.Contains(body, "Confirmation required") || strings.Contains(body, "nothing was applied") || strings.Contains(body, "Failed to execute apply")
	})
	require.Contains(t, body, "Confirmation required", "another target's work pauses for apply-confirm")
	assert.Contains(t, body, "`eu`, `us`")
	assert.Contains(t, body, "**Direct execution**", "the comment discloses the direct change under the targets that run it")
	requireNoApplies(t, svc, dbName)

	confirm := runRolloutCommandWithFiles(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv+" --allow-unsafe", files)

	created := awaitConfirmedApply(t, svc, dbName, confirm)

	operations, err := svc.Storage().ApplyOperations().ListByApply(t.Context(), created.ID)
	require.NoError(t, err)
	deploymentOf := map[int64]string{}
	for _, op := range operations {
		deploymentOf[op.ID] = op.Deployment
	}
	tasks, err := svc.Storage().Tasks().GetByApplyID(t.Context(), created.ID)
	require.NoError(t, err)
	modes := map[string]string{}
	for _, task := range tasks {
		require.NotNil(t, task.ApplyOperationID)
		modes[deploymentOf[*task.ApplyOperationID]] = task.ExecutionMode
	}
	assert.Equal(t, map[string]string{"eu": "direct", "us": "direct"}, modes, "each target's task carries its own plan's verdict")
}

// Two targets planned against schemas of their own, both needing the column.
// eu also drops nickname, and us, which has drifted, drops legacy_id besides,
// a change the primary plan does not carry. The plan comment discloses us's
// unsafe change under us, so the apply runs every target the way a single
// target runs: without --allow-unsafe it is refused, naming us's change, and
// with it, it pauses on the comment that discloses each target's changes and
// apply-confirm runs us's own plan.
func TestE2EIndependentRolloutRunsAnotherTargetsDisclosedUnsafeChange(t *testing.T) {
	dbName := "webhook_rollout_unsafe"
	withNickname := strings.Replace(usersBaseSchema, "  PRIMARY KEY", "  `nickname` varchar(64) DEFAULT NULL,\n  PRIMARY KEY", 1)
	withLegacyID := strings.Replace(withNickname, "  PRIMARY KEY", "  `legacy_id` bigint DEFAULT NULL,\n  PRIMARY KEY", 1)
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: withNickname},
		{name: "us", liveSchema: withLegacyID},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	plan := runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)
	planBody := awaitCommentContaining(t, plan, "DROP COLUMN `legacy_id`")
	assert.Contains(t, planBody, "unsafe change", "the comment discloses us's unsafe change")
	assert.NotContains(t, planBody, "cannot apply every target", "another target's unsafe change is not refused")
	assert.Contains(t, planBody, "schemabot apply -e "+driftEnv)
	assert.Equal(t, "action_required", rolloutCheck(t, svc, dbName).Conclusion)

	refused := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, refused, "Apply rejected")
	assert.Contains(t, body, "`users` on target `us`", "the refusal names the target whose unsafe change needs the opt-in")
	assert.Contains(t, body, "--allow-unsafe")
	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe")
	body = awaitCapture(t, apply.comments, "the apply command's answer", func(body string) bool {
		return strings.Contains(body, "Confirmation required") || strings.Contains(body, "nothing was applied")
	})
	require.Contains(t, body, "Confirmation required", "another target's work pauses for apply-confirm")
	requireNoApplies(t, svc, dbName)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv+" --allow-unsafe")
	created := awaitConfirmedApply(t, svc, dbName, confirm)
	requireTaskDDLByDeployment(t, svc, created, map[string][]string{
		"eu": {"DROP COLUMN `nickname`"},
		"us": {"DROP COLUMN `nickname`", "DROP COLUMN `legacy_id`"},
	})
}

// requireTaskDDLByDeployment asserts that each deployment's task in the apply
// carries every listed fragment, so each target is shown to run its own plan.
func requireTaskDDLByDeployment(t *testing.T, svc *api.Service, created *storage.Apply, want map[string][]string) {
	t.Helper()
	operations, err := svc.Storage().ApplyOperations().ListByApply(t.Context(), created.ID)
	require.NoError(t, err)
	deploymentOf := map[int64]string{}
	for _, op := range operations {
		deploymentOf[op.ID] = op.Deployment
	}
	tasks, err := svc.Storage().Tasks().GetByApplyID(t.Context(), created.ID)
	require.NoError(t, err)
	ddl := map[string]string{}
	for _, task := range tasks {
		require.NotNil(t, task.ApplyOperationID)
		ddl[deploymentOf[*task.ApplyOperationID]] += task.DDL
	}
	require.Len(t, ddl, len(want), "every target with work runs a task, and no other target does")
	for deployment, fragments := range want {
		for _, fragment := range fragments {
			assert.Contains(t, ddl[deployment], fragment, "deployment %s", deployment)
		}
	}
}

// The same rollout, planned automatically when the PR opens. The primary's own
// plan is empty, but the check is pending on us, so auto-plan must post its
// comment rather than skip it as a no-op: a check that is not passing always
// has a comment on the PR explaining it.
func TestE2EAutoPlanPostsCommentWhenOnlyAnotherTargetHasWork(t *testing.T) {
	dbName := "webhook_rollout_autoplan"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)

	result := runRolloutWebhook(t, svc, dbName, buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action: "opened", headSHA: "abc123", headRef: "feature-branch",
	}, nil))

	body := awaitCommentContaining(t, result, "### Target `us`\n\n```sql\n")
	assert.Contains(t, body, "### Target `eu`\n\nNo schema changes detected\n\n")
	assert.Equal(t, "action_required", rolloutCheck(t, svc, dbName).Conclusion)
}

// Two targets expected to mirror each other, where the primary target (eu)
// already has the column and us has drifted behind it. The apply command runs
// the same rollup the plan command does, so the drift blocks: the apply refuses
// without running anything and the check fails closed with a drift block,
// rather than passing on the primary's empty plan.
func TestE2EConvergedPrimaryWithDriftedMirrorBlocksApply(t *testing.T) {
	dbName := "webhook_rollout_mirror_drift"
	svc := setupE2EReviewDriftService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "could not confirm that the other targets do")
	assert.Contains(t, body, "diverged: us")
	assert.Contains(t, body, "nothing was applied")

	requireNoApplies(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "failure", check.Conclusion)
	assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, check.BlockingReason)
}

// Two targets expected to mirror each other, where the primary target (eu)
// still needs the column and us already has it. The rollout round runs for a
// primary target with work too, so us's drift blocks the apply: nothing runs,
// no lock is taken, and the check fails closed with a drift block rather than
// letting the primary plan run on a target it no longer describes.
func TestE2EDriftedMirrorBlocksApplyWhenTheReviewedTargetHasWork(t *testing.T) {
	dbName := "webhook_rollout_mirror_drift_work"
	svc := setupE2EReviewDriftService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersWithEmailSchema},
	})
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "nothing was applied")
	assert.Contains(t, body, "SchemaBot could not confirm the plan of every target")
	assert.Contains(t, body, "diverged: us")
	assert.NotContains(t, body, "The primary target already has this schema", "the primary target has work of its own")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "failure", check.Conclusion)
	assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, check.BlockingReason)
}

// Both targets need the column, so the apply pauses on a comment that renders
// each target's own plan. Before the operator confirms, us can no longer be
// planned. apply-confirm plans every target again, and a target it cannot plan
// is unknown work, so it refuses, runs nothing on either target, and releases
// the pending confirmation.
func TestE2EApplyConfirmRefusesWhenATargetCanNoLongerBePlanned(t *testing.T) {
	dbName := "webhook_rollout_confirm_errored"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "Confirmation required")

	admin := openDriftDB(t, driftDSN(t, ""))
	_, err := admin.ExecContext(t.Context(), "DROP DATABASE `"+dbName+"_us`")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "SchemaBot could not confirm the plan of every target")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	assert.Equal(t, "failure", rolloutCheck(t, svc, dbName).Conclusion, "a target that could not be planned fails the check closed")
}

// When every target already has the PR's schema, the apply command still
// answers with the no-changes plan comment and records a passing check: the
// rollout round confirms the rollout is done instead of assuming it.
func TestE2EConvergedRolloutApplyReportsNoChanges(t *testing.T) {
	dbName := "webhook_rollout_converged"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersWithEmailSchema},
	}, api.PlanIndependent)

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "No schema changes detected")

	requireNoApplies(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "success", check.Conclusion)
	assert.False(t, check.HasChanges)
}

// An operator confirms a plan in which both targets still needed the column,
// and by the time they confirm eu has converged while us's schema has changed,
// so us would now run a statement the confirmed round never planned. The
// confirm's re-plan of eu is empty, and apply-confirm must answer from the
// whole rollout rather than that empty plan. us's work is not what the
// confirmation was given against, so it refuses, runs nothing on us, releases
// the pending confirmation, and leaves the check pending.
func TestE2EApplyConfirmOnConvergedPrimaryWithPendingTargetRefuses(t *testing.T) {
	dbName := "webhook_rollout_confirm"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)

	runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)
	plans, err := svc.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	var reviewed *storage.Plan
	for _, plan := range plans {
		if plan.Database == dbName && plan.PrimaryPlanIdentifier == "" {
			reviewed = plan
		}
	}
	require.NotNil(t, reviewed, "the plan command stores the primary target's plan")

	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		Owner:         "octocat/hello-world#1",
		PendingPlanID: reviewed.PlanIdentifier,
	}))
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err = eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) DEFAULT NULL")
	require.NoError(t, err)
	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "1 of 2 targets need this change")
	assert.Contains(t, body, "nothing was applied")

	requireNoApplies(t, svc, dbName)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "the refused confirmation releases the pending lock")
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion)
	assert.True(t, check.HasChanges)
}

// The primary target (eu) already has the column and us does not, so the
// apply pauses on a comment that shows us's plan alone. Before the operator
// confirms, eu loses the column and needs the change again. The confirmed
// comment never showed eu's plan, so apply-confirm refuses, runs nothing on
// either target, and releases the pending confirmation.
func TestE2EApplyConfirmRefusesWhenConvergedPrimaryGainsChanges(t *testing.T) {
	dbName := "webhook_rollout_primary_regained"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Confirmation required")
	assert.Contains(t, body, "The primary target already has this schema")

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` DROP COLUMN `email`")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body = awaitCommentContaining(t, confirm, "now has changes of its own")
	assert.Contains(t, body, "showed the primary target already at the desired schema")
	assert.Contains(t, body, "nothing was applied")

	requireNoApplies(t, svc, dbName)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "the refused confirmation releases the pending lock")
}

// usersWithLegacySchema is a live schema carrying a column the PR's schema
// does not declare, so bringing it to the PR's schema drops that column.
const usersWithLegacySchema = "CREATE TABLE `users` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `name` varchar(255) NOT NULL,\n" +
	"  `legacy` varchar(64) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// requireNoApplyLock asserts that no lock is held on the rollout fixture's
// database, so a refused command leaves nothing for the next one to clear.
func requireNoApplyLock(t *testing.T, svc *api.Service, dbName string) {
	t.Helper()
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "a refused command holds no lock")
}

// The primary target (eu) already has the column, and us needs it too, but
// bringing us to the PR's schema also drops a column only us carries. That drop
// is unsafe, so the apply asks for the same opt-in a single target would:
// without --allow-unsafe it is refused before taking a lock, naming us's table,
// and with it, apply-confirm runs us's plan though the primary target has
// nothing to do.
func TestE2EConvergedPrimaryRunsUnsafeWorkOnAnotherTargetUnderTheOptIn(t *testing.T) {
	dbName := "webhook_rollout_member_unsafe"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersWithLegacySchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	refused := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, refused, "Apply rejected")
	assert.Contains(t, body, "`users` on target `us`", "the refusal names the target and the table whose change needs the opt-in")
	assert.Contains(t, body, "--allow-unsafe")
	assert.NotContains(t, body, "schemabot apply-confirm", "a refused apply offers no confirmation")
	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
	assert.True(t, check.HasChanges)

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe")
	body = awaitCapture(t, apply.comments, "the apply command's answer", func(body string) bool {
		return strings.Contains(body, "Confirmation required") || strings.Contains(body, "nothing was applied")
	})
	require.Contains(t, body, "Confirmation required")
	assert.Contains(t, body, "DROP COLUMN `legacy`", "the comment the operator confirms shows us's plan")

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv+" --allow-unsafe")
	created := awaitConfirmedApply(t, svc, dbName, confirm)
	requireTaskDDLByDeployment(t, svc, created, map[string][]string{
		"us": {"ADD COLUMN `email`", "DROP COLUMN `legacy`"},
	})
}

// The primary (eu) is already at the PR's schema, and only us's plan
// drops a column on `users`, a table another open pull request last changed.
// Whether a destructive change is this pull request's to make is part of what
// --allow-unsafe consents to, so the unsafe refusal and the comment the
// operator confirms both name the other pull request, though the primary
// plan changes nothing.
func TestE2EAnotherTargetsDestructiveChangeDisclosesAttributedTable(t *testing.T) {
	dbName := "webhook_rollout_member_attributed"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersWithLegacySchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	now := time.Now()
	_, err := svc.Storage().Tasks().Create(t.Context(), &storage.Task{
		TaskIdentifier: fmt.Sprintf("task_%s_%d", dbName, now.UnixNano()),
		ApplyID:        1,
		PlanID:         1,
		Database:       dbName,
		DatabaseType:   "mysql",
		Engine:         "spirit",
		Repository:     "octocat/hello-world",
		PullRequest:    2,
		Environment:    driftEnv,
		State:          state.Task.Completed,
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `legacy` varchar(64) DEFAULT NULL",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)
	command := func(comment string) *planFlowResult {
		return runRolloutWebhookWithFiles(t, svc, dbName, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil),
			map[string]string{"users.sql": usersWithEmailSchema}, func(mux *http.ServeMux) { registerOpenPullRequest(mux, 2) })
	}
	const owner = "[octocat/hello-world#2](https://github.com/octocat/hello-world/pull/2)"

	body := awaitCommentContaining(t, command("schemabot apply -e "+driftEnv), "Apply rejected")
	assert.Contains(t, body, "`users` on target `us`", "the refusal names us's unsafe change")
	assert.Contains(t, body, "⚠️ **Check before applying**")
	assert.Contains(t, body, owner, "the refusal names the open pull request that last changed the table us would drop a column on")

	body = awaitCapture(t, command("schemabot apply -e "+driftEnv+" --allow-unsafe").comments, "the apply command's answer", func(body string) bool {
		return strings.Contains(body, "Confirmation required") || strings.Contains(body, "nothing was applied")
	})
	require.Contains(t, body, "Confirmation required")
	assert.Contains(t, body, owner, "the comment the operator confirms names the other pull request too")
	requireNoApplies(t, svc, dbName)
}

// The primary target (eu) already has the column and us does not, so the
// apply pauses on a comment that shows us's `ADD COLUMN`. Before the operator
// confirms, us gains a narrower `email` column out of band, so its plan is now a
// `MODIFY COLUMN` the comment never showed. The confirmation was given against
// the statements on that comment, so apply-confirm refuses, runs nothing on
// either target, and releases the pending confirmation.
func TestE2EApplyConfirmRefusesWhenAnotherTargetsStatementsChange(t *testing.T) {
	dbName := "webhook_rollout_member_changed"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Confirmation required")
	assert.Contains(t, body, "ADD COLUMN `email`", "the comment shows the plan us would run")

	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err := us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body = awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the plan of target us/"+dbName+"-us-target differs from what the confirmed round planned, in its statements")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// Both targets need the column, so the apply pauses on a comment that renders
// each target's own `ADD COLUMN`. Before the operator confirms, the reviewed
// target (eu) gains a narrower `email` column out of band, so its own re-plan is
// now a `MODIFY COLUMN` the comment never showed, while us is unchanged.
// apply-confirm refuses, runs nothing, releases the pending confirmation, and
// tells the operator it is the primary plan that changed.
func TestE2EApplyConfirmRefusesWhenTheReviewedTargetsStatementsChange(t *testing.T) {
	dbName := "webhook_rollout_reviewed_changed"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Confirmation required")
	assert.Contains(t, body, "Each target runs its own plan")

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body = awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of the primary target differs from the confirmed plan in its statements")
	assert.NotContains(t, body, "other than the primary", "the primary target's plan changed, not the other targets'")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// The operator confirmed a round where both eu (the primary target) and us
// needed the email column. Before the confirm, us gains the column out of band,
// so no other target has work left, and eu's schema changes too. The re-plan of
// eu now runs a statement the confirmed comment never showed, so apply-confirm
// refuses, names the primary target, and releases the lock rather than
// applying it.
func TestE2EApplyConfirmRefusesWhenOnlyTheReviewedTargetsStatementsChange(t *testing.T) {
	dbName := "webhook_rollout_reviewed_only_changed"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "Confirmation required")

	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err := us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL DEFAULT NULL")
	require.NoError(t, err)
	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err = eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of the primary target differs from the confirmed plan in its statements")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// The reviewed primary (eu) needs ADD email while us needs MODIFY email. By
// confirm, eu has converged and us needs the same ADD the comment showed for eu.
// Reordering or shrinking the rollout to make us primary must refuse: identical
// DDL on another target does not transfer the reviewed consent. If every member
// converged, confirmation can still report no changes because it runs no work.
func TestE2EApplyConfirmRefusesAChangedPrimaryWithIdenticalDDL(t *testing.T) {
	for _, tc := range []struct {
		name      string
		dbName    string
		members   []string
		converged bool
	}{
		{name: "reordered", dbName: "webhook_primary_reordered", members: []string{"us", "eu"}},
		{name: "shrunk", dbName: "webhook_primary_replaced", members: []string{"us"}},
		{name: "converged", dbName: "webhook_primary_no_work", members: []string{"us", "eu"}, converged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := setupE2ERolloutService(t, tc.dbName, []deploymentSpec{
				{name: "eu", liveSchema: usersBaseSchema},
				{name: "us", liveSchema: strings.Replace(usersWithEmailSchema, "varchar(255) DEFAULT NULL", "varchar(16) DEFAULT NULL", 1)},
			}, api.PlanIndependent)
			t.Cleanup(func() {
				_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), tc.dbName, "mysql")
			})

			apply := runRolloutCommand(t, svc, tc.dbName, "schemabot apply -e "+driftEnv)
			body := awaitCommentContaining(t, apply, "Confirmation required")
			assert.Contains(t, body, "ADD COLUMN `email`")
			assert.Contains(t, body, "MODIFY COLUMN `email`")
			lock, err := svc.Storage().Locks().Get(t.Context(), tc.dbName, "mysql")
			require.NoError(t, err)
			require.NotNil(t, lock)
			pinned, err := svc.Storage().Plans().Get(t.Context(), lock.PendingPlanID)
			require.NoError(t, err)
			require.NotNil(t, pinned)
			require.Equal(t, "eu", pinned.Deployment)

			eu := openDriftDB(t, driftDSN(t, tc.dbName+"_eu"))
			_, err = eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) DEFAULT NULL")
			require.NoError(t, err)
			us := openDriftDB(t, driftDSN(t, tc.dbName+"_us"))
			_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` DROP COLUMN `email`")
			require.NoError(t, err)
			if tc.converged {
				_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) DEFAULT NULL")
				require.NoError(t, err)
			}
			changed := rolloutServiceOver(t, svc, tc.dbName, tc.members...)

			confirm := runRolloutCommand(t, changed, tc.dbName, "schemabot apply-confirm -e "+driftEnv)
			if tc.converged {
				body = awaitCommentContaining(t, confirm, "No Changes Detected")
				assert.Contains(t, body, "Lock released")
				requireNoApplies(t, changed, tc.dbName)
				requireNoApplyLock(t, changed, tc.dbName)
				return
			}
			body = awaitCommentContaining(t, confirm, "nothing was applied")
			assert.Contains(t, body, "the primary target is not the one the confirmed plan reviewed")
			assert.Contains(t, body, "Run apply again for this environment")
			requireNoApplies(t, changed, tc.dbName)
			requireNoApplyLock(t, changed, tc.dbName)

			plans, err := changed.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
			require.NoError(t, err)
			var current *storage.Plan
			for _, plan := range plans {
				if plan.Database == tc.dbName && plan.Deployment == "us" && plan.PrimaryPlanIdentifier == "" {
					current = plan
					break
				}
			}
			require.NotNil(t, current, "confirm stored the new primary's plan")
			require.Len(t, pinned.FlatDDLChanges(), 1)
			require.Len(t, current.FlatDDLChanges(), 1)
			assert.Equal(t, pinned.FlatDDLChanges()[0].DDL, current.FlatDDLChanges()[0].DDL, "the refusal is for target identity, not different DDL")
			var emailColumns int
			err = us.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'users' AND column_name = 'email'").Scan(&emailColumns)
			require.NoError(t, err)
			assert.Zero(t, emailColumns, "the unconfirmed ADD did not run on us")
		})
	}
}

// The reviewed primary (eu) needs ADD email, us needs MODIFY email, and ap
// needs ADD email. By confirm, eu has converged, us needs the same ADD the
// comment showed for eu, and ap still needs its reviewed ADD. Reordering the
// rollout so us leads leaves other-target work, so the confirmation is judged
// against every target's work: identical DDL on us must not inherit eu's
// consent, and nothing runs on us or ap.
func TestE2EApplyConfirmRefusesAChangedPrimaryWhileAnotherTargetHasWork(t *testing.T) {
	const dbName = "webhook_primary_moved_member_work"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: strings.Replace(usersWithEmailSchema, "varchar(255) DEFAULT NULL", "varchar(16) DEFAULT NULL", 1)},
		{name: "ap", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Confirmation required")
	require.Contains(t, body, "MODIFY COLUMN `email`")

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) DEFAULT NULL")
	require.NoError(t, err)
	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` DROP COLUMN `email`")
	require.NoError(t, err)
	changed := rolloutServiceOver(t, svc, dbName, "us", "eu", "ap")

	confirm := runRolloutCommand(t, changed, dbName, "schemabot apply-confirm -e "+driftEnv)
	body = awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "the primary target is not the one the confirmed plan reviewed")
	requireNoApplies(t, changed, dbName)
	requireNoApplyLock(t, changed, dbName)
}

// The reviewed primary (eu) already has email, so the comment shows only us's
// ADD. The rollout is then reordered so us leads, with the same ADD. The
// refusal names the moved primary rather than claiming the primary target
// gained changes the comment did not show, and releases the confirmation.
func TestE2EApplyConfirmConvergedPrimaryMovedNamesTheMovedPrimary(t *testing.T) {
	const dbName = "webhook_converged_primary_moved"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Confirmation required")
	require.Contains(t, body, "The primary target already has this schema")
	require.Contains(t, body, "ADD COLUMN `email`")
	changed := rolloutServiceOver(t, svc, dbName, "us", "eu")

	confirm := runRolloutCommand(t, changed, dbName, "schemabot apply-confirm -e "+driftEnv)
	body = awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "the primary target is not the one the confirmed plan reviewed")
	assert.NotContains(t, body, "now has changes of its own")
	requireNoApplies(t, changed, dbName)
	requireNoApplyLock(t, changed, dbName)
}

// rolloutServiceOver builds a service over svc's storage whose environment
// routes dbName to the named deployments alone, as a server whose rollout
// topology changed after a confirmation was given would. The deployments'
// databases, the stored plans and the lock are left as svc's commands left them.
func rolloutServiceOver(t *testing.T, svc *api.Service, dbName string, names ...string) *api.Service {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	deployments := make(map[string]api.DeploymentTarget, len(names))
	ternClients := make(map[string]tern.Client, len(names))
	for _, name := range names {
		client, err := tern.NewLocalClient(tern.LocalConfig{
			Database:  dbName,
			Type:      "mysql",
			TargetDSN: driftDSN(t, dbName+"_"+name),
		}, svc.Storage(), logger)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })
		deployments[name] = api.DeploymentTarget{Targets: []api.TargetEntry{{Target: dbName + "-" + name + "-target"}}}
		ternClients[name+"/"+driftEnv] = client
	}
	serverConfig := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			dbName: {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					driftEnv: {Deployments: deployments, DeploymentOrder: names},
				},
			},
		},
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}
	shrunk := api.New(svc.Storage(), serverConfig, ternClients, logger)
	t.Cleanup(func() { _ = shrunk.Close() })
	return shrunk
}

// awaitRolloutApply waits for the confirmation to create an apply for dbName,
// failing the test if the confirm posts a refusal instead.
func awaitRolloutApply(t *testing.T, svc *api.Service, dbName string, confirm *planFlowResult) *storage.Apply {
	t.Helper()
	var created *storage.Apply
	require.Eventually(t, func() bool {
		applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
		if err != nil {
			return false
		}
		for _, a := range applies {
			if a.Database == dbName {
				created = a
				return true
			}
		}
		select {
		case posted := <-confirm.comments:
			require.NotContains(t, posted, "nothing was applied", "the confirmation must create the apply")
		default:
		}
		return false
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "the confirmation creates an apply")
	return created
}

// The operator confirmed a round where eu (the primary target) and us both
// needed the email column. Before the confirm, us is removed from the rollout,
// so eu is the environment's only target, and eu gains a narrower email column
// out of band, so its re-plan is now a `MODIFY COLUMN` the confirmed comment
// never showed. The confirmation was given on a rollout's comment, so
// apply-confirm still holds eu to it: it refuses, runs nothing, and releases
// the pending confirmation.
func TestE2EApplyConfirmRechecksTheReviewedTargetAfterTheRolloutShrinks(t *testing.T) {
	dbName := "webhook_rollout_shrunk_changed"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Confirmation required")
	assert.Contains(t, body, "Each target runs its own plan")

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)
	shrunk := rolloutServiceOver(t, svc, dbName, "eu")

	confirm := runRolloutCommand(t, shrunk, dbName, "schemabot apply-confirm -e "+driftEnv)
	body = awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of the primary target differs from the confirmed plan in its statements")

	requireNoApplies(t, shrunk, dbName)
	requireNoApplyLock(t, shrunk, dbName)
}

// The same rollout shrinks to eu before the confirm, but eu's plan is still the
// `ADD COLUMN` the confirmed comment showed, so the confirmation covers it and
// apply-confirm runs it on eu.
func TestE2EApplyConfirmRunsTheConfirmedPlanAfterTheRolloutShrinks(t *testing.T) {
	dbName := "webhook_rollout_shrunk_unchanged"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "Confirmation required")
	shrunk := rolloutServiceOver(t, svc, dbName, "eu")

	confirm := runRolloutCommand(t, shrunk, dbName, "schemabot apply-confirm -e "+driftEnv)
	created := awaitRolloutApply(t, shrunk, dbName, confirm)

	tasks, err := shrunk.Storage().Tasks().GetByApplyID(t.Context(), created.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "only eu is left in the rollout")
	assert.Equal(t, "users", tasks[0].TableName)
	assert.Contains(t, tasks[0].DDL, "ADD COLUMN `email`")
}

// The operator confirmed a round where eu (the primary target) and us both
// needed the email column, and the confirmed plan also ended eu's namespace
// with a finalizer. Before the confirm, us gains the column out of band, so no
// other target has work left. eu's re-plan carries the same `ADD COLUMN` but no
// finalizer, so it is not the work the confirmed comment showed: apply-confirm
// compares every part of the plan, not only its table statements, and refuses.
func TestE2EApplyConfirmComparesTheReviewedTargetsWholePlan(t *testing.T) {
	dbName := "webhook_rollout_reviewed_finalizer"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "Confirmation required")

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the paused apply pins its plan")
	confirmed, err := svc.Storage().Plans().Get(t.Context(), lock.PendingPlanID)
	require.NoError(t, err)
	require.NotNil(t, confirmed)
	require.NotEmpty(t, confirmed.Namespaces)
	confirmed.ID = 0
	confirmed.PlanIdentifier = "plan-with-finalizer-" + dbName
	for _, namespace := range confirmed.Namespaces {
		namespace.Finalize = true
	}
	_, err = svc.Storage().Plans().Create(t.Context(), confirmed)
	require.NoError(t, err)
	lock.PendingPlanID = confirmed.PlanIdentifier
	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), lock))

	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of the primary target differs from the confirmed plan in which namespaces it finalizes")
	assert.NotContains(t, body, "in its statements", "the statements are the same on both sides, so the refusal does not point at them")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// Two targets reshape the users primary key under the direct execution policy,
// so the apply pauses on a rollout comment that shows each target's plan. Before
// the confirm, us gets the reshape out of band, leaving only eu (the reviewed
// target) with work, and the way eu's unchanged statement runs moves:
//
//   - newly direct: the confirmed comment showed the reshape running through the
//     schema change engine, and the re-plan routes it to native DDL;
//   - newly blocked: the confirmed comment disclosed it as direct execution, and
//     the policy is then switched off, so the re-plan blocks it.
//
// How each statement runs is part of the plan the confirmation was given
// against, so apply-confirm refuses and names that as what differs. It does not
// stop to disclose the change for another confirmation or reject it as blocked,
// as a single target's apply-confirm would: it runs nothing, leaves eu's primary
// key alone, and releases the lock so the operator reviews the rollout again.
func TestE2EApplyConfirmRefusesWhenHowTheReviewedTargetsStatementRunsMoves(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dbName     string
		pinnedMode string
		confirmVia func(t *testing.T, svc *api.Service, dbName string) *api.Service
	}{
		{
			name:       "newly direct",
			dbName:     "webhook_rollout_reviewed_newly_direct",
			pinnedMode: "",
			confirmVia: func(_ *testing.T, svc *api.Service, _ string) *api.Service { return svc },
		},
		{
			name:       "newly blocked",
			dbName:     "webhook_rollout_reviewed_newly_blocked",
			pinnedMode: "direct",
			confirmVia: func(t *testing.T, svc *api.Service, dbName string) *api.Service {
				return rolloutServiceOver(t, svc, dbName, "eu", "us")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbName := tc.dbName
			preReshape := "CREATE TABLE `users` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `tenant_id` bigint unsigned NOT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
			svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
				{name: "eu", liveSchema: preReshape, engineMetadata: directPolicyMetadata},
				{name: "us", liveSchema: preReshape, engineMetadata: directPolicyMetadata},
			}, api.PlanIndependent)
			t.Cleanup(func() {
				_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
			})
			files := map[string]string{"users.sql": pkSwapSchema}

			apply := runRolloutCommandWithFiles(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe", files)
			body := awaitCommentContaining(t, apply, "Confirmation required")
			require.Contains(t, body, "**Direct execution**", "the policy routes the reshape to direct execution on both targets")

			lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
			require.NoError(t, err)
			require.NotNil(t, lock, "the paused apply pins its plan")
			confirmed, err := svc.Storage().Plans().Get(t.Context(), lock.PendingPlanID)
			require.NoError(t, err)
			require.NotNil(t, confirmed)
			require.Len(t, confirmed.FlatDDLChanges(), 1)
			require.Equal(t, "direct", confirmed.FlatDDLChanges()[0].ExecutionMode)
			if tc.pinnedMode != "direct" {
				confirmed.ID = 0
				confirmed.PlanIdentifier = "plan-engine-routed-" + dbName
				for _, namespace := range confirmed.Namespaces {
					for i := range namespace.Tables {
						namespace.Tables[i].ExecutionMode = tc.pinnedMode
						namespace.Tables[i].ModeReason = ""
					}
				}
				_, err = svc.Storage().Plans().Create(t.Context(), confirmed)
				require.NoError(t, err)
				lock.PendingPlanID = confirmed.PlanIdentifier
				require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), lock))
			}

			us := openDriftDB(t, driftDSN(t, dbName+"_us"))
			_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`,`tenant_id`)")
			require.NoError(t, err)

			confirmer := tc.confirmVia(t, svc, dbName)
			confirm := runRolloutCommandWithFiles(t, confirmer, dbName, "schemabot apply-confirm -e "+driftEnv+" --allow-unsafe", files)
			body = awaitCommentContaining(t, confirm, "nothing was applied")
			assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of the primary target differs from the confirmed plan in how its statements run")
			assert.NotContains(t, body, "Changes run differently", "a rollout confirmation is refused, not re-disclosed for another confirmation")

			requireNoApplies(t, svc, dbName)
			requireNoApplyLock(t, svc, dbName)
			assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, dbName+"_eu", "users"),
				"the refused confirm left eu's primary key unchanged")
		})
	}
}

// planResultFailingStorage fails every plan-result write to stored check
// state, so a test can observe what the PR's check shows when the write that
// records a pending rollout never lands.
type planResultFailingStorage struct {
	storage.Storage
}

func (s *planResultFailingStorage) Checks() storage.CheckStore {
	return &planResultFailingCheckStore{CheckStore: s.Storage.Checks()}
}

type planResultFailingCheckStore struct {
	storage.CheckStore
}

func (s *planResultFailingCheckStore) UpsertPlanResult(context.Context, *storage.Check, storage.PlanDriftState) (bool, error) {
	return false, errors.New("store plan result: injected failure")
}

// The primary target (eu) already has the column and us does not, and the PR
// carries a passing check from before. When storing the check record fails, a
// command that finds us still needs the change must not leave that stale pass
// as the PR's check: it publishes a failing check from the rollout round. A plan
// scoped to one environment, a plan across every environment, and an apply
// paused for confirmation each store the record on a path of their own, so all
// three are covered.
func TestE2EUnstoredPendingRolloutFailsCheckClosed(t *testing.T) {
	rollout := []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}
	pending := "(1 of 2 targets need this change)"
	for _, tc := range []struct {
		name    string
		dbName  string
		command string
		specs   []deploymentSpec
		summary string
	}{
		{name: "plan one environment", dbName: "webhook_rollout_unstored_plan", command: "schemabot plan -e " + driftEnv, specs: rollout, summary: pending},
		{name: "plan every environment", dbName: "webhook_rollout_unstored_plan_all", command: "schemabot plan", specs: rollout, summary: pending},
		{name: "apply", dbName: "webhook_rollout_unstored_apply", command: "schemabot apply -e " + driftEnv, specs: rollout, summary: pending},
		// A single target with work of its own fails closed the same way, and
		// the summary names no target count.
		{name: "plan one target", dbName: "webhook_rollout_unstored_single", command: "schemabot plan -e " + driftEnv, specs: rollout[1:], summary: "could not record this plan's result; re-run plan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := setupE2ERolloutServiceWithStorage(t, tc.dbName, tc.specs, api.PlanIndependent, func(st storage.Storage) storage.Storage {
				return &planResultFailingStorage{Storage: st}
			})
			require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
				Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123", Environment: driftEnv,
				DatabaseType: "mysql", DatabaseName: tc.dbName, Status: checkStatusCompleted, Conclusion: "success",
			}))

			result := runRolloutCommand(t, svc, tc.dbName, tc.command)

			run := awaitCapture(t, result.checkRuns, "a failing Check Run", func(run checkRunCapture) bool {
				return run.Conclusion == checkConclusionFailure
			})
			require.NotNil(t, run.Output)
			assert.Contains(t, run.Output.Summary, tc.summary)
			requireNoApplies(t, svc, tc.dbName)
		})
	}
}

// lockRaceStorage answers every conditional lock acquire as if another owner
// took the lock between the apply's pre-check and its acquire.
type lockRaceStorage struct {
	storage.Storage
}

func (s *lockRaceStorage) Locks() storage.LockStore {
	return &lockRaceLockStore{LockStore: s.Storage.Locks()}
}

type lockRaceLockStore struct {
	storage.LockStore
}

func (s *lockRaceLockStore) AcquireIfPendingPlanID(context.Context, *storage.Lock, string) error {
	return storage.ErrLockHeld
}

// The primary target (eu) already has the column and us does not, and the PR
// carries a passing check from an earlier plan. The apply loses the lock race
// after the rollout round found us's work, so it exits before pausing for
// confirmation. The stored check state must already record us's pending work
// by then: an apply that ends early never leaves the earlier pass standing.
func TestE2EConvergedPrimaryApplyLosingTheLockRecordsPendingWork(t *testing.T) {
	dbName := "webhook_rollout_lock_race"
	svc := setupE2ERolloutServiceWithStorage(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent, func(st storage.Storage) storage.Storage {
		return &lockRaceStorage{Storage: st}
	})
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123", Environment: driftEnv,
		DatabaseType: "mysql", DatabaseName: dbName, Status: checkStatusCompleted, Conclusion: "success",
	}))

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "Failed to acquire lock")
	assert.NotContains(t, body, "schemabot apply-confirm", "an apply that lost the lock offers no confirmation")

	requireNoApplies(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the earlier pass is replaced")
	assert.True(t, check.HasChanges)
}

// seedUsersCopy puts an unfinished copy of `users` on a target: the shadow table
// the engine builds rows into, plus a checkpoint recording a different statement
// than the one the PR's plan hands the engine, so applying that plan discards it.
func seedUsersCopy(t *testing.T, physicalDB string) {
	t.Helper()
	db := openDriftDB(t, driftDSN(t, physicalDB))
	_, err := db.ExecContext(t.Context(), fmt.Sprintf(
		"CREATE TABLE `%s` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", utils.NewTableName("users")))
	require.NoError(t, err, "seed shadow table")

	cp := checkpoint.NewTable(db, utils.CheckpointTableName("users"), checkpoint.Transient)
	require.NoError(t, cp.Create(t.Context()), "create checkpoint table")
	require.NoError(t, cp.Write(t.Context(), checkpoint.Record{
		Statement:       "ALTER TABLE `users` ADD INDEX `idx_name` (`name`)",
		CopierWatermark: `{"Key":["id"],"LowerBound":3952903346}`,
		Position:        "mysql-bin.024891:19443021",
	}), "write checkpoint row")
}

// The primary target (eu) already has the column and us does not, but us
// holds an unfinished copy of `users` made for a different statement, so
// applying us's plan throws that copy away. The comment the operator would
// confirm carries no copy disclosure for another target, so the apply refuses
// before pausing: it takes no lock, runs nothing, leaves the copy in place, and
// the check keeps blocking merge.
func TestE2EConvergedPrimaryRefusesToDiscardAnotherTargetsCopy(t *testing.T) {
	dbName := "webhook_rollout_member_copy"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	seedUsersCopy(t, dbName+"_us")

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "nothing was applied")
	assert.Contains(t, body, "target us: applying its plan discards the unfinished copy of users",
		"the refusal names the target and the table whose copy would be lost")
	assert.NotContains(t, body, "schemabot apply-confirm", "a refused apply offers no confirmation")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	requireUsersCopyIntact(t, dbName+"_us")
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
}

// The primary target (eu) already has the column and us does not, so the
// apply pauses on a comment that shows us's plan. Before the operator confirms,
// an unfinished copy of `users` made for a different statement appears on us.
// The confirmed comment disclosed no copy, so apply-confirm refuses, runs
// nothing, leaves the copy in place, and releases the pending confirmation.
func TestE2EApplyConfirmRefusesToDiscardACopyThatAppearedOnAnotherTarget(t *testing.T) {
	dbName := "webhook_rollout_member_copy_confirm"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "Confirmation required")

	seedUsersCopy(t, dbName+"_us")

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "target us: applying its plan discards the unfinished copy of users")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	requireUsersCopyIntact(t, dbName+"_us")
}

// requireUsersCopyIntact asserts the unfinished copy seeded by seedUsersCopy is
// still on the target, so a refused apply provably discarded nothing.
func requireUsersCopyIntact(t *testing.T, physicalDB string) {
	t.Helper()
	db := openDriftDB(t, driftDSN(t, physicalDB))
	for _, table := range []string{utils.NewTableName("users"), utils.CheckpointTableName("users")} {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			physicalDB, table).Scan(&count))
		assert.Equal(t, 1, count, "the refused apply left %s on the target", table)
	}
}

// The primary (eu) already has the column, and bringing us to the PR's
// schema also drops a column only us carries. The plan comment shows us's plan
// with its unsafe change disclosed under us, and offers the apply, which
// --allow-unsafe lets run on every target. The check keeps blocking merge on
// us's work.
func TestE2EPlanDisclosesAnotherTargetsUnsafeWork(t *testing.T) {
	dbName := "webhook_rollout_plan_member_unsafe"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersWithLegacySchema},
	}, api.PlanIndependent)

	plan := runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)
	body := awaitCommentContaining(t, plan, "DROP COLUMN `legacy`")
	assert.Contains(t, body, "**Issues**: 1 unsafe change detected", "us's unsafe change is disclosed under us")
	assert.NotContains(t, body, "cannot apply every target")
	assert.Contains(t, body, "schemabot apply -e "+driftEnv)

	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion)
}
