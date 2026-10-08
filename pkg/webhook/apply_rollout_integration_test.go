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
// state of another pull request, or sets how the fake GitHub answers.
func runRolloutWebhookWithFiles(t *testing.T, svc *api.Service, dbName string, req *http.Request, files map[string]string, register ...func(*http.ServeMux, *planFlowResult)) *planFlowResult {
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
		r(mux, result)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	return result
}

// awaitRolloutApply waits for the command to create an apply on the rollout
// fixture's database, failing if it posts a refusal, an execution failure or a
// pause for apply-confirm instead.
func awaitRolloutApply(t *testing.T, svc *api.Service, dbName string, command *planFlowResult) *storage.Apply {
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
		case posted := <-command.comments:
			require.NotContains(t, posted, "Failed to execute apply", "the command must create the apply")
			require.NotContains(t, posted, "nothing was applied", "the command must create the apply")
			require.NotContains(t, posted, "Confirmation required", "the command must create the apply")
		default:
		}
		return false
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "the command creates an apply")
	return created
}

// pinRolloutConfirmation leaves the rollout fixture where an apply that stopped
// for apply-confirm leaves it: the plan command stores every target's plan,
// bound to the primary target's plan, and the lock pins that plan for
// confirmation. nil files plans the PR's default schema. It returns the plan
// command's result, whose comment is the one the confirmation is given against.
func pinRolloutConfirmation(t *testing.T, svc *api.Service, dbName string, files map[string]string) *planFlowResult {
	t.Helper()
	if files == nil {
		files = map[string]string{"users.sql": usersWithEmailSchema}
	}
	plan := runRolloutCommandWithFiles(t, svc, dbName, "schemabot plan -e "+driftEnv, files)
	plans, err := svc.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	var reviewed *storage.Plan
	for _, plan := range plans {
		if plan.Database == dbName && plan.PrimaryPlanIdentifier == "" && (reviewed == nil || plan.ID > reviewed.ID) {
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
	return plan
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
// apply command does not report "no changes": it posts a comment that shows
// us's own plan and, in the same step, creates one apply that runs us's plan
// and settles eu, which has nothing to do.
func TestE2EConvergedPrimaryWithPendingTargetAppliesEveryTarget(t *testing.T) {
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
	body := awaitCommentContaining(t, apply, "ADD COLUMN `email`")
	assert.Contains(t, body, " · rolling out to target `us`\n", "only the target that needs the change is named")
	assert.NotContains(t, body, "`eu`", "eu is already at the desired schema, so it is not named")
	assert.NotContains(t, body, "✅ **No schema changes detected**", "a target still has work, so the comment never closes as a no-op")
	assert.NotContains(t, body, "Confirmation required", "the apply runs every target's plan in one step")
	assert.NotContains(t, body, "schemabot apply-confirm")

	created := awaitRolloutApply(t, svc, dbName, apply)
	check = rolloutCheck(t, svc, dbName)
	assert.NotEqual(t, "success", check.Conclusion, "us still needs the change while the apply runs, so the check never passes on eu's empty plan")
	assert.True(t, check.HasChanges)

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
// plan: the apply command posts a comment that renders every target's plan and,
// in the same step, creates one apply that drives the column onto both targets.
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
	body := awaitCommentContaining(t, apply, "ADD COLUMN `email`")
	assert.Contains(t, body, " · rolling out to both targets\n", "the plan runs on every target")
	assert.NotContains(t, body, "Confirmation required", "the apply runs every target's plan in one step")

	created := awaitRolloutApply(t, svc, dbName, apply)

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
// target. The comment the apply command posts discloses that direct change
// on the plan both targets run, and the apply it creates in the same step
// gives us's task us's own direct verdict.
func TestE2EApplyRunsAnotherTargetsDisclosedDirectChange(t *testing.T) {
	dbName := "webhook_rollout_direct"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersPreReshapeSchema, engineMetadata: directPolicyMetadata},
		{name: "us", liveSchema: usersPreReshapeSchema, engineMetadata: directPolicyMetadata},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	files := map[string]string{"users.sql": pkSwapSchema}

	runRolloutCommandWithFiles(t, svc, dbName, "schemabot plan -e "+driftEnv, files)

	apply := runRolloutCommandWithFiles(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe", files)
	body := awaitCommentContaining(t, apply, "**Direct execution**")
	assert.Contains(t, body, " · rolling out to both targets\n", "the direct change is disclosed on the plan every target runs")

	created := awaitRolloutApply(t, svc, dbName, apply)

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

// usersPreReshapeSchema is the live `users` table pkSwapSchema reshapes: the
// primary key gains `tenant_id`, which the schema change engine refuses and the
// direct execution policy routes to native DDL.
const usersPreReshapeSchema = "CREATE TABLE `users` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `tenant_id` bigint unsigned NOT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// The primary target (eu) already has the reshaped primary key, and us, on the
// direct execution policy, still needs it, so the only change the apply would
// run is us's native DDL, which has no cutover. --defer-cutover has nothing to
// act on there, though the primary plan alone shows no direct change: the apply
// command refuses the flag before taking a lock, and an apply-confirm of a
// pending confirmation refuses it while keeping that confirmation, as they do
// for one target.
func TestE2EDeferCutoverRefusedWhenOnlyAnotherTargetRunsDirectChanges(t *testing.T) {
	dbName := "webhook_rollout_direct_defer"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: strings.TrimSuffix(pkSwapSchema, ";"), engineMetadata: directPolicyMetadata},
		{name: "us", liveSchema: usersPreReshapeSchema, engineMetadata: directPolicyMetadata},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	files := map[string]string{"users.sql": pkSwapSchema}

	deferred := runRolloutCommandWithFiles(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe --defer-cutover", files)
	body := awaitCommentContaining(t, deferred, "has no effect on this plan")
	assert.Contains(t, body, msgDeferCutoverAllDirect)
	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)

	pinRolloutConfirmation(t, svc, dbName, files)

	confirm := runRolloutCommandWithFiles(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv+" --allow-unsafe --defer-cutover", files)
	body = awaitCommentContaining(t, confirm, "has no effect on this plan")
	assert.Contains(t, body, fmt.Sprintf(msgDeferCutoverAllDirectConfirm, driftEnv))
	requireNoApplies(t, svc, dbName)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the refused flag keeps the pending confirmation")
	assert.NotEmpty(t, lock.PendingPlanID)
}

// usersWithTenantIndexSchema reshapes the primary key of `users`, as
// pkSwapSchema does, and also indexes tenant_id.
const usersWithTenantIndexSchema = "CREATE TABLE `users` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `tenant_id` bigint unsigned NOT NULL,\n" +
	"  PRIMARY KEY (`id`,`tenant_id`),\n" +
	"  KEY `idx_tenant` (`tenant_id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// The primary target (eu), on the direct execution policy, needs only the
// primary key reshape, which runs as native DDL with no cutover. us already has
// the reshaped key and needs only the index, which the schema change engine
// copies and cuts over. --defer-cutover has us's cutover to defer, so the apply
// runs every target in one step and keeps the flag, though the primary plan
// alone has nothing to defer.
func TestE2EApplyKeepsDeferCutoverForAnotherTargetsCutover(t *testing.T) {
	dbName := "webhook_rollout_mixed_defer"
	usersWithTenantIndexLive := strings.Replace(strings.TrimSuffix(usersWithTenantIndexSchema, ";"),
		"  PRIMARY KEY (`id`,`tenant_id`),\n", "  PRIMARY KEY (`id`),\n", 1)
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithTenantIndexLive, engineMetadata: directPolicyMetadata},
		{name: "us", liveSchema: strings.TrimSuffix(pkSwapSchema, ";")},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	files := map[string]string{"users.sql": usersWithTenantIndexSchema}

	plan := runRolloutCommandWithFiles(t, svc, dbName, "schemabot plan -e "+driftEnv, files)
	planBody := awaitCommentContaining(t, plan, "**Direct execution**")
	assert.Contains(t, planBody, "idx_tenant", "the comment shows us's index")

	apply := runRolloutCommandWithFiles(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe --defer-cutover", files)
	created := awaitRolloutApply(t, svc, dbName, apply)
	assert.True(t, storage.ParseApplyOptions(created.Options).DeferCutover, "the apply defers us's cutover")
}

// Two targets planned against schemas of their own, both needing the column.
// eu also drops nickname, and us, which has drifted, drops legacy_id besides,
// a change the primary plan does not carry. The plan comment discloses us's
// unsafe change under us, so the apply runs every target the way a single
// target runs: without --allow-unsafe it is refused, naming us's change, and
// with it, the apply runs us's own plan in the same step.
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
	created := awaitRolloutApply(t, svc, dbName, apply)
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

	body := awaitCommentContaining(t, result, "ADD COLUMN `email`")
	assert.Contains(t, body, " · rolling out to target `us`\n")
	assert.NotContains(t, body, "`eu`", "eu is already at the desired schema, so it is not named")
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
	assert.NotContains(t, body, "Target `eu` already has this schema", "eu has work of its own")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "failure", check.Conclusion)
	assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, check.BlockingReason)
}

// Both targets need the column, and a pending confirmation pins a round that
// planned each target's own plan. Before the operator confirms, us can no longer be
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

	pinRolloutConfirmation(t, svc, dbName, nil)

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

	pinRolloutConfirmation(t, svc, dbName, nil)

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) DEFAULT NULL")
	require.NoError(t, err)
	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "Target `us` needs this change")
	assert.Contains(t, body, "nothing was applied")

	requireNoApplies(t, svc, dbName)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "the refused confirmation releases the pending lock")
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion)
	assert.True(t, check.HasChanges)
}

// The primary target (eu) already has the column and us does not, and a
// pending confirmation pins a round that showed us's plan alone. Before the
// operator confirms, eu loses the column and needs the change again. The confirmed
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

	pinRolloutConfirmation(t, svc, dbName, nil)

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` DROP COLUMN `email`")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "now has changes of its own")
	assert.Contains(t, body, "showed target `eu` already at the desired schema")
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
// and with it, the apply runs us's plan though the primary target has nothing
// to do.
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
	body = awaitCommentContaining(t, apply, "DROP COLUMN `legacy`")
	assert.NotContains(t, body, "Confirmation required", "the comment the apply posts shows us's plan, and the apply runs it in one step")
	created := awaitRolloutApply(t, svc, dbName, apply)
	requireTaskDDLByDeployment(t, svc, created, map[string][]string{
		"us": {"ADD COLUMN `email`", "DROP COLUMN `legacy`"},
	})
}

// The primary (eu) is already at the PR's schema, and only us's plan
// drops a column on `users`, a table another open pull request last changed.
// Whether a destructive change is this pull request's to make is part of what
// --allow-unsafe consents to, so the unsafe refusal names the other pull
// request, though the primary plan changes nothing. With the opt-in, the apply
// runs us's plan in one step, as a single target's apply does.
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
			map[string]string{"users.sql": usersWithEmailSchema}, func(mux *http.ServeMux, _ *planFlowResult) { registerOpenPullRequest(mux, 2) })
	}
	const owner = "[octocat/hello-world#2](https://github.com/octocat/hello-world/pull/2)"

	body := awaitCommentContaining(t, command("schemabot apply -e "+driftEnv), "Apply rejected")
	assert.Contains(t, body, "`users` on target `us`", "the refusal names us's unsafe change")
	assert.Contains(t, body, "⚠️ **Check before applying**")
	assert.Contains(t, body, owner, "the refusal names the open pull request that last changed the table us would drop a column on")

	awaitRolloutApply(t, svc, dbName, command("schemabot apply -e "+driftEnv+" --allow-unsafe"))
}

// The primary target (eu) already has the column and us does not, and a
// pending confirmation pins a round that showed us's `ADD COLUMN`. Before the
// operator confirms, us gains a narrower `email` column out of band, so its plan is now a
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

	pinRolloutConfirmation(t, svc, dbName, nil)

	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err := us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the plan of target `us/"+dbName+"-us-target` differs from what the confirmed round planned, in its statements")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// Both targets need the column, and a pending confirmation pins a round that
// showed each target's own `ADD COLUMN`. Before the operator confirms, the reviewed
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

	pinRolloutConfirmation(t, svc, dbName, nil)

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of target `eu` differs from the confirmed plan in its statements")
	assert.NotContains(t, body, "showed only the plan of target", "eu's plan changed, not the other targets'")
	assert.NotContains(t, body, "primary target", "a refusal names the target, as the plan comment does")

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

	pinRolloutConfirmation(t, svc, dbName, nil)

	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err := us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL DEFAULT NULL")
	require.NoError(t, err)
	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err = eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of target `eu` differs from the confirmed plan in its statements")

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

			plan := pinRolloutConfirmation(t, svc, tc.dbName, nil)
			body := awaitCommentContaining(t, plan, "MODIFY COLUMN `email`")
			assert.Contains(t, body, "ADD COLUMN `email`")
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
			assert.Contains(t, body, "target `us` is not the target the confirmed plan reviewed")
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

	plan := pinRolloutConfirmation(t, svc, dbName, nil)
	awaitCommentContaining(t, plan, "MODIFY COLUMN `email`")

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(255) DEFAULT NULL")
	require.NoError(t, err)
	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err = us.ExecContext(t.Context(), "ALTER TABLE `users` DROP COLUMN `email`")
	require.NoError(t, err)
	changed := rolloutServiceOver(t, svc, dbName, "us", "eu", "ap")

	confirm := runRolloutCommand(t, changed, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "target `us` is not the target the confirmed plan reviewed")
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

	plan := pinRolloutConfirmation(t, svc, dbName, nil)
	awaitCommentContaining(t, plan, "ADD COLUMN `email`")
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock)
	pinned, err := svc.Storage().Plans().Get(t.Context(), lock.PendingPlanID)
	require.NoError(t, err)
	require.NotNil(t, pinned)
	require.Equal(t, "eu", pinned.Deployment)
	require.False(t, pinned.HasWork(), "the confirmation pins the converged primary target's empty plan")
	changed := rolloutServiceOver(t, svc, dbName, "us", "eu")

	confirm := runRolloutCommand(t, changed, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "target `us` is not the target the confirmed plan reviewed")
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

	pinRolloutConfirmation(t, svc, dbName, nil)

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
	require.NoError(t, err)
	shrunk := rolloutServiceOver(t, svc, dbName, "eu")

	confirm := runRolloutCommand(t, shrunk, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of target `eu` differs from the confirmed plan in its statements")

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

	pinRolloutConfirmation(t, svc, dbName, nil)
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

	pinRolloutConfirmation(t, svc, dbName, nil)

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the pending confirmation pins its plan")
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
	assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of target `eu` differs from the confirmed plan in which namespaces it finalizes")
	assert.NotContains(t, body, "in its statements", "the statements are the same on both sides, so the refusal does not point at them")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// Two targets reshape the users primary key under the direct execution policy,
// and a pending confirmation pins a round that showed each target's plan. Before
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

			pinRolloutConfirmation(t, svc, dbName, files)

			lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
			require.NoError(t, err)
			require.NotNil(t, lock, "the pending confirmation pins its plan")
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
			body := awaitCommentContaining(t, confirm, "nothing was applied")
			assert.Contains(t, body, "This confirmation no longer covers what the apply would run: the re-plan of target `eu` differs from the confirmed plan in how its statements run")
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
// each store the record on a path of their own, so all three are covered.
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
// after the rollout round found us's work, so it exits before running
// anything. The stored check state must already record us's pending work
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

// afterLockStorage runs onAcquire once, straight after the apply command takes
// its lock, so a test can change a target or the storage between the comment
// the apply posts and the re-plan it runs from. Once failRoundOf is set, reading
// the rollout round stored with that plan fails.
type afterLockStorage struct {
	storage.Storage
	onAcquire   func(lock *storage.Lock)
	failRoundOf string
}

func (s *afterLockStorage) Locks() storage.LockStore {
	return &afterLockLockStore{LockStore: s.Storage.Locks(), owner: s}
}

func (s *afterLockStorage) Plans() storage.PlanStore {
	return &roundFailingPlanStore{PlanStore: s.Storage.Plans(), owner: s}
}

type afterLockLockStore struct {
	storage.LockStore
	owner *afterLockStorage
}

func (s *afterLockLockStore) AcquireIfPendingPlanID(ctx context.Context, lock *storage.Lock, pendingPlanID string) error {
	if err := s.LockStore.AcquireIfPendingPlanID(ctx, lock, pendingPlanID); err != nil {
		return err
	}
	if hook := s.owner.onAcquire; hook != nil {
		s.owner.onAcquire = nil
		hook(lock)
	}
	return nil
}

type roundFailingPlanStore struct {
	storage.PlanStore
	owner *afterLockStorage
}

func (s *roundFailingPlanStore) List(ctx context.Context, opts storage.ListPlansOptions) ([]*storage.Plan, error) {
	if failing := s.owner.failRoundOf; failing != "" && opts.PrimaryPlanIdentifier == failing {
		return nil, errors.New("list rollout round: injected failure")
	}
	return s.PlanStore.List(ctx, opts)
}

// The primary target (eu) already has the column and us does not, so the
// apply command posts us's `ADD COLUMN` and runs it in the same step. Between
// that comment and the re-plan the apply runs from, us gains a narrower `email`
// column out of band, so its plan is now a `MODIFY COLUMN` the comment never
// showed. As with a single target whose DDL drifted, the apply stops for
// apply-confirm on a fresh comment showing us's plan as it is now, runs
// nothing, and pins the confirmation to that plan, so confirming runs the
// `MODIFY COLUMN`.
func TestE2EApplyStopsForConfirmationWhenAnotherTargetsPlanChangesWhileStarting(t *testing.T) {
	dbName := "webhook_rollout_member_changed_auto"
	var hooked *afterLockStorage
	svc := setupE2ERolloutServiceWithStorage(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent, func(st storage.Storage) storage.Storage {
		hooked = &afterLockStorage{Storage: st}
		return hooked
	})
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	hooked.onAcquire = func(*storage.Lock) {
		_, err := us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
		require.NoError(t, err)
	}

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "The plan for target `us` changed before this apply could start")
	assert.Contains(t, body, "- `users` (alter) now runs a different statement\n", "the cause names how us's plan changed")
	assert.Contains(t, body, "Confirmation required")
	assert.Contains(t, body, "MODIFY COLUMN `email`", "the comment shows us's plan as it is now")
	assert.Contains(t, body, "schemabot apply-confirm -e "+driftEnv)

	requireNoApplies(t, svc, dbName)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the stopped apply keeps its lock for the confirmation")
	repinned, err := svc.Storage().Plans().Get(t.Context(), lock.PendingPlanID)
	require.NoError(t, err)
	require.NotNil(t, repinned)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
	assert.True(t, check.HasChanges)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	created := awaitRolloutApply(t, svc, dbName, confirm)
	requireTaskDDLByDeployment(t, svc, created, map[string][]string{
		"us": {"MODIFY COLUMN `email`"},
	})
}

// Both targets (eu, the primary, and us) need the column, so the apply command
// posts both plans and runs them in the same step. Between that comment and the
// re-plan the apply runs from, eu gains a narrower `email` column out of band,
// so the primary's own plan is now a `MODIFY COLUMN`. The apply stops for
// apply-confirm, and the comment names how the primary's statements differ from
// the plan the apply started from, as it does for any other target.
func TestE2EApplyNamesThePrimarysDriftWhenItStopsARolloutWhileStarting(t *testing.T) {
	dbName := "webhook_rollout_primary_changed_auto"
	var hooked *afterLockStorage
	svc := setupE2ERolloutServiceWithStorage(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent, func(st storage.Storage) storage.Storage {
		hooked = &afterLockStorage{Storage: st}
		return hooked
	})
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	hooked.onAcquire = func(*storage.Lock) {
		_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
		require.NoError(t, err)
	}

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "The plan for target `eu` changed before this apply could start")
	assert.Contains(t, body, "- `users` (alter) now runs a different statement\n", "the cause names the primary's drifted statement")
	assert.Contains(t, body, "MODIFY COLUMN `email`", "the comment shows eu's plan as it is now")
	assert.Contains(t, body, "Confirmation required")
	requireNoApplies(t, svc, dbName)
}

// awaitApplyLockReleased waits for a command that posts before it releases its
// lock to finish releasing it.
func awaitApplyLockReleased(t *testing.T, svc *api.Service, dbName, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, msg)
}

// The primary target (eu) already has the column and us does not, so the
// apply command posts us's `ADD COLUMN` and runs it in the same step. Between
// that comment and the re-plan the apply runs from, eu's primary key is
// reshaped out of band, so bringing eu back to the PR's schema now means
// dropping a primary key, which the engine refuses. No confirmation could run
// that plan, so the apply rejects it outright and releases its lock instead of
// asking for an apply-confirm that would only be rejected in turn.
func TestE2EApplyRejectsAPrimaryPlanTheEngineBlocksWhileStarting(t *testing.T) {
	dbName := "webhook_rollout_primary_blocked_auto"
	var hooked *afterLockStorage
	svc := setupE2ERolloutServiceWithStorage(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent, func(st storage.Storage) storage.Storage {
		hooked = &afterLockStorage{Storage: st}
		return hooked
	})
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	hooked.onAcquire = func(*storage.Lock) {
		_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `name`)")
		require.NoError(t, err)
	}

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "⛔ Apply rejected")
	assert.Contains(t, body, "the engine refuses to execute")
	assert.Contains(t, body, "`users`")
	assert.NotContains(t, body, "Confirmation required", "a plan the engine blocks is never offered for confirmation")
	assert.NotContains(t, body, "schemabot apply-confirm")

	awaitApplyLockReleased(t, svc, dbName, "a rejected apply releases its lock")
	requireNoApplies(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.NotEqual(t, "success", check.Conclusion, "nothing ran, so the check keeps blocking merge")
	assert.True(t, check.HasChanges)
}

// The primary target (eu) already has the column and us does not, so the
// comment the apply command posts is the only place us's plan is shown before
// it runs. When GitHub rejects that comment, nothing runs: the apply releases
// its lock so a retry of the command starts over, and the check keeps blocking
// merge on us's pending change.
func TestE2EApplyRunsNothingWhenTheCommentShowingAnotherTargetsPlanCannotBePosted(t *testing.T) {
	dbName := "webhook_rollout_member_disclosure_fails"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutWebhookWithFiles(t, svc, dbName, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e " + driftEnv, isPR: true}, nil),
		map[string]string{"users.sql": usersWithEmailSchema},
		func(_ *http.ServeMux, result *planFlowResult) { result.FailCommentPost.Store(true) })
	// The comment showing us's plan was attempted; GitHub rejected it.
	awaitCommentContaining(t, apply, "ADD COLUMN `email`")

	awaitApplyLockReleased(t, svc, dbName, "an apply whose comment never landed releases its lock")
	requireNoApplies(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
	assert.True(t, check.HasChanges)
}

// The primary target (eu) already has the column and us does not, so the
// apply command posts us's plan and runs it in the same step. Before the apply
// is created it re-reads the rollout round stored with the plan it posted, to
// confirm us's work is what that comment showed. When that read fails, nothing
// is known about us's work, so the apply runs nothing, releases its lock so a
// retry of the command starts over, and the check keeps blocking merge.
func TestE2EApplyReleasesTheLockWhenItCannotVerifyAnotherTargetsWork(t *testing.T) {
	dbName := "webhook_rollout_member_unverified_auto"
	var hooked *afterLockStorage
	svc := setupE2ERolloutServiceWithStorage(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent, func(st storage.Storage) storage.Storage {
		hooked = &afterLockStorage{Storage: st}
		return hooked
	})
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	hooked.onAcquire = func(lock *storage.Lock) { hooked.failRoundOf = lock.PendingPlanID }

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "SchemaBot could not verify the plans this apply covers, so nothing was applied. Run apply again")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
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
	assert.Contains(t, body, "target `us`: applying its plan discards the unfinished copy of users",
		"the refusal names the target and the table whose copy would be lost")
	assert.NotContains(t, body, "schemabot apply-confirm", "a refused apply offers no confirmation")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	requireUsersCopyIntact(t, dbName+"_us")
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
}

// The primary target (eu) already has the column and us does not, so the
// apply command posts us's plan and runs it in the same step. Between that
// comment and the re-plan the apply runs from, us gains a narrower `email`
// column and an unfinished copy of `users` made for a different statement.
// us's plan changed, but a fresh confirmation could never run it, since it
// would discard the copy, so the apply refuses rather than asking: it runs
// nothing, releases its lock, leaves the copy in place, and offers no
// apply-confirm.
func TestE2EApplyRefusesRatherThanAskingWhenAnotherTargetsChangedPlanCannotRun(t *testing.T) {
	dbName := "webhook_rollout_member_changed_copy"
	var hooked *afterLockStorage
	svc := setupE2ERolloutServiceWithStorage(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent, func(st storage.Storage) storage.Storage {
		hooked = &afterLockStorage{Storage: st}
		return hooked
	})
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
	us := openDriftDB(t, driftDSN(t, dbName+"_us"))
	hooked.onAcquire = func(*storage.Lock) {
		_, err := us.ExecContext(t.Context(), "ALTER TABLE `users` ADD COLUMN `email` varchar(16) DEFAULT NULL")
		require.NoError(t, err)
		seedUsersCopy(t, dbName+"_us")
	}

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "nothing was applied")
	assert.Contains(t, body, "target `us`: applying its plan discards the unfinished copy of users")
	assert.NotContains(t, body, "Confirmation required")
	assert.NotContains(t, body, "schemabot apply-confirm", "a refused apply offers no confirmation")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	requireUsersCopyIntact(t, dbName+"_us")
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
}

// The primary target (eu) already has the column and us does not, and a
// pending confirmation pins a round that showed us's plan. Before the operator confirms,
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

	pinRolloutConfirmation(t, svc, dbName, nil)

	seedUsersCopy(t, dbName+"_us")

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body := awaitCommentContaining(t, confirm, "nothing was applied")
	assert.Contains(t, body, "target `us`: applying its plan discards the unfinished copy of users")

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
