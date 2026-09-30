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

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": usersWithEmailSchema}, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	return result
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

// Two targets planned against schemas of their own, where the reviewed primary
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
	assert.Contains(t, body, "The reviewed target already has this schema")
	assert.Contains(t, body, "ADD COLUMN `email`", "the comment shows the plan us would run")
	assert.Contains(t, body, "schemabot apply-confirm -e "+driftEnv)
	assert.Contains(t, body, "**target `eu`**\n\nNo schema changes detected\n\n", "eu is shown already at the desired schema")
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
// every target's plan, since the one-step gates read only the reviewed plan,
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
	assert.Contains(t, body, "**targets `eu`, `us`**", "the comment names every target the plan runs on")
	assert.Contains(t, body, "ADD COLUMN `email`")
	assert.Contains(t, body, "schemabot apply-confirm -e "+driftEnv)
	requireNoApplies(t, svc, dbName)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)

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

// Two targets planned against schemas of their own, both needing the column.
// eu also drops nickname, which the comment discloses as unsafe. us has drifted
// and drops legacy_id besides, a change the reviewed plan does not carry, so the
// comment's unsafe disclosure never names it. The operator's --allow-unsafe
// covers only what was disclosed, so the apply refuses before it pauses for a
// confirmation, runs nothing, and leaves the check pending.
func TestE2EIndependentRolloutRefusesAnUnsafeTargetChangeTheCommentDidNotDisclose(t *testing.T) {
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

	runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)
	assert.Equal(t, "action_required", rolloutCheck(t, svc, dbName).Conclusion)

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv+" --allow-unsafe")
	body := awaitCapture(t, apply.comments, "the apply command's answer", func(body string) bool {
		return strings.Contains(body, "Confirmation required") || strings.Contains(body, "nothing was applied")
	})
	require.NotContains(t, body, "Confirmation required", "no confirmation is pinned for work apply creation refuses")
	assert.Contains(t, body, "target us/"+dbName+"-us-target: its plan carries an unsafe change for table &#34;users&#34;")
	requireNoApplies(t, svc, dbName)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "a refusal before the pause holds no lock")
	assert.Equal(t, "action_required", rolloutCheck(t, svc, dbName).Conclusion)
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

	body := awaitCommentContaining(t, result, "Targets diverge — what applies where:")
	assert.Contains(t, body, "**target `eu`**\n\nNo schema changes detected\n\n")
	assert.Contains(t, body, "**target `us`**\n\n```sql\n")
	assert.Equal(t, "action_required", rolloutCheck(t, svc, dbName).Conclusion)
}

// Two targets expected to mirror each other, where the reviewed primary (eu)
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
	require.NotNil(t, reviewed, "the plan command stores the reviewed primary plan")

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

// The reviewed primary (eu) already has the column and us does not, so the
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
	assert.Contains(t, body, "The reviewed target already has this schema")

	eu := openDriftDB(t, driftDSN(t, dbName+"_eu"))
	_, err := eu.ExecContext(t.Context(), "ALTER TABLE `users` DROP COLUMN `email`")
	require.NoError(t, err)

	confirm := runRolloutCommand(t, svc, dbName, "schemabot apply-confirm -e "+driftEnv)
	body = awaitCommentContaining(t, confirm, "now has changes of its own")
	assert.Contains(t, body, "showed the reviewed target already at the desired schema")
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

// The reviewed primary (eu) already has the column, and us needs it too, but
// bringing us to the PR's schema also drops a column only us carries. That drop
// is unsafe, and the comment the operator would confirm renders us's statements
// without the unsafe disclosure a reviewed plan carries, so the apply refuses
// before pausing: it takes no lock, runs nothing, and names the target and
// table whose change it cannot run, while the check keeps blocking merge.
func TestE2EConvergedPrimaryRefusesUnsafeWorkOnAnotherTarget(t *testing.T) {
	dbName := "webhook_rollout_member_unsafe"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersWithLegacySchema},
	}, api.PlanIndependent)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "nothing was applied")
	assert.Contains(t, body, "target us/"+dbName+"-us-target: its plan carries an unsafe change for table &#34;users&#34;",
		"the refusal names the target and the table whose change it cannot run")
	assert.Contains(t, body, "cannot run that or disclose it for confirmation")
	assert.NotContains(t, body, "schemabot apply-confirm", "a refused apply offers no confirmation")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "us still needs the change, so the check keeps blocking merge")
	assert.True(t, check.HasChanges)
}

// The reviewed primary (eu) already has the column and us does not, so the
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
	assert.Contains(t, body, "1 of 2 targets need this change: us")
	assert.Contains(t, body, "were not on the comment this apply acts on")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
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

// The reviewed primary (eu) already has the column and us does not, and the PR
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

// The reviewed primary (eu) already has the column and us does not, and the PR
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

// The reviewed primary (eu) already has the column and us does not, but us
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

// The reviewed primary (eu) already has the column and us does not, so the
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

// The reviewed primary (eu) already has the column, and bringing us to the PR's
// schema also drops a column only us carries. An apply from this PR refuses that
// work whatever its flags, so the plan comment shows us's plan and says why it
// cannot be applied from here instead of offering an apply command that would
// be refused. The check keeps blocking merge on us's work.
func TestE2EPlanOffersNoApplyForAnotherTargetsRefusedWork(t *testing.T) {
	dbName := "webhook_rollout_plan_member_unsafe"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersWithLegacySchema},
	}, api.PlanIndependent)

	plan := runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)
	body := awaitCommentContaining(t, plan, "This PR cannot apply the other targets' plans")
	assert.Contains(t, body, "DROP COLUMN `legacy`", "the comment still shows the plan us would run")
	assert.Contains(t, body, "its plan carries an unsafe change for table \"users\"")
	assert.NotContains(t, body, "schemabot apply -e", "an apply that is refused whatever its flags is never offered")

	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion)
}
