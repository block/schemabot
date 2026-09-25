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
	"github.com/block/schemabot/pkg/storage"
)

// runRolloutCommand drives one PR comment command against the rollout fixture
// and returns the capture of what SchemaBot posts in response.
func runRolloutCommand(t *testing.T, svc *api.Service, dbName, command string) *planFlowResult {
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
	h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: command, isPR: true}, nil))
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
// apply command must neither report "no changes" nor run us's statements, which
// the comment never showed; it refuses, names the target still behind, and
// leaves the check pending.
func TestE2EConvergedPrimaryWithPendingTargetBlocksCheckAndApply(t *testing.T) {
	dbName := "webhook_rollout_pending"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersWithEmailSchema},
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)

	runRolloutCommand(t, svc, dbName, "schemabot plan -e "+driftEnv)

	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "a target that still needs the change keeps the check from passing")
	assert.True(t, check.HasChanges, "work on a non-primary target is work on the rollout")
	assert.Equal(t, "1 of 2 targets need this change", check.ChangeSummary)
	assert.Empty(t, check.BlockingReason, "independent targets differing is not drift")

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	body := awaitCommentContaining(t, apply, "1 of 2 targets need this change")
	assert.Contains(t, body, "The reviewed target already has this schema")
	assert.Contains(t, body, "us", "the refusal names the target still behind")
	assert.Contains(t, body, "nothing was applied")
	assert.NotContains(t, body, "No schema changes detected")

	requireNoApplies(t, svc, dbName)
	check = rolloutCheck(t, svc, dbName)
	assert.Equal(t, "action_required", check.Conclusion, "the refused apply leaves the check pending")
	assert.True(t, check.HasChanges)
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
	body := awaitCommentContaining(t, apply, "No schema changes detected")
	assert.Contains(t, body, "Planned separately for all 2 targets")

	requireNoApplies(t, svc, dbName)
	check := rolloutCheck(t, svc, dbName)
	assert.Equal(t, "success", check.Conclusion)
	assert.False(t, check.HasChanges)
}

// An operator confirms a plan in which the reviewed primary (eu) still needed
// the change, and by the time they confirm eu has converged while us has not.
// The confirm's re-plan of eu is empty, and apply-confirm must answer from the
// whole rollout rather than that empty plan: it refuses, runs nothing on us,
// releases the pending confirmation, and leaves the check pending.
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
// scoped to one environment, a plan across every environment, and a refused
// apply each store the record on a path of their own, so all three are covered.
func TestE2EUnstoredPendingRolloutFailsCheckClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dbName  string
		command string
	}{
		{name: "plan one environment", dbName: "webhook_rollout_unstored_plan", command: "schemabot plan -e " + driftEnv},
		{name: "plan every environment", dbName: "webhook_rollout_unstored_plan_all", command: "schemabot plan"},
		{name: "apply", dbName: "webhook_rollout_unstored_apply", command: "schemabot apply -e " + driftEnv},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := setupE2ERolloutServiceWithStorage(t, tc.dbName, []deploymentSpec{
				{name: "eu", liveSchema: usersWithEmailSchema},
				{name: "us", liveSchema: usersBaseSchema},
			}, api.PlanIndependent, func(st storage.Storage) storage.Storage {
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
			assert.Contains(t, run.Output.Summary, "1 of 2 targets need this change")
			requireNoApplies(t, svc, tc.dbName)
		})
	}
}
