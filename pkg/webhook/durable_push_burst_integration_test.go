//go:build integration

package webhook

import (
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// pushBurstQuietPeriod leaves room for three pushes a second apart, plus a
	// full second, before the first push becomes claimable, even after the
	// inbox stores its not-before time to the second.
	pushBurstQuietPeriod = 4 * time.Second
	pushBurstDriver      = "burst-test/webhook-driver-0"
	// pushBurstPullRequestRead is the request the driver makes to confirm a
	// claimed head against the live pull request.
	pushBurstPullRequestRead = "GET /repos/octocat/hello-world/pulls/1"
)

// pushBurstFixture is a durable webhook handler with a synchronize quiet
// period, in front of a fake GitHub that serves one pull request whose live
// head is fixed for the test.
type pushBurstFixture struct {
	h        *Handler
	store    storage.WebhookEventStore
	requests *githubRequestCounter
	result   *planFlowResult
}

func newPushBurstFixture(t *testing.T, dbName, liveHead string) *pushBurstFixture {
	t.Helper()
	svc := setupE2EService(t, dbName)

	// The inbox is shared across this package's tests, so leftover rows from
	// earlier tests would otherwise be claimed ahead of this test's pushes.
	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })
	_, err = schemabotDB.ExecContext(t.Context(), "DELETE FROM webhook_events")
	require.NoError(t, err)

	mux := http.NewServeMux()
	requests := &githubRequestCounter{counts: map[string]int{}}
	server := httptest.NewServer(requests.wrap(mux))
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	result.HeadSHAs = []string{liveHead}
	registerFreshBaseComparison(t, mux, liveHead)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger,
		WithDurableWebhookDispatch(), WithSynchronizeQuietPeriod(pushBurstQuietPeriod), WithInsecureSkipSignatureVerification())
	return &pushBurstFixture{h: h, store: svc.Storage().WebhookEvents(), requests: requests, result: result}
}

// push delivers one synchronize webhook that moves the pull request from
// before to head.
func (f *pushBurstFixture) push(t *testing.T, deliveryID, before, head string) {
	t.Helper()
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize", beforeSHA: before, headSHA: head}, nil)
	req.Header.Set(headerDeliveryID, deliveryID)
	rr := httptest.NewRecorder()
	f.h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

// waitBetweenPushes spaces two pushes a second apart. The inbox records when
// a delivery was received to the second, and a delivery covers an earlier one
// only when its recorded time is later, so pushes a full second apart are
// always recorded in order.
func waitBetweenPushes() {
	time.Sleep(time.Second)
}

// requestsSince returns the GitHub requests made since before was taken, by
// request, leaving out requests whose count did not change.
func (f *pushBurstFixture) requestsSince(before map[string]int) map[string]int {
	made := map[string]int{}
	for request, count := range f.requests.snapshot() {
		if n := count - before[request]; n > 0 {
			made[request] = n
		}
	}
	return made
}

// requireNothingClaimable asserts that every stored push is still waiting out
// the quiet period.
func (f *pushBurstFixture) requireNothingClaimable(t *testing.T) {
	t.Helper()
	claimed, err := f.store.FindNext(t.Context(), pushBurstDriver, time.Minute)
	require.NoError(t, err)
	require.Nil(t, claimed, "every push must wait out the quiet period before a driver can claim it")
}

// drive waits for wantDeliveryID to become the next claimable delivery, drives
// it, and returns its stored state.
func (f *pushBurstFixture) drive(t *testing.T, wantDeliveryID string) string {
	t.Helper()
	ctx := t.Context()
	var claimed *storage.WebhookEvent
	deadline := time.Now().Add(webhookIntegrationPollDeadline)
	for claimed == nil {
		require.True(t, time.Now().Before(deadline), "timed out waiting for %s to become claimable", wantDeliveryID)
		next, err := f.store.FindNext(ctx, pushBurstDriver, time.Minute)
		require.NoError(t, err)
		if next == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		require.Equal(t, wantDeliveryID, next.DeliveryID)
		claimed = next
	}
	f.h.driveClaimedDurableWebhook(ctx, 0, f.store, claimed)
	row, err := f.store.GetByDeliveryID(ctx, storage.WebhookProviderGitHub, wantDeliveryID)
	require.NoError(t, err)
	require.NotNil(t, row)
	return row.State
}

// requirePlanOf asserts that a plan comment and a Check Run were published,
// and that the Check Run is on head.
func (f *pushBurstFixture) requirePlanOf(t *testing.T, dbName, head string) {
	t.Helper()
	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, dbName)
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNow(t, fmt.Sprintf("timed out waiting for the plan comment of %s", head))
	}
	select {
	case cr := <-f.result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, head, cr.HeadSHA, "the Check Run must be published on the pull request's current head")
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNow(t, fmt.Sprintf("timed out waiting for the Check Run of %s", head))
	}
}

// A rebase force-pushes a PR twice within a few seconds. Both synchronize
// deliveries wait out the quiet period, so when the first is claimed the
// second is already in the inbox. The live pull request confirms the first
// push's head is no longer current, so the first delivery is superseded for
// the cost of that one pull request read, and only the newest head is
// planned: one plan comment and one Check Run on that head.
func TestE2EDurableDispatchPlansOnlyTheNewestHeadOfAPushBurst(t *testing.T) {
	const (
		dbName     = "webhook_push_burst"
		firstHead  = "burst-head-1"
		newestHead = "burst-head-2"
	)
	f := newPushBurstFixture(t, dbName, newestHead)
	f.push(t, "burst-push-1", "abc123", firstHead)
	waitBetweenPushes()
	f.push(t, "burst-push-2", firstHead, newestHead)
	f.requireNothingClaimable(t)

	before := f.requests.snapshot()
	assert.Equal(t, storage.WebhookEventSuperseded, f.drive(t, "burst-push-1"))
	assert.Equal(t, map[string]int{pushBurstPullRequestRead: 1}, f.requestsSince(before),
		"the superseded push must cost one pull request read and nothing else")

	assert.Equal(t, storage.WebhookEventCompleted, f.drive(t, "burst-push-2"))
	f.requirePlanOf(t, dbName, newestHead)
}

// A PR is force-pushed away from a head and then back to it, and the webhook
// of the earlier push arrives last. The delivery that carries the PR's current
// head is claimed while that late delivery sits behind it in the inbox. The
// live pull request shows the claimed head is still current, so the delivery
// is kept and planned; the late delivery's head is no longer the PR's head, so
// it ends superseded and publishes nothing. The current head ends with its
// plan and Check Run.
func TestE2EDurableDispatchKeepsTheCurrentHeadWhenAnOlderPushArrivesLast(t *testing.T) {
	const (
		dbName       = "webhook_push_cycle"
		currentHead  = "cycle-head-current"
		replacedHead = "cycle-head-replaced"
	)
	f := newPushBurstFixture(t, dbName, currentHead)
	f.push(t, "cycle-push-back-to-current", replacedHead, currentHead)
	waitBetweenPushes()
	f.push(t, "cycle-push-away-arriving-late", currentHead, replacedHead)
	f.requireNothingClaimable(t)

	assert.Equal(t, storage.WebhookEventCompleted, f.drive(t, "cycle-push-back-to-current"),
		"the delivery that carries the current head must be planned, not superseded by the late delivery")
	f.requirePlanOf(t, dbName, currentHead)

	before := f.requests.snapshot()
	assert.Equal(t, storage.WebhookEventSuperseded, f.drive(t, "cycle-push-away-arriving-late"),
		"the late delivery finds the pull request has left its head, so it ends superseded")
	assert.Equal(t, []string{pushBurstPullRequestRead}, slices.Sorted(maps.Keys(f.requestsSince(before))),
		"the late delivery may read the pull request, and must publish nothing on a head the pull request has left")
}

// A PR on head Z is pushed to A, then to B, then back to A, and the webhook of
// the push from A to B arrives last. All three deliveries wait in the inbox.
// Both deliveries that carry A are kept and planned, because the live pull
// request shows A is still its head; the first publishes A's plan and Check
// Run. The late delivery's head is no longer the PR's head, so it ends
// superseded and publishes nothing. The current head is never left without a
// plan and a Check Run.
func TestE2EDurableDispatchKeepsTheCurrentHeadThroughAReplayedPushCycle(t *testing.T) {
	const (
		dbName       = "webhook_push_replay"
		startHead    = "replay-head-z"
		currentHead  = "replay-head-a"
		replacedHead = "replay-head-b"
	)
	f := newPushBurstFixture(t, dbName, currentHead)
	f.push(t, "replay-push-z-to-a", startHead, currentHead)
	waitBetweenPushes()
	f.push(t, "replay-push-b-to-a", replacedHead, currentHead)
	waitBetweenPushes()
	f.push(t, "replay-push-a-to-b-arriving-late", currentHead, replacedHead)
	f.requireNothingClaimable(t)

	assert.Equal(t, storage.WebhookEventCompleted, f.drive(t, "replay-push-z-to-a"),
		"a delivery that carries the current head must be planned, even with later deliveries behind it")
	f.requirePlanOf(t, dbName, currentHead)

	assert.Equal(t, storage.WebhookEventCompleted, f.drive(t, "replay-push-b-to-a"),
		"the later delivery that carries the current head must be planned, not superseded by the late delivery")
	// The Check Run already on A is refreshed rather than recreated, which
	// this fake GitHub does not serve, so only the plan comment is awaited.
	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNow(t, fmt.Sprintf("timed out waiting for the second plan comment of %s", currentHead))
	}

	before := f.requests.snapshot()
	assert.Equal(t, storage.WebhookEventSuperseded, f.drive(t, "replay-push-a-to-b-arriving-late"),
		"the late delivery finds the pull request has left its head, so it ends superseded")
	assert.Equal(t, []string{pushBurstPullRequestRead}, slices.Sorted(maps.Keys(f.requestsSince(before))),
		"the late delivery may read the pull request, and must publish nothing on a head the pull request has left")
}
