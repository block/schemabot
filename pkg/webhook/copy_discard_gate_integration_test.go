//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/utils"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// indexAddSchema declares an index the target's `events` table does not have,
// so the plan is a single ALTER that the schema change engine copies rows for.
const indexAddSchema = "CREATE TABLE `events` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `actor_id` bigint unsigned NOT NULL,\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  INDEX `idx_actor_id` (`actor_id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// seedAbandonedCopy puts an unfinished copy of `events` on the target: the
// shadow table the engine builds rows into, plus a checkpoint recording a
// different statement than the one this plan will hand it. That is what an
// apply stopped partway through and then re-planned against edited schema
// leaves behind, and it is the state a fresh apply would destroy.
func seedAbandonedCopy(t *testing.T, dbName string) {
	t.Helper()
	seedPreChangeEvents(t, dbName)
	seedCopyArtifacts(t, dbName)
}

// seedPreChangeEvents creates the `events` table in its pre-change shape, so
// the schema the PR declares plans as a single ALTER against it.
func seedPreChangeEvents(t *testing.T, dbName string) {
	t.Helper()
	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()), "connect to target")

	_, err = db.ExecContext(t.Context(), "CREATE TABLE `events` (\n"+
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"+
		"  `actor_id` bigint unsigned NOT NULL,\n"+
		"  PRIMARY KEY (`id`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "seed pre-change events table")
}

// seedCopyArtifacts puts the unfinished copy itself on the target: the shadow
// table and a checkpoint recording a different statement than the one the
// PR's plan will hand the engine.
func seedCopyArtifacts(t *testing.T, dbName string) {
	t.Helper()
	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()), "connect to target")

	_, err = db.ExecContext(t.Context(), fmt.Sprintf(
		"CREATE TABLE `%s` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", utils.NewTableName("events")))
	require.NoError(t, err, "seed shadow table")

	cp := checkpoint.NewTable(db, utils.CheckpointTableName("events"), checkpoint.Transient)
	require.NoError(t, cp.Create(t.Context()), "create checkpoint table")
	require.NoError(t, cp.Write(t.Context(), checkpoint.Record{
		Statement:       "ALTER TABLE `events` ADD INDEX `idx_actor_created` (`actor_id`, `created_at`)",
		CopierWatermark: `{"Key":["id"],"LowerBound":3952903346}`,
		Position:        "mysql-bin.024891:19443021",
	}), "write checkpoint row")
}

// requireCopyIntact asserts the unfinished copy seeded on the target is still
// there in full: the shadow table holding the copied rows, and the checkpoint
// that says where the copy stopped. A gate that discloses a discard and then
// performs it anyway is worse than no gate, so a stopped apply has to leave
// both behind for the copy to still be resumable.
func requireCopyIntact(t *testing.T, dbName string) {
	t.Helper()
	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()), "connect to target")

	for _, table := range []string{utils.NewTableName("events"), utils.CheckpointTableName("events")} {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			dbName, table).Scan(&count))
		assert.Equal(t, 1, count, "the stopped apply left %s on the target", table)
	}

	var checkpoints int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s`", utils.CheckpointTableName("events"))).Scan(&checkpoints))
	assert.Positive(t, checkpoints, "the copy's checkpoint row still records where it stopped")
}

// discardGateFixture stands up a database whose target already holds an
// unfinished copy that the PR's schema change cannot continue, and returns the
// handler and the fake GitHub the comments land on.
type discardGateFixture struct {
	handler *Handler
	result  *planFlowResult
	svc     *api.Service
	client  *gh.Client
	dbName  string
}

// disclosingPlanID stores a plan made against the target as it is now — copy
// and all — and returns its identifier. It stands for the comment an operator
// was shown: pinning a confirmation to it is what records that they saw the
// discard before agreeing to it.
func (f discardGateFixture) disclosingPlanID(t *testing.T) string {
	t.Helper()

	installClient := ghclient.NewInstallationClientWithSlug(f.client, testLogger(), "schemabot")
	schemaResult, err := f.handler.createManagedSchemaRequestFromPR(t.Context(), installClient,
		"octocat/hello-world", 1, "staging", "", action.Apply)
	require.NoError(t, err)

	prNumber := int32(1)
	planResp, err := f.handler.executePlanWithTransientRetry(t.Context(), api.PlanRequest{
		Database:          schemaResult.Database,
		Environment:       "staging",
		Type:              schemaResult.Type,
		SchemaFiles:       schemaResult.SchemaFiles,
		Repository:        "octocat/hello-world",
		PullRequest:       &prNumber,
		HeadSHA:           &schemaResult.HeadSHA,
		SchemaPath:        schemaResult.SchemaPath,
		IgnoredNamespaces: schemaResult.IgnoredNamespaces,
		SourceTrusted:     true,
	}, "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotEmpty(t, planResp.DiscardedCopies(), "the plan behind the confirmation must disclose the copy")

	stored, err := f.svc.Storage().Plans().Get(t.Context(), planResp.PlanID)
	require.NoError(t, err)
	require.NotNil(t, stored, "the disclosing plan must be stored for apply-confirm to load")
	return planResp.PlanID
}

// requireLockEventually waits for the durable state a stop leaves behind. The
// disclosure is posted before it is recorded, so the comment arriving on the
// fake GitHub does not mean the confirmation has been re-pinned yet.
func (f discardGateFixture) requireLockEventually(t *testing.T, check func(*storage.Lock) bool, msg string) *storage.Lock {
	t.Helper()

	var lock *storage.Lock
	require.Eventually(t, func() bool {
		var err error
		lock, err = f.svc.Storage().Locks().Get(t.Context(), f.dbName, "mysql")
		if err != nil || lock == nil {
			return false
		}
		return check(lock)
	}, webhookIntegrationPollDeadline, 50*time.Millisecond, msg)
	return lock
}

func setupDiscardGate(t *testing.T, dbName string) discardGateFixture {
	t.Helper()

	svc := setupE2EService(t, dbName)
	seedAbandonedCopy(t, dbName)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"events.sql": indexAddSchema}, schemabotConfig, dbName)

	return discardGateFixture{handler: newE2EHandler(t, svc, client), result: result, svc: svc, client: client, dbName: dbName}
}

// auditsColumnAddSchema declares a column the target's `audits` table does not
// have, so a two-file schema plans as one ALTER per table.
const auditsColumnAddSchema = "CREATE TABLE `audits` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `note` varchar(64) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// seedTwoTableTargetWithOneCopy stands up a target whose plan is two ALTERs but
// whose unfinished copy covers only `events`. The verdict on that copy depends
// on the statement grouping the apply will use: a joined batch reads the shadow
// table of both its tables and finds one missing (the copy is incomplete for
// the batch), while a per-table batch meets the copy alone and fails only the
// statement comparison. The two verdicts render differently, which is what lets
// a test see which shape the prediction actually ran.
func seedTwoTableTargetWithOneCopy(t *testing.T, dbName string) {
	t.Helper()
	seedPreChangeEvents(t, dbName)

	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()), "connect to target")

	_, err = db.ExecContext(t.Context(), "CREATE TABLE `audits` (\n"+
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"+
		"  PRIMARY KEY (`id`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "seed pre-change audits table")

	_, err = db.ExecContext(t.Context(), fmt.Sprintf(
		"CREATE TABLE `%s` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", utils.NewTableName("events")))
	require.NoError(t, err, "seed shadow table for events")
}

func setupGroupedDiscardGate(t *testing.T, dbName string) discardGateFixture {
	t.Helper()

	svc := setupE2EService(t, dbName)
	seedTwoTableTargetWithOneCopy(t, dbName)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux,
		map[string]string{"events.sql": indexAddSchema, "audits.sql": auditsColumnAddSchema},
		schemabotConfig, dbName)

	return discardGateFixture{handler: newE2EHandler(t, svc, client), result: result, svc: svc, client: client, dbName: dbName}
}

// A --defer-cutover apply hands the engine every ALTER as one batch, and the
// engine reads the shadow table of every table in that batch before it can
// resume. A copy covering only one of the plan's tables is therefore
// destroyed, and the paused comment must give the joined batch's verdict —
// the copy is incomplete for the batch — not the per-table verdict of a shape
// this apply will not run. The grouping decision crosses the webhook, the API
// server, and the plan client to reach the engine, and this rendered verdict
// is only reachable when every one of those hops delivers it.
func TestE2EDeferCutoverApplyDisclosesTheJoinedBatchVerdict(t *testing.T) {
	f := setupGroupedDiscardGate(t, "webhook_copy_grouped_gate")

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging --defer-cutover",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "⚠️ **Applying destroys work in progress**",
			"the locked comment discloses the copy the apply would destroy")
		assert.Contains(t, body, "`events`")
		assert.Contains(t, body, "it covers only some of the tables this schema change alters",
			"the verdict is the joined batch's: the copy is incomplete for the batch")
		assert.NotContains(t, body, "the schema change differs from the one that started it",
			"a per-table verdict here means the prediction ran the shape this apply will not use")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the downgraded plan comment")
	}
}

// The consent gate re-plans when the operator confirms, and a --defer-cutover
// confirm is about to run the joined batch, so the re-plan must predict that
// shape. The operator here is confirming a comment that disclosed no copy, so
// the gate stops the apply — and its disclosure must carry the joined batch's
// verdict on the copy that appeared since.
func TestE2EDeferCutoverConfirmRechecksAgainstTheJoinedBatch(t *testing.T) {
	const dbName = "webhook_copy_grouped_recheck"
	f := setupGroupedDiscardGate(t, dbName)

	require.NoError(t, f.svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		Owner:         "octocat/hello-world#1",
		PendingPlanID: "plan-disclosing-no-copy",
	}))

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging --defer-cutover",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "⚠️ **Applying destroys work in progress**",
			"the confirm stops and discloses the copy instead of dispatching over it")
		assert.Contains(t, body, "it covers only some of the tables this schema change alters",
			"the verdict is the joined batch's: the copy is incomplete for the batch")
		assert.NotContains(t, body, "the schema change differs from the one that started it",
			"a per-table verdict here means the re-plan ran the shape this confirm will not use")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the stopped apply-confirm comment")
	}
}

// An apply that would throw away an unfinished copy on the target never runs
// in one step on its own. The locked comment discloses the copy and pauses the
// automatic apply, so the operator decides whether hours of copied rows are
// expendable before anything is destroyed.
func TestE2EDiscardingCopyDowngradesToConfirm(t *testing.T) {
	f := setupDiscardGate(t, "webhook_copy_discard_gate")

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "⚠️ **Applying destroys work in progress**",
			"the locked comment discloses the copy the apply would destroy")
		assert.Contains(t, body, "`events`")
		assert.Contains(t, body, "the schema change differs from the one that started it")
		assert.Contains(t, body, "⚠️ **Automatic apply paused**: Applying destroys work in progress on the target\n")
		assert.Contains(t, body, "schemabot apply-confirm -e staging",
			"the paused comment carries the confirm command copy-pasteably rather than describing it")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the downgraded plan comment")
	}

	// The pause is only real if nothing was spent making it: the copy is still
	// on the target, and the lock still holds the PR's claim on the database
	// pinned to the plan the operator was shown.
	requireCopyIntact(t, "webhook_copy_discard_gate")

	lock, err := f.svc.Storage().Locks().Get(t.Context(), "webhook_copy_discard_gate", "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the paused apply keeps the database locked for this PR")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner, "the lock still names the PR that claimed the database")
	assert.NotEmpty(t, lock.PendingPlanID, "the lock pins the plan apply-confirm loads")

	plan, err := f.svc.Storage().Plans().Get(t.Context(), lock.PendingPlanID)
	require.NoError(t, err)
	require.NotNil(t, plan, "the pinned plan is the disclosing one that was stored")
	assert.Equal(t, "webhook_copy_discard_gate", plan.Database)
}

// The lock is acquired before the comment that discloses the copy is posted, and
// it records what that comment tells the operator. A disclosure GitHub rejects
// therefore leaves no confirmation behind at all: consent for a comment nobody
// saw would let the next confirm destroy the copy unasked, and a confirmation
// with no comment to act on is one the operator could only clear by unlocking
// the database by hand.
func TestE2EDiscardingCopyLeavesNoConfirmationWhenTheDisclosureCannotBePosted(t *testing.T) {
	const dbName = "webhook_copy_discard_downgrade_post_fails"
	f := setupDiscardGate(t, dbName)
	f.result.FailCommentPost.Store(true)

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	// The disclosure was attempted — GitHub rejected it.
	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "⚠️ **Applying destroys work in progress**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the disclosure post attempt")
	}

	require.Eventually(t, func() bool {
		lock, err := f.svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, webhookIntegrationPollDeadline, 100*time.Millisecond,
		"a pending confirmation whose comment never landed must not be left holding the database")

	requireCopyIntact(t, dbName)
}

// The copy on the target is read fresh on every plan, so a copy can appear
// between the plan the automatic apply stored and the re-plan its dispatch
// runs: another apply starts one, or an adopted copy's checkpoint ages out.
// The dispatch-time re-plan gate downgrades to manual confirmation instead of
// destroying, unattended, a copy the reviewed comment never disclosed. The
// window has no user action inside it, so the test drives the dispatch core
// directly with a stored plan made before the copy existed.
func TestE2EReplanDiscardingCopyDowngradesToConfirm(t *testing.T) {
	dbName := "webhook_copy_replan_gate"
	svc := setupE2EService(t, dbName)
	seedPreChangeEvents(t, dbName)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"events.sql": indexAddSchema}, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")

	const repo = "octocat/hello-world"
	const pr = 1
	schemaResult, err := h.createManagedSchemaRequestFromPR(t.Context(), installClient, repo, pr, "staging", "", action.Apply)
	require.NoError(t, err)

	// The plan the operator reviewed: made while the target holds no copy, so
	// it discloses none.
	prNumber := int32(pr)
	planResp, err := h.executePlanWithTransientRetry(t.Context(), api.PlanRequest{
		Database:          schemaResult.Database,
		Environment:       "staging",
		Type:              schemaResult.Type,
		SchemaFiles:       schemaResult.SchemaFiles,
		Repository:        repo,
		PullRequest:       &prNumber,
		HeadSHA:           &schemaResult.HeadSHA,
		SchemaPath:        schemaResult.SchemaPath,
		IgnoredNamespaces: schemaResult.IgnoredNamespaces,
		SourceTrusted:     true,
	}, repo, pr)
	require.NoError(t, err)
	require.True(t, planResp.HasChanges(), "the schema declares an index the target does not have")
	require.Empty(t, planResp.DiscardedCopies(), "the reviewed plan must disclose no copy for the window to exist")

	storedPlan, err := svc.Storage().Plans().Get(t.Context(), planResp.PlanID)
	require.NoError(t, err)
	require.NotNil(t, storedPlan)

	// The lock the automatic apply holds across the dispatch, pinned to the
	// plan the operator was shown.
	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Owner:         fmt.Sprintf("%s#%d", repo, pr),
		Repository:    repo,
		PullRequest:   pr,
		PendingPlanID: planResp.PlanID,
	}))

	// The copy appears inside the window: after the reviewed plan, before the
	// dispatch re-plans.
	seedCopyArtifacts(t, dbName)

	h.executeApply(t.Context(), installClient, repo, pr, schemaResult, "staging", 1, "testuser",
		CommandResult{Action: action.Apply, Environment: "staging", Found: true, IsMention: true},
		storedPlan, planResp.PlanID, false)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "⚠️ **Applying destroys work in progress**",
			"the downgraded comment discloses the copy the reviewed comment never showed")
		assert.Contains(t, body, "`events`")
		assert.Contains(t, body, "⚠️ **Automatic apply paused**: Applying destroys work in progress on the target\n")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the downgraded plan comment")
	}

	// The downgrade spends nothing: the copy is untouched and no apply was
	// dispatched against it.
	requireCopyIntact(t, dbName)
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), repo, pr)
	require.NoError(t, err)
	for _, a := range applies {
		assert.NotEqual(t, dbName, a.Database, "the paused dispatch must not have started an apply")
	}

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the paused apply keeps the database locked for this PR")
	assert.Equal(t, fmt.Sprintf("%s#%d", repo, pr), lock.Owner)
}

// The copy on the target is read fresh on every plan, so one can appear between
// the comment an operator confirms and the moment the apply dispatches: another
// apply starts a copy, or an adopted copy's checkpoint ages out. A confirmation
// carries what its comment disclosed, so an apply the operator agreed to while
// nothing was at stake stops rather than destroying the copy — and it moves the
// pending confirmation onto the comment that discloses it, so the operator has a
// stop they can actually pass.
func TestE2EApplyConfirmStopsWhenCopyAppearedAfterDisclosure(t *testing.T) {
	const dbName = "webhook_copy_discard_recheck"
	f := setupDiscardGate(t, dbName)

	// The operator is confirming a comment that told them nothing about a copy.
	require.NoError(t, f.svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		Owner:         "octocat/hello-world#1",
		PendingPlanID: "plan-disclosing-no-copy",
	}))

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "⚠️ **Applying destroys work in progress**",
			"the confirm stops and discloses the copy instead of dispatching over it")
		assert.Contains(t, body, "`events`")
		assert.Contains(t, body, "⚠️ **Apply stopped**: Applying destroys work in progress on the target\n",
			"the operator issued this apply themselves, so nothing automatic was paused")
		assert.NotContains(t, body, "Automatic apply paused")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
		assert.NotContains(t, body, "Schema Change Status", "the apply must not have started")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the stopped apply-confirm comment")
	}

	// The pending confirmation moved onto the plan behind the comment just
	// posted, and records that this one discloses the discard — without that the
	// next confirm would load the comment that disclosed nothing and stop again.
	lock := f.requireLockEventually(t, func(l *storage.Lock) bool {
		return l.DisclosedCopyDiscard && l.PendingPlanID != "plan-disclosing-no-copy"
	}, "the stop must re-pin the confirmation onto the plan it just disclosed")
	plan, err := f.svc.Storage().Plans().Get(t.Context(), lock.PendingPlanID)
	require.NoError(t, err)
	require.NotNil(t, plan, "the re-pinned confirmation must name a stored plan")
	assert.Equal(t, dbName, plan.Database)
	assert.Equal(t, "staging", plan.Environment,
		"the re-pinned plan names the environment the operator was shown")

	applies, err := f.svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, a := range applies {
		assert.NotEqual(t, dbName, a.Database, "no apply for %s should have been started", dbName)
	}
}

// A discard the operator was already shown is one they agreed to. The
// confirmation records that disclosure, so the apply it authorizes runs and
// throws the copy away rather than asking a second time — a stop that reappeared
// on every confirm would be unpassable.
func TestE2EApplyConfirmProceedsWhenDiscardWasDisclosed(t *testing.T) {
	const dbName = "webhook_copy_discard_consented"
	f := setupDiscardGate(t, dbName)

	require.NoError(t, f.svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:         dbName,
		DatabaseType:         "mysql",
		Repository:           "octocat/hello-world",
		PullRequest:          1,
		Owner:                "octocat/hello-world#1",
		PendingPlanID:        f.disclosingPlanID(t),
		DisclosedCopyDiscard: true,
	}))

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	require.Eventually(t, func() bool {
		select {
		case body := <-f.result.comments:
			assert.NotContains(t, body, msgCopyDiscardDowngrade,
				"a discard the operator already agreed to must not stop the apply again")
			return strings.Contains(body, "Schema Change Applied")
		default:
			return false
		}
	}, webhookIntegrationPollDeadline, 200*time.Millisecond,
		"expected the confirmed apply to discard the copy and run to completion")

	// The copy the operator agreed to lose is gone, and the index they asked for
	// is on the table.
	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	var shadowTables int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		dbName, utils.NewTableName("events")).Scan(&shadowTables))
	assert.Zero(t, shadowTables, "the discarded copy's shadow table must not survive the apply")

	var indexes int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = ? AND table_name = 'events' AND index_name = 'idx_actor_id'",
		dbName).Scan(&indexes))
	assert.Positive(t, indexes, "the confirmed schema change must be on the target")
}

// A stop re-pins the pending confirmation onto the comment that discloses the
// copy, but only while the lock still carries the intent the stopping apply
// observed. An operator who issued a rollback while the gate ran owns the
// pending confirmation now, and overwriting it would answer "no pending
// rollback" to the rollback-confirm they are about to send.
func TestRepinPendingConfirmationPreservesANewerIntent(t *testing.T) {
	const dbName = "webhook_copy_discard_repin"
	f := setupDiscardGate(t, dbName)

	const repo = "octocat/hello-world"
	const pr = 1
	acquire := func(t *testing.T, pendingPlanID string) {
		t.Helper()
		require.NoError(t, f.svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
			DatabaseName:  dbName,
			DatabaseType:  "mysql",
			Repository:    repo,
			PullRequest:   pr,
			Owner:         fmt.Sprintf("%s#%d", repo, pr),
			PendingPlanID: pendingPlanID,
		}))
	}

	// The rollback the operator issued while the gate ran holds the pending
	// confirmation, so the stop leaves it alone.
	acquire(t, "rollback:the-operator-just-asked-for-this")
	require.NoError(t, f.handler.repinPendingConfirmation(t.Context(), repo, pr, dbName, "mysql",
		"plan-the-apply-observed", "plan-disclosing-the-copy", true))

	lock, err := f.svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "rollback:the-operator-just-asked-for-this", lock.PendingPlanID,
		"the newer intent must survive the stop")
	assert.False(t, lock.DisclosedCopyDiscard,
		"declining to re-pin leaves the copy gate armed for the next confirm")

	// With the observed intent still in place, the re-pin moves the confirmation
	// onto the disclosing plan and records what that comment showed.
	acquire(t, "plan-the-apply-observed")
	require.NoError(t, f.handler.repinPendingConfirmation(t.Context(), repo, pr, dbName, "mysql",
		"plan-the-apply-observed", "plan-disclosing-the-copy", true))

	lock, err = f.svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "plan-disclosing-the-copy", lock.PendingPlanID)
	assert.True(t, lock.DisclosedCopyDiscard)
}

// The consent record's whole claim is that the operator was shown the copy, so
// it must not survive a disclosure that never reached them. When the comment
// fails to post, nothing is recorded and the confirmation stays pinned where it
// was, leaving the gate armed for the next attempt.
func TestE2EApplyConfirmRecordsNoConsentWhenTheDisclosureCannotBePosted(t *testing.T) {
	const dbName = "webhook_copy_discard_post_fails"
	f := setupDiscardGate(t, dbName)
	f.result.FailCommentPost.Store(true)

	require.NoError(t, f.svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		Owner:         "octocat/hello-world#1",
		PendingPlanID: "plan-disclosing-no-copy",
	}))

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	// The disclosure was attempted — GitHub rejected it.
	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "⚠️ **Applying destroys work in progress**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the disclosure post attempt")
	}

	// Consent the operator never saw must not be on the lock, and the copy the
	// gate protects is still on the target.
	require.Never(t, func() bool {
		lock, err := f.svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		if err != nil || lock == nil {
			return false
		}
		return lock.DisclosedCopyDiscard || lock.PendingPlanID != "plan-disclosing-no-copy"
	}, 2*time.Second, 100*time.Millisecond,
		"a disclosure that never reached the operator must record no consent")

	requireCopyIntact(t, dbName)
}
