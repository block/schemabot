//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// directPolicyMetadata enables direct execution with a row bound the seeded
// tables sit well inside.
var directPolicyMetadata = map[string]string{
	"direct_execution":                "true",
	"direct_execution_max_table_rows": "1000000",
}

// appPrimaryKeyColumns returns the app table's primary-key column names in
// ordinal order, so tests can assert whether the reshape landed on the target.
func appPrimaryKeyColumns(t *testing.T, dbName, tableName string) []string {
	t.Helper()
	db, err := sql.Open("block-mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(t.Context(), `
		SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`, dbName, tableName)
	require.NoError(t, err, "query PK columns")
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var col string
		require.NoError(t, rows.Scan(&col))
		cols = append(cols, col)
	}
	require.NoError(t, rows.Err())
	return cols
}

// With the direct execution policy enabled, the policy is the approval: an
// apply whose plan routes a change to native MySQL DDL runs in one step, with
// no apply-confirm, and the reshaped primary key lands on the target. The
// primary-key reshape is also an unsafe change, so the apply still carries
// --allow-unsafe; the unsafe gate is independent of direct execution.
func TestE2EDirectPlanAppliesInOneStep(t *testing.T) {
	dbName := "webhook_direct_apply"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{engineMetadata: directPolicyMetadata})
	seedPKSwapTargetTable(t, dbName)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	applyReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging --allow-unsafe",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, applyReq)
	require.Equal(t, http.StatusOK, rr.Code)

	// The apply posts its locked plan comment, which carries the direct
	// disclosure, and then runs. The direct statement is synchronous and the
	// table is empty, so the apply may terminalize before the first progress
	// poll: after the locked comment, accept either a progress comment
	// followed by a summary, or the summary directly.
	sawDisclosure := false
	sawApplied := false
	deadline := time.After(webhookIntegrationPollDeadline)
	for !sawApplied {
		select {
		case body := <-result.comments:
			assert.NotContains(t, body, "**Confirmation required**", "a direct change needs no apply-confirm")
			if strings.Contains(body, "⚙️ **Direct execution**") {
				sawDisclosure = true
				assert.Contains(t, body, "will run as native MySQL DDL, not through Spirit")
			}
			if strings.Contains(body, "Schema Change Applied") {
				sawApplied = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for the direct apply to complete")
		}
	}
	assert.True(t, sawDisclosure, "the apply's locked comment discloses the direct change")

	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	var ourApply *storage.Apply
	for _, a := range applies {
		if a.Database == dbName {
			ourApply = a
			break
		}
	}
	require.NotNil(t, ourApply, "expected an apply record for database %s", dbName)
	assert.Equal(t, "staging", ourApply.Environment)

	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
		if err != nil || check == nil {
			return false
		}
		return check.Conclusion == "success"
	}, webhookIntegrationPollDeadline, 200*time.Millisecond,
		"expected the stored check to transition to success after the direct apply completes")

	assert.Equal(t, []string{"id", "tenant_id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the direct apply reshaped the primary key on the target")
}

// --defer-cutover on a plan whose every change runs as native MySQL DDL is
// rejected before any lock is taken: direct statements have no cutover to
// defer, so the flag would be silently meaningless — the PR gets a command
// error telling the operator to re-run without it.
func TestE2EDeferCutoverRejectedOnAllDirectPlan(t *testing.T) {
	dbName := "webhook_direct_defer"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{engineMetadata: directPolicyMetadata})
	seedPKSwapTargetTable(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging --defer-cutover",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "`--defer-cutover` has no effect on this plan")
		assert.Contains(t, body, "Re-run without the flag")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the defer-cutover rejection comment")
	}

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "a rejected apply must not leave the database locked")
}

// newlyDirectFixture is a database whose policy routes the primary-key swap to
// direct execution, planned once, with helpers to stand in for a plan the
// operator was shown before the verdict moved.
type newlyDirectFixture struct {
	dbName       string
	svc          *api.Service
	h            *Handler
	result       *planFlowResult
	client       *ghclient.InstallationClient
	schemaResult *ghclient.SchemaRequestResult
	directPlan   *apitypes.PlanResponse
}

func setupNewlyDirectFixture(t *testing.T, dbName string) *newlyDirectFixture {
	t.Helper()
	return setupNewlyDirectFixtureWithStorage(t, dbName, nil)
}

// setupNewlyDirectFixtureWithStorage is setupNewlyDirectFixture with the
// service's storage wrapped by wrapStorage, so a test can interleave another
// command's storage writes with the stop.
func setupNewlyDirectFixtureWithStorage(t *testing.T, dbName string, wrapStorage func(storage.Storage) storage.Storage) *newlyDirectFixture {
	t.Helper()
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{engineMetadata: directPolicyMetadata, wrapStorage: wrapStorage})
	seedPKSwapTargetTable(t, dbName)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")

	schemaResult, err := h.createManagedSchemaRequestFromPR(t.Context(), installClient, "octocat/hello-world", 1, "staging", "", action.Apply)
	require.NoError(t, err)

	prNumber := int32(1)
	_, planResp, err := h.executePlanProtoWithTransientRetry(t.Context(), api.PlanRequest{
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
	require.Len(t, planResp.DirectChanges(), 1, "the policy routes the primary-key swap to direct execution")

	return &newlyDirectFixture{dbName: dbName, svc: svc, h: h, result: result, client: installClient, schemaResult: schemaResult, directPlan: planResp}
}

// engineRoutedPlan returns the direct plan as the operator would have been
// shown it before the verdict moved: the same statement, run through the
// engine, so its comment carried no direct execution disclosure.
func (f *newlyDirectFixture) engineRoutedPlan(t *testing.T) *storage.Plan {
	t.Helper()
	plan, err := f.svc.Storage().Plans().Get(t.Context(), f.directPlan.PlanID)
	require.NoError(t, err)
	require.NotNil(t, plan)
	for _, ns := range plan.Namespaces {
		for i := range ns.Tables {
			ns.Tables[i].ExecutionMode = ""
			ns.Tables[i].ModeReason = ""
		}
	}
	return plan
}

// storeEngineRoutedPlan persists engineRoutedPlan as a plan of its own, so a
// pending confirmation can be pinned to it.
func (f *newlyDirectFixture) storeEngineRoutedPlan(t *testing.T) *storage.Plan {
	t.Helper()
	plan := f.engineRoutedPlan(t)
	plan.ID = 0
	plan.PlanIdentifier = "plan-engine-routed-" + f.dbName
	_, err := f.svc.Storage().Plans().Create(t.Context(), plan)
	require.NoError(t, err)
	return plan
}

func (f *newlyDirectFixture) pinLock(t *testing.T, planID string) {
	t.Helper()
	require.NoError(t, f.svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  f.dbName,
		DatabaseType:  "mysql",
		Owner:         "octocat/hello-world#1",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		PendingPlanID: planID,
	}))
}

func (f *newlyDirectFixture) comment(t *testing.T, command string) {
	t.Helper()
	rr := httptest.NewRecorder()
	f.h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: command, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
}

// awaitComment returns the first posted comment containing want.
func (f *newlyDirectFixture) awaitComment(t *testing.T, want string) string {
	t.Helper()
	deadline := time.After(webhookIntegrationPollDeadline)
	for {
		select {
		case body := <-f.result.comments:
			if strings.Contains(body, want) {
				return body
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a comment containing %q", want)
		}
	}
}

func (f *newlyDirectFixture) pendingPlanID(t *testing.T) string {
	t.Helper()
	lock, err := f.svc.Storage().Locks().Get(t.Context(), f.dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the paused apply keeps the database locked for this PR")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
	return lock.PendingPlanID
}

// requireNewlyDirectDisclosure asserts body is a paused comment disclosing the
// primary-key swap that newly runs as native MySQL DDL.
func requireNewlyDirectDisclosure(t *testing.T, body string) {
	t.Helper()
	assert.Contains(t, body, "Changes run differently from the plan this apply was started from")
	assert.Contains(t, body, "`users` now runs as direct execution")
	assert.Contains(t, body, "⚙️ **Direct execution**: 1 change will run as native MySQL DDL, not through Spirit",
		"the paused comment discloses the native DDL the earlier comment never showed")
	assert.Contains(t, body, "**Confirmation required** — review the plan above, then confirm manually:\n")
}

// Engine refusals are judged against the live table, so the re-plan an
// automatic apply runs before dispatch can route a statement to native MySQL
// DDL that the plan behind the apply's comment ran through Spirit, with its DDL
// unchanged. That comment never disclosed the native DDL, so the apply stops
// against a comment that does, and the pending confirmation moves onto the
// re-plan that comment renders. The window has no user action inside it, so
// the test drives the dispatch core directly.
func TestE2EReplanNewlyDirectDowngradesToConfirm(t *testing.T) {
	f := setupNewlyDirectFixture(t, "webhook_direct_replan_newly_direct")
	shown := f.engineRoutedPlan(t)
	f.pinLock(t, f.directPlan.PlanID)

	f.h.executeApply(t.Context(), f.client, "octocat/hello-world", 1, f.schemaResult, "staging", 1, "testuser",
		CommandResult{Action: action.Apply, Environment: "staging", Found: true, IsMention: true, AllowUnsafe: true},
		shown, shown, f.directPlan.PlanID, false)

	body := f.awaitComment(t, "Confirmation required")
	requireNewlyDirectDisclosure(t, body)
	assert.Contains(t, body, "schemabot apply-confirm -e staging --allow-unsafe")

	requireNoApplies(t, f.svc, f.dbName)
	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, f.dbName, "users"),
		"the stopped apply left the primary key unchanged")
	assert.NotEqual(t, f.directPlan.PlanID, f.pendingPlanID(t),
		"the pending confirmation moved onto the re-plan the paused comment renders")
}

// A statement can also move to direct execution between a paused comment and
// the operator's apply-confirm. The confirmation is checked against the plan
// its comment rendered, so the confirm stops and discloses the native DDL
// rather than running it; confirming the comment that does disclose it then
// applies the change.
func TestE2EApplyConfirmStopsWhenAChangeNewlyRunsDirect(t *testing.T) {
	f := setupNewlyDirectFixture(t, "webhook_direct_confirm_newly_direct")
	shown := f.storeEngineRoutedPlan(t)
	f.pinLock(t, shown.PlanIdentifier)

	f.comment(t, "schemabot apply-confirm -e staging --allow-unsafe")
	requireNewlyDirectDisclosure(t, f.awaitComment(t, "Changes run differently"))
	requireNoApplies(t, f.svc, f.dbName)
	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, f.dbName, "users"),
		"the stopped confirm left the primary key unchanged")
	assert.NotEqual(t, shown.PlanIdentifier, f.pendingPlanID(t),
		"the pending confirmation moved onto the re-plan that discloses the direct change")

	f.comment(t, "schemabot apply-confirm -e staging --allow-unsafe")
	f.awaitComment(t, "Schema Change Applied")
	require.Eventually(t, func() bool {
		return slices.Equal([]string{"id", "tenant_id"}, appPrimaryKeyColumns(t, f.dbName, "users"))
	}, webhookIntegrationPollDeadline, 200*time.Millisecond,
		"confirming the comment that discloses the direct change applies it")
}

// An operator confirms a plan whose comment ran every statement through the
// engine, but the re-plan now routes the primary-key swap to direct execution,
// so the apply stops to disclose it. Between the stop reading the lock and
// moving the pending confirmation onto the disclosing plan, the operator's
// rollback on the same PR pins its plan. The rollback's pin survives, no apply
// starts, and the operator is told the lock changed under the apply.
func TestE2EApplyConfirmNewlyDirectStopKeepsRollbackPinnedAfterItsRead(t *testing.T) {
	const dbName = "webhook_direct_rb_race"
	locks := &concurrentRollbackLocks{
		moment: pinDuringConfirmRepin,
		pin: &storage.Lock{
			DatabaseName: dbName, DatabaseType: "mysql", Owner: "octocat/hello-world#1",
			Repository: "octocat/hello-world", PullRequest: 1,
			PendingPlanID: rollbackPendingPlanPrefix + "plan_concurrent",
		},
	}
	f := setupNewlyDirectFixtureWithStorage(t, dbName, func(st storage.Storage) storage.Storage {
		locks.LockStore = st.Locks()
		return &concurrentRollbackStorage{Storage: st, locks: locks}
	})
	shown := f.storeEngineRoutedPlan(t)
	// The lock is taken on the wrapped store's inner store so the rollback's
	// pin is held back for the stop.
	require.NoError(t, locks.LockStore.Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName, DatabaseType: "mysql", Owner: "octocat/hello-world#1",
		Repository: "octocat/hello-world", PullRequest: 1,
		PendingPlanID: shown.PlanIdentifier,
	}))

	f.comment(t, "schemabot apply-confirm -e staging --allow-unsafe")
	requireNewlyDirectDisclosure(t, f.awaitComment(t, "Changes run differently"))
	refusal := f.awaitComment(t, "changed the lock on")
	assert.Contains(t, refusal, "`"+dbName+"`")
	assert.Contains(t, refusal, "Re-run `schemabot apply -e staging`")
	require.NoError(t, locks.pinErr, "the concurrent rollback must have pinned the lock")

	assert.Equal(t, locks.pin.PendingPlanID, f.pendingPlanID(t), "the concurrent rollback's pin must survive the stop")
	requireNoApplies(t, f.svc, dbName)
	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the stopped confirm left the primary key unchanged")
}

// --defer-cutover has nothing to act on when every change runs as native DDL.
// apply-confirm rejects it on such a plan and keeps the pending confirmation,
// so re-running apply-confirm without the flag still applies the plan the
// operator was shown.
func TestE2EApplyConfirmRejectsDeferCutoverOnAllDirectPlan(t *testing.T) {
	f := setupNewlyDirectFixture(t, "webhook_direct_confirm_defer")
	f.pinLock(t, f.directPlan.PlanID)

	f.comment(t, "schemabot apply-confirm -e staging --allow-unsafe --defer-cutover")
	body := f.awaitComment(t, "`--defer-cutover` has no effect on this plan")
	assert.Contains(t, body, "The pending confirmation is preserved")

	requireNoApplies(t, f.svc, f.dbName)
	assert.Equal(t, f.directPlan.PlanID, f.pendingPlanID(t), "the rejection keeps the pending confirmation pinned")
}

// An automatic apply records the pending changes in stored check state before
// it dispatches, so the merge gate blocks on them before the target changes.
// When that write fails, nothing runs: the direct primary-key swap never
// reaches the target, the lock is released, and the PR is told to retry.
func TestE2EAutomaticApplyDoesNotDispatchWhenCheckStateCannotBeStored(t *testing.T) {
	dbName := "webhook_direct_apply_unstored_check"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{
		engineMetadata: directPolicyMetadata,
		wrapStorage: func(st storage.Storage) storage.Storage {
			return &planResultFailingStorage{Storage: st}
		},
	})
	seedPKSwapTargetTable(t, dbName)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging --allow-unsafe",
		isPR:    true,
	}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "SchemaBot could not record the check state for this apply. Nothing was applied.")
		assert.NotContains(t, body, "Applying automatically", "no plan comment announces an apply that never dispatches")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the check state error comment")
	}

	requireNoApplies(t, svc, dbName)
	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the direct statement never ran on the target")
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "the failed apply releases its lock so the retry can acquire it")
}
