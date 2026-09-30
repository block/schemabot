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

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
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

// Engine refusals are judged against the live table, so the re-plan an
// automatic apply runs before dispatch can route a statement to native MySQL
// DDL that the plan behind the apply's comment ran through Spirit, with its DDL
// unchanged. That comment never disclosed the native DDL, so the apply stops
// against a comment that does rather than running it. The window has no user
// action inside it, so the test drives the dispatch core directly with a
// stored plan whose verdict for the statement is the engine's default path.
func TestE2EReplanNewlyDirectDowngradesToConfirm(t *testing.T) {
	dbName := "webhook_direct_replan_newly_direct"
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
	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")

	const repo = "octocat/hello-world"
	const pr = 1
	schemaResult, err := h.createManagedSchemaRequestFromPR(t.Context(), installClient, repo, pr, "staging", "", action.Apply)
	require.NoError(t, err)

	prNumber := int32(pr)
	_, planResp, err := h.executePlanProtoWithTransientRetry(t.Context(), api.PlanRequest{
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
	require.Len(t, planResp.DirectChanges(), 1, "the policy routes the primary-key swap to direct execution")

	// The plan the operator was shown: the same statement, run through the
	// engine, so its comment carried no direct execution disclosure.
	storedPlan, err := svc.Storage().Plans().Get(t.Context(), planResp.PlanID)
	require.NoError(t, err)
	require.NotNil(t, storedPlan)
	for _, ns := range storedPlan.Namespaces {
		for i := range ns.Tables {
			ns.Tables[i].ExecutionMode = ""
			ns.Tables[i].ModeReason = ""
		}
	}

	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Owner:         fmt.Sprintf("%s#%d", repo, pr),
		Repository:    repo,
		PullRequest:   pr,
		PendingPlanID: planResp.PlanID,
	}))

	h.executeApply(t.Context(), installClient, repo, pr, schemaResult, "staging", 1, "testuser",
		CommandResult{Action: action.Apply, Environment: "staging", Found: true, IsMention: true, AllowUnsafe: true},
		storedPlan, planResp.PlanID, false)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Changes run differently from the plan this apply was started from")
		assert.Contains(t, body, "`users` now runs as direct execution")
		assert.Contains(t, body, "⚙️ **Direct execution**: 1 change will run as native MySQL DDL, not through Spirit",
			"the downgraded comment discloses the native DDL the reviewed comment never showed")
		assert.Contains(t, body, "**Confirmation required** — review the plan above, then confirm manually:\n")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the downgraded plan comment")
	}

	requireNoApplies(t, svc, dbName)
	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the stopped apply left the primary key unchanged")
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the paused apply keeps the database locked for this PR")
	assert.Equal(t, fmt.Sprintf("%s#%d", repo, pr), lock.Owner)
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
