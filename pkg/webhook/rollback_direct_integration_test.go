//go:build integration

// Rollback direct-execution consent and gating webhook integration tests.

package webhook

import (
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
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// completeDirectPKSwapApply plans and applies the composite-primary-key schema
// against a target holding the single-column key, waits for it to complete,
// and returns the stored apply. With the direct execution policy enabled the
// reshape runs as native DDL, leaving the target with PK (id, tenant_id) and
// original files captured for rollback.
func completeDirectPKSwapApply(t *testing.T, svc *api.Service, dbName string) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	prNumber := int32(1)
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &prNumber,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": pkSwapSchema}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes, "expected the primary-key reshape in the plan")

	applyResp, applyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	require.Eventually(t, func() bool {
		a, err := svc.Storage().Applies().Get(ctx, applyID)
		return err == nil && a != nil && state.IsState(a.State, state.Apply.Completed)
	}, webhookIntegrationPollDeadline, 500*time.Millisecond, "direct apply should complete")

	require.Equal(t, []string{"id", "tenant_id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the direct apply reshaped the primary key on the target")

	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	return storedApply
}

// A rollback whose reverse DDL the engine refuses follows the same consent
// model as a direct apply: the rollback plan comment discloses the ⚙️
// direct-execution routing, and rollback-confirm is the operator's consent
// against that disclosure. A confirm carrying --defer-cutover is rejected (an
// all-direct rollback plan has no cutover to defer) but preserves the pending
// rollback, so a re-run without the flag still executes and restores the
// original primary key on the target.
func TestE2ERollbackDirectPlanDisclosesThenConfirmExecutes(t *testing.T) {
	dbName := "webhook_rb_direct"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{
		engineMetadata: map[string]string{
			"direct_execution":                "true",
			"direct_execution_max_table_rows": "1000000",
		},
	})
	seedPKSwapTargetTable(t, dbName)

	storedApply := completeDirectPKSwapApply(t, svc, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	rollbackReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier),
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, rollbackReq)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Rollback Plan")
		assert.Contains(t, body, "⚙️ **Direct execution**", "the rollback plan comment discloses the direct change")
		assert.Contains(t, body, "runs as native MySQL DDL")
		assert.Contains(t, body, "`users`")
		assert.Contains(t, body, "schemabot rollback-confirm -e staging")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the rollback plan comment")
	}

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the rollback command pins the plan on the lock for the confirm step")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
	require.True(t, strings.HasPrefix(lock.PendingPlanID, rollbackPendingPlanPrefix))

	// --defer-cutover has nothing to defer on this all-direct rollback plan, so
	// the confirm is rejected — but the rejection must not discard the pending
	// rollback: the lock keeps pinning the disclosed plan so a bare re-run of
	// rollback-confirm still executes it.
	flaggedConfirmReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging --defer-cutover",
		isPR:    true,
	}, nil)

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, flaggedConfirmReq)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "`--defer-cutover` has no effect on this rollback")
		assert.Contains(t, body, "The pending rollback is preserved")
		assert.Contains(t, body, "schemabot rollback-confirm -e staging")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the defer-cutover rejection comment")
	}

	preserved, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, preserved, "the rejected confirm must keep the pending rollback locked")
	assert.Equal(t, lock.Owner, preserved.Owner)
	assert.Equal(t, lock.PendingPlanID, preserved.PendingPlanID,
		"the lock must still pin the rollback plan the operator confirmed against")
	assert.Equal(t, []string{"id", "tenant_id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the rejected confirm must not have executed the rollback")

	confirmReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, confirmReq)
	require.Equal(t, http.StatusOK, rr.Code)

	// The direct statement is synchronous and the table is empty, so the
	// rollback may terminalize before the first progress poll: scan comments
	// until the terminal rollback summary arrives.
	gotSummary := false
	deadline := time.After(webhookIntegrationPollDeadline)
	for !gotSummary {
		select {
		case body := <-result.comments:
			if strings.Contains(body, "Rollback Complete") {
				gotSummary = true
				assert.Contains(t, body, "Rolled back successfully")
			}
		case <-deadline:
			t.Fatal("timed out waiting for the rollback summary comment")
		}
	}

	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the confirmed direct rollback restored the original primary key on the target")
}

// A rollback whose reverse DDL the engine refuses and the direct execution
// policy does not route is rejected before a plan is pinned: after a direct
// apply reshaped the primary key of a small table, the table grows past the
// policy's size bound, so the reverse reshape resolves to blocked. The PR gets
// a ⛔ rollback rejection naming the table and the refusal, nothing is pinned
// for confirmation, and the database lock stays free.
func TestE2ERollbackRejectedOnBlockedReversePlan(t *testing.T) {
	dbName := "webhook_rb_blocked"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{
		engineMetadata: map[string]string{
			"direct_execution":                "true",
			"direct_execution_max_table_rows": "10",
		},
	})
	seedPKSwapTargetTable(t, dbName)

	storedApply := completeDirectPKSwapApply(t, svc, dbName)

	// Grow the table past the policy bound so the reverse reshape can no
	// longer be routed to direct execution.
	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	values := strings.TrimSuffix(strings.Repeat("(1),", 50), ",")
	_, err = db.ExecContext(t.Context(), "INSERT INTO `users` (`tenant_id`) VALUES "+values)
	require.NoError(t, err, "seed rows past the direct-execution size bound")
	_, err = db.ExecContext(t.Context(), "ANALYZE TABLE `users`")
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	rollbackReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier),
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, rollbackReq)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Rollback Plan")
		assert.Contains(t, body, "⛔ Rollback rejected")
		assert.Contains(t, body, "not supported by the schema-change engine")
		assert.Contains(t, body, "`users`")
		assert.NotContains(t, body, "schemabot rollback-confirm",
			"a rejected rollback must not coach a confirmation that can never succeed")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the rollback rejection comment")
	}

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "a rejected rollback must not leave the database locked")
}

// A pinned rollback plan carrying engine-blocked changes can never execute, so
// rollback-confirm rejects it instead of starting an apply guaranteed to fail:
// the PR gets a rejection explaining the plan cannot run, and the lock is
// released so re-running the rollback command re-plans through the up-front
// gate.
func TestE2ERollbackConfirmRejectsBlockedPinnedPlan(t *testing.T) {
	dbName := "webhook_rb_blocked_pin"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	planID := "plan_rbblockedpin1"
	_, err := svc.Storage().Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: planID,
		Database:       dbName,
		DatabaseType:   "mysql",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			dbName: {
				Tables: []storage.TableChange{{
					Table:         "users",
					DDL:           "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`)",
					Operation:     "alter",
					ExecutionMode: "blocked",
					ModeReason:    "the schema-change engine cannot change a table's PRIMARY KEY",
				}},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, svc.Storage().Locks().Acquire(ctx, &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Owner:         "octocat/hello-world#1",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		PendingPlanID: rollbackPendingPlanID(planID),
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	confirmReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, confirmReq)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "The pinned rollback plan contains changes the schema-change engine does not support")
		assert.Contains(t, body, "The lock has been released")
		assert.Contains(t, body, "schemabot rollback <apply-id> -e staging")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the blocked-plan rejection comment")
	}

	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "the rejected confirm must release the lock so a fresh rollback can re-plan")
}
