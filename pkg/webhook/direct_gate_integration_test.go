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

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// appPrimaryKeyColumns returns the app table's primary-key column names in
// ordinal order, so tests can assert whether the reshape landed on the target.
func appPrimaryKeyColumns(t *testing.T, dbName, tableName string) []string {
	t.Helper()
	db, err := sql.Open("block-mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	rows, err := db.QueryContext(t.Context(), `
		SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`, dbName, tableName)
	require.NoError(t, err, "query PK columns")
	defer utils.CloseAndLog(rows)
	var cols []string
	for rows.Next() {
		var col string
		require.NoError(t, rows.Scan(&col))
		cols = append(cols, col)
	}
	require.NoError(t, rows.Err())
	return cols
}

// With the direct execution policy enabled, an apply whose plan routes a
// change to native MySQL DDL never runs automatically: the locked plan comment
// discloses the direct change and pauses the automatic apply, and a subsequent
// apply-confirm — the operator's consent against that disclosure — executes
// the change so the reshaped primary key actually lands on the target. The
// primary-key reshape is also an unsafe operation, so both commands carry
// --allow-unsafe; the direct downgrade layers on top of the unsafe gate
// rather than replacing it. An apply-confirm that carries --defer-cutover is
// rejected (an all-direct plan has no cutover to defer) but preserves the
// pending confirmation, so a re-run without the flag still executes.
func TestE2EDirectPlanDowngradesToConfirmThenApplies(t *testing.T) {
	dbName := "webhook_direct_confirm"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{
		engineMetadata: map[string]string{
			"direct_execution":                "true",
			"direct_execution_max_table_rows": "1000000",
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
	applyReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging --allow-unsafe",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, applyReq)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "⚙️ **Direct execution**", "the locked comment discloses the direct change")
		assert.Contains(t, body, "runs as native MySQL DDL")
		assert.Contains(t, body, "⚠️ **Automatic apply paused**: Plan contains direct-execution changes — review the disclosure and confirm manually")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the downgraded plan comment")
	}

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the downgraded apply holds the lock for the confirm step")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)

	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the paused apply must not have touched the target")

	// --defer-cutover has nothing to defer on this all-direct plan, so the
	// confirm is rejected — but the rejection must not discard the pending
	// confirmation: the lock keeps pinning the disclosed plan so a bare
	// re-run of apply-confirm still executes it.
	flaggedConfirmReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging --allow-unsafe --defer-cutover",
		isPR:    true,
	}, nil)

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, flaggedConfirmReq)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "`--defer-cutover` has no effect on this plan")
		assert.Contains(t, body, "The pending confirmation is preserved")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the defer-cutover rejection comment")
	}

	preserved, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, preserved, "the rejected confirm must keep the pending confirmation locked")
	assert.Equal(t, lock.Owner, preserved.Owner)
	assert.Equal(t, lock.PendingPlanID, preserved.PendingPlanID,
		"the lock must still pin the plan the operator confirmed against")
	assert.Equal(t, []string{"id"}, appPrimaryKeyColumns(t, dbName, "users"),
		"the rejected confirm must not have executed the direct change")

	confirmReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging --allow-unsafe",
		isPR:    true,
	}, nil)

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, confirmReq)
	require.Equal(t, http.StatusOK, rr.Code)

	// The direct statement is synchronous and the table is empty, so the apply
	// may terminalize before the first progress poll: accept either a progress
	// comment followed by a summary, or the summary directly.
	select {
	case body := <-result.comments:
		hasProgress := strings.Contains(body, "Schema Change Status")
		hasApplied := strings.Contains(body, "Schema Change Applied")
		require.True(t, hasProgress || hasApplied,
			"expected progress or applied comment, got: %s", body[:min(len(body), 200)])
		if hasProgress {
			select {
			case summary := <-result.comments:
				assert.Contains(t, summary, "Schema Change Applied")
			case <-time.After(webhookIntegrationPollDeadline):
				t.Fatal("timed out waiting for the apply summary comment")
			}
		}
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for any apply comment")
	}

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
		"the confirmed direct apply reshaped the primary key on the target")
}

// --defer-cutover on a plan whose every change runs as native MySQL DDL is
// rejected before any lock is taken: direct statements have no cutover to
// defer, so the flag would be silently meaningless — the PR gets a command
// error telling the operator to re-run without it.
func TestE2EDeferCutoverRejectedOnAllDirectPlan(t *testing.T) {
	dbName := "webhook_direct_defer"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{
		engineMetadata: map[string]string{
			"direct_execution":                "true",
			"direct_execution_max_table_rows": "1000000",
		},
	})
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
