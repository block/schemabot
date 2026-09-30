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
)

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
