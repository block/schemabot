//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/utils"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// indexAddSchema declares an index the target's `events` table does not have,
// so the plan is a single ALTER the schema-change engine copies rows for.
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
	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(t.Context(), "CREATE TABLE `events` (\n"+
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"+
		"  `actor_id` bigint unsigned NOT NULL,\n"+
		"  PRIMARY KEY (`id`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "seed pre-change events table")

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

// discardGateFixture stands up a database whose target already holds an
// unfinished copy that the PR's schema change cannot continue, and returns the
// handler and the fake GitHub the comments land on.
type discardGateFixture struct {
	handler *Handler
	result  *planFlowResult
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

	return discardGateFixture{handler: newE2EHandler(t, svc, client), result: result}
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
}
