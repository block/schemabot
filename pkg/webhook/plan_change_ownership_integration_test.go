//go:build integration

// Integration tests for the plan comment's drop-attribution annotation: the
// disclosure that keeps a plan from presenting a drop of a table another pull
// request owns as a change the planning pull request proposes.

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

const (
	ownershipUsersSchema = "CREATE TABLE `users` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `name` varchar(255) NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

	ownershipUsersWithEmailSchema = "CREATE TABLE `users` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `name` varchar(255) NOT NULL,\n" +
		"  `email` varchar(255) NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

	ownershipReconcileSchema = "CREATE TABLE `reconcile_state` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `pending` tinyint(1) NOT NULL DEFAULT '0',\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
)

// SchemaBot's workflow is schema-first: a pull request applies its schema change
// and merges afterwards, so between those two moments the live database carries
// a table no merged tree describes. A plan on a different pull request diffs its
// own schema files against that database and reads the table as one to drop. The
// plan comment names the pull request the table belongs to, so an operator
// deciding whether to reconcile the database to the declared schema can see
// that the table traces to a change that has not merged yet.
func TestE2EPlanFlagsDropOfTableAnOpenPullRequestOwns(t *testing.T) {
	dbName := "webhook_drop_ownership"
	svc := setupE2EService(t, dbName)
	resetOwnershipHistory(t, dbName)

	// The other pull request applies its new table, then stays open.
	applyDeclaredSchemaForPullRequest(t, svc, dbName, 2, map[string]string{
		"users.sql":           ownershipUsersSchema,
		"reconcile_state.sql": ownershipReconcileSchema,
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)
	registerOpenPullRequest(mux, 2)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "DROP TABLE", "the plan still shows the diff it computed")
		assert.Contains(t, body, "🛑 **Check before applying**")
		assert.Contains(t, body, "`reconcile_state`")
		assert.Contains(t, body, "[octocat/hello-world#2](https://github.com/octocat/hello-world/pull/2)")
		assert.Contains(t, body, "▶️ **To apply**",
			"reconciling to the declared schema stays the operator's call, so the command is still offered")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment")
	}
}

// Attribution is table-grained, so the same window is disclosed for a change
// that destroys part of a table the table itself survives. Another pull request
// adds a column and applies it without merging; a plan whose schema files
// predate that column reads it as one to drop, and the comment names the pull
// request that last changed the table.
func TestE2EPlanFlagsDroppedColumnOnTableAnOpenPullRequestOwns(t *testing.T) {
	dbName := "webhook_column_ownership"
	svc := setupE2EService(t, dbName)
	resetOwnershipHistory(t, dbName)

	// The other pull request applies its new column, then stays open.
	applyDeclaredSchemaForPullRequest(t, svc, dbName, 2, map[string]string{
		"users.sql": ownershipUsersWithEmailSchema,
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)
	registerOpenPullRequest(mux, 2)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "DROP COLUMN", "the plan still shows the diff it computed")
		assert.Contains(t, body, "🛑 **Check before applying**")
		assert.Contains(t, body, "`users`", "the notice names the table, the grain task history records")
		assert.Contains(t, body, "[octocat/hello-world#2](https://github.com/octocat/hello-world/pull/2)")
		assert.Contains(t, body, "▶️ **To apply**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment")
	}
}

// Once the pull request that applied the table is closed, the table is nobody's
// open claim: the plan renders the drop with no attribution notice, so a
// legitimate cleanup carries no warning it does not need.
func TestE2EPlanOffersDropWhenTheOwningPullRequestIsClosed(t *testing.T) {
	dbName := "webhook_drop_owner_closed"
	svc := setupE2EService(t, dbName)
	resetOwnershipHistory(t, dbName)

	applyDeclaredSchemaForPullRequest(t, svc, dbName, 2, map[string]string{
		"users.sql":           ownershipUsersSchema,
		"reconcile_state.sql": ownershipReconcileSchema,
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)
	registerMergedPullRequest(mux, 2)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "DROP TABLE")
		assert.NotContains(t, body, "🛑 **Check before applying**")
		assert.Contains(t, body, "▶️ **To apply**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment")
	}
}

// A plan that cannot establish who owns a table it would drop must not present
// the drop as unremarkable. The lookup fails toward ownership: the entry is
// annotated as unresolved, and the underlying failure stays in the server logs
// rather than the public comment.
func TestE2EPlanFlagsDropAsUnresolvedWhenOwnershipLookupFails(t *testing.T) {
	dbName := "webhook_drop_ownership_error"
	svc := setupE2EServiceWithStorage(t, dbName, func(st storage.Storage) storage.Storage {
		return &ownershipLookupFailureStorage{Storage: st}
	})
	seedOwnershipReconcileTable(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "🛑 **Check before applying**")
		assert.Contains(t, body, "`reconcile_state`")
		assert.Contains(t, body, "ownership could not be established")
		assert.NotContains(t, body, ownershipLookupFailureMessage, "a public comment must not carry the raw lookup error")
		assert.Contains(t, body, "▶️ **To apply**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment")
	}
}

// A pull request that applied a table earlier and now removes it from its own
// schema files is undoing its own work, not another pull request's. Stored task
// history names it as the table's owner, so the annotation would fire on every
// pull request that iterates on a schema change before merging; the planning
// pull request's own history is passed over and the drop renders unremarked.
func TestE2EPlanOffersDropOfATableThisPullRequestApplied(t *testing.T) {
	dbName := "webhook_drop_self_owned"
	svc := setupE2EService(t, dbName)
	resetOwnershipHistory(t, dbName)

	// The planning pull request's own earlier apply created both tables.
	applyDeclaredSchemaForPullRequest(t, svc, dbName, 1, map[string]string{
		"users.sql":           ownershipUsersSchema,
		"reconcile_state.sql": ownershipReconcileSchema,
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "DROP TABLE")
		assert.Contains(t, body, "`reconcile_state`")
		assert.NotContains(t, body, "🛑 **Check before applying**",
			"a pull request undoing its own applied change owes no attribution notice")
		assert.Contains(t, body, "▶️ **To apply**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment")
	}
}

// The table's owning pull request is known, but its state cannot be read from
// GitHub — a revoked installation, a deleted pull request, a repository
// SchemaBot no longer reaches. Whether the claim is still open is exactly the
// question the annotation answers, so a plan that cannot answer it annotates
// the drop as unresolved rather than presenting it as this pull request's to
// make.
func TestE2EPlanFlagsDropAsUnresolvedWhenTheOwningPullRequestCannotBeRead(t *testing.T) {
	dbName := "webhook_drop_owner_unreadable"
	svc := setupE2EService(t, dbName)
	resetOwnershipHistory(t, dbName)

	applyDeclaredSchemaForPullRequest(t, svc, dbName, 2, map[string]string{
		"users.sql":           ownershipUsersSchema,
		"reconcile_state.sql": ownershipReconcileSchema,
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)
	registerUnreadablePullRequest(mux, 2)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "🛑 **Check before applying**")
		assert.Contains(t, body, "`reconcile_state`")
		assert.Contains(t, body, "ownership could not be established")
		assert.NotContains(t, body, "octocat/hello-world#2",
			"a pull request whose state is unknown must not be presented as an established owner")
		assert.Contains(t, body, "▶️ **To apply**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment")
	}
}

// An apply is blocked for carrying a destructive change and is told how to
// override that block with --allow-unsafe. The override is consent to destroy
// the data, and whether the change is this pull request's to make is part of
// what is being consented to, so the attribution travels onto the comment that
// coaches the override rather than only onto the plan comment.
func TestE2EUnsafeBlockedApplyDisclosesAttributedChange(t *testing.T) {
	dbName := "webhook_unsafe_block_ownership"
	svc := setupE2EService(t, dbName)
	resetOwnershipHistory(t, dbName)

	applyDeclaredSchemaForPullRequest(t, svc, dbName, 2, map[string]string{
		"users.sql":           ownershipUsersSchema,
		"reconcile_state.sql": ownershipReconcileSchema,
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)
	registerOpenPullRequest(mux, 2)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Unsafe")
		assert.Contains(t, body, "🛑 **Check before applying**")
		assert.Contains(t, body, "`reconcile_state`")
		assert.Contains(t, body, "[octocat/hello-world#2](https://github.com/octocat/hello-world/pull/2)")
		assert.Contains(t, body, "--allow-unsafe",
			"the override is still offered; the attribution informs the decision to take it")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the unsafe-blocked comment")
	}
}

// An apply that carries --allow-unsafe passes the unsafe gate, acquires the
// lock, and proceeds automatically. That locked comment leaves the operator no
// decision — the attribution disclosure coaches re-planning, which was
// actionable on the plan and unsafe-blocked comments the operator already
// passed through — so it is omitted here.
func TestE2ELockedAutoApplyCommentOmitsAttributedChange(t *testing.T) {
	dbName := "webhook_locked_apply_ownership"
	svc := setupE2EService(t, dbName)
	resetOwnershipHistory(t, dbName)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	applyDeclaredSchemaForPullRequest(t, svc, dbName, 2, map[string]string{
		"users.sql":           ownershipUsersSchema,
		"reconcile_state.sql": ownershipReconcileSchema,
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ownershipUsersSchema}, schemabotConfig, dbName)
	registerOpenPullRequest(mux, 2)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging --allow-unsafe",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Lock acquired by")
		assert.Contains(t, body, "Applying automatically")
		assert.NotContains(t, body, "Check before applying")
		assert.NotContains(t, body, "octocat/hello-world#2")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the locked apply comment")
	}
}

// applyDeclaredSchemaForPullRequest plans and applies a pull request's declared
// schema, waiting for the apply to complete. The target then carries the tables
// those files declare, and storage records which pull request changed them.
func applyDeclaredSchemaForPullRequest(t *testing.T, svc *api.Service, dbName string, pr int32, files map[string]string) {
	t.Helper()
	ctx := t.Context()

	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &pr,
		SchemaFiles: map[string]*ternv1.SchemaFiles{dbName: {Files: files}},
	})
	require.NoError(t, err)

	applyResp, applyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	require.Eventually(t, func() bool {
		apply, err := svc.Storage().Applies().Get(ctx, applyID)
		return err == nil && apply != nil && state.IsState(apply.State, state.Apply.Completed)
	}, webhookIntegrationPollDeadline, 500*time.Millisecond, "the other pull request's apply should complete")

	tasks, err := svc.Storage().Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks, "the apply must record which tables it changed")
}

// seedOwnershipReconcileTable creates the table on the target without an apply,
// so the plan sees a drop for an object storage has no history for.
func seedOwnershipReconcileTable(t *testing.T, dbName string) {
	t.Helper()
	db, err := sql.Open("block-mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `reconcile_state` (\n"+
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"+
		"  `pending` tinyint(1) NOT NULL DEFAULT '0',\n"+
		"  PRIMARY KEY (`id`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "seed the table the plan will drop")
}

// resetOwnershipHistory clears the shared storage database of task and apply
// rows a previous run of the same test left behind, so the ownership lookup
// reads only what this run records.
func resetOwnershipHistory(t *testing.T, dbName string) {
	t.Helper()
	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	ctx := t.Context()
	_, err = db.ExecContext(ctx, "DELETE FROM tasks WHERE database_name = ?", dbName)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "DELETE ao FROM apply_operations ao JOIN applies a ON a.id = ao.apply_id WHERE a.database_name = ?", dbName)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "DELETE FROM applies WHERE database_name = ?", dbName)
	require.NoError(t, err)
}

// registerOpenPullRequest serves an open pull request other than the one the
// plan flow runs on, so the ownership lookup can resolve its state.
func registerOpenPullRequest(mux *http.ServeMux, pr int) {
	registerPullRequestState(mux, pr, "open", false)
}

// registerMergedPullRequest serves a merged pull request, which GitHub reports
// as closed.
func registerMergedPullRequest(mux *http.ServeMux, pr int) {
	registerPullRequestState(mux, pr, "closed", true)
}

// registerUnreadablePullRequest serves a pull request SchemaBot cannot read.
// The status is the non-retryable kind: the client already retries an
// availability failure, so what reaches the ownership lookup is a failure
// retrying does not fix.
func registerUnreadablePullRequest(mux *http.ServeMux, pr int) {
	mux.HandleFunc(fmt.Sprintf("GET /repos/octocat/hello-world/pulls/%d", pr), func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	})
}

func registerPullRequestState(mux *http.ServeMux, pr int, prState string, merged bool) {
	mux.HandleFunc(fmt.Sprintf("GET /repos/octocat/hello-world/pulls/%d", pr), func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			State:  &prState,
			Merged: &merged,
			Head:   &gh.PullRequestBranch{Ref: new("other-branch"), SHA: new("otherhead")},
			Base:   &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User:   &gh.User{Login: new("otheruser")},
		})
	})
}

const ownershipLookupFailureMessage = "object ownership store unavailable"

// ownershipLookupFailureStorage makes every object-ownership lookup fail, so a
// plan that would drop an object cannot establish whether another pull request
// owns it.
type ownershipLookupFailureStorage struct {
	storage.Storage
}

func (s *ownershipLookupFailureStorage) Tasks() storage.TaskStore {
	return &ownershipLookupFailureTasks{TaskStore: s.Storage.Tasks()}
}

type ownershipLookupFailureTasks struct {
	storage.TaskStore
}

func (s *ownershipLookupFailureTasks) FindTableOwners(context.Context, storage.TableRef) ([]storage.TableOwner, error) {
	return nil, errors.New(ownershipLookupFailureMessage)
}
