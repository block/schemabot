//go:build integration

package webhook

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// planCommentFakeGitHub captures comment creates (returning REST id and
// GraphQL node id), comment deletes, and minimizeComment mutations, with
// per-comment failure injection for the retry scenarios. It also serves the
// PR fetch with a configurable current head so sweeps can verify head
// freshness.
type planCommentFakeGitHub struct {
	mu            sync.Mutex
	nextID        int64
	created       []int64
	minimized     []string
	deleted       []int64
	failNodes     map[string]bool
	failDeleteIDs map[int64]bool
	headSHA       string
}

func (f *planCommentFakeGitHub) createComment() (int64, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := f.nextID
	f.created = append(f.created, id)
	return id, fmt.Sprintf("IC_node%d", id)
}

func (f *planCommentFakeGitHub) minimize(nodeID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNodes[nodeID] {
		return false
	}
	f.minimized = append(f.minimized, nodeID)
	return true
}

func (f *planCommentFakeGitHub) deleteComment(commentID int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDeleteIDs[commentID] {
		return false
	}
	f.deleted = append(f.deleted, commentID)
	return true
}

func (f *planCommentFakeGitHub) setMinimizeFails(nodeID string, fails bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNodes == nil {
		f.failNodes = map[string]bool{}
	}
	f.failNodes[nodeID] = fails
}

func (f *planCommentFakeGitHub) setDeleteFails(commentID int64, fails bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDeleteIDs == nil {
		f.failDeleteIDs = map[int64]bool{}
	}
	f.failDeleteIDs[commentID] = fails
}

func (f *planCommentFakeGitHub) minimizedNodes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.minimized...)
}

func (f *planCommentFakeGitHub) deletedCommentIDs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.deleted...)
}

func (f *planCommentFakeGitHub) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.created)
}

func (f *planCommentFakeGitHub) setCurrentHead(sha string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headSHA = sha
}

func (f *planCommentFakeGitHub) currentHead() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headSHA
}

// setupFakeGitHubForPlanComments serves the comment create and delete REST
// endpoints and the GraphQL minimizeComment mutation against any repo/PR.
func setupFakeGitHubForPlanComments(t *testing.T) (*ghclient.InstallationClient, *planCommentFakeGitHub) {
	t.Helper()

	fake := &planCommentFakeGitHub{nextID: 1000}
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	mux.HandleFunc("POST /repos/", func(w http.ResponseWriter, _ *http.Request) {
		id, nodeID := fake.createComment()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "node_id": nodeID})
	})

	mux.HandleFunc("DELETE /repos/", func(w http.ResponseWriter, r *http.Request) {
		segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		commentID, err := strconv.ParseInt(segments[len(segments)-1], 10, 64)
		require.NoError(t, err, "comment delete path must end in the comment id")
		if !fake.deleteComment(commentID) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// PR fetch: serves the fake's configured current head so sweeps that
	// verify head freshness see the branch state the test laid out.
	mux.HandleFunc("GET /repos/", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			State: new("open"),
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new(fake.currentHead()),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("basesha"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Variables map[string]string `json:"variables"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		if !fake.minimize(req.Variables["id"]) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"errors": []map[string]any{{"message": "injected minimize failure"}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"minimizeComment": map[string]any{
					"minimizedComment": map[string]any{"isMinimized": true},
				},
			},
		})
	})

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return ghclient.NewInstallationClient(client, logger), fake
}

// setupPlanCommentHandler builds a webhook handler over real SchemaBot storage
// and the plan-comment fake GitHub, clearing prior rows for the given repo.
// deleteUnactioned opts the server into the delete-based retirement policy;
// false exercises the default minimize-based policy.
func setupPlanCommentHandler(t *testing.T, repo string, deleteUnactioned bool) (*Handler, storage.Storage, *planCommentFakeGitHub) {
	t.Helper()
	ctx := t.Context()

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	// Redundant close for early-exit leak safety: svc.Close below owns the
	// handle (the store is built over it), so this close is expected to see an
	// already-closed DB and must discard the error.
	t.Cleanup(func() { _ = schemabotDB.Close() })
	st := mysqlstore.New(schemabotDB)

	for _, stmt := range []string{
		"DELETE FROM plan_comments WHERE repository = ?",
		"DELETE FROM applies WHERE repository = ?",
		"DELETE FROM plans WHERE repository = ?",
		"DELETE FROM locks WHERE repository = ?",
	} {
		_, err := schemabotDB.ExecContext(ctx, stmt, repo)
		require.NoError(t, err)
	}

	installClient, fake := setupFakeGitHubForPlanComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := &api.ServerConfig{DeleteUnactionedPlanComments: deleteUnactioned}
	svc := api.New(st, cfg, map[string]tern.Client{}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	return NewHandler(svc, factory, nil, logger), st, fake
}

// createRunningApplyForHead records a plan and a running apply for the given
// head, making any plan comment rendered at that head apply-owned.
func createRunningApplyForHead(t *testing.T, st storage.Storage, repo string, pr int, database, environment, headSHA string) {
	t.Helper()
	ctx := t.Context()

	planID, err := st.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan_%s_%s", database, headSHA),
		Database:       database,
		DatabaseType:   "mysql",
		Repository:     repo,
		PullRequest:    pr,
		Environment:    environment,
		HeadSHA:        headSHA,
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)
	lock := &storage.Lock{
		DatabaseName: database,
		DatabaseType: "mysql",
		Repository:   repo,
		PullRequest:  pr,
		Owner:        fmt.Sprintf("%s#%d", repo, pr),
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, database, "mysql")
	require.NoError(t, err)
	_, err = st.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_%s_%s", database, headSHA),
		LockID:          lock.ID,
		PlanID:          planID,
		Database:        database,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     environment,
		Engine:          "spirit",
		State:           state.Apply.Running,
	})
	require.NoError(t, err)
}

func unretiredHeads(t *testing.T, st storage.Storage, repo string, pr int, database, databaseType string) []string {
	t.Helper()
	comments, err := st.PlanComments().ListUnretiredForSlot(t.Context(), repo, pr, database, databaseType)
	require.NoError(t, err)
	heads := make([]string, len(comments))
	for i, c := range comments {
		heads[i] = c.HeadSHA
	}
	return heads
}

// TestPlanCommentSupersedeDeletesUnactionedPriorComments exercises the
// noise-reduction UX on a frequently updated PR: each newly posted plan
// comment deletes the prior plan comments it supersedes (older head, or a
// refresh of the same head and environment scope) when no apply ever acted on
// them — an unactioned plan's DDL never ran, so its comment is pure timeline
// noise. A same-head comment for different environments and comments for
// other databases stay expanded, and every retired comment's storage row
// survives with its identifiers.
func TestPlanCommentSupersedeDeletesUnactionedPriorComments(t *testing.T) {
	const repo = "org/plan-retire-lifecycle"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	slot := planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging", "production"},
		HeadSHA:      "sha1",
	}

	fake.setCurrentHead("sha1")

	// A comment for another database on the same PR must never be touched.
	otherSlot := slot
	otherSlot.Database = "billing"
	h.postTrackedPlanComment(repo, 42, 12345, otherSlot, "billing plan")

	// First comment in the orders slot: nothing to supersede.
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at sha1")
	assert.Empty(t, fake.deletedCommentIDs())
	assert.Equal(t, []string{"sha1"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))

	// A new head supersedes the sha1 comment; no apply acted on it.
	slot.HeadSHA = "sha2"
	fake.setCurrentHead("sha2")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at sha2")
	assert.Equal(t, []int64{1002}, fake.deletedCommentIDs(), "the unactioned sha1 comment is deleted from GitHub")
	assert.Empty(t, fake.minimizedNodes(), "an unactioned comment is deleted, never minimized")
	assert.Equal(t, []string{"sha2"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))

	// A manual single-environment plan on the same head covers a narrower
	// scope: it does not supersede the combined comment, and the combined
	// comment does not retroactively retire it.
	stagingSlot := slot
	stagingSlot.Environments = []string{"staging"}
	h.postTrackedPlanComment(repo, 42, 12345, stagingSlot, "staging-only plan at sha2")
	assert.Len(t, fake.deletedCommentIDs(), 1, "different scope on the same head retires nothing")
	assert.Equal(t, []string{"sha2", "sha2"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))

	// Re-running the staging-only plan on the same head refreshes it: the
	// older staging-only comment is deleted, the combined comment stays.
	h.postTrackedPlanComment(repo, 42, 12345, stagingSlot, "staging-only plan at sha2, again")
	assert.Equal(t, []int64{1002, 1004}, fake.deletedCommentIDs())
	comments, err := st.PlanComments().ListUnretiredForSlot(t.Context(), repo, 42, "orders", "mysql")
	require.NoError(t, err)
	require.Len(t, comments, 2)
	assert.Equal(t, "production,staging", comments[0].EnvironmentScope)
	assert.Equal(t, "staging", comments[1].EnvironmentScope)

	// The billing slot was never touched.
	assert.Equal(t, []string{"sha1"}, unretiredHeads(t, st, repo, 42, "billing", "mysql"))
	assert.Equal(t, 5, fake.createCount(), "every post reached GitHub exactly once")
}

// TestPlanCommentWithoutSlotIdentityPostsUntracked covers the error-only
// comment a plan posts when every environment failed before a database or
// head resolved: with no slot identity to key tracking, the comment still
// posts (visibility first) but is not tracked, so no empty-identity row can
// make error-only comments for different databases supersede each other.
func TestPlanCommentWithoutSlotIdentityPostsUntracked(t *testing.T) {
	const repo = "org/plan-retire-untracked"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	h.postTrackedPlanComment(repo, 42, 12345, planCommentSlot{}, "plan failed in every environment")

	assert.Equal(t, 1, fake.createCount(), "the error-only comment still posts")
	assert.Empty(t, fake.deletedCommentIDs())
	assert.Empty(t, fake.minimizedNodes())
	comments, err := st.PlanComments().ListUnretiredForSlot(t.Context(), repo, 42, "", "")
	require.NoError(t, err)
	assert.Empty(t, comments, "no row is tracked under an empty slot identity")
}

// TestPlanCommentApplyOwnedHeadIsMinimizedNotDeleted covers the safety hold:
// once an apply exists for the head a plan comment was rendered at, that
// comment is the operational record of what ran. When a newer plan comment
// supersedes it, the comment is minimized — collapsed in the timeline but
// still expandable — never deleted.
func TestPlanCommentApplyOwnedHeadIsMinimizedNotDeleted(t *testing.T) {
	const repo = "org/plan-retire-apply-owned"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	slot := planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "shaA",
	}
	fake.setCurrentHead("shaA")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaA")

	// The shaA plan becomes an apply before the next push.
	createRunningApplyForHead(t, st, repo, 42, "orders", "staging", "shaA")

	slot.HeadSHA = "shaB"
	fake.setCurrentHead("shaB")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaB")

	assert.Equal(t, []string{"IC_node1001"}, fake.minimizedNodes(),
		"the apply-owned shaA comment is minimized, keeping it expandable")
	assert.Empty(t, fake.deletedCommentIDs(), "an apply-owned comment must never be deleted")
	assert.Equal(t, []string{"shaB"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))
}

// TestPlanCommentDeleteFailureRetriesOnNextSupersede covers the retry
// semantics: a failed GitHub delete call leaves the row unretired, so the
// next plan comment in the slot picks the comment up again. The failure mode
// is only extra noise on the PR — a comment is never recorded as deleted
// unless GitHub confirmed it.
func TestPlanCommentDeleteFailureRetriesOnNextSupersede(t *testing.T) {
	const repo = "org/plan-retire-retry"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	slot := planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "shaA",
	}
	fake.setCurrentHead("shaA")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaA")
	fake.setDeleteFails(1001, true)

	slot.HeadSHA = "shaB"
	fake.setCurrentHead("shaB")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaB")
	assert.Empty(t, fake.deletedCommentIDs(), "the injected failure leaves the shaA comment on the PR")
	assert.Equal(t, []string{"shaA", "shaB"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"),
		"a failed delete must not be recorded as deleted")

	// GitHub recovers; the next supersede retires both stale comments.
	fake.setDeleteFails(1001, false)
	slot.HeadSHA = "shaC"
	fake.setCurrentHead("shaC")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaC")
	assert.ElementsMatch(t, []int64{1001, 1002}, fake.deletedCommentIDs())
	assert.Equal(t, []string{"shaC"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))
}

// insertPlanCommentRow records an already-posted plan comment directly in
// storage, so a test can lay out a slot's history without the supersede pass
// that posting through the handler would run.
func insertPlanCommentRow(t *testing.T, st storage.Storage, repo string, pr int, database, scope, headSHA string, commentID int64, nodeID string) {
	t.Helper()
	require.NoError(t, st.PlanComments().Insert(t.Context(), &storage.PlanComment{
		Repository:       repo,
		PullRequest:      pr,
		DatabaseName:     database,
		DatabaseType:     "mysql",
		EnvironmentScope: scope,
		HeadSHA:          headSHA,
		GitHubCommentID:  commentID,
		GitHubNodeID:     nodeID,
	}))
}

// TestStalePlanCommentsDeletedWithoutNewComment exercises the up-to-date-PR
// UX: when the current head's plan outcome supersedes prior comments without
// posting a new comment — an auto-plan that resolves to no changes — plan
// comments rendered at older heads with no apply are deleted, since the
// pending DDL and apply prompt they advertise no longer match the branch and
// nothing ever ran them. A comment already rendered at the current head stays
// expanded (it may be the only visible plan for its environment scope), other
// databases' slots are untouched, and the sweep never posts a comment of its
// own.
func TestStalePlanCommentsDeletedWithoutNewComment(t *testing.T) {
	const repo = "org/plan-retire-stale-sweep"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	insertPlanCommentRow(t, st, repo, 42, "orders", "production,staging", "shaA", 9001, "IC_stale_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaB", 9002, "IC_current_shaB")
	insertPlanCommentRow(t, st, repo, 42, "billing", "production,staging", "shaA", 9003, "IC_billing_shaA")
	fake.setCurrentHead("shaB")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.retireStalePlanComments(t.Context(), client, repo, 42, "orders", "mysql", "shaB")

	assert.Equal(t, []int64{9001}, fake.deletedCommentIDs(),
		"only the prior-head orders comment is deleted")
	assert.Empty(t, fake.minimizedNodes())
	assert.Equal(t, []string{"shaB"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"),
		"the current-head comment stays expanded and the stale row is recorded deleted")
	assert.Equal(t, []string{"shaA"}, unretiredHeads(t, st, repo, 42, "billing", "mysql"),
		"another database's slot is untouched")
	assert.Equal(t, 0, fake.createCount(), "the sweep posts no comment")
}

// TestStalePlanCommentSweepSkipsWhenHeadMoved covers the concurrent-push
// safety of the no-new-comment sweep: a plan outcome computed for an older
// head must never retire the current head's live plan comment, because
// nothing would replace it and a deleted comment cannot be restored. When the
// PR head has moved past the sweep's head, the sweep leaves every comment
// expanded — the current head's own plan outcome supersedes the slot.
func TestStalePlanCommentSweepSkipsWhenHeadMoved(t *testing.T) {
	const repo = "org/plan-retire-stale-head-moved"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaA", 9001, "IC_old_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaC", 9002, "IC_live_shaC")
	fake.setCurrentHead("shaC")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.retireStalePlanComments(t.Context(), client, repo, 42, "orders", "mysql", "shaB")

	assert.Empty(t, fake.deletedCommentIDs(),
		"a sweep for a superseded head must not touch the slot")
	assert.Empty(t, fake.minimizedNodes())
	assert.ElementsMatch(t, []string{"shaA", "shaC"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"),
		"every comment stays expanded until the current head's own sweep")
}

// TestStalePlanCommentSweepMinimizesApplyOwnedHead covers the safety hold on
// the no-new-comment sweep: once an apply exists for the head a plan comment
// was rendered at, that comment is the operational record of what ran. A
// later head resolving to no changes — for example after the applied change
// lands and the schema converges — minimizes it, keeping the record
// expandable, while a stale comment with no apply is deleted outright.
func TestStalePlanCommentSweepMinimizesApplyOwnedHead(t *testing.T) {
	const repo = "org/plan-retire-stale-apply-owned"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	insertPlanCommentRow(t, st, repo, 42, "inventory", "staging", "shaA", 9001, "IC_owned_shaA")
	// A second stale comment with no apply proves the two retirement paths
	// run side by side in one sweep: shaZ is deleted while shaA is minimized.
	insertPlanCommentRow(t, st, repo, 42, "inventory", "staging", "shaZ", 9002, "IC_unowned_shaZ")
	fake.setCurrentHead("shaB")

	// The shaA plan becomes an apply before the next push.
	createRunningApplyForHead(t, st, repo, 42, "inventory", "staging", "shaA")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.retireStalePlanComments(t.Context(), client, repo, 42, "inventory", "mysql", "shaB")

	assert.Equal(t, []string{"IC_owned_shaA"}, fake.minimizedNodes(),
		"the apply-owned shaA comment is minimized, never deleted")
	assert.Equal(t, []int64{9002}, fake.deletedCommentIDs(),
		"the unowned stale comment is deleted in the same sweep")
	assert.Empty(t, unretiredHeads(t, st, repo, 42, "inventory", "mysql"),
		"both comments are recorded retired")
}

// TestPRWideStalePlanCommentSweep covers the PR whose new head resolves no
// schema config at all — the author dropped the schema change, or the commit
// only ever touched unrelated files. There is no database left to key a slot
// with, so the slot sweep cannot run, yet earlier heads' plan comments are
// exactly what such a push leaves behind: still advertising that head's DDL and
// its apply prompt while only the check run moves on. Every database's
// unactioned stale comment is deleted, the current head's stays expanded, and
// no comment is posted.
func TestPRWideStalePlanCommentSweep(t *testing.T) {
	const repo = "org/plan-retire-pr-wide-sweep"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	insertPlanCommentRow(t, st, repo, 42, "orders", "production,staging", "shaA", 9001, "IC_orders_shaA")
	insertPlanCommentRow(t, st, repo, 42, "billing", "staging", "shaA", 9002, "IC_billing_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaB", 9003, "IC_orders_shaB")
	// A different PR in the same repo proves the sweep is scoped to one PR.
	insertPlanCommentRow(t, st, repo, 77, "orders", "staging", "shaA", 9004, "IC_other_pr_shaA")
	fake.setCurrentHead("shaB")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.retireStalePlanCommentsForPR(t.Context(), client, repo, 42, "shaB")

	assert.ElementsMatch(t, []int64{9001, 9002}, fake.deletedCommentIDs(),
		"every database's prior-head comment is deleted, not just one slot's")
	assert.Empty(t, fake.minimizedNodes())
	assert.Equal(t, []string{"shaB"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"),
		"the current-head comment stays expanded")
	assert.Empty(t, unretiredHeads(t, st, repo, 42, "billing", "mysql"))
	assert.Equal(t, []string{"shaA"}, unretiredHeads(t, st, repo, 77, "orders", "mysql"),
		"another PR's comments are untouched")
	assert.Equal(t, 0, fake.createCount(), "the sweep posts no comment")
}

// TestPRWideStalePlanCommentSweepSkipsWhenHeadMoved covers the same
// concurrent-push safety as the slot sweep, for the sweep that has no database
// to key on: a delivery for a head the branch has already moved past must leave
// every comment expanded, because nothing would replace what it retired.
func TestPRWideStalePlanCommentSweepSkipsWhenHeadMoved(t *testing.T) {
	const repo = "org/plan-retire-pr-wide-head-moved"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaA", 9001, "IC_old_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaC", 9002, "IC_live_shaC")
	fake.setCurrentHead("shaC")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.retireStalePlanCommentsForPR(t.Context(), client, repo, 42, "shaB")

	assert.Empty(t, fake.deletedCommentIDs(),
		"a sweep for a superseded head must not touch the PR")
	assert.Empty(t, fake.minimizedNodes())
	assert.ElementsMatch(t, []string{"shaA", "shaC"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))
}

// TestPlanCommentSupersedeDefaultPolicyMinimizes covers the default
// minimize-based retirement policy, for a server that has not opted into
// deletion: a superseded plan comment no apply acted on is minimized —
// collapsed in the timeline but still expandable — and a superseded comment
// whose head an apply owns stays fully expanded as the operational record of
// what ran. Nothing is ever deleted.
func TestPlanCommentSupersedeDefaultPolicyMinimizes(t *testing.T) {
	const repo = "org/plan-retire-default-policy"
	h, st, fake := setupPlanCommentHandler(t, repo, false)

	slot := planCommentSlot{
		Database:     "payments",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "sha1",
	}
	fake.setCurrentHead("sha1")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at sha1")

	// No apply acted on sha1; the next head minimizes its comment.
	slot.HeadSHA = "sha2"
	fake.setCurrentHead("sha2")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at sha2")
	assert.Equal(t, []string{"IC_node1001"}, fake.minimizedNodes(),
		"the unactioned sha1 comment is minimized, not deleted")
	assert.Empty(t, fake.deletedCommentIDs(), "the default policy never deletes")
	assert.Equal(t, []string{"sha2"}, unretiredHeads(t, st, repo, 42, "payments", "mysql"))

	// An apply now owns sha2; the next head leaves its comment fully expanded.
	createRunningApplyForHead(t, st, repo, 42, "payments", "staging", "sha2")
	slot.HeadSHA = "sha3"
	fake.setCurrentHead("sha3")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at sha3")
	assert.Equal(t, []string{"IC_node1001"}, fake.minimizedNodes(),
		"the apply-owned sha2 comment stays fully expanded")
	assert.Empty(t, fake.deletedCommentIDs())
	assert.ElementsMatch(t, []string{"sha2", "sha3"}, unretiredHeads(t, st, repo, 42, "payments", "mysql"),
		"an apply-owned comment stays unretired under the default policy")
}

// TestPlanCommentMinimizeFailureRetriesOnNextSupersede covers the retry
// semantics of the minimize call: a failed GitHub minimize leaves the row
// unretired, so the next plan comment in the slot picks the comment up again.
// The failure mode is only extra noise on the PR — a comment is never recorded
// as minimized unless GitHub confirmed it.
func TestPlanCommentMinimizeFailureRetriesOnNextSupersede(t *testing.T) {
	const repo = "org/plan-retire-minimize-retry"
	h, st, fake := setupPlanCommentHandler(t, repo, false)

	slot := planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "shaA",
	}
	fake.setCurrentHead("shaA")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaA")
	fake.setMinimizeFails("IC_node1001", true)

	slot.HeadSHA = "shaB"
	fake.setCurrentHead("shaB")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaB")
	assert.Empty(t, fake.minimizedNodes(), "the injected failure leaves the shaA comment expanded")
	assert.Equal(t, []string{"shaA", "shaB"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"),
		"a failed minimize must not be recorded as minimized")

	// GitHub recovers; the next supersede retires both stale comments.
	fake.setMinimizeFails("IC_node1001", false)
	slot.HeadSHA = "shaC"
	fake.setCurrentHead("shaC")
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaC")
	assert.ElementsMatch(t, []string{"IC_node1001", "IC_node1002"}, fake.minimizedNodes())
	assert.Equal(t, []string{"shaC"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))
}

// TestPlanCommentPostForStaleHeadRetiresNothing covers the concurrent-push
// safety of the post-path sweep: a push can land while the plan that produced
// a comment was still running, so the comment posts for a head the PR has
// already moved past. Its sweep must not retire anything — retiring cross-head
// priors anchored to the stale head could take down the current head's live
// comment with nothing replacing it. The current head's own plan outcome
// sweeps the slot instead, including the stale comment.
func TestPlanCommentPostForStaleHeadRetiresNothing(t *testing.T) {
	const repo = "org/plan-retire-stale-post"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "sha1", 9001, "IC_prior_sha1")
	fake.setCurrentHead("sha2")

	h.postTrackedPlanComment(repo, 42, 12345, planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "shaOld",
	}, "plan at shaOld, posted after the branch moved to sha2")

	assert.Empty(t, fake.deletedCommentIDs(), "a stale-head post must not retire other comments")
	assert.Empty(t, fake.minimizedNodes())
	assert.ElementsMatch(t, []string{"sha1", "shaOld"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"),
		"every comment stays expanded until the current head's own sweep")
}

// TestPlanCommentSweepSkipsRowsPostedAfterAnchor covers the ordering guard on
// the post-path sweep: a row inserted after the sweep's own comment belongs to
// a concurrently posted plan comment, and only that comment's own sweep may
// decide between the two. Without the guard, two concurrent posts would each
// see the other as superseded and take both down, leaving the PR with no
// visible plan comment.
func TestPlanCommentSweepSkipsRowsPostedAfterAnchor(t *testing.T) {
	const repo = "org/plan-retire-anchor-order"
	h, st, fake := setupPlanCommentHandler(t, repo, true)

	fake.setCurrentHead("sha2")
	h.postTrackedPlanComment(repo, 42, 12345, planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "sha2",
	}, "plan at sha2")

	// A concurrent post for a newer head lands in storage after the sha2
	// comment's row but before its sweep lists the slot.
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "sha3", 9100, "IC_concurrent_sha3")

	comments, err := st.PlanComments().ListUnretiredForSlot(t.Context(), repo, 42, "orders", "mysql")
	require.NoError(t, err)
	require.Len(t, comments, 2)
	anchor := comments[0]
	if anchor.HeadSHA != "sha2" {
		anchor = comments[1]
	}
	require.Equal(t, "sha2", anchor.HeadSHA)

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.retireSupersededPlanComments(t.Context(), client, anchor)

	assert.Empty(t, fake.deletedCommentIDs(), "the concurrently posted newer comment must not be retired")
	assert.Empty(t, fake.minimizedNodes())
	assert.ElementsMatch(t, []string{"sha2", "sha3"}, unretiredHeads(t, st, repo, 42, "orders", "mysql"))
}
