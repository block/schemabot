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
// GraphQL node id) and minimizeComment mutations, with per-node failure
// injection for the retry scenarios. It also serves the PR fetch with a
// configurable current head so sweeps can verify head freshness.
type planCommentFakeGitHub struct {
	mu        sync.Mutex
	nextID    int64
	created   []int64
	minimized []string
	failNodes map[string]bool
	headSHA   string
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

func (f *planCommentFakeGitHub) setMinimizeFails(nodeID string, fails bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNodes == nil {
		f.failNodes = map[string]bool{}
	}
	f.failNodes[nodeID] = fails
}

func (f *planCommentFakeGitHub) minimizedNodes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.minimized...)
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

// setupFakeGitHubForPlanComments serves the comment-create REST endpoint and
// the GraphQL minimizeComment mutation against any repo/PR.
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
func setupPlanCommentHandler(t *testing.T, repo string) (*Handler, storage.Storage, *planCommentFakeGitHub) {
	t.Helper()
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
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

	svc := api.New(st, &api.ServerConfig{}, map[string]tern.Client{}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	return NewHandler(svc, factory, nil, logger), st, fake
}

func unminimizedHeads(t *testing.T, st storage.Storage, repo string, pr int, database, databaseType string) []string {
	t.Helper()
	comments, err := st.PlanComments().ListUnminimizedForSlot(t.Context(), repo, pr, database, databaseType)
	require.NoError(t, err)
	heads := make([]string, len(comments))
	for i, c := range comments {
		heads[i] = c.HeadSHA
	}
	return heads
}

// TestPlanCommentSupersedeMinimizesPriorComments exercises the noise-reduction
// UX on a frequently updated PR: each newly posted plan comment collapses the
// prior plan comments it supersedes (older head, or a refresh of the same
// head and environment scope), while a same-head comment for different
// environments and comments for other databases stay expanded. Minimized
// comments stay expandable on GitHub — the record is hidden, never lost.
func TestPlanCommentSupersedeMinimizesPriorComments(t *testing.T) {
	const repo = "org/plan-min-lifecycle"
	h, st, fake := setupPlanCommentHandler(t, repo)

	slot := planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging", "production"},
		HeadSHA:      "sha1",
	}

	// A comment for another database on the same PR must never be touched.
	otherSlot := slot
	otherSlot.Database = "billing"
	h.postTrackedPlanComment(repo, 42, 12345, otherSlot, "billing plan")

	// First comment in the orders slot: nothing to supersede.
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at sha1")
	assert.Empty(t, fake.minimizedNodes())
	assert.Equal(t, []string{"sha1"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"))

	// A new head supersedes the sha1 comment.
	slot.HeadSHA = "sha2"
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at sha2")
	assert.Equal(t, []string{"IC_node1002"}, fake.minimizedNodes(), "the sha1 comment is minimized on GitHub")
	assert.Equal(t, []string{"sha2"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"))

	// A manual single-environment plan on the same head covers a narrower
	// scope: it does not supersede the combined comment, and the combined
	// comment does not retroactively hide it.
	stagingSlot := slot
	stagingSlot.Environments = []string{"staging"}
	h.postTrackedPlanComment(repo, 42, 12345, stagingSlot, "staging-only plan at sha2")
	assert.Len(t, fake.minimizedNodes(), 1, "different scope on the same head minimizes nothing")
	assert.Equal(t, []string{"sha2", "sha2"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"))

	// Re-running the staging-only plan on the same head refreshes it: the
	// older staging-only comment collapses, the combined comment stays.
	h.postTrackedPlanComment(repo, 42, 12345, stagingSlot, "staging-only plan at sha2, again")
	assert.Equal(t, []string{"IC_node1002", "IC_node1004"}, fake.minimizedNodes())
	comments, err := st.PlanComments().ListUnminimizedForSlot(t.Context(), repo, 42, "orders", "mysql")
	require.NoError(t, err)
	require.Len(t, comments, 2)
	assert.Equal(t, "production,staging", comments[0].EnvironmentScope)
	assert.Equal(t, "staging", comments[1].EnvironmentScope)

	// The billing slot was never touched.
	assert.Equal(t, []string{"sha1"}, unminimizedHeads(t, st, repo, 42, "billing", "mysql"))
	assert.Equal(t, 5, fake.createCount(), "every post reached GitHub exactly once")
}

// TestPlanCommentWithoutSlotIdentityPostsUntracked covers the error-only
// comment a plan posts when every environment failed before a database or
// head resolved: with no slot identity to key tracking, the comment still
// posts (visibility first) but is not tracked, so no empty-identity row can
// make error-only comments for different databases supersede each other.
func TestPlanCommentWithoutSlotIdentityPostsUntracked(t *testing.T) {
	const repo = "org/plan-min-untracked"
	h, st, fake := setupPlanCommentHandler(t, repo)

	h.postTrackedPlanComment(repo, 42, 12345, planCommentSlot{}, "plan failed in every environment")

	assert.Equal(t, 1, fake.createCount(), "the error-only comment still posts")
	assert.Empty(t, fake.minimizedNodes())
	comments, err := st.PlanComments().ListUnminimizedForSlot(t.Context(), repo, 42, "", "")
	require.NoError(t, err)
	assert.Empty(t, comments, "no row is tracked under an empty slot identity")
}

// TestPlanCommentApplyOwnedHeadStaysExpanded covers the safety hold: once an
// apply exists for the head a plan comment was rendered at, that comment is
// the operational record of what ran and a newer plan comment must not hide
// it — the PR keeps showing the plan that produced the apply.
func TestPlanCommentApplyOwnedHeadStaysExpanded(t *testing.T) {
	const repo = "org/plan-min-apply-owned"
	h, st, fake := setupPlanCommentHandler(t, repo)
	ctx := t.Context()

	slot := planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "shaA",
	}
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaA")

	// The shaA plan becomes an apply before the next push.
	planID, err := st.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_owned_shaA",
		Database:       "orders",
		DatabaseType:   "mysql",
		Repository:     repo,
		PullRequest:    42,
		Environment:    "staging",
		HeadSHA:        "shaA",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)
	lock := &storage.Lock{
		DatabaseName: "orders",
		DatabaseType: "mysql",
		Repository:   repo,
		PullRequest:  42,
		Owner:        fmt.Sprintf("%s#42", repo),
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, "orders", "mysql")
	require.NoError(t, err)
	_, err = st.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_owned_shaA",
		LockID:          lock.ID,
		PlanID:          planID,
		Database:        "orders",
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     42,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Running,
	})
	require.NoError(t, err)

	slot.HeadSHA = "shaB"
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaB")

	assert.Empty(t, fake.minimizedNodes(), "the apply-owned shaA comment must stay expanded")
	assert.Equal(t, []string{"shaA", "shaB"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"))
}

// TestPlanCommentMinimizeFailureRetriesOnNextSupersede covers the retry
// semantics: a failed GitHub minimize call leaves the row unminimized, so the
// next plan comment in the slot picks the comment up again. The failure mode
// is only extra noise on the PR — a comment is never recorded as hidden
// unless GitHub confirmed it.
func TestPlanCommentMinimizeFailureRetriesOnNextSupersede(t *testing.T) {
	const repo = "org/plan-min-retry"
	h, st, fake := setupPlanCommentHandler(t, repo)

	slot := planCommentSlot{
		Database:     "orders",
		DatabaseType: "mysql",
		Environments: []string{"staging"},
		HeadSHA:      "shaA",
	}
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaA")
	fake.setMinimizeFails("IC_node1001", true)

	slot.HeadSHA = "shaB"
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaB")
	assert.Empty(t, fake.minimizedNodes(), "the injected failure leaves the shaA comment expanded")
	assert.Equal(t, []string{"shaA", "shaB"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"),
		"a failed minimize must not be recorded as minimized")

	// GitHub recovers; the next supersede retires both stale comments.
	fake.setMinimizeFails("IC_node1001", false)
	slot.HeadSHA = "shaC"
	h.postTrackedPlanComment(repo, 42, 12345, slot, "plan at shaC")
	assert.ElementsMatch(t, []string{"IC_node1001", "IC_node1002"}, fake.minimizedNodes())
	assert.Equal(t, []string{"shaC"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"))
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

// TestStalePlanCommentsMinimizedWithoutNewComment exercises the up-to-date-PR
// UX: when the current head's plan outcome supersedes prior comments without
// posting a new comment — an auto-plan that resolves to no changes — plan
// comments rendered at older heads collapse, since the pending DDL and apply
// prompt they advertise no longer match the branch. A comment already
// rendered at the current head stays expanded (it may be the only visible
// plan for its environment scope), other databases' slots are untouched, and
// the sweep never posts a comment of its own.
func TestStalePlanCommentsMinimizedWithoutNewComment(t *testing.T) {
	const repo = "org/plan-min-stale-sweep"
	h, st, fake := setupPlanCommentHandler(t, repo)

	insertPlanCommentRow(t, st, repo, 42, "orders", "production,staging", "shaA", 9001, "IC_stale_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaB", 9002, "IC_current_shaB")
	insertPlanCommentRow(t, st, repo, 42, "billing", "production,staging", "shaA", 9003, "IC_billing_shaA")
	fake.setCurrentHead("shaB")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.minimizeStalePlanComments(t.Context(), client, repo, 42, "orders", "mysql", "shaB")

	assert.Equal(t, []string{"IC_stale_shaA"}, fake.minimizedNodes(),
		"only the prior-head orders comment is minimized")
	assert.Equal(t, []string{"shaB"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"),
		"the current-head comment stays expanded and the stale row is recorded minimized")
	assert.Equal(t, []string{"shaA"}, unminimizedHeads(t, st, repo, 42, "billing", "mysql"),
		"another database's slot is untouched")
	assert.Equal(t, 0, fake.createCount(), "the sweep posts no comment")
}

// TestStalePlanCommentSweepSkipsWhenHeadMoved covers the concurrent-push
// safety of the no-new-comment sweep: a plan outcome computed for an older
// head must never hide the current head's live plan comment, because nothing
// would replace it and minimized comments are never automatically restored.
// When the PR head has moved past the sweep's head, the sweep leaves every
// comment expanded — the current head's own plan outcome supersedes the slot.
func TestStalePlanCommentSweepSkipsWhenHeadMoved(t *testing.T) {
	const repo = "org/plan-min-stale-head-moved"
	h, st, fake := setupPlanCommentHandler(t, repo)

	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaA", 9001, "IC_old_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaC", 9002, "IC_live_shaC")
	fake.setCurrentHead("shaC")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.minimizeStalePlanComments(t.Context(), client, repo, 42, "orders", "mysql", "shaB")

	assert.Empty(t, fake.minimizedNodes(),
		"a sweep for a superseded head must not touch the slot")
	assert.ElementsMatch(t, []string{"shaA", "shaC"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"),
		"every comment stays expanded until the current head's own sweep")
}

// TestStalePlanCommentSweepKeepsApplyOwnedHeadExpanded covers the safety hold
// on the no-new-comment sweep: once an apply exists for the head a plan
// comment was rendered at, that comment is the operational record of what ran.
// A later head resolving to no changes — for example after the applied change
// lands and the schema converges — must not hide it.
func TestStalePlanCommentSweepKeepsApplyOwnedHeadExpanded(t *testing.T) {
	const repo = "org/plan-min-stale-apply-owned"
	h, st, fake := setupPlanCommentHandler(t, repo)
	ctx := t.Context()

	insertPlanCommentRow(t, st, repo, 42, "inventory", "staging", "shaA", 9001, "IC_owned_shaA")
	// A second stale comment with no apply proves the sweep ran and the
	// shaA comment survived the hold specifically, not a sweep-wide skip.
	insertPlanCommentRow(t, st, repo, 42, "inventory", "staging", "shaZ", 9002, "IC_unowned_shaZ")
	fake.setCurrentHead("shaB")

	// The shaA plan becomes an apply before the next push.
	planID, err := st.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_stale_owned_shaA",
		Database:       "inventory",
		DatabaseType:   "mysql",
		Repository:     repo,
		PullRequest:    42,
		Environment:    "staging",
		HeadSHA:        "shaA",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)
	lock := &storage.Lock{
		DatabaseName: "inventory",
		DatabaseType: "mysql",
		Repository:   repo,
		PullRequest:  42,
		Owner:        fmt.Sprintf("%s#42", repo),
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, "inventory", "mysql")
	require.NoError(t, err)
	_, err = st.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_stale_owned_shaA",
		LockID:          lock.ID,
		PlanID:          planID,
		Database:        "inventory",
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     42,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Running,
	})
	require.NoError(t, err)

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.minimizeStalePlanComments(t.Context(), client, repo, 42, "inventory", "mysql", "shaB")

	assert.Equal(t, []string{"IC_unowned_shaZ"}, fake.minimizedNodes(),
		"the sweep minimizes the unowned stale comment but the apply-owned shaA comment stays expanded")
	assert.Equal(t, []string{"shaA"}, unminimizedHeads(t, st, repo, 42, "inventory", "mysql"))
}

// TestPRWideStalePlanCommentSweep covers the PR whose new head resolves no
// schema config at all — the author dropped the schema change, or the commit
// only ever touched unrelated files. There is no database left to key a slot
// with, so the slot sweep cannot run, yet earlier heads' plan comments are
// exactly what such a push leaves behind: still advertising that head's DDL and
// its apply prompt while only the check run moves on. Every database's stale
// comment collapses, the current head's stays expanded, and no comment is
// posted.
func TestPRWideStalePlanCommentSweep(t *testing.T) {
	const repo = "org/plan-min-pr-wide-sweep"
	h, st, fake := setupPlanCommentHandler(t, repo)

	insertPlanCommentRow(t, st, repo, 42, "orders", "production,staging", "shaA", 9001, "IC_orders_shaA")
	insertPlanCommentRow(t, st, repo, 42, "billing", "staging", "shaA", 9002, "IC_billing_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaB", 9003, "IC_orders_shaB")
	// A different PR in the same repo proves the sweep is scoped to one PR.
	insertPlanCommentRow(t, st, repo, 77, "orders", "staging", "shaA", 9004, "IC_other_pr_shaA")
	fake.setCurrentHead("shaB")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.minimizeStalePlanCommentsForPR(t.Context(), client, repo, 42, "shaB")

	assert.ElementsMatch(t, []string{"IC_orders_shaA", "IC_billing_shaA"}, fake.minimizedNodes(),
		"every database's prior-head comment collapses, not just one slot's")
	assert.Equal(t, []string{"shaB"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"),
		"the current-head comment stays expanded")
	assert.Empty(t, unminimizedHeads(t, st, repo, 42, "billing", "mysql"))
	assert.Equal(t, []string{"shaA"}, unminimizedHeads(t, st, repo, 77, "orders", "mysql"),
		"another PR's comments are untouched")
	assert.Equal(t, 0, fake.createCount(), "the sweep posts no comment")
}

// TestPRWideStalePlanCommentSweepSkipsWhenHeadMoved covers the same
// concurrent-push safety as the slot sweep, for the sweep that has no database
// to key on: a delivery for a head the branch has already moved past must leave
// every comment expanded, because nothing would replace what it hid.
func TestPRWideStalePlanCommentSweepSkipsWhenHeadMoved(t *testing.T) {
	const repo = "org/plan-min-pr-wide-head-moved"
	h, st, fake := setupPlanCommentHandler(t, repo)

	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaA", 9001, "IC_old_shaA")
	insertPlanCommentRow(t, st, repo, 42, "orders", "staging", "shaC", 9002, "IC_live_shaC")
	fake.setCurrentHead("shaC")

	client, err := h.clientForRepo(repo, 12345)
	require.NoError(t, err)
	h.minimizeStalePlanCommentsForPR(t.Context(), client, repo, 42, "shaB")

	assert.Empty(t, fake.minimizedNodes(),
		"a sweep for a superseded head must not touch the PR")
	assert.ElementsMatch(t, []string{"shaA", "shaC"}, unminimizedHeads(t, st, repo, 42, "orders", "mysql"))
}
