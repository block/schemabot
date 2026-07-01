//go:build integration

// Aggregate check-run and check-run-rerequest webhook integration tests.

package webhook

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestE2EAggregateCheck verifies that a multi-env plan creates a single aggregate
// "SchemaBot" check run that rolls up per-database checks, and that the aggregate
// record is persisted in storage with the correct conclusion.
func TestE2EAggregateCheck(t *testing.T) {
	dbName := "webhook_aggregate_check"
	svc := setupE2EServiceMultiEnv(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	// Trigger multi-env plan
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Drain the comment
	select {
	case <-result.comments:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Collect the aggregate check run (only aggregates are created, no per-database)
	var aggCR checkRunCapture
	select {
	case aggCR = <-result.checkRuns:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for aggregate check run")
	}

	assert.Equal(t, "SchemaBot", aggCR.Name)
	assert.Equal(t, "completed", aggCR.Status)
	assert.Equal(t, "action_required", aggCR.Conclusion)

	// Verify aggregate check record persisted in storage
	ctx := t.Context()
	aggCheck, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1,
		aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggCheck, "expected aggregate check record in storage")
	assert.Equal(t, "action_required", aggCheck.Conclusion)
	assert.Equal(t, "completed", aggCheck.Status)
	assert.True(t, aggCheck.HasChanges)
	assert.Equal(t, "abc123", aggCheck.HeadSHA)
}

// TestE2EAggregateCheckStaleCleanup verifies that when a new commit removes all schema
// changes from a PR, the stale per-database checks and aggregate check are re-created
// on the new HEAD SHA with "success" conclusion. This reproduces the scenario where a
// user pushes a commit that reverts their schema change.
func TestE2EAggregateCheckStaleCleanup(t *testing.T) {
	dbName := "webhook_aggregate_stale"
	svc := setupE2EServiceMultiEnv(t, dbName)
	ctx := t.Context()

	// Seed per-database checks and aggregate as if a plan already ran on the first commit.
	for _, env := range []string{"staging", "production"} {
		check := &storage.Check{
			Repository:   "octocat/hello-world",
			PullRequest:  1,
			HeadSHA:      "oldsha111",
			Environment:  env,
			DatabaseType: "mysql",
			DatabaseName: dbName,
			CheckRunID:   100,
			HasChanges:   true,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionActionRequired,
		}
		require.NoError(t, svc.Storage().Checks().Upsert(ctx, check))
	}
	aggCheck := &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   200,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, aggCheck))

	// Set up fake GitHub server that returns NO changed files (simulating revert commit)
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR info with new HEAD SHA
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("newsha222"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// Empty PR files — the revert commit means no schema files changed
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{})
	})

	// Capture check run creates and updates on the new SHA
	checkRuns := make(chan checkRunCapture, 10)
	stalePlanChecksCleaned := func() bool {
		for _, env := range []string{"staging", "production"} {
			check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, env, "mysql", dbName)
			if err != nil || check == nil {
				return false
			}
			if check.HeadSHA != "newsha222" || check.Conclusion != checkConclusionSuccess || check.HasChanges {
				return false
			}
		}
		return true
	}
	var prematurePassingAggregate atomic.Bool
	var checkRunIDCounter atomic.Int64
	checkRunIDCounter.Store(300)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Name == aggregateCheckName && body.Conclusion == checkConclusionSuccess && !stalePlanChecksCleaned() {
			prematurePassingAggregate.Store(true)
		}
		checkRuns <- body
		id := checkRunIDCounter.Add(1)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Name == aggregateCheckName && body.Conclusion == checkConclusionSuccess && !stalePlanChecksCleaned() {
			prematurePassingAggregate.Store(true)
		}
		checkRuns <- body
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)

	// Send synchronize event with new HEAD SHA
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "synchronize",
		headSHA: "newsha222",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// cleanupStaleChecks marks the plan-only records as success and publishes the
	// aggregate. The passing-aggregate path should not race ahead while stale
	// action_required records still exist.
	select {
	case cr := <-checkRuns:
		assert.Equal(t, aggregateCheckName, cr.Name)
		assert.Equal(t, checkConclusionSuccess, cr.Conclusion)
		assert.False(t, prematurePassingAggregate.Load(), "passing aggregate was published before stale per-database records were cleaned")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for aggregate check run")
	}

	// Poll for per-database storage records to be updated by cleanupStaleChecks.
	for _, env := range []string{"staging", "production"} {
		deadline := time.After(5 * time.Second)
		for {
			check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, env, "mysql", dbName)
			if err == nil && check != nil && check.HeadSHA == "newsha222" {
				assert.Equal(t, checkConclusionSuccess, check.Conclusion)
				assert.False(t, check.HasChanges)
				assert.Empty(t, check.BlockingReason)
				break
			}
			select {
			case <-deadline:
				t.Fatalf("timed out waiting for %s check to update to new SHA", env)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	// Poll for aggregate storage update.
	var storedAgg *storage.Check
	deadline2 := time.After(5 * time.Second)
	for {
		storedAgg, _ = svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1,
			aggregateSentinel, aggregateSentinel, aggregateSentinel)
		if storedAgg != nil && storedAgg.HeadSHA == "newsha222" {
			break
		}
		select {
		case <-deadline2:
			t.Fatal("timed out waiting for aggregate storage update")
		case <-time.After(100 * time.Millisecond):
		}
	}
	assert.Equal(t, checkConclusionSuccess, storedAgg.Conclusion)
	assert.False(t, storedAgg.HasChanges)
	assert.False(t, prematurePassingAggregate.Load(), "passing aggregate was published before stale per-database records were cleaned")
}

// TestE2ENewHeadPlanPreservesInProgressApplyOwnership verifies the case where
// an older commit has a running apply, then a newer commit is pushed and
// auto-planned. The running apply remains authoritative for the stored check
// state: the new commit's plan result must not take ownership or overwrite it,
// the aggregate on the new commit stays in_progress (blocking merge), and the
// apply's own completion still lands its real result.
func TestE2ENewHeadPlanPreservesInProgressApplyOwnership(t *testing.T) {
	dbName := "webhook_new_head_replans"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Seed the old commit's running apply. In production this is the apply that
	// started before the author pushed a newer commit to the PR branch.
	apply := &storage.Apply{
		ApplyIdentifier: "apply-old-head",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Running,
		Engine:          "spirit",
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply, err = svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// Seed old check state owned by that apply. ApplyID makes terminal updates
	// conditional, so the apply can only complete the check state it owns.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   100,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	}))
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   200,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	}))

	// Fake GitHub now serves the newer PR commit and schema files. Auto-plan
	// produces a plan result for the new commit, but the running apply keeps
	// ownership of the stored check state.
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "synchronize",
		headSHA: "abc123",
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// The aggregate is re-created on the new commit and stays in_progress
	// because the apply-owned per-database row still blocks it.
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, aggregateCheckName, cr.Name)
		assert.Equal(t, "abc123", cr.HeadSHA)
		assert.Equal(t, checkStatusInProgress, cr.Status)
		assert.Empty(t, cr.Conclusion)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for new-head aggregate check run")
	}

	// The running apply keeps ownership: the new commit's plan result must not
	// overwrite the in-progress check state or clear the apply ID.
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, "oldsha111", check.HeadSHA)
	assert.Equal(t, checkStatusInProgress, check.Status)
	assert.Empty(t, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)

	// The apply still owns the check state, so its completion lands the real
	// terminal result instead of missing ownership.
	apply.State = state.Apply.Completed
	updated, err := h.updateCheckRecordForApplyResult(ctx, "octocat/hello-world", 1, apply)
	require.NoError(t, err)
	assert.True(t, updated, "apply completion should update the check state it owns")

	check, err = svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)
}

func TestE2EAggregateCheckStaleCleanupBlocksStartedApply(t *testing.T) {
	dbName := "webhook_aggregate_stale_apply"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	apply := &storage.Apply{
		ApplyIdentifier: "apply-reverted-commit",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Running,
		Engine:          "spirit",
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply, err = svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   100,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	}))

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   200,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("newsha222"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{})
	})

	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 300})
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 300})
	})

	h := newE2EHandler(t, svc, client)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "synchronize",
		headSHA: "newsha222",
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case cr := <-checkRuns:
		assert.Equal(t, aggregateCheckName, cr.Name)
		assert.Equal(t, "newsha222", cr.HeadSHA)
		assert.Equal(t, checkStatusInProgress, cr.Status)
		assert.Empty(t, cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for in-progress aggregate check run")
	}

	installClient := ghclient.NewInstallationClient(client, h.logger)
	h.postPassingAggregates(ctx, installClient, "octocat/hello-world", 1, "newsha222")
	select {
	case cr := <-checkRuns:
		require.NotEqual(t, checkConclusionSuccess, cr.Conclusion, "passing aggregate must not be published while a started apply blocks the PR")
	case <-time.After(250 * time.Millisecond):
	}

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, "newsha222", check.HeadSHA)
	assert.Equal(t, checkStatusInProgress, check.Status)
	assert.Empty(t, check.Conclusion)
	assert.True(t, check.HasChanges)
	assert.Equal(t, applyID, check.ApplyID)
	assert.Equal(t, schemaRemovedAfterApplyBlock.blockingReason, check.BlockingReason)
	assert.Equal(t, schemaRemovedAfterApplyBlock.message, check.ErrorMessage)

	apply.State = state.Apply.Completed
	updated, err := h.updateCheckRecordForApplyResult(ctx, "octocat/hello-world", 1, apply)
	require.NoError(t, err)
	assert.True(t, updated, "old apply completion should finish the owning row without marking it successful")

	check, err = svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.Equal(t, schemaRemovedAfterApplyBlock.blockingReason, check.BlockingReason)
	assert.Equal(t, schemaRemovedAfterApplyBlock.message, check.ErrorMessage)

	h.updateAggregateCheck(ctx, installClient, "octocat/hello-world", 1, "newsha222")
	select {
	case cr := <-checkRuns:
		assert.Equal(t, aggregateCheckName, cr.Name)
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionActionRequired, cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for terminal blocking aggregate check run")
	}
}

// TestE2EPassingAggregateOnNonSchemaPR verifies that when a PR doesn't touch schema
// files and allowed_environments is configured, passing aggregate checks are posted
// so branch protection isn't blocked.
func TestE2EPassingAggregateOnNonSchemaPR(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging", "production"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — no schema files
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
		})
	})

	// Git tree — no schemabot.yaml
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("abc123"),
			Entries:   []*gh.TreeEntry{},
			Truncated: new(false),
		})
	})

	// Capture check runs
	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no schema files in PR")

	// Wait for both passing aggregates (staging + production)
	seen := map[string]bool{}
	for i := range 2 {
		select {
		case cr := <-checkRuns:
			seen[cr.Name] = true
			assert.Equal(t, "completed", cr.Status)
			assert.Equal(t, "success", cr.Conclusion)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for passing aggregate check run %d/2, seen: %v", i+1, seen)
		}
	}
	assert.True(t, seen["SchemaBot (staging)"], "expected SchemaBot (staging) check")
	assert.True(t, seen["SchemaBot (production)"], "expected SchemaBot (production) check")
}

// An aggregate participant does not own the required check on a repo — the
// leader does. On a PR that touches none of its schema, a participant posts no
// check run at all (rather than a passing "No managed schema changes" aggregate
// that would add a per-tenant row near the merge button).
func TestE2EParticipantSilentOnNonSchemaPR(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{
		AllowedEnvironments: []string{"staging", "production"},
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		},
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — no schema files.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
		})
	})

	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{SHA: new("abc123"), Entries: []*gh.TreeEntry{}, Truncated: new(false)})
	})

	// Any check-run POST here is a failure — a participant must stay silent.
	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "aggregate participant, staying silent")

	// The skip is synchronous (it returns before scheduling any passing
	// aggregate), so a brief drain confirms no check run was posted.
	select {
	case cr := <-checkRuns:
		t.Fatalf("participant posted a check run on a non-schema PR: %q", cr.Name)
	case <-time.After(2 * time.Second):
	}
}

// TestE2ECheckRunRerequestReplansCurrentPR verifies that rerunning a SchemaBot
// Check Run from GitHub reuses auto-plan discovery and republishes aggregate
// check state for the current PR head.
func TestE2ECheckRunRerequestReplansCurrentPR(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("abc123"),
			Entries:   []*gh.TreeEntry{},
			Truncated: new(false),
		})
	})

	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)
	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{
		checkName: "SchemaBot (staging)",
		headSHA:   "abc123",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no schema files in PR")

	select {
	case cr := <-checkRuns:
		assert.Equal(t, "SchemaBot (staging)", cr.Name)
		assert.Equal(t, "abc123", cr.HeadSHA)
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionSuccess, cr.Conclusion)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for rerun aggregate check")
	}
}

// TestE2ECheckRunRerequestIgnoresStaleHeadSHA verifies that rerunning an old
// Check Run cannot publish check state for a commit that is no longer the PR
// head.
func TestE2ECheckRunRerequestIgnoresStaleHeadSHA(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	var fileListCalls atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("newsha222")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		fileListCalls.Add(1)
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
		})
	})

	checkRuns := make(chan checkRunCapture, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)
	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{
		checkName: "SchemaBot (staging)",
		headSHA:   "oldsha111",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "stale head SHA")
	assert.Equal(t, int64(0), fileListCalls.Load(), "stale check reruns should stop before config discovery")

	select {
	case cr := <-checkRuns:
		t.Fatalf("stale check rerun should not publish a check run, got: %+v", cr)
	case <-time.After(250 * time.Millisecond):
	}
}

// TestE2EPassingAggregateSynchronizeUpdatesNewSHA verifies that when a non-schema PR
// receives a synchronize event (force push / new commit), the passing aggregate check
// is recreated on the new HEAD SHA — not left stale on the old commit.
func TestE2EPassingAggregateSynchronizeUpdatesNewSHA(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	var currentHead atomic.Value
	currentHead.Store("sha1aaa")

	// PR info. This endpoint represents the current head SHA for the PR, so
	// update it before sending each webhook event.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		headSHA := currentHead.Load().(string)
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: &headSHA},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — no schema files
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
		})
	})

	// Git tree — no schemabot.yaml
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/sha1aaa", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA: new("sha1aaa"), Entries: []*gh.TreeEntry{}, Truncated: new(false),
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/sha2bbb", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA: new("sha2bbb"), Entries: []*gh.TreeEntry{}, Truncated: new(false),
		})
	})

	// Capture check runs with HEAD SHA
	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// Step 1: PR opened with sha1
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "sha1aaa",
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case cr := <-checkRuns:
		assert.Equal(t, "sha1aaa", cr.HeadSHA, "first aggregate should be on the opened SHA")
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run on opened event")
	}

	// Step 2: synchronize with sha2 (force push)
	currentHead.Store("sha2bbb")
	req = buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "synchronize",
		headSHA: "sha2bbb",
	}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case cr := <-checkRuns:
		assert.Equal(t, "sha2bbb", cr.HeadSHA, "aggregate must be recreated on the new SHA after synchronize")
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out — aggregate was not recreated on new SHA after synchronize")
	}
}

// TestE2EAggregateUpdateSkipsStaleHeadSHA verifies that aggregate updates are
// gated by the current PR commit SHA. A stale driver must not publish a check
// run for an older commit after the PR branch has moved.
func TestE2EAggregateUpdateSkipsStaleHeadSHA(t *testing.T) {
	dbName := "webhook_stale_sha_guard"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Seed per-database check state from an older commit. The aggregate update
	// below will try to use this old SHA.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// GitHub reports that the PR is now on a newer commit.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("newsha222"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	checkRuns := make(chan checkRunCapture, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClient(client, h.logger)
	h.updateAggregateCheck(ctx, installClient, "octocat/hello-world", 1, "oldsha111")

	// The old-SHA aggregate update should be skipped entirely: no GitHub check
	// run and no stored aggregate row.
	select {
	case cr := <-checkRuns:
		t.Fatalf("stale aggregate update should not publish a check run, got: %+v", cr)
	case <-time.After(250 * time.Millisecond):
	}

	aggregate, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	assert.Nil(t, aggregate)
}

// TestE2EDisabledRepoChecksSkipAggregatePublishing verifies that a server-side
// repository safety hatch suppresses GitHub Check Runs while preserving stored
// per-database check state for SchemaBot's own safety decisions.
func TestE2EDisabledRepoChecksSkipAggregatePublishing(t *testing.T) {
	dbName := "webhook_disabled_repo_checks"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	enableChecks := false
	svc.Config().Repos = map[string]api.RepoConfig{
		"octocat/hello-world": {EnableChecks: &enableChecks},
	}

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	var githubCalls atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		githubCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		githubCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")
	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClient(client, h.logger)

	h.updateAggregateCheck(ctx, installClient, "octocat/hello-world", 1, "abc123")
	h.postPassingAggregates(ctx, installClient, "octocat/hello-world", 1, "abc123")
	h.postFailingAggregates(ctx, installClient, "octocat/hello-world", 1, "abc123", map[string]string{
		"staging": "Plan failed",
	})

	assert.Equal(t, int64(0), githubCalls.Load(), "disabled check publishing should not call GitHub")

	storedCheck, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, storedCheck, "per-database check state should remain available")
	assert.Equal(t, checkConclusionActionRequired, storedCheck.Conclusion)

	aggregate, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	assert.Nil(t, aggregate, "disabled check publishing should not store aggregate check state")
}

// TestE2EPassingAggregateRequiresGitHubHeadVerification verifies that passing
// aggregate paths still verify the current PR commit before publishing a check.
func TestE2EPassingAggregateRequiresGitHubHeadVerification(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})
	ctx := t.Context()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// The helper cannot verify the PR head because GitHub is unavailable.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	checkRuns := make(chan checkRunCapture, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClient(client, h.logger)
	h.postPassingAggregates(ctx, installClient, "octocat/hello-world", 1, "abc123")

	// Without head verification, SchemaBot must not publish or store a passing
	// aggregate check that could incorrectly unblock branch protection.
	select {
	case cr := <-checkRuns:
		t.Fatalf("passing aggregate should not publish without current head verification, got: %+v", cr)
	case <-time.After(250 * time.Millisecond):
	}

	aggregate, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	assert.Nil(t, aggregate)
}

// TestE2EPassingAggregateOnSQLWithoutSchemabotYAML verifies that when a PR touches
// .sql files but the directory has no schemabot.yaml (not onboarded), passing
// aggregate checks are still posted.
func TestE2EPassingAggregateOnSQLWithoutSchemabotYAML(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — .sql file but in a directory without schemabot.yaml
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("legacy-service/schema/users.sql"), Status: new("modified")},
		})
	})

	// Git tree — has the .sql file but no schemabot.yaml anywhere
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA: new("abc123"),
			Entries: []*gh.TreeEntry{
				{
					Path: new("legacy-service/schema/users.sql"),
					Mode: new("100644"),
					Type: new("blob"),
					SHA:  new("blobsha001"),
					Size: new(100),
				},
			},
			Truncated: new(false),
		})
	})

	// Capture check runs
	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no schema files in PR")

	// Should post passing aggregate even though .sql files changed
	select {
	case cr := <-checkRuns:
		assert.Equal(t, "SchemaBot (staging)", cr.Name)
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for passing aggregate check run")
	}
}

// TestE2EPassingAggregateWithoutAllowedEnvs verifies that when allowed_environments
// is not configured (single instance mode), a global "SchemaBot" passing aggregate
// is posted for non-schema PRs.
func TestE2EPassingAggregateWithoutAllowedEnvs(t *testing.T) {
	dbName := "webhook_no_aggregate"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — no schema files
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
		})
	})

	// Git tree — empty
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("abc123"),
			Entries:   []*gh.TreeEntry{},
			Truncated: new(false),
		})
	})

	// Capture check runs
	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no schema files in PR")

	// Single-instance mode posts a global "SchemaBot" passing aggregate
	select {
	case cr := <-checkRuns:
		assert.Equal(t, "SchemaBot", cr.Name)
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for passing aggregate check run")
	}
}

// TestE2EFailingAggregateOnPlanError verifies that when a plan fails for all
// environments (e.g., database not configured), a failing aggregate check is
// posted so branch protection shows a clear failure instead of waiting forever.
func TestE2EFailingAggregateOnPlanError(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// schemabot.yaml references a database not configured on the server
	schemabotConfig := "database: unconfigured-db\ntype: mysql\n"
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, "unconfigured-db")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Should get a comment with the error
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Error")
		assert.Contains(t, body, "unconfigured-db")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for error comment")
	}

	// Should also get a failing aggregate check run
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, "SchemaBot (staging)", cr.Name)
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "failure", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for failing aggregate check run")
	}
}
