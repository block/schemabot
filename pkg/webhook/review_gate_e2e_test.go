//go:build integration

package webhook

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// setupFakeGitHubForReviewGate extends the standard plan flow with review gate
// support: CODEOWNERS content, PR reviews endpoint, and a settings store toggle.
func setupFakeGitHubForReviewGate(
	t *testing.T,
	mux *http.ServeMux,
	schemaFiles map[string]string,
	schemabotConfig string,
	ns string,
	codeownersContent string, // empty = no CODEOWNERS file
	reviews []*gh.PullRequestReview,
) *planFlowResult {
	t.Helper()

	result := &planFlowResult{
		comments:  make(chan string, 10),
		reactions: make(chan string, 10),
		checkRuns: make(chan checkRunCapture, 10),
	}

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR reviews
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reviews)
	})

	// PR changed files
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		files := []*gh.CommitFile{
			{Filename: new("schema/" + ns + "/users.sql"), Status: new("added")},
		}
		_ = json.NewEncoder(w).Encode(files)
	})

	// Build tree entries
	var treeEntries []*gh.TreeEntry
	blobIndex := 0
	blobContents := make(map[string]string)

	if schemabotConfig != "" {
		configSHA := "configsha001"
		blobContents[configSHA] = schemabotConfig
		treeEntries = append(treeEntries, &gh.TreeEntry{
			Path: new("schema/schemabot.yaml"),
			Mode: new("100644"),
			Type: new("blob"),
			SHA:  new(configSHA),
			Size: new(len(schemabotConfig)),
		})
	}

	for name, content := range schemaFiles {
		sha := fmt.Sprintf("blobsha%03d", blobIndex)
		blobIndex++
		blobContents[sha] = content
		treeEntries = append(treeEntries, &gh.TreeEntry{
			Path: new("schema/" + ns + "/" + name),
			Mode: new("100644"),
			Type: new("blob"),
			SHA:  new(sha),
			Size: new(len(content)),
		})
	}

	// Git tree
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("abc123"),
			Entries:   treeEntries,
			Truncated: new(false),
		})
	})

	// Blob content
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/", func(w http.ResponseWriter, r *http.Request) {
		sha := r.URL.Path[len("/repos/octocat/hello-world/git/blobs/"):]
		c, ok := blobContents[sha] //nolint:staticcheck // c is used via new() generic helper
		if !ok {
			http.NotFound(w, r)
			return
		}
		resp := gh.Blob{
			Encoding: new("base64"),
		}
		resp.SHA = new(sha)
		resp.Content = new(base64.StdEncoding.EncodeToString([]byte(c)))
		resp.Size = new(len(c))
		_ = json.NewEncoder(w).Encode(resp)
	})

	// Contents API — schemabot.yaml + CODEOWNERS
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Path[len("/repos/octocat/hello-world/contents/"):]

		if filePath == "schema/schemabot.yaml" && schemabotConfig != "" {
			content := gh.RepositoryContent{
				Encoding: new("base64"),
			}
			content.Name = new("schemabot.yaml")
			content.Path = new("schema/schemabot.yaml")
			content.Content = new(base64.StdEncoding.EncodeToString([]byte(schemabotConfig)))
			_ = json.NewEncoder(w).Encode(content)
			return
		}

		if filePath == ".github/CODEOWNERS" && codeownersContent != "" {
			content := gh.RepositoryContent{
				Encoding: new("base64"),
			}
			content.Name = new("CODEOWNERS")
			content.Path = new(".github/CODEOWNERS")
			content.Content = new(base64.StdEncoding.EncodeToString([]byte(codeownersContent)))
			_ = json.NewEncoder(w).Encode(content)
			return
		}

		http.NotFound(w, r)
	})

	// Capture comments
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	// Capture reactions
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	// Capture check runs
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	return result
}

// TestE2EApplyBlockedByReviewGate verifies that `schemabot apply` posts a
// "Review Required" comment when require_review is enabled and no CODEOWNERS
// approval exists.
func TestE2EApplyBlockedByReviewGate(t *testing.T) {
	dbName := "webhook_review_gate_blocked"
	svc := setupE2EService(t, dbName)

	// Enable review gate
	require.NoError(t, svc.Storage().Settings().Set(t.Context(), "require_review", "true"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// No reviews, CODEOWNERS requires @dba-team
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"* @dba-team\n",
		nil, // no reviews
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Verify "Review Required" comment was posted (not a plan)
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Review Required")
		assert.Contains(t, body, "@dba-team")
		assert.Contains(t, body, "approval from a code owner")
		assert.NotContains(t, body, "Schema Change Plan")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for review gate comment")
	}
}

// TestE2EApplyProceedsWithApproval verifies that `schemabot apply` proceeds
// normally when require_review is enabled and a CODEOWNERS member has approved.
func TestE2EApplyProceedsWithApproval(t *testing.T) {
	dbName := "webhook_review_gate_approved"
	svc := setupE2EService(t, dbName)

	// Enable review gate
	require.NoError(t, svc.Storage().Settings().Set(t.Context(), "require_review", "true"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// bob is a CODEOWNER and has approved
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"* @bob\n",
		[]*gh.PullRequestReview{
			{
				User:        &gh.User{Login: new("bob")},
				State:       new(ghclient.ReviewApproved),
				SubmittedAt: &gh.Timestamp{Time: time.Now()},
			},
		},
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should get a plan comment (not blocked), confirming the gate passed
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.Contains(t, body, "CREATE TABLE")
		assert.NotContains(t, body, "Review Required")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for plan comment")
	}
}

// TestE2EApplyBlockedBySelfApproval verifies that the PR author's own approval
// does not satisfy the review gate when they are also a CODEOWNER.
func TestE2EApplyBlockedBySelfApproval(t *testing.T) {
	dbName := "webhook_review_gate_self"
	svc := setupE2EService(t, dbName)

	require.NoError(t, svc.Storage().Settings().Set(t.Context(), "require_review", "true"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// testuser is both PR author and CODEOWNER, and approved their own PR
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"* @testuser @bob\n",
		[]*gh.PullRequestReview{
			{
				User:        &gh.User{Login: new("testuser")},
				State:       new(ghclient.ReviewApproved),
				SubmittedAt: &gh.Timestamp{Time: time.Now()},
			},
		},
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Review Required")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for review gate comment")
	}
}

// TestE2EApplyNoCodeownersFile verifies that when no CODEOWNERS file exists
// and no reviews are present, the review gate blocks apply.
func TestE2EApplyNoCodeownersFile_Blocked(t *testing.T) {
	dbName := "webhook_review_gate_no_co"
	svc := setupE2EService(t, dbName)

	require.NoError(t, svc.Storage().Settings().Set(t.Context(), "require_review", "true"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// No CODEOWNERS file, no reviews
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"", // no CODEOWNERS
		nil,
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Review Required")
		assert.Contains(t, body, "at least one approval")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for review gate comment")
	}
}

// TestE2EApplyNoCodeownersFile_Approved verifies that when no CODEOWNERS file
// exists but someone (not the author) has approved, the gate passes.
func TestE2EApplyNoCodeownersFile_Approved(t *testing.T) {
	dbName := "webhook_review_gate_noco_ok"
	svc := setupE2EService(t, dbName)

	require.NoError(t, svc.Storage().Settings().Set(t.Context(), "require_review", "true"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// No CODEOWNERS, but bob approved
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"", // no CODEOWNERS
		[]*gh.PullRequestReview{
			{
				User:        &gh.User{Login: new("bob")},
				State:       new(ghclient.ReviewApproved),
				SubmittedAt: &gh.Timestamp{Time: time.Now()},
			},
		},
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.NotContains(t, body, "Review Required")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for plan comment")
	}
}

// TestE2EApplyTeamSlugApproval verifies that when CODEOWNERS contains a team
// slug, SchemaBot expands it via the GitHub Teams API and matches the approving
// reviewer against the team membership.
func TestE2EApplyTeamSlugApproval(t *testing.T) {
	dbName := "webhook_review_gate_team"
	svc := setupE2EService(t, dbName)

	require.NoError(t, svc.Storage().Settings().Set(t.Context(), "require_review", "true"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// CODEOWNERS has only a team slug; bob (a team member) approved
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"* @octocat/dba-team\n",
		[]*gh.PullRequestReview{
			{
				User:        &gh.User{Login: new("bob")},
				State:       new(ghclient.ReviewApproved),
				SubmittedAt: &gh.Timestamp{Time: time.Now()},
			},
		},
	)

	// Team members endpoint — bob is a member
	mux.HandleFunc("GET /orgs/octocat/teams/dba-team/members", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]*gh.User{
			{Login: new("bob")},
			{Login: new("carol")},
		})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Team member approved — should proceed to plan
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.NotContains(t, body, "Review Required")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for plan comment")
	}
}

// TestE2EApplyTeamSlugBlocked verifies that when CODEOWNERS has a team slug
// and the approver is NOT a team member, the gate blocks.
func TestE2EApplyTeamSlugBlocked(t *testing.T) {
	dbName := "webhook_review_gate_team_no"
	svc := setupE2EService(t, dbName)

	require.NoError(t, svc.Storage().Settings().Set(t.Context(), "require_review", "true"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// CODEOWNERS has only a team slug; dave (NOT a team member) approved
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"* @octocat/dba-team\n",
		[]*gh.PullRequestReview{
			{
				User:        &gh.User{Login: new("dave")},
				State:       new(ghclient.ReviewApproved),
				SubmittedAt: &gh.Timestamp{Time: time.Now()},
			},
		},
	)

	// Team members — dave is not in the list
	mux.HandleFunc("GET /orgs/octocat/teams/dba-team/members", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]*gh.User{
			{Login: new("bob")},
			{Login: new("carol")},
		})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Review Required")
		assert.Contains(t, body, "@octocat/dba-team")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for review gate comment")
	}
}

// TestE2EApplyReviewGateDisabled verifies that apply proceeds normally when
// require_review is not set.
func TestE2EApplyReviewGateDisabled(t *testing.T) {
	dbName := "webhook_review_gate_off"
	svc := setupE2EService(t, dbName)

	// Explicitly ensure require_review is off (shared DB may have leftovers)
	require.NoError(t, svc.Storage().Settings().Delete(t.Context(), "require_review"))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	// Set up full flow — review gate should not even check CODEOWNERS
	result := setupFakeGitHubForReviewGate(t, mux, schemaFiles, schemabotConfig, dbName,
		"* @dba-team\n", // CODEOWNERS exists but gate is off
		nil,             // no reviews
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should proceed to plan (gate is off, so no review check)
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.NotContains(t, body, "Review Required")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for plan comment")
	}
}
