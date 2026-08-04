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

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func TestReviewGateErrorDetailTeamMembership(t *testing.T) {
	err := fmt.Errorf("expand team @octocat/schema-admins: %w", ghclient.ErrTeamMembershipUnreadable)

	detail := reviewGateErrorDetail(err)

	assert.Contains(t, detail, "Review gate check failed; see server logs for details")
	assert.Contains(t, detail, "GitHub App can read organization members")
	assert.NotContains(t, detail, "expand team @octocat/schema-admins",
		"raw error text must never render in PR markdown")
}

func TestReviewGateErrorDetailGeneric(t *testing.T) {
	detail := reviewGateErrorDetail(assert.AnError)

	assert.Contains(t, detail, "Review gate check failed; see server logs for details")
	assert.NotContains(t, detail, assert.AnError.Error(),
		"raw error text must never render in PR markdown")
	assert.NotContains(t, detail, "GitHub App can read organization members")
}

func setupReviewGateHandler(t *testing.T, config *api.ServerConfig) (*Handler, *http.ServeMux) {
	t.Helper()
	if config == nil {
		config = &api.ServerConfig{}
	}

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	svc := api.New(&emptyStorage{}, config, nil, logger)

	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)
	return h, mux
}

func registerPREndpoint(mux *http.ServeMux, prAuthor string) {
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		pr := &gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new(prAuthor)},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pr)
	})
}

func registerCodeownersEndpoint(mux *http.ServeMux, content string, found bool) {
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/.github/CODEOWNERS", func(w http.ResponseWriter, _ *http.Request) {
		if !found {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(&gh.ErrorResponse{
				Response: &http.Response{StatusCode: http.StatusNotFound},
				Message:  "Not Found",
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.RepositoryContent{
			Type:     new("file"),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte(content))),
		})
	})
	// Fallback for other CODEOWNERS locations
	if !found {
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/CODEOWNERS", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(&gh.ErrorResponse{
				Response: &http.Response{StatusCode: http.StatusNotFound},
				Message:  "Not Found",
			})
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/docs/CODEOWNERS", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(&gh.ErrorResponse{
				Response: &http.Response{StatusCode: http.StatusNotFound},
				Message:  "Not Found",
			})
		})
	}
}

func registerReviewsEndpoint(mux *http.ServeMux, reviews []*gh.PullRequestReview) {
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reviews)
	})
}

func TestCheckReviewGate_Disabled(t *testing.T) {
	h, _ := setupReviewGateHandler(t, nil)

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	assert.Nil(t, result, "gate should return nil when disabled")
}

func TestCheckReviewGate_NoReviewsBlocks(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"bob"}, result.OperatorReviewers)
	assert.Empty(t, result.OtherReviewers)
}

func TestCheckReviewGate_OperatorUserApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

func TestCheckReviewGate_AdminUserApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.AdminUsers = []string{"mona"}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("mona")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

// A repo admin's approval satisfies the review gate for any database managed
// through that repository, without granting approval authority on databases
// managed through other repositories.
func TestCheckReviewGate_RepoAdminUserApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {AdminUsers: []string{"kara"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("kara")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "kara")
}

// A repo admin team's member approval satisfies the review gate for any
// database managed through that repository, resolved via GitHub team
// membership, and the configured team appears among the required reviewers.
func TestCheckReviewGate_RepoAdminTeamApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {AdminTeams: []string{"octocat/repo-admins"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/repo-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "octocat/repo-admins")
}

// An approval from someone outside the repo admin team does not satisfy the
// review gate: team membership is resolved via GitHub, not assumed.
func TestCheckReviewGate_RepoAdminTeamNonMemberBlocked(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {AdminTeams: []string{"octocat/repo-admins"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/repo-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
}

// An approval from a user who is only a repo admin of a different repository
// does not satisfy the review gate for this repository's PRs.
func TestCheckReviewGate_RepoAdminOfOtherRepoBlocked(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.AdminUsers = []string{"mona"}
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/other-repo": {AdminUsers: []string{"kara"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("kara")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.NotContains(t, result.OperatorReviewers, "kara")
	assert.NotContains(t, result.OtherReviewers, "kara")
}

func TestCheckReviewGate_NotApproved(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, "alice", result.PRAuthor)
	assert.Contains(t, result.OperatorReviewers, "bob")
	assert.Contains(t, result.OperatorReviewers, "carol")
}

func TestCheckReviewGate_SelfApprovalBlocked(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"alice", "bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("alice")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "self-approval should be blocked")
}

func TestCheckReviewGate_DisabledByDefault(t *testing.T) {
	h, _ := setupReviewGateHandler(t, actorAuthTestConfig(false))

	enabled := h.isReviewGateEnabled("octocat/hello-world")
	assert.False(t, enabled)
}

func TestCheckReviewGate_OperatorTeamApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorTeams = []string{"octocat/db-admins"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/db-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

func TestCheckReviewGate_OperatorTeamNotApproved(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorTeams = []string{"octocat/db-admins"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/db-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
}

// The review-required comment splits reviewers by how close each principal is
// to the change: the database's own operators get their own leading section —
// they are the reviewers the author should ping — while the broader fallback
// principals (global admins, then repo admins) form the "other authorized
// reviewers" section.
func TestCheckReviewGate_OperatorsSplitFromOtherReviewers(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.AdminTeams = []string{"octocat/global-admins"}
		cfg.ReviewPolicy.AdminUsers = []string{"kara"}
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {
				AdminTeams: []string{"octocat/repo-admins"},
				AdminUsers: []string{"dave"},
			},
		}
		db := cfg.Databases["orders"]
		db.OperatorTeams = []string{"octocat/db-admins"}
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, nil)

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"octocat/db-admins", "bob"}, result.OperatorReviewers)
	assert.Equal(t, []string{
		"octocat/global-admins", "kara",
		"octocat/repo-admins", "dave",
	}, result.OtherReviewers)
}

func TestCheckReviewGate_CodeownersIgnoredByDefault(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @dave\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"bob"}, result.OperatorReviewers)
	assert.Empty(t, result.OtherReviewers)
}

func TestCheckReviewGate_CodeownersOptIn(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.IncludeCodeowners = true
		cfg.ReviewPolicy.IncludeDatabaseOperators = new(false)
	}))

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "schema/payments/ @bob\nschema/orders/ @carol\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/payments")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "bob")

	result, err = h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/orders")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "carol")
}

func TestCheckReviewGate_NoConfiguredReviewersErrors(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.IncludeDatabaseOperators = new(false)
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, nil)

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "no configured reviewers")
}

func reviewGateTestConfig(opts ...func(*api.ServerConfig)) *api.ServerConfig {
	cfg := actorAuthTestConfig(false)
	cfg.ReviewPolicy = api.ReviewPolicyConfig{Enabled: true}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// TestEnforceReviewGate pins the review gate's three-way disposition: an
// internal evaluation failure (a GitHub reviews read here) is not a merit
// block — the gate returns the error so the command stays retryable — while
// missing approval blocks on the merits without error. The evaluation-failure
// comment must stay sanitized: no raw GitHub error text in PR markdown.
func TestEnforceReviewGate(t *testing.T) {
	schemaResult := &ghclient.SchemaRequestResult{Database: "orders", SchemaPath: "schema/testdb"}

	registerCommentsCapture := func(mux *http.ServeMux) chan string {
		comments := make(chan string, 10)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		return comments
	}

	t.Run("evaluation failure returns error without blocking", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		comments := registerCommentsCapture(mux)
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Resource not accessible by integration"})
		})

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply")
		require.Error(t, err)
		assert.False(t, blocked, "evaluation failure must not report a merit block")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Review gate check failed; see server logs")
			assert.NotContains(t, body, "Resource not accessible", "raw GitHub error text must never render in PR markdown")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for evaluation-failure comment")
		}
	})

	t.Run("missing approval blocks on the merits without error", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{})
		comments := registerCommentsCapture(mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply")
		require.NoError(t, err)
		assert.True(t, blocked)

		select {
		case body := <-comments:
			assert.Contains(t, body, "Review Required")
			assert.Contains(t, body, "@bob")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for review-required comment")
		}
	})

	t.Run("approved review passes without comment", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{
			{
				User:        &gh.User{Login: new("bob")},
				State:       new(ghclient.ReviewApproved),
				SubmittedAt: &gh.Timestamp{Time: time.Now()},
			},
		})
		comments := registerCommentsCapture(mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply")
		require.NoError(t, err)
		assert.False(t, blocked)
		assert.Empty(t, comments, "an approved review must not draw a gate comment")
	})
}
