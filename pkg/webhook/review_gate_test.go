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

	assert.Contains(t, detail, "Review gate check failed")
	assert.Contains(t, detail, "team membership cannot be read")
	assert.Contains(t, detail, "GitHub App can read organization members")
}

func TestReviewGateErrorDetailGeneric(t *testing.T) {
	detail := reviewGateErrorDetail(assert.AnError)

	assert.Contains(t, detail, "Review gate check failed")
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

	client, err := h.ghClient.ForInstallation(12345)
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

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"bob"}, result.RequiredReviewers)
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

	client, err := h.ghClient.ForInstallation(12345)
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

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

func TestCheckReviewGate_NotApproved(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, "alice", result.PRAuthor)
	assert.Contains(t, result.RequiredReviewers, "bob")
	assert.Contains(t, result.RequiredReviewers, "carol")
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

	client, err := h.ghClient.ForInstallation(12345)
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

	client, err := h.ghClient.ForInstallation(12345)
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

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
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

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"bob"}, result.RequiredReviewers)
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

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/payments")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Contains(t, result.RequiredReviewers, "bob")

	result, err = h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "orders", "schema/orders")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Contains(t, result.RequiredReviewers, "carol")
}

func TestCheckReviewGate_NoConfiguredReviewersErrors(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.IncludeDatabaseOperators = new(false)
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, nil)

	client, err := h.ghClient.ForInstallation(12345)
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
