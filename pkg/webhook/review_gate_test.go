package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// mockSettingsStore implements storage.SettingsStore for testing.
type mockSettingsStore struct {
	settings map[string]string
}

func (m *mockSettingsStore) Get(_ context.Context, key string) (*storage.Setting, error) {
	if v, ok := m.settings[key]; ok {
		return &storage.Setting{Key: key, Value: v}, nil
	}
	return nil, nil
}

func (m *mockSettingsStore) Set(_ context.Context, key, value string) error {
	m.settings[key] = value
	return nil
}

func (m *mockSettingsStore) List(_ context.Context) ([]*storage.Setting, error) {
	var result []*storage.Setting
	for k, v := range m.settings {
		result = append(result, &storage.Setting{Key: k, Value: v})
	}
	return result, nil
}

func (m *mockSettingsStore) Delete(_ context.Context, key string) error {
	delete(m.settings, key)
	return nil
}

// reviewGateMockStorage implements storage.Storage with a working SettingsStore.
type reviewGateMockStorage struct {
	settings *mockSettingsStore
}

func (m *reviewGateMockStorage) Locks() storage.LockStore                      { return nil }
func (m *reviewGateMockStorage) Plans() storage.PlanStore                      { return nil }
func (m *reviewGateMockStorage) Applies() storage.ApplyStore                   { return nil }
func (m *reviewGateMockStorage) Tasks() storage.TaskStore                      { return nil }
func (m *reviewGateMockStorage) ApplyLogs() storage.ApplyLogStore              { return nil }
func (m *reviewGateMockStorage) ApplyComments() storage.ApplyCommentStore      { return nil }
func (m *reviewGateMockStorage) VitessApplyData() storage.VitessApplyDataStore { return nil }
func (m *reviewGateMockStorage) Checks() storage.CheckStore                    { return nil }
func (m *reviewGateMockStorage) Settings() storage.SettingsStore               { return m.settings }
func (m *reviewGateMockStorage) Ping(_ context.Context) error                  { return nil }
func (m *reviewGateMockStorage) Close() error                                  { return nil }

func setupReviewGateHandler(t *testing.T, settings map[string]string) (*Handler, *http.ServeMux) {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	st := &reviewGateMockStorage{
		settings: &mockSettingsStore{settings: settings},
	}
	svc := api.New(st, nil, nil, logger)

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
	h, _ := setupReviewGateHandler(t, map[string]string{})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	assert.Nil(t, result, "gate should return nil when disabled")
}

func TestCheckReviewGate_EnabledNoCodeowners_NoReviews(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "", false)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "no CODEOWNERS + no reviews = blocked")
}

func TestCheckReviewGate_EnabledNoCodeowners_WithApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "", false)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved, "no CODEOWNERS + any non-self approval = approved")
}

func TestCheckReviewGate_ApprovedByCodeowner(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @bob @carol\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

func TestCheckReviewGate_NotApproved(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @bob @carol\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, "alice", result.PRAuthor)
	assert.Contains(t, result.Owners, "bob")
	assert.Contains(t, result.Owners, "carol")
}

func TestCheckReviewGate_SelfApprovalBlocked(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @alice @bob\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("alice")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "self-approval should be blocked")
}

func TestCheckReviewGate_RepoSpecificSetting(t *testing.T) {
	h, _ := setupReviewGateHandler(t, map[string]string{
		"require_review":                     "false",
		"require_review:octocat/hello-world": "true",
	})

	// Repo-specific override is true, so gate should be enabled
	enabled, err := h.isReviewGateEnabled(t.Context(), "octocat/hello-world")
	require.NoError(t, err)
	assert.True(t, enabled)

	// Different repo falls back to global (false)
	enabled, err = h.isReviewGateEnabled(t.Context(), "other/repo")
	require.NoError(t, err)
	assert.False(t, enabled)
}

func TestCheckReviewGate_TeamSlugExpansion(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @octocat/dba-team\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	// Team members endpoint — bob is a member of dba-team
	mux.HandleFunc("GET /orgs/octocat/teams/dba-team/members", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]*gh.User{
			{Login: new("bob")},
			{Login: new("carol")},
		})
	})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved, "team member approval should pass gate")
}

func TestCheckReviewGate_TeamSlugNotApproved(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @octocat/dba-team\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	// dave is NOT a member of dba-team
	mux.HandleFunc("GET /orgs/octocat/teams/dba-team/members", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]*gh.User{
			{Login: new("bob")},
			{Login: new("carol")},
		})
	})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "non-team-member approval should not pass gate")
}

func TestCheckReviewGate_ApprovedByNonOwner(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @bob\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.ghClient.ForInstallation(12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/testdb")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "approval by non-owner should not pass gate")
}

func TestCheckReviewGate_PathScopedOwners(t *testing.T) {
	h, mux := setupReviewGateHandler(t, map[string]string{"require_review": "true"})

	registerPREndpoint(mux, "alice")
	// payments/ owned by bob, orders/ owned by carol
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

	// bob approved — should pass for payments (bob owns it)
	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/payments")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved, "bob owns schema/payments/ and approved")

	// bob approved — should NOT pass for orders (carol owns it)
	result, err = h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "schema/orders")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "bob does not own schema/orders/")
	assert.Contains(t, result.Owners, "carol")
}
