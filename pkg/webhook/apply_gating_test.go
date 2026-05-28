package webhook

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func TestFilterFailingNonSchemaBotChecks(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []ghclient.PRCheckStatus
		wantLen   int
		wantNames []string
	}{
		{
			name:     "empty statuses returns nil",
			statuses: nil,
			wantLen:  0,
		},
		{
			name: "all passing checks returns no failures",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "success"},
				{Name: "CI / lint", Status: "completed", Conclusion: "success"},
			},
			wantLen: 0,
		},
		{
			name: "failure is caught",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "failure"},
				{Name: "CI / lint", Status: "completed", Conclusion: "success"},
			},
			wantLen:   1,
			wantNames: []string{"CI / unit-tests"},
		},
		{
			name: "error and timed_out are caught",
			statuses: []ghclient.PRCheckStatus{
				{Name: "security-scan", Status: "completed", Conclusion: "error"},
				{Name: "CI / integration", Status: "completed", Conclusion: "timed_out"},
			},
			wantLen:   2,
			wantNames: []string{"security-scan", "CI / integration"},
		},
		{
			name: "SchemaBot checks are excluded",
			statuses: []ghclient.PRCheckStatus{
				{Name: "SchemaBot Apply: /mysql/payments", Status: "completed", Conclusion: "failure", IsSchemaBot: true},
				{Name: "SchemaBot (staging)", Status: "completed", Conclusion: "failure", IsSchemaBot: true},
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "failure"},
			},
			wantLen:   1,
			wantNames: []string{"CI / unit-tests"},
		},
		{
			name: "neutral and skipped are ignored",
			statuses: []ghclient.PRCheckStatus{
				{Name: "informational-check", Status: "completed", Conclusion: "neutral"},
				{Name: "optional-check", Status: "completed", Conclusion: "skipped"},
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
			},
			wantLen:   1,
			wantNames: []string{"CI / lint"},
		},
		{
			name: "in-progress checks are not considered failing",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "in_progress", Conclusion: ""},
				{Name: "CI / lint", Status: "queued", Conclusion: ""},
			},
			wantLen: 0,
		},
		{
			name: "mixed statuses",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "success"},
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
				{Name: "CI / integration", Status: "in_progress", Conclusion: ""},
				{Name: "SchemaBot Apply: /mysql/db", Status: "completed", Conclusion: "action_required", IsSchemaBot: true},
				{Name: "optional", Status: "completed", Conclusion: "neutral"},
				{Name: "security", Status: "completed", Conclusion: "error"},
			},
			wantLen:   2,
			wantNames: []string{"CI / lint", "security"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failing := filterFailingNonSchemaBotChecks(tt.statuses, nil)
			require.Len(t, failing, tt.wantLen)
			for i, name := range tt.wantNames {
				assert.Equal(t, name, failing[i].Name)
			}
		})
	}
}

func TestFilterFailingNonSchemaBotChecks_RequiredChecks(t *testing.T) {
	tests := []struct {
		name      string
		config    *api.ServerConfig
		statuses  []ghclient.PRCheckStatus
		wantNames []string
	}{
		{
			name:   "configured failing check blocks when present",
			config: &api.ServerConfig{RequiredChecks: []string{"Required Review"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "Required Review", Status: "completed", Conclusion: "failure"},
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
			},
			wantNames: []string{"Required Review"},
		},
		{
			name:   "unlisted failing check is ignored when configured check is present",
			config: &api.ServerConfig{RequiredChecks: []string{"Required Review", "Security scan"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "Required Review", Status: "completed", Conclusion: "success"},
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
			},
			wantNames: nil,
		},
		{
			name:   "all checks apply when no configured check is present",
			config: &api.ServerConfig{RequiredChecks: []string{"Required Review"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
				{Name: "Security scan", Status: "completed", Conclusion: "error"},
			},
			wantNames: []string{"CI / lint", "Security scan"},
		},
		{
			name:   "SchemaBot check does not activate required check filter",
			config: &api.ServerConfig{RequiredChecks: []string{"SchemaBot (staging)"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "SchemaBot (staging)", Status: "completed", Conclusion: "failure", IsSchemaBot: true},
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
			},
			wantNames: []string{"CI / lint"},
		},
		{
			name:   "empty required checks preserves default behavior",
			config: &api.ServerConfig{RequiredChecks: []string{}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
			},
			wantNames: []string{"CI / lint"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failing := filterFailingNonSchemaBotChecks(tt.statuses, tt.config)
			require.Len(t, failing, len(tt.wantNames))
			for i, name := range tt.wantNames {
				assert.Equal(t, name, failing[i].Name)
			}
		})
	}
}

func TestFilterInProgressNonSchemaBotChecks(t *testing.T) {
	statuses := []ghclient.PRCheckStatus{
		{Name: "CI / tests", Status: "in_progress", Conclusion: ""},
		{Name: "CI / lint", Status: "completed", Conclusion: "success"},
		{Name: "Security scan", Status: "queued", Conclusion: ""},
		{Name: "SchemaBot (staging)", Status: "in_progress", Conclusion: "", IsSchemaBot: true},
		{Name: "Deploy preview", Status: "pending", Conclusion: ""},
	}

	inProgress := filterInProgressNonSchemaBotChecks(statuses, nil)
	require.Len(t, inProgress, 3)
	assert.Equal(t, "CI / tests", inProgress[0].Name)
	assert.Equal(t, "in_progress", inProgress[0].State)
	assert.Equal(t, "Security scan", inProgress[1].Name)
	assert.Equal(t, "queued", inProgress[1].State)
	assert.Equal(t, "Deploy preview", inProgress[2].Name)
	assert.Equal(t, "pending", inProgress[2].State)
}

func TestFilterInProgressNonSchemaBotChecks_RequiredChecks(t *testing.T) {
	tests := []struct {
		name      string
		config    *api.ServerConfig
		statuses  []ghclient.PRCheckStatus
		wantNames []string
	}{
		{
			name:   "configured in-progress check blocks when present",
			config: &api.ServerConfig{RequiredChecks: []string{"Required Review"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "Required Review", Status: "queued", Conclusion: ""},
				{Name: "CI / tests", Status: "in_progress", Conclusion: ""},
			},
			wantNames: []string{"Required Review"},
		},
		{
			name:   "unlisted in-progress check is ignored when configured check is present",
			config: &api.ServerConfig{RequiredChecks: []string{"Required Review"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "Required Review", Status: "completed", Conclusion: "success"},
				{Name: "CI / tests", Status: "in_progress", Conclusion: ""},
			},
			wantNames: nil,
		},
		{
			name:   "all checks apply when no configured check is present",
			config: &api.ServerConfig{RequiredChecks: []string{"Required Review"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / tests", Status: "in_progress", Conclusion: ""},
				{Name: "Deploy preview", Status: "pending", Conclusion: ""},
			},
			wantNames: []string{"CI / tests", "Deploy preview"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inProgress := filterInProgressNonSchemaBotChecks(tt.statuses, tt.config)
			require.Len(t, inProgress, len(tt.wantNames))
			for i, name := range tt.wantNames {
				assert.Equal(t, name, inProgress[i].Name)
			}
		})
	}
}

// rollupNode is a single GraphQL statusCheckRollup contexts node, used by tests
// to build mock GraphQL responses. Set Typename to "CheckRun" or "StatusContext"
// and populate the matching fields.
type rollupNode struct {
	Typename   string
	Name       string // CheckRun.name
	Status     string // CheckRun.status (uppercase: COMPLETED, IN_PROGRESS, ...)
	Conclusion string // CheckRun.conclusion (uppercase: SUCCESS, FAILURE, ...)
	AppSlug    string // CheckRun.checkSuite.app.slug
	Context    string // StatusContext.context
	State      string // StatusContext.state (uppercase)
}

// rollupGraphQLHandler returns an http.HandlerFunc that responds to a GraphQL
// statusCheckRollup query with the supplied contexts.
func rollupGraphQLHandler(nodes []rollupNode) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		out := make([]map[string]any, 0, len(nodes))
		for _, n := range nodes {
			node := map[string]any{"__typename": n.Typename}
			switch n.Typename {
			case "CheckRun":
				node["name"] = n.Name
				node["status"] = n.Status
				node["conclusion"] = n.Conclusion
				node["checkSuite"] = map[string]any{"app": map[string]any{"slug": n.AppSlug}}
			case "StatusContext":
				node["context"] = n.Context
				node["state"] = n.State
			}
			out = append(out, node)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"repository": map[string]any{"object": map[string]any{
				"statusCheckRollup": map[string]any{"contexts": map[string]any{
					"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
					"nodes":    out,
				}},
			}}},
		})
	}
}

func TestEnforcePassingChecks(t *testing.T) {
	t.Run("permission error blocks with actionable message", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		// Return a GraphQL permission error
		mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"errors": []map[string]any{
					{"message": "Resource not accessible by integration", "type": "FORBIDDEN"},
				},
			})
		})

		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block on permission error")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Apply Blocked")
			assert.Contains(t, body, "Commit statuses: Read")
			assert.NotContains(t, body, "Resource not accessible", "should not expose raw GraphQL error")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for permission error comment")
		}
	})

	t.Run("API failure blocks apply (fail-closed)", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when API fails")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Apply Blocked")
			assert.Contains(t, body, "Unable to verify")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for API failure comment")
		}
	})

	t.Run("failing checks block apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		mux.HandleFunc("POST /graphql", rollupGraphQLHandler([]rollupNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "COMPLETED", Conclusion: "FAILURE", AppSlug: "github-actions"},
			{Typename: "CheckRun", Name: "CI / lint", Status: "COMPLETED", Conclusion: "SUCCESS", AppSlug: "github-actions"},
		}))

		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when checks are failing")

		select {
		case body := <-comments:
			assert.Contains(t, body, "CI / tests")
			assert.Contains(t, body, "failure")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for failing-checks comment")
		}
	})

	t.Run("required checks ignore unlisted failures when configured check is present", func(t *testing.T) {
		client, mux := setupGitHubServer(t)

		mux.HandleFunc("POST /graphql", rollupGraphQLHandler([]rollupNode{
			{Typename: "CheckRun", Name: "Required Review", Status: "COMPLETED", Conclusion: "SUCCESS", AppSlug: "review-gate"},
			{Typename: "CheckRun", Name: "CI / tests", Status: "COMPLETED", Conclusion: "FAILURE", AppSlug: "github-actions"},
		}))

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review"}}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "should allow when configured checks pass")
	})

	t.Run("required checks fall back to all checks when none are present", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		mux.HandleFunc("POST /graphql", rollupGraphQLHandler([]rollupNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "COMPLETED", Conclusion: "FAILURE", AppSlug: "github-actions"},
		}))

		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review"}}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block on all checks when configured checks are absent")

		select {
		case body := <-comments:
			assert.Contains(t, body, "CI / tests")
			assert.Contains(t, body, "failure")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for failing-checks comment")
		}
	})

	t.Run("all passing allows apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)

		mux.HandleFunc("POST /graphql", rollupGraphQLHandler([]rollupNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "COMPLETED", Conclusion: "SUCCESS", AppSlug: "github-actions"},
			{Typename: "CheckRun", Name: "SchemaBot (staging)", Status: "COMPLETED", Conclusion: "ACTION_REQUIRED", AppSlug: "schemabot"},
		}))

		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "should allow when all non-SchemaBot checks pass")
	})

	t.Run("in-progress checks block apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		mux.HandleFunc("POST /graphql", rollupGraphQLHandler([]rollupNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "IN_PROGRESS", Conclusion: "", AppSlug: "github-actions"},
		}))

		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when checks are in-progress")

		select {
		case body := <-comments:
			assert.Contains(t, body, "still running")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for in-progress-checks comment")
		}
	})

	t.Run("variant app slug excluded from gate", func(t *testing.T) {
		client, mux := setupGitHubServer(t)

		mux.HandleFunc("POST /graphql", rollupGraphQLHandler([]rollupNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "COMPLETED", Conclusion: "SUCCESS", AppSlug: "github-actions"},
			{Typename: "CheckRun", Name: "SchemaBot (staging)", Status: "COMPLETED", Conclusion: "FAILURE", AppSlug: "schemabot-at-acme-staging"},
		}))

		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot-at-acme-staging")
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "should not block on own failed check with variant slug")
	})

	t.Run("disabled by config", func(t *testing.T) {
		client, _ := setupGitHubServer(t)
		installClient := ghclient.NewInstallationClient(client, testLogger())

		falseVal := false
		service := api.New(nil, &api.ServerConfig{RequirePassingChecks: &falseVal}, nil, testLogger())
		h := &Handler{
			service: service,
			logger:  testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "should not block when disabled")
	})
}
