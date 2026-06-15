package webhook

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func TestFilterNonPassingNonSchemaBotChecks(t *testing.T) {
	tests := []struct {
		name       string
		statuses   []ghclient.PRCheckStatus
		wantLen    int
		wantNames  []string
		wantStates []string
	}{
		{
			name:     "empty statuses returns nil",
			statuses: nil,
			wantLen:  0,
		},
		{
			name: "all passing checks block nothing",
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
			wantLen:    2,
			wantNames:  []string{"security-scan", "CI / integration"},
			wantStates: []string{"error", "timed_out"},
		},
		{
			// A check whose run was cancelled, never started, went stale, or
			// stopped awaiting action has not verified the PR. Each of these
			// conclusions blocks apply, while SchemaBot's own checks remain
			// excluded.
			name: "checks completed without success block apply",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "cancelled"},
				{Name: "CI / lint", Status: "completed", Conclusion: "action_required"},
				{Name: "CI / integration", Status: "completed", Conclusion: "stale"},
				{Name: "CI / build", Status: "completed", Conclusion: "startup_failure"},
				{Name: "SchemaBot (staging)", Status: "completed", Conclusion: "cancelled", IsSchemaBot: true},
				{Name: "CI / docs", Status: "completed", Conclusion: "success"},
			},
			wantLen:    4,
			wantNames:  []string{"CI / unit-tests", "CI / lint", "CI / integration", "CI / build"},
			wantStates: []string{"cancelled", "action_required", "stale", "startup_failure"},
		},
		{
			// Conclusions SchemaBot does not recognize block apply, so the
			// gate fails closed if GitHub introduces a new conclusion.
			name: "unknown conclusion blocks apply",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / new-check", Status: "completed", Conclusion: "some_future_conclusion"},
			},
			wantLen:    1,
			wantNames:  []string{"CI / new-check"},
			wantStates: []string{"some_future_conclusion"},
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
			name: "in-progress checks are excluded from the completed-check filter",
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
			notPassing := filterNonPassingNonSchemaBotChecks(tt.statuses, nil)
			require.Len(t, notPassing, tt.wantLen)
			for i, name := range tt.wantNames {
				assert.Equal(t, name, notPassing[i].Name)
			}
			for i, state := range tt.wantStates {
				assert.Equal(t, state, notPassing[i].State)
			}
		})
	}
}

func TestFilterNonPassingNonSchemaBotChecks_RequiredChecks(t *testing.T) {
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
			notPassing := filterNonPassingNonSchemaBotChecks(tt.statuses, tt.config)
			require.Len(t, notPassing, len(tt.wantNames))
			for i, name := range tt.wantNames {
				assert.Equal(t, name, notPassing[i].Name)
			}
		})
	}
}

func TestIsAggregateCheckName(t *testing.T) {
	cases := []struct {
		name          string
		checkName     string
		aggregateBase string
		want          bool
	}{
		{name: "base name matches", checkName: "SchemaBot", aggregateBase: "SchemaBot", want: true},
		{name: "environment-scoped name matches", checkName: "SchemaBot (staging)", aggregateBase: "SchemaBot", want: true},
		{name: "custom base environment-scoped name matches", checkName: "SchemaBot X (production)", aggregateBase: "SchemaBot X", want: true},
		{name: "different base does not match", checkName: "SchemaBot X (staging)", aggregateBase: "SchemaBot", want: false},
		{name: "prefix without parenthesized suffix does not match", checkName: "SchemaBot Lint", aggregateBase: "SchemaBot", want: false},
		{name: "unrelated CI check does not match", checkName: "CI / unit-tests", aggregateBase: "SchemaBot", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isAggregateCheckName(tc.checkName, tc.aggregateBase))
		})
	}
}

// TestFilterNonPassingNonSchemaBotChecks_SiblingAggregate covers the
// split-deployment topology where another deployment in the promotion chain
// publishes its aggregate Check Run under a different GitHub App. When that
// App's slug is configured as trusted, the aggregate is classified as a
// SchemaBot check and never blocks applies. When the App is untrusted, the
// same-named check is treated as ordinary external CI and its non-passing
// conclusion blocks applies — name alone can never unblock the gate, so a
// spoofed aggregate name cannot hide a failing check.
func TestFilterNonPassingNonSchemaBotChecks_SiblingAggregate(t *testing.T) {
	t.Run("trusted sibling aggregate does not block", func(t *testing.T) {
		statuses := []ghclient.PRCheckStatus{
			{Name: "SchemaBot (production)", Status: "completed", Conclusion: "action_required", AppSlug: "schemabot-production", IsSchemaBot: true},
			{Name: "CI / unit-tests", Status: "completed", Conclusion: "failure"},
		}

		notPassing := filterNonPassingNonSchemaBotChecks(statuses, nil)

		require.Len(t, notPassing, 1)
		assert.Equal(t, "CI / unit-tests", notPassing[0].Name)
		assert.Equal(t, "failure", notPassing[0].State)
	})

	t.Run("untrusted aggregate-named check blocks", func(t *testing.T) {
		statuses := []ghclient.PRCheckStatus{
			{Name: "SchemaBot (production)", Status: "completed", Conclusion: "action_required", AppSlug: "github-actions", IsSchemaBot: false},
		}

		notPassing := filterNonPassingNonSchemaBotChecks(statuses, nil)

		require.Len(t, notPassing, 1)
		assert.Equal(t, "SchemaBot (production)", notPassing[0].Name)
		assert.Equal(t, "action_required", notPassing[0].State)
	})
}

// TestFilterInProgressNonSchemaBotChecks_SiblingAggregate verifies the same
// identity rule for in-flight checks: a trusted sibling deployment's
// mid-apply aggregate does not block, while an untrusted aggregate-named
// check that is still running blocks like any other in-progress CI.
func TestFilterInProgressNonSchemaBotChecks_SiblingAggregate(t *testing.T) {
	t.Run("trusted sibling aggregate does not block", func(t *testing.T) {
		statuses := []ghclient.PRCheckStatus{
			{Name: "SchemaBot (production)", Status: "in_progress", Conclusion: "", AppSlug: "schemabot-production", IsSchemaBot: true},
			{Name: "CI / tests", Status: "in_progress", Conclusion: ""},
		}

		inProgress := filterInProgressNonSchemaBotChecks(statuses, nil)

		require.Len(t, inProgress, 1)
		assert.Equal(t, "CI / tests", inProgress[0].Name)
	})

	t.Run("untrusted aggregate-named check blocks", func(t *testing.T) {
		statuses := []ghclient.PRCheckStatus{
			{Name: "SchemaBot (production)", Status: "in_progress", Conclusion: "", AppSlug: "github-actions", IsSchemaBot: false},
		}

		inProgress := filterInProgressNonSchemaBotChecks(statuses, nil)

		require.Len(t, inProgress, 1)
		assert.Equal(t, "SchemaBot (production)", inProgress[0].Name)
	})
}

// TestFlagUntrustedAggregateNamedChecks verifies that the spoof/misconfig
// signal states the check's actual gating impact: when required_checks
// narrowing excludes a non-required untrusted aggregate-named check from the
// gate, the warning says so instead of claiming the check will block applies.
func TestFlagUntrustedAggregateNamedChecks(t *testing.T) {
	newHandler := func(buf *bytes.Buffer) *Handler {
		return &Handler{logger: slog.New(slog.NewTextHandler(buf, nil))}
	}
	untrustedAggregate := ghclient.PRCheckStatus{
		Name: "SchemaBot (production)", Status: "completed", Conclusion: "action_required",
		AppSlug: "github-actions", IsSchemaBot: false,
	}

	t.Run("gating check is reported as blocking", func(t *testing.T) {
		var buf bytes.Buffer
		h := newHandler(&buf)

		h.flagUntrustedAggregateNamedChecks(t.Context(),
			[]ghclient.PRCheckStatus{untrustedAggregate},
			nil, "octocat/hello-world", 1, "abc123", "staging")

		assert.Contains(t, buf.String(), "will block applies unless passing")
		assert.Contains(t, buf.String(), "app_slug=github-actions")
	})

	t.Run("check excluded by required_checks narrowing is reported as non-gating", func(t *testing.T) {
		var buf bytes.Buffer
		h := newHandler(&buf)
		config := &api.ServerConfig{RequiredChecks: []string{"Owner Owl"}}
		statuses := []ghclient.PRCheckStatus{
			untrustedAggregate,
			{Name: "Owner Owl", Status: "completed", Conclusion: "success"},
		}

		h.flagUntrustedAggregateNamedChecks(t.Context(),
			statuses, config, "octocat/hello-world", 1, "abc123", "staging")

		assert.Contains(t, buf.String(), "required_checks narrowing keeps it from gating applies")
		assert.NotContains(t, buf.String(), "will block applies unless passing")
	})
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

// A configured required check that GitHub has not reported on the commit must
// block apply: the gate fails closed and surfaces the missing check by name so
// the operator knows which check still has to report before apply can proceed.
func TestMissingRequiredChecks(t *testing.T) {
	tests := []struct {
		name       string
		config     *api.ServerConfig
		statuses   []ghclient.PRCheckStatus
		wantNames  []string
		wantStates []string
	}{
		{
			name:     "no required checks configured demands nothing",
			config:   &api.ServerConfig{RequiredChecks: nil},
			statuses: nil,
		},
		{
			name:   "one of two required checks absent blocks on the missing one",
			config: &api.ServerConfig{RequiredChecks: []string{"check-a", "check-b"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "check-a", Status: "completed", Conclusion: "success"},
			},
			wantNames:  []string{"check-b"},
			wantStates: []string{"not reported"},
		},
		{
			name:   "no required check reported blocks on all of them",
			config: &api.ServerConfig{RequiredChecks: []string{"check-a", "check-b"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "incidental", Status: "completed", Conclusion: "success"},
			},
			wantNames:  []string{"check-a", "check-b"},
			wantStates: []string{"not reported", "not reported"},
		},
		{
			name:   "all required checks present demand nothing",
			config: &api.ServerConfig{RequiredChecks: []string{"check-a", "check-b"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "check-a", Status: "completed", Conclusion: "success"},
				{Name: "check-b", Status: "in_progress", Conclusion: ""},
			},
			wantNames: nil,
		},
		{
			name:   "required check reported by SchemaBot is not demanded",
			config: &api.ServerConfig{RequiredChecks: []string{"SchemaBot (staging)"}},
			statuses: []ghclient.PRCheckStatus{
				{Name: "SchemaBot (staging)", Status: "completed", Conclusion: "success", IsSchemaBot: true},
			},
			wantNames: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			missing := missingRequiredChecks(tt.statuses, tt.config)
			require.Len(t, missing, len(tt.wantNames))
			for i, name := range tt.wantNames {
				assert.Equal(t, name, missing[i].Name)
			}
			for i, state := range tt.wantStates {
				assert.Equal(t, state, missing[i].State)
			}
		})
	}
}

// checkStatusNode is a single check run or commit status used by tests to build
// mock REST responses. Set Typename to "CheckRun" or "StatusContext" and
// populate the matching fields.
type checkStatusNode struct {
	Typename   string
	Name       string // CheckRun.name
	Status     string // CheckRun.status
	Conclusion string // CheckRun.conclusion
	AppSlug    string // CheckRun.checkSuite.app.slug
	Context    string // StatusContext.context
	State      string // StatusContext.state
}

// registerCheckStatusRESTHandlers registers REST responses for commit statuses
// and check runs on the standard apply-gating test repository/ref.
func registerCheckStatusRESTHandlers(mux *http.ServeMux, nodes []checkStatusNode) {
	registerCheckStatusRESTHandlersForRef(mux, "abc123", func() []checkStatusNode { return nodes })
}

func registerCheckStatusRESTHandlersForRef(mux *http.ServeMux, ref string, nodes func() []checkStatusNode) {
	base := "/repos/octocat/hello-world/commits/" + ref
	mux.HandleFunc("GET "+base+"/status", func(w http.ResponseWriter, _ *http.Request) {
		writeCommitStatusResponse(w, nodes())
	})
	mux.HandleFunc("GET "+base+"/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		writeCheckRunsResponse(w, nodes())
	})
}

func writeCommitStatusResponse(w http.ResponseWriter, nodes []checkStatusNode) {
	statuses := make([]map[string]any, 0)
	for _, n := range nodes {
		if n.Typename != "StatusContext" {
			continue
		}
		statuses = append(statuses, map[string]any{"context": n.Context, "state": n.State})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"statuses": statuses, "total_count": len(statuses)})
}

func writeCheckRunsResponse(w http.ResponseWriter, nodes []checkStatusNode) {
	checkRuns := make([]map[string]any, 0)
	for _, n := range nodes {
		if n.Typename != "CheckRun" {
			continue
		}
		checkRuns = append(checkRuns, map[string]any{
			"name":       n.Name,
			"status":     n.Status,
			"conclusion": n.Conclusion,
			"app":        map[string]any{"slug": n.AppSlug},
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": checkRuns, "total_count": len(checkRuns)})
}

func TestEnforcePassingChecks(t *testing.T) {
	t.Run("permission error blocks with actionable message", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/status", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Resource not accessible by integration"})
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Resource not accessible by integration"})
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
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block on permission error")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Apply Blocked")
			assert.Contains(t, body, "PR check statuses")
			assert.Contains(t, body, "Checks")
			assert.Contains(t, body, "Commit statuses")
			assert.NotContains(t, body, "Resource not accessible", "should not expose raw GitHub API error")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for permission error comment")
		}
	})

	t.Run("API failure blocks apply (fail-closed)", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/status", func(w http.ResponseWriter, _ *http.Request) {
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
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
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

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "completed", Conclusion: "failure", AppSlug: "github-actions"},
			{Typename: "CheckRun", Name: "CI / lint", Status: "completed", Conclusion: "success", AppSlug: "github-actions"},
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
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
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

	// A push or concurrency-group rule can cancel a CI run after the apply
	// command was issued. The cancelled run never verified the PR, so the
	// apply is blocked and the comment names the cancelled check.
	t.Run("cancelled checks block apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "completed", Conclusion: "cancelled", AppSlug: "github-actions"},
			{Typename: "CheckRun", Name: "CI / lint", Status: "completed", Conclusion: "success", AppSlug: "github-actions"},
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
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when a check was cancelled")

		select {
		case body := <-comments:
			assert.Contains(t, body, "CI / tests")
			assert.Contains(t, body, "cancelled")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for cancelled-checks comment")
		}
	})

	t.Run("required checks ignore unlisted failures when configured check is present", func(t *testing.T) {
		client, mux := setupGitHubServer(t)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "Required Review", Status: "completed", Conclusion: "success", AppSlug: "review-gate"},
			{Typename: "CheckRun", Name: "CI / tests", Status: "completed", Conclusion: "failure", AppSlug: "github-actions"},
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review"}}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "should allow when configured checks pass")
	})

	t.Run("required status check avoids unrelated check-run read", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		checkRunsCalled := false

		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/status", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"statuses": []map[string]any{{"context": "Required Review", "state": "success"}},
			})
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			checkRunsCalled = true
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Resource not accessible by integration"})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review"}}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "status-only required gate should pass without reading check runs")
		assert.False(t, checkRunsCalled, "unrelated check runs should not be fetched once required statuses are found")
	})

	t.Run("required checks fall back to all checks when none are present", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "completed", Conclusion: "failure", AppSlug: "github-actions"},
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

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review"}}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
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

	// With two required checks configured, an apply must wait until every one
	// has reported. When only the first has reported (and passed), the apply is
	// blocked because the second required check has not reported yet, and the
	// comment names the missing check so the operator knows what to wait for.
	t.Run("absent required check blocks even when present required check passes", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "Required Review", Status: "completed", Conclusion: "success", AppSlug: "review-gate"},
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

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review", "Security scan"}}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when a required check has not reported")

		select {
		case body := <-comments:
			assert.Contains(t, body, "have not reported on this commit")
			assert.Contains(t, body, "| `Security scan` | not reported |")
			assert.Contains(t, body, "Verify the name in `required_checks` matches the check exactly and that it runs on this PR")
			assert.NotContains(t, body, "Cannot apply while PR checks are still running",
				"a never-reported check must not get the wait-and-retry remediation")
			assert.NotContains(t, body, "Required Review", "the passing required check should not be listed as blocking")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for missing-required-check comment")
		}
	})

	// Required checks are configured but none has reported on the commit; only
	// an unrelated incidental check has passed. The apply must not slip through
	// on incidental success — every configured required check still has to
	// report, so the apply is blocked and the comment names each missing check.
	t.Run("no required check reported blocks despite passing incidental check", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "CI / lint", Status: "completed", Conclusion: "success", AppSlug: "github-actions"},
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

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review", "Security scan"}}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when no required check has reported")

		select {
		case body := <-comments:
			assert.Contains(t, body, "have not reported on this commit")
			assert.Contains(t, body, "| `Required Review` | not reported |")
			assert.Contains(t, body, "| `Security scan` | not reported |")
			assert.Contains(t, body, "Verify the name in `required_checks` matches the check exactly and that it runs on this PR")
			assert.NotContains(t, body, "Cannot apply while PR checks are still running",
				"never-reported required checks must not get the wait-and-retry remediation")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for missing-required-checks comment")
		}
	})

	// Triage must be able to identify which required check blocked the apply
	// from the logs alone, without reproducing the gate. The block log names the
	// never-reported required checks rather than only counting them.
	t.Run("block log names the never-reported required checks", func(t *testing.T) {
		client, mux := setupGitHubServer(t)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "Required Review", Status: "completed", Conclusion: "success", AppSlug: "review-gate"},
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		var logBuf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))
		installClient := ghclient.NewInstallationClient(client, logger)
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review", "Security scan"}}, nil, logger)
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    logger,
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when a required check has not reported")

		logs := logBuf.String()
		assert.Contains(t, logs, "apply blocked: PR checks have not finished verifying this commit")
		assert.Contains(t, logs, "missing_required_count=1")
		assert.Contains(t, logs, "in_progress_count=0")
		assert.Contains(t, logs, "missing_required_checks")
		assert.Contains(t, logs, "Security scan", "the block log must name the never-reported required check")
		assert.NotContains(t, logs, "Required Review", "the reported required check must not be named as missing")
	})

	// A still-running non-required check and a never-reported required check can
	// block the same apply. The single comment surfaces both causes with their
	// own remediation: wait-and-retry for the running check, verify-the-name for
	// the never-reported required check.
	t.Run("in-progress and never-reported checks each get their own remediation", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "Required Review", Status: "in_progress", Conclusion: "", AppSlug: "review-gate"},
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

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review", "Security scan"}}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.True(t, blocked, "should block when a required check is running and another has not reported")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Cannot apply while PR checks are still running:")
			assert.Contains(t, body, "| `Required Review` | in_progress |")
			assert.Contains(t, body, "Wait for checks to complete and retry:")
			assert.Contains(t, body, "These required checks have not reported on this commit:")
			assert.Contains(t, body, "| `Security scan` | not reported |")
			assert.Contains(t, body, "Verify the name in `required_checks` matches the check exactly and that it runs on this PR")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for combined in-progress and not-reported comment")
		}
	})

	// When every configured required check has reported and passed, the apply
	// proceeds even though unrelated incidental checks are not in the required
	// set.
	t.Run("all required checks present and passing allows apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "Required Review", Status: "completed", Conclusion: "success", AppSlug: "review-gate"},
			{Typename: "CheckRun", Name: "Security scan", Status: "completed", Conclusion: "success", AppSlug: "security-app"},
			{Typename: "CheckRun", Name: "CI / tests", Status: "in_progress", Conclusion: "", AppSlug: "github-actions"},
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{RequiredChecks: []string{"Required Review", "Security scan"}}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "should allow when all required checks reported and passed")
	})

	t.Run("all passing allows apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "completed", Conclusion: "success", AppSlug: "github-actions"},
			{Typename: "CheckRun", Name: "SchemaBot (staging)", Status: "completed", Conclusion: "action_required", AppSlug: "schemabot"},
		})

		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		ctx := t.Context()
		blocked := h.enforcePassingChecks(ctx, installClient, "octocat/hello-world", 1, 12345, "abc123", "staging")
		assert.False(t, blocked, "should allow when all non-SchemaBot checks pass")
	})

	t.Run("in-progress checks block apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "in_progress", Conclusion: "", AppSlug: "github-actions"},
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
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
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

		registerCheckStatusRESTHandlers(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "CI / tests", Status: "completed", Conclusion: "success", AppSlug: "github-actions"},
			{Typename: "CheckRun", Name: "SchemaBot (staging)", Status: "completed", Conclusion: "failure", AppSlug: "schemabot-at-acme-staging"},
		})

		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot-at-acme-staging")
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{}, nil, testLogger())
		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
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
