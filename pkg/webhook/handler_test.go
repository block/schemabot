package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func testLogger() *slog.Logger {
	return slog.Default()
}

func TestVerifyHMAC(t *testing.T) {
	secret := []byte("test-secret")

	body := []byte(`{"test": "payload"}`)

	// Generate valid signature
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	t.Run("valid signature", func(t *testing.T) {
		assert.True(t, verifyHMAC(validSig, body, secret))
	})

	t.Run("invalid signature", func(t *testing.T) {
		assert.False(t, verifyHMAC("sha256=deadbeef", body, secret))
	})

	t.Run("empty signature", func(t *testing.T) {
		assert.False(t, verifyHMAC("", body, secret))
	})

	t.Run("wrong prefix", func(t *testing.T) {
		assert.False(t, verifyHMAC("sha1=abc", body, secret))
	})

	t.Run("invalid hex", func(t *testing.T) {
		assert.False(t, verifyHMAC("sha256=not-hex!", body, secret))
	})
}

func TestWebhookRejectsInvalidSignature(t *testing.T) {
	h := &Handler{
		webhookSecretsByApp: map[string][]byte{defaultAppName: []byte("secret")},
		logger:              testLogger(),
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(`{}`))
	req.Header.Set("X-Hub-Signature-256", "sha256=invalid")
	req.Header.Set("X-GitHub-Event", "issue_comment")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestWebhookRejectsOversizedBody(t *testing.T) {
	h := &Handler{
		webhookSecretsByApp: map[string][]byte{defaultAppName: []byte("secret")},
		logger:              testLogger(),
	}

	body := strings.NewReader(strings.Repeat("a", maxWebhookBodyBytes+1))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", body)
	req.Header.Set("X-Hub-Signature-256", "sha256=invalid")
	req.Header.Set("X-GitHub-Event", "issue_comment")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.Contains(t, rr.Body.String(), "request body too large")
}

func TestWebhookIgnoresUnknownEvents(t *testing.T) {
	h := &Handler{logger: testLogger()} // No secret — skip validation

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(`{}`))
	req.Header.Set("X-GitHub-Event", "deployment")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "ignored")
}

func TestRenderPRCommentSupportChannelFooter(t *testing.T) {
	t.Run("disabled without service config", func(t *testing.T) {
		h := &Handler{}
		assert.Equal(t, "hello", h.renderPRComment("hello"))
	})

	t.Run("does not append to normal comments", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: "#schema-help",
				URL:  "https://example.com/schema-help",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		body := h.renderPRComment("hello\n")

		assert.Equal(t, "hello\n", body)
	})

	t.Run("appends to help comments", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: "#schema-help",
				URL:  "https://example.com/schema-help",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		body := h.renderPRComment(templates.RenderHelpComment())

		assert.Contains(t, body, "> 💬 Support: [#schema-help](https://example.com/schema-help).")
	})

	t.Run("appends to error comments", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: "#schema-help",
				URL:  "https://example.com/schema-help",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		body := h.renderPRComment(templates.RenderInvalidCommand())

		assert.Contains(t, body, "> 💬 Support: [#schema-help](https://example.com/schema-help).")
	})

	t.Run("appends to apply failure comments", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: "#schema-help",
				URL:  "https://example.com/schema-help",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		body := h.renderPRComment(templates.PreviewCommentApplyFailed())

		assert.Contains(t, body, "> 💬 Support: [#schema-help](https://example.com/schema-help).")
	})

	t.Run("appends to reconciliation required comments", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: "#schema-help",
				URL:  "https://example.com/schema-help",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		body := h.renderPRComment(templates.RenderSchemaChangeReconciliationRequired(templates.SchemaChangeReconciliationData{
			RequestedBy: "alice",
			Timestamp:   "2026-06-14 12:34:56",
			Items: []templates.SchemaChangeReconciliationItem{{
				Database:    "orders",
				Environment: "staging",
				ApplyID:     "apply-1234",
				State:       "completed",
			}},
		}))

		assert.Contains(t, body, "> 💬 Support: [#schema-help](https://example.com/schema-help).")
	})

	t.Run("escapes markdown link text", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: `team]ops\help`,
				URL:  "https://example.com/support",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		body := h.renderPRComment(templates.RenderHelpComment())

		assert.Contains(t, body, `[team\]ops\\help](https://example.com/support)`)
	})

	t.Run("does not duplicate footer", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: "#schema-help",
				URL:  "https://example.com/schema-help",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		once := h.renderPRComment(templates.RenderHelpComment())
		twice := h.renderPRComment(once)

		assert.Equal(t, once, twice)
	})

	t.Run("does not append to plan comments", func(t *testing.T) {
		cfg := &api.ServerConfig{
			SupportChannel: api.SupportChannelConfig{
				Name: "#schema-help",
				URL:  "https://example.com/schema-help",
			},
		}
		h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

		body := h.renderPRComment("## MySQL Schema Change Plan\n\nplan summary\n\n---\n\n💡 **To apply** all schema changes from this PR, comment:\n```\nschemabot apply -e staging\n```")

		assert.NotContains(t, body, "Support:")
	})
}

func TestHandleSchemaRequestErrorRendersConfigNotAuthorized(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}

	h.handleSchemaRequestError("octocat/hello-world", 1, 12345, "production", "", "hubot", "apply", &schemaConfigOutsideAllowedDirsError{
		Database:     "orders",
		DatabaseType: "mysql",
		SchemaPath:   "services/orders/schema",
	})

	body := requireComment(t, comments, "config-not-authorized comment")
	assert.Contains(t, body, "SchemaBot Configuration Not Authorized")
	assert.Contains(t, body, "SchemaBot found a `schemabot.yaml` configuration")
	assert.Contains(t, body, "`services/orders/schema`")
	assert.Contains(t, body, "`databases.orders.allowed_dirs`")
	assert.NotContains(t, body, "No `schemabot.yaml` configuration file was found")
}

func TestCheckRunRerequestIgnoresNonSchemaBotCheck(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{
		checkName: "CI / tests",
		headSHA:   "abc123",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "non-SchemaBot check")
}

func TestCheckRunRerequestIgnoresStaleHeadSHA(t *testing.T) {
	client, mux := setupGitHubServer(t)

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": "newsha222", "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	factory := &fakeClientFactory{client: installClient}
	service := api.New(&emptyStorage{}, &api.ServerConfig{
		AllowedEnvironments: []string{"staging"},
	}, nil, testLogger())

	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
		logger:    testLogger(),
	}

	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{
		checkName: "SchemaBot (staging)",
		headSHA:   "oldsha111",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "stale head SHA")
}

// newTestHandler creates a Handler wired to a fake GitHub API server.
// Returns the handler and a channel that receives posted comment bodies.
func newTestHandler(t *testing.T) (*Handler, chan string, chan string) {
	t.Helper()
	client, mux := setupGitHubServer(t)

	comments := make(chan string, 10)
	reactions := make(chan string, 10)

	// Capture comment POST requests
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	// Capture reaction POST requests
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	factory := &fakeClientFactory{client: installClient}
	service := api.New(&emptyStorage{}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{},
	}, nil, testLogger())

	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
		logger:    testLogger(),
	}
	return h, comments, reactions
}

func TestWebhookHelpCommand(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot help",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "help posted")

	select {
	case body := <-comments:
		assert.Contains(t, body, "SchemaBot Help")
		assert.Contains(t, body, "schemabot plan")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookInvalidCommand(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot foobar",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "invalid command")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Invalid Command")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookMissingEnvForApply(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "missing environment flag")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Missing Argument")
		assert.Contains(t, body, "-e")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookYesFlagRejectedOnNonApply(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	for _, cmd := range []string{
		"schemabot plan -e staging -y",
		"schemabot apply-confirm -e staging -y",
	} {
		t.Run(cmd, func(t *testing.T) {
			req := buildWebhookRequest(t, webhookPayloadOpts{
				comment: cmd,
				isPR:    true,
			}, nil)

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			require.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), "unsupported flag")

			select {
			case body := <-comments:
				assert.Contains(t, body, "-y")
				assert.Contains(t, body, "not supported for")
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for error comment")
			}
		})
	}
}

func TestWebhookControlCommandMissingApplyID(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		action  string
	}{
		{name: "stop", comment: "schemabot stop -e staging", action: "stop"},
		{name: "cancel", comment: "schemabot cancel -e staging", action: "cancel"},
		{name: "start", comment: "schemabot start -e staging", action: "start"},
		{name: "cutover", comment: "schemabot cutover -e staging", action: "cutover"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, comments, _ := newTestHandler(t)

			req := buildWebhookRequest(t, webhookPayloadOpts{
				comment: tt.comment,
				isPR:    true,
			}, nil)

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			require.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.action+" started")

			select {
			case body := <-comments:
				assert.Contains(t, body, "Missing Apply ID")
				assert.Contains(t, body, "schemabot "+tt.action+" <apply-id> -e <environment>")
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for missing apply ID comment")
			}
		})
	}
}

func TestWebhookBotCommentIgnored(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment:  "schemabot help",
		userType: "Bot",
		isPR:     true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "event ignored")

	// Bot path returns before launching any goroutines, so channel is guaranteed empty.
	select {
	case <-comments:
		t.Fatal("should not post a comment for bot users")
	default:
	}
}

func TestWebhookNotAPRComment(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot help",
		isPR:    false, // regular issue, not a PR
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "event ignored")

	// Non-PR path returns before launching any goroutines, so channel is guaranteed empty.
	select {
	case <-comments:
		t.Fatal("should not post a comment for non-PR issues")
	default:
	}
}

func TestWebhookNoMention(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "just a regular comment with no trigger word",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no SchemaBot command")

	// No-mention path returns before launching any goroutines, so channel is guaranteed empty.
	select {
	case <-comments:
		t.Fatal("should not post a comment when not mentioned")
	default:
	}
}

func TestWebhookIgnoresSchemaBotProse(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	for _, comment := range []string{
		"With `schemabot.yaml` at `files/migrations/`, the app uses declarative schema changes.",
		"```sh\nschemabot plan -e staging\n```",
	} {
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: comment,
			isPR:    true,
		}, nil)

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "no SchemaBot command")

		select {
		case <-comments:
			t.Fatal("should not post a comment for prose or examples that mention SchemaBot")
		default:
		}
	}
}

func TestWebhookEyesReaction(t *testing.T) {
	h, _, reactions := newTestHandler(t)

	// Use an env-scoped command that reaches the reaction point (after all
	// skip/filter checks). Help returns before the reaction fires.
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case reaction := <-reactions:
		assert.Equal(t, "eyes", reaction)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for eyes reaction")
	}
}

func TestWebhookPhase2CommandNotYetAvailable(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	// revert is the only remaining Phase 2 command not yet available via PR
	// comments (apply, apply-confirm, unlock, stop, start, cutover, and skip-revert
	// are now implemented).
	cmds := []struct {
		comment string
		action  string
	}{
		{"schemabot revert -e staging", "revert"},
	}

	for _, cmd := range cmds {
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: cmd.comment,
			isPR:    true,
		}, nil)

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "not yet implemented")

		select {
		case body := <-comments:
			assert.Contains(t, body, "not yet available via PR comments")
			assert.Contains(t, body, cmd.action)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for comment for %q", cmd.comment)
		}
	}
}

func TestWebhookSignatureValidation(t *testing.T) {
	h, comments, _ := newTestHandler(t)
	secret := []byte("webhook-secret")
	h.webhookSecretsByApp = map[string][]byte{defaultAppName: secret}

	t.Run("valid signature accepted", func(t *testing.T) {
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help",
			isPR:    true,
		}, secret)

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)

		select {
		case body := <-comments:
			assert.Contains(t, body, "SchemaBot Help")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for comment")
		}
	})

	t.Run("invalid signature rejected", func(t *testing.T) {
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help",
			isPR:    true,
		}, []byte("wrong-secret"))

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnauthorized, rr.Code)
	})
}

func TestWebhookMultipleCommandsSequential(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	// Send help, then invalid — both should produce correct responses
	commands := []struct {
		comment  string
		contains string
	}{
		{"schemabot help", "SchemaBot Help"},
		{"schemabot foobar", "Invalid Command"},
	}

	for _, cmd := range commands {
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: cmd.comment,
			isPR:    true,
		}, nil)

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)

		select {
		case body := <-comments:
			assert.Contains(t, body, cmd.contains)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for comment for %q", cmd.comment)
		}
	}
}

// PR close cleanup is tested in the integration suite (TestE2EPRCloseCleanup)
// which has real storage via testcontainers.

func TestWebhookPREditIgnored(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "edited"}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "action ignored")
}

func TestWebhookPlanWithLintViolations(t *testing.T) {
	// Verify that lint violations from LintSchema are rendered in the plan comment.
	// This tests the template rendering path that the handler uses when posting
	// plan comments — both single-env and multi-env.
	t.Run("single env plan with lint violations", func(t *testing.T) {
		data := templates.PlanCommentData{
			Database:    "testapp",
			Environment: "staging",
			RequestedBy: "testuser",
			IsMySQL:     true,
			Changes: []templates.KeyspaceChangeData{
				{
					Keyspace: "testapp",
					Statements: []string{
						"CREATE TABLE `bad_table` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
					},
				},
			},
			LintViolations: []templates.LintViolationData{
				{Message: "Primary key uses signed integer type", Table: "bad_table", LinterName: "primary_key"},
				{Message: "Column uses utf8 charset", Table: "users", LinterName: "allow_charset"},
			},
		}

		rendered := templates.RenderPlanComment(data)
		assert.Contains(t, rendered, "Lint Warnings")
		assert.Contains(t, rendered, "[bad_table] Primary key uses signed integer type")
		assert.Contains(t, rendered, "[users] Column uses utf8 charset")
		assert.Contains(t, rendered, "CREATE TABLE")
	})

	t.Run("multi env plan with lint violations", func(t *testing.T) {
		changes := []templates.KeyspaceChangeData{
			{
				Keyspace: "testapp",
				Statements: []string{
					"CREATE TABLE `bad_table` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
				},
			},
		}
		lintViolations := []templates.LintViolationData{
			{Message: "Primary key uses signed integer type", Table: "bad_table", LinterName: "primary_key"},
		}

		data := templates.MultiEnvPlanCommentData{
			Database:     "testapp",
			IsMySQL:      true,
			RequestedBy:  "testuser",
			Environments: []string{"staging", "production"},
			Plans: map[string]*templates.PlanCommentData{
				"staging":    {Database: "testapp", Environment: "staging", IsMySQL: true, Changes: changes, LintViolations: lintViolations},
				"production": {Database: "testapp", Environment: "production", IsMySQL: true, Changes: changes, LintViolations: lintViolations},
			},
			Errors: map[string]string{},
		}

		rendered := templates.RenderMultiEnvPlanComment(data)
		assert.Contains(t, rendered, "Lint Warnings")
		assert.Contains(t, rendered, "[bad_table] Primary key uses signed integer type")
		assert.Contains(t, rendered, "CREATE TABLE")
		// Identical plans get deduplicated — combined header
		assert.Contains(t, rendered, "Staging & Production")
	})
}

func TestWebhookConcurrentRequests(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	var wg sync.WaitGroup
	n := 5

	for range n {
		wg.Go(func() {
			req := buildWebhookRequest(t, webhookPayloadOpts{
				comment: "schemabot help",
				isPR:    true,
			}, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Code)
		})
	}

	wg.Wait()

	// All 5 should produce comments
	received := 0
	for {
		select {
		case body := <-comments:
			assert.Contains(t, body, "SchemaBot Help")
			received++
			if received == n {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only received %d/%d comments", received, n)
		}
	}
}

func TestWebhookRepoAllowlistRejectsUnregisteredRepo(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	factory := &fakeClientFactory{client: installClient}

	service := api.New(nil, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"org/allowed-repo": {},
		},
	}, nil, testLogger())

	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
		logger:    testLogger(),
	}

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "repository not registered")

	select {
	case body := <-comments:
		t.Fatalf("unexpected rejection comment: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWebhookRepoAllowlistAllowsRegisteredRepo(t *testing.T) {
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	factory := &fakeClientFactory{client: installClient}

	service := api.New(nil, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {},
		},
	}, nil, testLogger())

	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
		logger:    testLogger(),
	}

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot help",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "help posted")
}

func TestWebhookRepoAllowlistEmptyAllowsAll(t *testing.T) {
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	factory := &fakeClientFactory{client: installClient}

	service := api.New(nil, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{},
	}, nil, testLogger())

	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
		logger:    testLogger(),
	}

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot help",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "help posted")
}

func TestWebhookRepoAllowlistPullRequestRejectsUnregistered(t *testing.T) {
	service := api.New(nil, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"org/allowed-repo": {},
		},
	}, nil, testLogger())

	h := &Handler{
		service: service,
		logger:  testLogger(),
	}

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "repository not registered")
}

// PR close bypass of the allowlist is tested in the integration suite
// (TestE2EPRCloseCleanup) which has real storage via testcontainers.

// A panic inside a webhook-spawned goroutine must not crash the server:
// goSafe recovers it, logs the stack, and posts an error comment on the PR
// so the user gets feedback instead of silence. This is the recovery wrapper
// every async command dispatch (apply, plan, rollback, reactions) runs under.
func TestGoSafeRecoversPanicAndPostsErrorComment(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	installClient := ghclient.NewInstallationClient(client, testLogger())

	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}

	h.goSafe("octocat/hello-world", 1, 12345, func() {
		panic("boom")
	})

	body := requireComment(t, comments, "panic recovery comment")
	assert.Contains(t, body, "Internal error: goroutine panic")
	assert.Contains(t, body, "boom")
}
