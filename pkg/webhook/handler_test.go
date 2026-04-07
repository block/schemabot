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

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func testLogger() *slog.Logger {
	return slog.Default()
}

func TestVerifySignature(t *testing.T) {
	secret := []byte("test-secret")
	h := &Handler{webhookSecret: secret}

	body := []byte(`{"test": "payload"}`)

	// Generate valid signature
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	t.Run("valid signature", func(t *testing.T) {
		assert.True(t, h.verifySignature(validSig, body))
	})

	t.Run("invalid signature", func(t *testing.T) {
		assert.False(t, h.verifySignature("sha256=deadbeef", body))
	})

	t.Run("empty signature", func(t *testing.T) {
		assert.False(t, h.verifySignature("", body))
	})

	t.Run("wrong prefix", func(t *testing.T) {
		assert.False(t, h.verifySignature("sha1=abc", body))
	})

	t.Run("invalid hex", func(t *testing.T) {
		assert.False(t, h.verifySignature("sha256=not-hex!", body))
	})
}

func TestWebhookRejectsInvalidSignature(t *testing.T) {
	h := &Handler{webhookSecret: []byte("secret"), logger: testLogger()}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(`{}`))
	req.Header.Set("X-Hub-Signature-256", "sha256=invalid")
	req.Header.Set("X-GitHub-Event", "issue_comment")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusUnauthorized, rr.Code)
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

	h := &Handler{
		ghClient: factory,
		logger:   testLogger(),
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

func TestWebhookEyesReaction(t *testing.T) {
	h, _, reactions := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot help",
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

	// Test remaining Phase 2 commands (apply, apply-confirm, unlock are now implemented)
	cmds := []struct {
		comment string
		action  string
	}{
		{"schemabot stop -e staging", "stop"},
		{"schemabot revert -e staging", "revert"},
		{"schemabot cutover -e staging", "cutover"},
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
	h.webhookSecret = secret

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

func TestWebhookPRClosedTriggersCleanup(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "closed"}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "PR close cleanup started")
}

func TestWebhookPREditIgnored(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "edited"}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "action ignored")
}

func TestWebhookPlanWithLintWarnings(t *testing.T) {
	// Verify that lint warnings from LintSchema are rendered in the plan comment.
	// This tests the template rendering path that the handler uses when posting
	// plan comments — both single-env and multi-env.
	t.Run("single env plan with lint warnings", func(t *testing.T) {
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
			LintWarnings: []templates.LintWarningData{
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

	t.Run("multi env plan with lint warnings", func(t *testing.T) {
		changes := []templates.KeyspaceChangeData{
			{
				Keyspace: "testapp",
				Statements: []string{
					"CREATE TABLE `bad_table` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
				},
			},
		}
		lintWarnings := []templates.LintWarningData{
			{Message: "Primary key uses signed integer type", Table: "bad_table", LinterName: "primary_key"},
		}

		data := templates.MultiEnvPlanCommentData{
			Database:     "testapp",
			IsMySQL:      true,
			RequestedBy:  "testuser",
			Environments: []string{"staging", "production"},
			Plans: map[string]*templates.PlanCommentData{
				"staging":    {Database: "testapp", Environment: "staging", IsMySQL: true, Changes: changes, LintWarnings: lintWarnings},
				"production": {Database: "testapp", Environment: "production", IsMySQL: true, Changes: changes, LintWarnings: lintWarnings},
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
