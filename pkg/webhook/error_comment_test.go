package webhook

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// postCommandError must always render a non-empty timestamp in the comment
// footer. A regression of the original bug renders "at  UTC" (two spaces) and
// is what the helper exists to prevent.
func TestPostCommandError_RendersTimestamp(t *testing.T) {
	original := templates.NowFunc
	t.Cleanup(func() { templates.NowFunc = original })
	templates.NowFunc = func() time.Time {
		return time.Date(2026, 5, 26, 12, 34, 56, 0, time.UTC)
	}

	client, mux := setupGitHubServer(t)
	bodies := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	h.postCommandError(
		"octocat/hello-world", 1, 12345,
		"plan", "staging", "alice",
		"boom",
	)

	select {
	case body := <-bodies:
		assert.NotContains(t, body, "at  UTC",
			"comment must not render empty timestamp")
		assert.Contains(t, body, "at 2026-05-26 12:34:56 UTC",
			"comment must render the stubbed timestamp from templates.NowFunc")
		assert.Contains(t, body, "*Requested by @alice")
		assert.Contains(t, body, "Plan Failed")
		assert.Contains(t, body, "`staging`")
		assert.Contains(t, body, "> boom")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for posted comment")
	}
}

func TestPostCommandErrorExplainsRemoteUnavailable(t *testing.T) {
	client, mux := setupGitHubServer(t)
	bodies := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	h.postCommandError(
		"octocat/hello-world", 1, 12345,
		"plan", "staging", "alice",
		"rpc error: code = Unavailable desc = no healthy upstream",
	)

	select {
	case body := <-bodies:
		assert.Contains(t, body, "SchemaBot could not reach the remote schema change service")
		assert.Contains(t, body, "No healthy upstream is available")
		assert.Contains(t, body, "If the problem persists, contact your SchemaBot operators")
		assert.NotContains(t, body, "rpc error")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for posted comment")
	}
}

// Internal operations (storage, GitHub API, lock bookkeeping) render a fixed
// summary with retry guidance and an error reference — their raw error text
// can carry hostnames, DSN fragments, or driver internals that must not reach
// a PR, so the reference is what links a user report to the server-side log.
func TestInternalErrorDetail(t *testing.T) {
	t.Run("retry variant renders retry guidance and the reference", func(t *testing.T) {
		detail := internalErrorDetail("Failed to acquire the apply lock.", "ab12cd34")

		assert.Equal(t, "Failed to acquire the apply lock. Internal SchemaBot error. Retry (error reference `ab12cd34`).", detail)
	})

	t.Run("no-retry variant omits the retry guidance", func(t *testing.T) {
		detail := internalErrorDetailNoRetry("Apply was accepted, but SchemaBot could not update the required status check.", "ab12cd34")

		assert.Equal(t, "Apply was accepted, but SchemaBot could not update the required status check. Internal SchemaBot error (error reference `ab12cd34`).", detail)
		assert.NotContains(t, detail, "retry")
	})
}

// Error references are short lowercase-hex identifiers safe to render inline
// in a PR comment, and distinct per failure so operators can match a report to
// one log line.
func TestNewErrorReference(t *testing.T) {
	ref := newErrorReference()

	assert.Regexp(t, "^[0-9a-f]{8}$", ref)
	assert.NotEqual(t, ref, newErrorReference())
}

func TestMappedUserFacingError(t *testing.T) {
	t.Run("remote deployment unavailable maps to fixed guidance", func(t *testing.T) {
		err := &api.RemoteDeploymentUnavailableError{
			Deployment: "pie",
			Target:     "orders-staging",
			Err:        status.Error(codes.Unavailable, "connection error: dial tcp 10.0.0.1:443"),
		}

		message, ok := mappedUserFacingError(err)

		require.True(t, ok)
		assert.Contains(t, message, "SchemaBot could not reach the remote deployment `pie`")
		assert.Contains(t, message, "If the problem persists, contact your SchemaBot operators")
		assert.NotContains(t, message, "10.0.0.1")
	})

	t.Run("grpc unavailable maps to fixed guidance", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "connection error: dial tcp 10.0.0.1:443")

		message, ok := mappedUserFacingError(err)

		require.True(t, ok)
		assert.Contains(t, message, "SchemaBot could not reach the remote schema change service")
		assert.NotContains(t, message, "10.0.0.1")
	})

	t.Run("other errors are left for the caller", func(t *testing.T) {
		message, ok := mappedUserFacingError(errors.New("invalid DDL"))

		assert.False(t, ok)
		assert.Empty(t, message)
	})
}

// Per-environment error cells render configuration and discovery errors whose
// wording SchemaBot authored; anything else is an internal failure and renders
// the fixed internal-error guidance with the caller's error reference.
func TestUserFacingSchemaRequestError(t *testing.T) {
	t.Run("environment config error renders its message", func(t *testing.T) {
		err := environmentConfigErrorf("database %q environment %q is not configured on this server", "orders", "staging")

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Equal(t, `database "orders" environment "staging" is not configured on this server`, got)
	})

	t.Run("wrapped environment config error renders the full message", func(t *testing.T) {
		err := fmt.Errorf("resolve configured environments for database %q: %w", "orders",
			asEnvironmentConfigError(&api.DatabaseNotConfiguredError{Database: "orders"}))

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Contains(t, got, `database "orders" is not configured on this server`)
	})

	t.Run("database not found renders its message", func(t *testing.T) {
		err := &ghclient.DatabaseNotFoundError{DatabaseName: "orders", AvailableDatabases: []string{"users"}}

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Equal(t, err.Error(), got)
	})

	t.Run("no schema files found renders its message", func(t *testing.T) {
		err := &ghclient.NoSchemaFilesError{SchemaRoot: "schema/orders", Environment: "staging"}

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Equal(t, `no schema files found under schema/orders for environment "staging"`, got)
	})

	t.Run("no schema files found names the ignored namespaces that were excluded", func(t *testing.T) {
		err := &ghclient.NoSchemaFilesError{
			SchemaRoot:        "schema/orders",
			Environment:       "staging",
			IgnoredNamespaces: []string{"fixtures_staging"},
		}

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Equal(t, `no schema files found under schema/orders for environment "staging" after excluding ignored namespaces [fixtures_staging]`, got)
	})

	t.Run("config sentinels render their message", func(t *testing.T) {
		err := fmt.Errorf("discover schema configs: %w", ghclient.ErrNoConfig)

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Equal(t, err.Error(), got)
	})

	t.Run("truncated repository tree renders its fail-closed message", func(t *testing.T) {
		err := fmt.Errorf("discover schemabot configs in repo octocat/hello-world ref abc123: %w", ghclient.ErrGitTreeTruncated)

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Equal(t, err.Error(), got)
	})

	t.Run("internal errors render the fixed internal-error guidance", func(t *testing.T) {
		err := errors.New(`fetch repository content at staging: GET http://10.0.0.1/repos: 500`)

		got := userFacingSchemaRequestError(err, "ab12cd34")

		assert.Equal(t, internalErrorDetail("Failed to prepare the schema change request for this environment.", "ab12cd34"), got)
		assert.NotContains(t, got, "10.0.0.1")
	})
}

// asEnvironmentConfigError must keep the original error in the chain so
// fan-out silent-skip logic can still branch on typed causes.
func TestEnvironmentConfigErrorPreservesCause(t *testing.T) {
	cause := &api.DatabaseNotConfiguredError{Database: "orders"}
	err := fmt.Errorf("resolve configured environments for database %q: %w", "orders", asEnvironmentConfigError(cause))

	var notConfigured *api.DatabaseNotConfiguredError
	require.ErrorAs(t, err, &notConfigured)
	assert.Equal(t, "orders", notConfigured.Database)

	var envConfigErr *environmentConfigError
	assert.ErrorAs(t, err, &envConfigErr)
}

// Control commands (start, stop, cancel) render operator-actionable guardrail
// rejections verbatim; untyped errors are internal failures (storage, Tern)
// and render the fixed internal-error guidance.
func TestControlCommandErrorDetail(t *testing.T) {
	t.Run("internal error renders the fixed internal-error guidance", func(t *testing.T) {
		err := errors.New("query apply: dial tcp 10.0.0.1:3306: connect: connection refused")

		got := controlCommandErrorDetail("stop", err, "ab12cd34")

		assert.Equal(t, internalErrorDetail("Failed to execute the stop command.", "ab12cd34"), got)
		assert.NotContains(t, got, "10.0.0.1")
	})

	t.Run("remote unavailable renders the fixed transport guidance", func(t *testing.T) {
		got := controlCommandErrorDetail("start", status.Error(codes.Unavailable, "no healthy upstream"), "ab12cd34")

		assert.Contains(t, got, "SchemaBot could not reach the remote schema change service")
		assert.NotContains(t, got, "rpc error")
	})
}

// Force unlock infers the database from the PR's schema configs; the two
// discovery outcomes with a user-side remedy render guidance, everything else
// is an internal failure.
func TestInferUnlockDatabaseErrorDetail(t *testing.T) {
	t.Run("multiple configs suggests scoping the command", func(t *testing.T) {
		got := inferUnlockDatabaseErrorDetail(fmt.Errorf("resolve config: %w", ghclient.ErrMultipleConfigs), "ab12cd34")

		assert.Contains(t, got, "Multiple SchemaBot configs match this PR")
		assert.Contains(t, got, "schemabot unlock -d <database> --force")
	})

	t.Run("no config explains there is nothing to unlock", func(t *testing.T) {
		got := inferUnlockDatabaseErrorDetail(fmt.Errorf("resolve config: %w", ghclient.ErrNoConfig), "ab12cd34")

		assert.Contains(t, got, "No SchemaBot config was found for this PR")
	})

	t.Run("internal error renders the fixed internal-error guidance", func(t *testing.T) {
		got := inferUnlockDatabaseErrorDetail(errors.New("list PR files: 500"), "ab12cd34")

		assert.Equal(t, internalErrorDetail("Failed to infer the database for force unlock.", "ab12cd34"), got)
	})
}
