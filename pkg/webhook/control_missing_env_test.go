package webhook

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
)

// storedApplyStorage serves one stored apply to the missing-environment
// lookup, and empty answers to every other read.
type storedApplyStorage struct {
	emptyStorage
	apply *storage.Apply
}

func (s *storedApplyStorage) Applies() storage.ApplyStore {
	return storedApplies{ApplyStore: &emptyApplyStore{}, apply: s.apply}
}

type storedApplies struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (a storedApplies) GetByApplyIdentifier(_ context.Context, identifier string) (*storage.Apply, error) {
	if a.apply == nil || identifier != a.apply.ApplyIdentifier {
		return nil, nil
	}
	return a.apply, nil
}

func stagingApplyOfThisPR() *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: "apply-abc123",
		Database:        "orders",
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
	}
}

// TestControlCommandMissingEnvironmentNamesTheApplysEnvironment covers a
// control command that names an apply but omits -e. Each deployment keeps
// its own storage, so the deployment that stores the apply answers with the
// exact command, including on an instance that otherwise ignores commands
// without -e, and every other deployment on the repo stays silent so the PR
// gets exactly one reply.
func TestControlCommandMissingEnvironmentNamesTheApplysEnvironment(t *testing.T) {
	serve := func(t *testing.T, cfg *api.ServerConfig, apply *storage.Apply, comment string) (string, chan string) {
		t.Helper()
		h, _, comments := newFanOutSkipHandler(t, cfg)
		h.service = api.New(&storedApplyStorage{apply: apply}, cfg, nil, testLogger())

		req := buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		return rr.Body.String(), comments
	}
	silentOnUnscoped := func(cfg *api.ServerConfig) *api.ServerConfig {
		respond := false
		cfg.RespondToUnscoped = &respond
		return cfg
	}

	t.Run("the deployment storing the apply replies even when it ignores unscoped commands", func(t *testing.T) {
		resp, comments := serve(t, silentOnUnscoped(nonAggregateConfig()), stagingApplyOfThisPR(), "schemabot stop apply-abc123")

		assert.Contains(t, resp, "missing environment flag")
		body := requireComment(t, comments, "missing environment comment")
		assert.Contains(t, body, "Apply `apply-abc123` runs in `staging`")
		assert.NotContains(t, body, "<environment>", "the reply names the apply's environment instead of a placeholder")
		assert.Contains(t, body, "schemabot stop apply-abc123 -e staging")
	})

	t.Run("an aggregate participant storing the apply replies", func(t *testing.T) {
		resp, comments := serve(t, aggregateParticipantConfig(), stagingApplyOfThisPR(), "schemabot cutover apply-abc123")

		assert.Contains(t, resp, "missing environment flag")
		body := requireComment(t, comments, "missing environment comment")
		assert.Contains(t, body, "schemabot cutover apply-abc123 -e staging")
	})

	t.Run("an environment-scoped deployment without the apply stays silent", func(t *testing.T) {
		cfg := nonAggregateConfig()
		cfg.AllowedEnvironments = []string{"production"}
		resp, comments := serve(t, cfg, nil, "schemabot stop apply-abc123")

		assert.Contains(t, resp, "missing environment deferred to the apply's deployment")
		assert.Empty(t, comments, "a deployment that does not store the apply must leave the reply to the one that does")
	})

	t.Run("an aggregate leader without the apply stays silent", func(t *testing.T) {
		resp, comments := serve(t, aggregateLeaderConfig(), nil, "schemabot cancel apply-abc123")

		assert.Contains(t, resp, "missing environment deferred to the apply's deployment")
		assert.Empty(t, comments, "a deployment that does not store the apply must leave the reply to the one that does")
	})

	t.Run("an apply in an environment this deployment does not serve is left to its deployment", func(t *testing.T) {
		cfg := nonAggregateConfig()
		cfg.AllowedEnvironments = []string{"production"}
		resp, comments := serve(t, cfg, stagingApplyOfThisPR(), "schemabot stop apply-abc123")

		assert.Contains(t, resp, "missing environment deferred to the apply's deployment")
		assert.Empty(t, comments)
	})

	t.Run("an apply of another PR is refused", func(t *testing.T) {
		apply := stagingApplyOfThisPR()
		apply.PullRequest = 7
		resp, comments := serve(t, silentOnUnscoped(nonAggregateConfig()), apply, "schemabot stop apply-abc123")

		assert.Contains(t, resp, "apply belongs to another PR")
		body := requireComment(t, comments, "error comment")
		assert.Contains(t, body, "Apply apply-abc123 is not associated with this PR")
		assert.NotContains(t, body, "-e staging", "the reply must not offer a command for another PR's apply")
	})

	t.Run("a single deployment without the apply posts the usual missing-environment reply", func(t *testing.T) {
		resp, comments := serve(t, nonAggregateConfig(), nil, "schemabot stop apply-def456")

		assert.Contains(t, resp, "missing environment flag")
		body := requireComment(t, comments, "missing environment comment")
		assert.Contains(t, body, "schemabot stop -e <environment>")
	})
}
