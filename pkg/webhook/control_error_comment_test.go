package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// controlOperationApplyStore serves the apply a control command loads. The
// first read is the webhook handler's own check of the apply; every later read
// is the control operation's, and fails with loadErr when one is set.
type controlOperationApplyStore struct {
	storage.ApplyStore
	apply   *storage.Apply
	loadErr error
	reads   atomic.Int32
}

func (s *controlOperationApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	if s.reads.Add(1) > 1 && s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.apply, nil
}

// controlStubTernClient stands in for an engine client that a refused control
// operation never calls.
type controlStubTernClient struct {
	tern.Client
}

func controlOperationTestApply(engine string) *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: "apply-123",
		Database:        "orders",
		Environment:     "staging",
		Deployment:      api.DefaultDeployment,
		Engine:          engine,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
	}
}

// A control command whose operation fails inside SchemaBot tells the PR which
// command did not complete and what to do next. The failure's own text, which
// here names an internal address, stays in the server log with the identifiers
// an operator needs to find it.
func TestControlCommandInternalFailureKeepsErrorOutOfComment(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	st := &planControlStorage{applies: &controlOperationApplyStore{
		apply:   controlOperationTestApply(storage.EngineSpirit),
		loadErr: errors.New(rawInfrastructureError),
	}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	h.handleStopCommand("octocat/hello-world", 1, 12345, "testuser", planControlCommandResult())

	body := requireComment(t, comments, "control operation failure comment")
	assert.Contains(t, body, "SchemaBot could not complete the stop command for apply `apply-123`. "+planControlRetryLine)
	requireNoInfrastructureDetail(t, body)
	assert.NotContains(t, body, "10.1.2.3")
	assert.NotContains(t, body, "failed to look up apply")
	logged := logs.String()
	assert.Contains(t, logged, "stop PR command failed")
	assert.Contains(t, logged, rawInfrastructureError)
	assert.Contains(t, logged, `"apply_id":"apply-123"`)
	assert.Contains(t, logged, `"repo":"octocat/hello-world"`)
	assert.Contains(t, logged, `"pr":1`)
	assert.Contains(t, logged, `"environment":"staging"`)
}

// A control command the operation refuses with text SchemaBot wrote shows that
// text, because it carries the next step the requester can take.
func TestControlCommandAuthoredRefusalShowsItsText(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	st := &planControlStorage{applies: &controlOperationApplyStore{
		apply: controlOperationTestApply(storage.EnginePlanetScale),
	}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
	h.service.RegisterTernClient(api.DefaultDeployment, "staging", &controlStubTernClient{})

	h.handleStopCommand("octocat/hello-world", 1, 12345, "testuser", planControlCommandResult())

	body := requireComment(t, comments, "control operation refusal comment")
	assert.Contains(t, body, "stop is not supported for this schema change; use cancel to permanently cancel it")
	assert.NotContains(t, body, "SchemaBot could not complete the stop command")
}

// A control command's failure line keeps an untyped failure's text, and any
// wrap around it, out of the comment.
func TestControlOperationFailureMessage(t *testing.T) {
	t.Run("an untyped failure asks for another attempt", func(t *testing.T) {
		got := controlOperationFailureMessage(errors.New(rawInfrastructureError), "cutover", "apply-123")

		assert.Equal(t, "SchemaBot could not complete the cutover command for apply `apply-123`. "+planControlRetryLine, got)
	})

	t.Run("a wrapped untyped failure keeps the wrap out of the line", func(t *testing.T) {
		wrapped := fmt.Errorf("failed to look up apply %q: %w", "apply-123", errors.New(rawInfrastructureError))

		got := controlOperationFailureMessage(wrapped, "skip-revert", "apply-123")

		assert.NotContains(t, got, "10.1.2.3")
		assert.Equal(t, "SchemaBot could not complete the skip-revert command for apply `apply-123`. "+planControlRetryLine, got)
	})
}
