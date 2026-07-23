package webhook

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/action"
)

// applyCommandCore returns a durability disposition (retry, err) that a future
// durable issue_comment driver consumes. The synchronous goSafe wrapper discards
// it, so these tests pin the contract directly on the core. See decision 0002.

// A command bootstrap failure (here, the per-installation GitHub client cannot be
// created) is a transient infrastructure failure the same delivery could clear on
// a later attempt, so the core must report it as retryable with a non-nil error.
func TestApplyCommandCoreBootstrapFailureIsRetryable(t *testing.T) {
	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{
			forInstallationErr: errors.New("installation token unavailable"),
		}),
		logger: testLogger(),
	}

	retry, err := h.applyCommandCore("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.Error(t, err)
	assert.True(t, retry, "a command bootstrap failure is a transient infra failure a durable driver should re-drive")
}

// Terminal outcomes are the command's answer — a static skip or a config-shape
// rejection the same input will always produce — so the core reports them as
// (retry=false, err=nil): a durable driver must not re-drive them.
func TestApplyCommandCoreTerminalDispositions(t *testing.T) {
	// An unscoped fan-out apply for a database this deployment does not own is a
	// deliberate silent no-op, not a failure.
	t.Run("unowned unscoped fan-out is terminal and silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		retry, err := h.applyCommandCore("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		require.NoError(t, err)
		assert.False(t, retry, "a non-owning fan-out skip is the command's terminal answer, not a retryable failure")
		assert.Empty(t, comments, "the terminal skip must stay silent")
	})

	// A schema-request rejection (database not configured on this server) posts
	// the command's answer as a PR comment; re-driving would only re-post it, so
	// it is terminal despite the visible error comment.
	t.Run("schema request rejection is terminal", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, nonAggregateConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		retry, err := h.applyCommandCore("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		require.NoError(t, err)
		assert.False(t, retry, "a config-shape rejection is the command's answer, not a transient failure")
		body := requireComment(t, comments, "database-not-configured apply error")
		assert.Contains(t, body, `database "orders" is not configured on this server`)
	})
}
