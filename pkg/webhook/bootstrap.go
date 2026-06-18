package webhook

import (
	"context"
	"fmt"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
)

// commandTimeout is the default deadline for processing a single PR command.
// Command handlers run as fire-and-forget goroutines after the webhook delivery
// has been acknowledged; the timeout bounds their lifetime so an unresponsive
// downstream (GitHub API, MySQL, Spirit) cannot leak a driver indefinitely.
const commandTimeout = 2 * time.Minute

// commandBootstrap sets up the standard per-command context with a deadline
// and the request-scoped PR-info cache, then creates a per-installation
// GitHub client.
//
// The PR-info cache wrapping is mandatory: command handlers call
// FetchPullRequest from multiple places (config discovery, review gate,
// checks gate, etc.) and the cache dedupes those calls within a single
// command invocation.
//
// Always safe to `defer cancel()` immediately after the call, even on
// error: on error the returned context is already cancelled and the
// returned cancel func is a no-op. The caller is responsible for
// handler-specific error reporting (HTTP response, PR comment, log line)
// because that contract varies between handlers.
func (h *Handler) commandBootstrap(repo string, installationID int64) (context.Context, context.CancelFunc, *ghclient.InstallationClient, error) {
	ctx, cancel := h.commandContext(commandTimeout)
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		cancel()
		return ctx, func() {}, nil, fmt.Errorf("create GitHub client for repo %q installation %d: %w", repo, installationID, err)
	}
	return ctx, cancel, client, nil
}

// commandContext returns the standard per-command context: a bounded deadline
// plus the request-scoped PR-info cache. Use this for handlers that do not
// need a GitHub client up front (e.g. handleUnlockCommand does its work
// directly against storage and only creates a client later, conditionally).
func (h *Handler) commandContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	ctx = ghclient.WithPRInfoCache(ctx)
	return ctx, cancel
}
