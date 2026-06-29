package github

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A previously-resolved installation id is served from cache without minting an
// App JWT or calling GitHub — repo-webhook deliveries for an already-seen repo
// must not pay a network round trip per event.
func TestInstallationIDForRepoCacheHit(t *testing.T) {
	c := &Client{
		repoInstallations: map[string]int64{"octocat/hello-world": 42},
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	id, err := c.InstallationIDForRepo(t.Context(), "octocat/hello-world")
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)
}

// A repo string that is not in "owner/name" form is rejected before any GitHub
// call, with an error naming the offending value so the cause is obvious from
// logs alone.
func TestInstallationIDForRepoInvalidForm(t *testing.T) {
	c := &Client{
		repoInstallations: map[string]int64{},
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	_, err := c.InstallationIDForRepo(t.Context(), "noslash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "owner/name form")
	assert.Contains(t, err.Error(), "noslash")
}
