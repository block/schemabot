package github

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	gh "github.com/google/go-github/v86/github"
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

// Only a 404 from the installation lookup means the App is not installed on the
// repository. Every other failure leaves the App's coverage of the repository
// unknown, so it must stay distinguishable from a confirmed absence: a caller
// that skips an uninstalled repository must never skip one GitHub simply failed
// to answer for.
func TestFindInstallationErrorClassifiesOnlyA404AsNotInstalled(t *testing.T) {
	cases := []struct {
		name         string
		status       int
		notInstalled bool
	}{
		{name: "not found", status: http.StatusNotFound, notInstalled: true},
		{name: "server error", status: http.StatusInternalServerError},
		{name: "bad gateway", status: http.StatusBadGateway},
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "forbidden", status: http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/repos/octo/repo/installation", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, `{"message":"status"}`)
			}))
			t.Cleanup(server.Close)
			client := gh.NewClient(server.Client())
			baseURL, err := url.Parse(server.URL + "/")
			require.NoError(t, err)
			client.BaseURL = baseURL

			_, _, lookupErr := client.Apps.FindRepositoryInstallation(t.Context(), "octo", "repo")
			require.Error(t, lookupErr)

			err = findInstallationError("octo/repo", lookupErr)
			assert.Equal(t, tc.notInstalled, errors.Is(err, ErrAppNotInstalled))
			assert.Contains(t, err.Error(), "find installation for octo/repo")
			assert.Contains(t, err.Error(), strconv.Itoa(tc.status), "the GitHub status stays in the error for triage")
		})
	}
}

// A lookup that never reached GitHub says nothing about whether the App is
// installed.
func TestFindInstallationErrorTransportFailureIsNotNotInstalled(t *testing.T) {
	err := findInstallationError("octo/repo", fmt.Errorf("dial tcp: connection refused"))
	assert.NotErrorIs(t, err, ErrAppNotInstalled)
	assert.Contains(t, err.Error(), "connection refused")
}
