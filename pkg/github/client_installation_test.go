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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A previously-resolved installation id is served from cache without minting an
// App JWT or calling GitHub — repo-webhook deliveries for an already-seen owner
// must not pay a network round trip per event.
func TestInstallationIDForRepoCacheHit(t *testing.T) {
	c := &Client{
		ownerInstallations: map[string]int64{"octocat": 42},
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
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
		ownerInstallations: map[string]int64{},
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
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

// installationLookupServer answers GitHub's installation lookup for any
// repository through the App-JWT transport and counts the lookups per
// owner/name. respond picks the status, installation id, and account login for
// each lookup; an id of zero with a 2xx status is never sent.
type installationLookupServer struct {
	mu      sync.Mutex
	lookups map[string]int
	respond func(owner, name string) (status int, id int64, account string)
}

func (s *installationLookupServer) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, count := range s.lookups {
		n += count
	}
	return n
}

func (s *installationLookupServer) count(repo string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups[repo]
}

func newInstallationLookupClient(t *testing.T, respond func(owner, name string) (int, int64, string)) (*Client, *installationLookupServer) {
	t.Helper()
	server := &installationLookupServer{lookups: map[string]int{}, respond: respond}
	client := &Client{
		appID:              1,
		privateKey:         testInstallationPrivateKey(t),
		logger:             slog.New(slog.DiscardHandler),
		ownerInstallations: map[string]int64{},
		appTransportBase: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
			if len(parts) != 4 || parts[0] != "repos" || parts[3] != "installation" {
				t.Errorf("unexpected App-JWT request %s %s", req.Method, req.URL.Path)
				return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(`{}`)), Header: http.Header{}, Request: req}, nil
			}
			owner, name := parts[1], parts[2]
			server.mu.Lock()
			server.lookups[owner+"/"+name]++
			server.mu.Unlock()
			status, id, account := server.respond(owner, name)
			body := `{"message":"status"}`
			if status < 300 {
				body = fmt.Sprintf(`{"id":%d,"account":{"login":%q}}`, id, account)
			}
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Request:    req,
			}, nil
		}),
	}
	client.storeAppSlug("schemabot")
	return client, server
}

// installedFor answers every lookup with one installation per owner, the way
// GitHub does for an App installed on each owner's account.
func installedFor(ids map[string]int64) func(owner, name string) (int, int64, string) {
	return func(owner, _ string) (int, int64, string) {
		id, ok := ids[strings.ToLower(owner)]
		if !ok {
			return http.StatusNotFound, 0, ""
		}
		return http.StatusOK, id, owner
	}
}

// A reconcile pass resolves the installation for every configured repository.
// A deployment serving many repositories of one organization pays one lookup
// for the owner, not one per repository, so a rollout of many replicas costs
// one lookup per replica.
func TestInstallationIDForRepoLooksUpEachOwnerOnce(t *testing.T) {
	client, server := newInstallationLookupClient(t, installedFor(map[string]int64{"octo": 7, "other": 9}))

	for i := range 174 {
		id, err := client.InstallationIDForRepo(t.Context(), fmt.Sprintf("octo/repo-%03d", i))
		require.NoError(t, err)
		assert.Equal(t, int64(7), id)
	}
	id, err := client.InstallationIDForRepo(t.Context(), "other/service")
	require.NoError(t, err)
	assert.Equal(t, int64(9), id)

	assert.Equal(t, 2, server.total(), "one lookup per owner")
	assert.Equal(t, 1, server.count("octo/repo-000"), "the owner's first repository pays the lookup")
	assert.Equal(t, 1, server.count("other/service"))
}

// GitHub logins are case-insensitive, so repositories written with different
// owner casing share one cached installation.
func TestInstallationIDForRepoOwnerIsCaseInsensitive(t *testing.T) {
	client, server := newInstallationLookupClient(t, installedFor(map[string]int64{"octo": 7}))

	id, err := client.InstallationIDForRepo(t.Context(), "Octo/a")
	require.NoError(t, err)
	assert.Equal(t, int64(7), id)
	id, err = client.InstallationIDForRepo(t.Context(), "octo/b")
	require.NoError(t, err)
	assert.Equal(t, int64(7), id)

	assert.Equal(t, 1, server.total())
	assert.Equal(t, 1, server.count("Octo/a"))
}

// A failed lookup caches nothing for the owner: the next repository asks
// GitHub again, and once that succeeds the owner is cached.
func TestInstallationIDForRepoDoesNotCacheAFailedLookup(t *testing.T) {
	var failed atomic.Bool
	client, server := newInstallationLookupClient(t, func(owner, name string) (int, int64, string) {
		if failed.CompareAndSwap(false, true) {
			return http.StatusBadGateway, 0, ""
		}
		return http.StatusOK, 7, owner
	})

	_, err := client.InstallationIDForRepo(t.Context(), "octo/a")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrAppNotInstalled, "a 502 leaves the App's coverage unknown")

	id, err := client.InstallationIDForRepo(t.Context(), "octo/b")
	require.NoError(t, err)
	assert.Equal(t, int64(7), id)
	id, err = client.InstallationIDForRepo(t.Context(), "octo/c")
	require.NoError(t, err)
	assert.Equal(t, int64(7), id)

	assert.Equal(t, 1, server.count("octo/a"))
	assert.Equal(t, 1, server.count("octo/b"))
	assert.Equal(t, 0, server.count("octo/c"), "the retry cached the owner")
}

// On a cold cache, a repository outside the App's installation still reports
// ErrAppNotInstalled, and the 404 caches nothing for its owner.
func TestInstallationIDForRepoNotInstalledOnAColdCache(t *testing.T) {
	client, server := newInstallationLookupClient(t, func(owner, name string) (int, int64, string) {
		if name == "outside" {
			return http.StatusNotFound, 0, ""
		}
		return http.StatusOK, 7, owner
	})

	_, err := client.InstallationIDForRepo(t.Context(), "octo/outside")
	require.ErrorIs(t, err, ErrAppNotInstalled)
	assert.Contains(t, err.Error(), "find installation for octo/outside")

	id, err := client.InstallationIDForRepo(t.Context(), "octo/inside")
	require.NoError(t, err)
	assert.Equal(t, int64(7), id)
	assert.Equal(t, 2, server.total())
}

// Concurrent misses for one owner, such as repo-level webhook deliveries
// arriving together right after a restart, share one lookup.
func TestInstallationIDForRepoCollapsesConcurrentMisses(t *testing.T) {
	client, server := newInstallationLookupClient(t, installedFor(map[string]int64{"octo": 7}))

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			id, err := client.InstallationIDForRepo(t.Context(), fmt.Sprintf("octo/repo-%d", i))
			assert.NoError(t, err)
			assert.Equal(t, int64(7), id)
		})
	}
	wg.Wait()

	assert.Equal(t, 1, server.total())
}

// An installation GitHub reports under another account cannot stand for the
// repository owner's other repositories, so it answers its own repository and
// is not cached for the owner.
func TestInstallationIDForRepoDoesNotCacheAnotherAccountsInstallation(t *testing.T) {
	client, server := newInstallationLookupClient(t, func(_, _ string) (int, int64, string) {
		return http.StatusOK, 7, "someone-else"
	})

	for _, repo := range []string{"octo/a", "octo/b"} {
		id, err := client.InstallationIDForRepo(t.Context(), repo)
		require.NoError(t, err)
		assert.Equal(t, int64(7), id)
	}
	assert.Equal(t, 2, server.total())
}
