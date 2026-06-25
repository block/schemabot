package etre

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/square/etre"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// New must produce a usable client when no HTTPClient is configured: the
// underlying Etre client does not default a nil HTTP client and would panic on
// the first request. This drives the real client over HTTP, not the mock.
func TestNewQueriesOverRealHTTPClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"orders","host":"orders.example:3306"}]`))
	}))
	defer srv.Close()

	c, err := New(Config{Addr: srv.URL, EntityType: "cluster"})
	require.NoError(t, err)

	got, err := c.QueryOne(t.Context(), map[string]string{"name": "orders"})
	require.NoError(t, err)
	assert.Equal(t, "orders.example:3306", StringField(got, "host"))
}

// mockClient builds an etre.MockEntityClient (shipped by the library) that
// records the query it received and returns the configured result.
func mockClient(gotQuery *string, entities []etre.Entity, queryErr error) etre.EntityClient {
	return etre.MockEntityClient{
		QueryFunc: func(_ context.Context, query string, _ etre.QueryFilter) ([]etre.Entity, error) {
			if gotQuery != nil {
				*gotQuery = query
			}
			return entities, queryErr
		},
	}
}

func TestQueryOneReturnsSingleMatch(t *testing.T) {
	entity := etre.Entity{"name": "orders", "region": "r1", "host": "orders.example:3306"}
	c := newClient(mockClient(nil, []etre.Entity{entity}, nil), "cluster", nil)

	got, err := c.QueryOne(t.Context(), map[string]string{"name": "orders", "region": "r1"})
	require.NoError(t, err)
	assert.Equal(t, "orders.example:3306", StringField(got, "host"))
}

// The selector is rendered into a deterministic, key-sorted Etre query.
func TestQueryOneBuildsSortedDeterministicQuery(t *testing.T) {
	var gotQuery string
	c := newClient(mockClient(&gotQuery, []etre.Entity{{"name": "orders"}}, nil), "cluster", nil)

	_, err := c.QueryOne(t.Context(), map[string]string{"region": "r1", "name": "orders", "env": "production"})
	require.NoError(t, err)
	assert.Equal(t, "env=production,name=orders,region=r1", gotQuery)
}

func TestQueryOneRequiresSelector(t *testing.T) {
	c := newClient(mockClient(nil, nil, nil), "cluster", nil)

	_, err := c.QueryOne(t.Context(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "selector is required")
}

func TestQueryOneFailsClosedWhenNotFound(t *testing.T) {
	c := newClient(mockClient(nil, nil, nil), "cluster", nil)

	_, err := c.QueryOne(t.Context(), map[string]string{"name": "missing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no etre")
}

// Not-found is a distinguishable sentinel so a resolver spanning multiple entity
// types can fall back on not-found while still aborting on ambiguous matches or
// query errors.
func TestQueryOneNotFoundIsErrNotFound(t *testing.T) {
	notFound := newClient(mockClient(nil, nil, nil), "cluster", nil)
	_, err := notFound.QueryOne(t.Context(), map[string]string{"name": "missing"})
	assert.ErrorIs(t, err, ErrNotFound)

	ambiguous := newClient(mockClient(nil, []etre.Entity{{"name": "x"}, {"name": "x"}}, nil), "cluster", nil)
	_, err = ambiguous.QueryOne(t.Context(), map[string]string{"name": "x"})
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrNotFound)

	queryErr := newClient(mockClient(nil, nil, fmt.Errorf("etre unreachable")), "cluster", nil)
	_, err = queryErr.QueryOne(t.Context(), map[string]string{"name": "x"})
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrNotFound)
}

// More than one match must fail closed rather than connect to an arbitrary one.
func TestQueryOneFailsClosedOnMultipleMatches(t *testing.T) {
	c := newClient(mockClient(nil, []etre.Entity{{"name": "orders"}, {"name": "orders"}}, nil), "cluster", nil)

	_, err := c.QueryOne(t.Context(), map[string]string{"name": "orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 etre")
	assert.Contains(t, err.Error(), "expected exactly one")
}

func TestQueryOnePropagatesQueryError(t *testing.T) {
	c := newClient(mockClient(nil, nil, fmt.Errorf("etre unreachable")), "cluster", nil)

	_, err := c.QueryOne(t.Context(), map[string]string{"name": "orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "etre unreachable")
}

func TestStringFieldHandlesMissingAndNonString(t *testing.T) {
	e := etre.Entity{"host": "db.example:3306", "port": 3306}
	assert.Equal(t, "db.example:3306", StringField(e, "host"))
	assert.Equal(t, "", StringField(e, "missing"))
	assert.Equal(t, "", StringField(e, "port"))
}

func TestNewValidatesConfig(t *testing.T) {
	_, err := New(Config{EntityType: "cluster"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "address is required")

	_, err = New(Config{Addr: "https://etre.example"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entity type is required")

	c, err := New(Config{Addr: "https://etre.example", EntityType: "cluster"})
	require.NoError(t, err)
	require.NotNil(t, c)
}

// Configured headers must be sent on every Etre request, so a deployment behind
// a header-routed proxy or mesh can supply the routing headers that path needs.
func TestNewSendsConfiguredHeaders(t *testing.T) {
	// Pass the captured headers back over a channel so the handler goroutine and
	// the test goroutine don't share a variable without synchronization.
	headerCh := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerCh <- r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"orders","host":"orders.example:3306"}]`))
	}))
	defer srv.Close()

	c, err := New(Config{
		Addr:       srv.URL,
		EntityType: "cluster",
		Headers:    map[string]string{"X-Env-Override": "production", "X-Route-Label": "etre"},
	})
	require.NoError(t, err)

	_, err = c.QueryOne(t.Context(), map[string]string{"name": "orders"})
	require.NoError(t, err)
	var gotHeaders http.Header
	select {
	case gotHeaders = <-headerCh:
	default:
		t.Fatal("Etre handler was not invoked")
	}
	assert.Equal(t, "production", gotHeaders.Get("X-Env-Override"))
	assert.Equal(t, "etre", gotHeaders.Get("X-Route-Label"))
}

// Wrapping a client to add headers must not mutate the original client's
// transport, so a shared client is safe to pass in.
func TestClientWithHeadersDoesNotMutateBase(t *testing.T) {
	base := &http.Client{}
	wrapped := clientWithHeaders(base, map[string]string{"X-Test": "1"})
	assert.Nil(t, base.Transport, "base client transport must be untouched")
	assert.NotNil(t, wrapped.Transport)
	assert.NotSame(t, base, wrapped)
}

// A configured unix socket makes every Etre request dial that socket instead of
// the Addr host — the path for reaching Etre through a local egress proxy. The
// Addr host is intentionally unresolvable to prove the socket is used, and the
// configured headers still ride along for the proxy to route by.
func TestNewDialsUnixSocket(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "etre.sock")
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "unix", socketPath)
	require.NoError(t, err)

	type captured struct {
		host    string
		headers http.Header
	}
	capCh := make(chan captured, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capCh <- captured{host: r.Host, headers: r.Header.Clone()}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"orders","host":"orders.example:3306"}]`))
	}))
	srv.Listener = ln
	srv.Start()
	defer srv.Close()

	c, err := New(Config{
		Addr:       "http://etre.invalid",
		EntityType: "cluster",
		UnixSocket: socketPath,
		Headers:    map[string]string{"X-Env-Override": "production"},
	})
	require.NoError(t, err)

	_, err = c.QueryOne(t.Context(), map[string]string{"name": "orders"})
	require.NoError(t, err)
	var got captured
	select {
	case got = <-capCh:
	default:
		t.Fatal("Etre handler was not invoked over the unix socket")
	}
	assert.Equal(t, "etre.invalid", got.host, "request Host (from Addr) is preserved for proxy routing")
	assert.Equal(t, "production", got.headers.Get("X-Env-Override"))
}
