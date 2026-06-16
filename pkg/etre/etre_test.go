package etre

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
