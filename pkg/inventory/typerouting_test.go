package inventory

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeResolver struct {
	target *Target
	gotReq Request
}

type fakeEnumerableResolver struct {
	fakeResolver
	requests []ProbeRequest
	err      error
}

func (f *fakeEnumerableResolver) Enumerate(context.Context) ([]ProbeRequest, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.requests, nil
}

func (f *fakeEnumerableResolver) UnenumerableDatabaseTypes() []string {
	return nil
}

var _ Enumerator = (*fakeEnumerableResolver)(nil)

func (f *fakeResolver) ResolveTarget(_ context.Context, req Request) (*Target, error) {
	f.gotReq = req
	return f.target, nil
}

// A data plane serving multiple engines composes one resolver per type and lets
// the request's database type select among them.
func TestTypeRoutingResolverDispatchesByType(t *testing.T) {
	mysql := &fakeResolver{target: &Target{Target: "m", DatabaseType: "mysql"}}
	vitess := &fakeResolver{target: &Target{Target: "v", DatabaseType: "vitess"}}
	r, err := NewTypeRoutingResolver(map[string]Resolver{"mysql": mysql, "vitess": vitess})
	require.NoError(t, err)

	got, err := r.ResolveTarget(t.Context(), Request{Target: "dsid-1", DatabaseType: "vitess"})
	require.NoError(t, err)
	assert.Equal(t, "v", got.Target)
	assert.Equal(t, "dsid-1", vitess.gotReq.Target, "the request is passed through to the engine resolver")
	assert.Empty(t, mysql.gotReq.Target, "the mysql resolver is not consulted for a vitess request")
}

// A startup probe walks every enumerable child in database-type order, so the
// combined request list and the list of types that cannot be probed are the
// same on every run regardless of how the type map iterates. Each child keeps
// its own internal order; only the concatenation order is the router's.
func TestTypeRoutingResolverEnumerate(t *testing.T) {
	mysql := &fakeEnumerableResolver{requests: []ProbeRequest{
		{Target: "mysql-z", DatabaseType: "mysql"},
		{Target: "mysql-a", DatabaseType: "mysql"},
	}}
	strata := &fakeEnumerableResolver{requests: []ProbeRequest{
		{Target: "strata-1", DatabaseType: "strata"},
	}}
	vitess := &fakeEnumerableResolver{requests: []ProbeRequest{
		{Target: "vitess-1", DatabaseType: "vitess"},
	}}
	postgres := &fakeResolver{}
	planetscale := &fakeResolver{}
	_, offersEnumeration := any(postgres).(Enumerator)
	assert.False(t, offersEnumeration)

	r, err := NewTypeRoutingResolver(map[string]Resolver{
		"vitess":      vitess,
		"postgres":    postgres,
		"strata":      strata,
		"planetscale": planetscale,
		"mysql":       mysql,
	})
	require.NoError(t, err)

	got, err := r.Enumerate(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []ProbeRequest{
		{Target: "mysql-z", DatabaseType: "mysql"},
		{Target: "mysql-a", DatabaseType: "mysql"},
		{Target: "strata-1", DatabaseType: "strata"},
		{Target: "vitess-1", DatabaseType: "vitess"},
	}, got)
	assert.Equal(t, []string{"planetscale", "postgres"}, r.UnenumerableDatabaseTypes())
}

// A router with nothing enumerable under it reports every registered type as
// uncovered and an empty list, which is what a data plane built from
// discovery-only resolvers looks like to the startup probe.
func TestTypeRoutingResolverEnumerateNothingEnumerable(t *testing.T) {
	r, err := NewTypeRoutingResolver(map[string]Resolver{
		"vitess": &fakeResolver{},
		"mysql":  &fakeResolver{},
	})
	require.NoError(t, err)

	got, err := r.Enumerate(t.Context())
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, []string{"mysql", "vitess"}, r.UnenumerableDatabaseTypes())
}

// A child that cannot list its targets fails the whole enumeration: a partial
// list would let a probe report the failed child's targets as covered when
// their state is unknown.
func TestTypeRoutingResolverEnumerateFailsClosedOnChildError(t *testing.T) {
	mysql := &fakeEnumerableResolver{requests: []ProbeRequest{{Target: "orders", DatabaseType: "mysql"}}}
	postgres := &fakeEnumerableResolver{err: errors.New("discovery unavailable")}
	r, err := NewTypeRoutingResolver(map[string]Resolver{"mysql": mysql, "postgres": postgres})
	require.NoError(t, err)

	got, err := r.Enumerate(t.Context())
	require.Error(t, err)
	assert.Nil(t, got)
	assert.EqualError(t, err, "enumerate postgres targets: discovery unavailable")
	assert.ErrorIs(t, err, postgres.err)
}

// A router nested under another router is itself enumerable, so the outer
// report has to ask it what it could not list rather than assume it covered
// everything registered under its key.
func TestTypeRoutingResolverUnenumerableDatabaseTypesIncludesNestedGaps(t *testing.T) {
	inner, err := NewTypeRoutingResolver(map[string]Resolver{
		"postgres": &fakeEnumerableResolver{requests: []ProbeRequest{{Target: "billing", DatabaseType: "postgres"}}},
		"strata":   &fakeResolver{},
		"vitess":   &fakeResolver{},
	})
	require.NoError(t, err)
	outer, err := NewTypeRoutingResolver(map[string]Resolver{
		"mysql":  &fakeEnumerableResolver{requests: []ProbeRequest{{Target: "orders", DatabaseType: "mysql"}}},
		"nested": inner,
		"vitess": &fakeResolver{},
	})
	require.NoError(t, err)

	got, err := outer.Enumerate(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []ProbeRequest{
		{Target: "orders", DatabaseType: "mysql"},
		{Target: "billing", DatabaseType: "postgres"},
	}, got)
	assert.Equal(t, []string{"strata", "vitess"}, outer.UnenumerableDatabaseTypes(),
		"the inner router's gaps surface once each, deduplicated against the outer's own")
}

// A typed-nil child passes construction because the interface value is not
// nil; enumeration has to report it as an error the way resolution does,
// never as a panic in a startup path.
func TestTypeRoutingResolverEnumerateTypedNilChild(t *testing.T) {
	var static *StaticResolver
	r, err := NewTypeRoutingResolver(map[string]Resolver{"mysql": static})
	require.NoError(t, err)

	_, err = r.ResolveTarget(t.Context(), Request{Target: "orders", DatabaseType: "mysql"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "static target resolver is nil")

	got, err := r.Enumerate(t.Context())
	require.Error(t, err)
	assert.Nil(t, got)
	assert.EqualError(t, err, "enumerate mysql targets: static target resolver is nil")
}

func TestTypeRoutingResolverRequiresDatabaseType(t *testing.T) {
	r, err := NewTypeRoutingResolver(map[string]Resolver{"mysql": &fakeResolver{}})
	require.NoError(t, err)

	_, err = r.ResolveTarget(t.Context(), Request{Target: "dsid-1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database type is required")
}

func TestTypeRoutingResolverFailsClosedForUnregisteredType(t *testing.T) {
	r, err := NewTypeRoutingResolver(map[string]Resolver{"mysql": &fakeResolver{}})
	require.NoError(t, err)

	_, err = r.ResolveTarget(t.Context(), Request{Target: "dsid-1", DatabaseType: "vitess"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no resolver registered for database type")
}

func TestNewTypeRoutingResolverValidatesConfig(t *testing.T) {
	_, err := NewTypeRoutingResolver(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one resolver")

	_, err = NewTypeRoutingResolver(map[string]Resolver{"": &fakeResolver{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database type key")

	_, err = NewTypeRoutingResolver(map[string]Resolver{"mysql": nil})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be nil")
}
