package inventory

import (
	"context"
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
}

func (f *fakeEnumerableResolver) Enumerate() []ProbeRequest {
	return f.requests
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

	assert.Equal(t, []ProbeRequest{
		{Target: "mysql-z", DatabaseType: "mysql"},
		{Target: "mysql-a", DatabaseType: "mysql"},
		{Target: "strata-1", DatabaseType: "strata"},
		{Target: "vitess-1", DatabaseType: "vitess"},
	}, r.Enumerate())
	assert.Equal(t, []string{"planetscale", "postgres"}, r.UnenumerableDatabaseTypes())
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
