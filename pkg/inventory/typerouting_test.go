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
