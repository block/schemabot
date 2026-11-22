package api

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// A namespace resolution goes to the data plane of the member it names, with
// the database, its engine type, the environment, the target, and the
// namespaces to resolve, bounded by the Service's deadline. A data plane too
// old to answer fails the resolution with an error telling the operator to
// upgrade it, and one whose engine cannot answer fails without that advice.
func TestServiceResolveNamespaces(t *testing.T) {
	member := routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "eu", Target: "testapp"}

	t.Run("the member's data plane answers", func(t *testing.T) {
		var deadline time.Time
		eu := &mockTernClient{resolveNamespacesHook: func(ctx context.Context, req *ternv1.ResolveNamespacesRequest) (*ternv1.ResolveNamespacesResponse, error) {
			deadline, _ = ctx.Deadline()
			return &ternv1.ResolveNamespacesResponse{Scope: "shop_cluster", Schemas: map[string]string{"shop": "shop_main", "shop_archive": "shop_main"}}, nil
		}}
		us := &mockTernClient{}
		before := time.Now()

		placement, err := twoDeploymentService(t, eu, us).ResolveNamespaces(t.Context(), "testapp", "production", member, []string{"shop", "shop_archive"})
		require.NoError(t, err)
		assert.Equal(t, NamespacePlacement{Scope: "shop_cluster", Schemas: map[string]string{"shop": "shop_main", "shop_archive": "shop_main"}}, placement)

		require.Len(t, eu.resolveNamespacesReqs, 1)
		req := eu.resolveNamespacesReqs[0]
		assert.Equal(t, "testapp", req.GetDatabase())
		assert.Equal(t, storage.DatabaseTypeMySQL, req.GetType())
		assert.Equal(t, "production", req.GetEnvironment())
		assert.Equal(t, "testapp", req.GetTarget())
		assert.Equal(t, []string{"shop", "shop_archive"}, req.GetNamespaces())
		assert.Empty(t, us.resolveNamespacesReqs, "another member's data plane is never asked")
		assert.False(t, deadline.Before(before.Add(namespaceResolutionTimeout)), "deadline %s is earlier than the resolution timeout", deadline)
		assert.False(t, deadline.After(time.Now().Add(namespaceResolutionTimeout)), "deadline %s is later than the resolution timeout", deadline)
	})

	t.Run("a data plane too old to answer", func(t *testing.T) {
		eu := &mockTernClient{resolveNamespacesHook: func(context.Context, *ternv1.ResolveNamespacesRequest) (*ternv1.ResolveNamespacesResponse, error) {
			return nil, fmt.Errorf("remote data plane predates namespace resolution: %w", tern.ErrResolveNamespacesUnimplemented)
		}}
		_, err := twoDeploymentService(t, eu, &mockTernClient{}).ResolveNamespaces(t.Context(), "testapp", "production", member, []string{"shop"})
		require.ErrorIs(t, err, tern.ErrResolveNamespacesUnimplemented)
		assert.ErrorContains(t, err, `the data plane of deployment "eu" cannot report which schemas the namespaces of database "testapp" address on target "testapp". Upgrade that data plane`)
	})

	t.Run("a data plane whose engine cannot answer", func(t *testing.T) {
		eu := &mockTernClient{resolveNamespacesHook: func(context.Context, *ternv1.ResolveNamespacesRequest) (*ternv1.ResolveNamespacesResponse, error) {
			return nil, fmt.Errorf("remote data plane cannot resolve namespaces: %w", tern.ErrResolveNamespacesUnsupported)
		}}
		_, err := twoDeploymentService(t, eu, &mockTernClient{}).ResolveNamespaces(t.Context(), "testapp", "production", member, []string{"shop"})
		require.ErrorIs(t, err, tern.ErrResolveNamespacesUnsupported)
		assert.ErrorContains(t, err, `the data plane of deployment "eu" cannot resolve the namespaces of database "testapp" of type "mysql" on target "testapp"`)
		assert.NotContains(t, err.Error(), "Upgrade")
	})

	t.Run("a resolution that fails", func(t *testing.T) {
		eu := &mockTernClient{resolveNamespacesHook: func(context.Context, *ternv1.ResolveNamespacesRequest) (*ternv1.ResolveNamespacesResponse, error) {
			return nil, errors.New(`target does not authorize namespace "shop"`)
		}}
		_, err := twoDeploymentService(t, eu, &mockTernClient{}).ResolveNamespaces(t.Context(), "testapp", "production", member, []string{"shop"})
		require.ErrorContains(t, err, `resolve namespaces of database "testapp" on target "testapp" of deployment "eu" in environment "production"`)
		assert.ErrorContains(t, err, "does not authorize namespace")
		assert.NotErrorIs(t, err, tern.ErrResolveNamespacesUnsupported)
	})
}
