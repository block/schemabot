package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type overrideRotatingBackend struct {
	dsn string
	err error
}

func (r *overrideRotatingBackend) ResolveTarget(_ context.Context, req inventory.Request) (*inventory.Target, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &inventory.Target{Target: req.Target, DatabaseType: "mysql", DSN: r.dsn}, nil
}

type replaceableOverrideResolver struct{ inventory.Resolver }

// Credential rotation and a rebuilt mapping both retire the cached connection.
// A failed resolution cannot bypass the decorator through the previous client.
func TestTargetRouterOverrideDecoratorRefresh(t *testing.T) {
	backend := &overrideRotatingBackend{dsn: "user:old@tcp(localhost:3306)/"}
	decorate := func(physical string) inventory.Resolver {
		resolver, err := inventory.NewOverrideResolver(backend, []inventory.TargetOverride{{Environment: "production", Target: "cluster", DatabaseType: "mysql", SchemaOverrides: map[string]string{"orders": physical}}}, nil)
		require.NoError(t, err)
		return resolver
	}
	resolver := &replaceableOverrideResolver{decorate("orders_sandbox")}
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)
	plan := func() error {
		_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Type: "mysql", Environment: "production", Target: "cluster"})
		return err
	}
	require.NoError(t, plan())
	assert.Equal(t, map[string]string{"orders": "orders_sandbox"}, created["orders"].schemaOverrides)
	backend.err = errors.New("credential refresh failed")
	require.ErrorContains(t, plan(), "credential refresh failed")
	require.Len(t, created, 1)
	backend.err = nil
	backend.dsn = "user:new@tcp(localhost:3306)/"
	require.NoError(t, plan())
	require.Len(t, created, 2)
	assert.Equal(t, map[string]string{"orders": "orders_sandbox"}, created["orders#2"].schemaOverrides)
	resolver.Resolver = decorate("orders_live")
	require.NoError(t, plan())
	require.Len(t, created, 3)
	assert.Equal(t, map[string]string{"orders": "orders_live"}, created["orders#3"].schemaOverrides)
	assert.True(t, created["orders#2"].closed)
}
