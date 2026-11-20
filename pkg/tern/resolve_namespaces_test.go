package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

func newResolveTestClient(databaseType, dsn string, overrides map[string]string) *LocalClient {
	return &LocalClient{config: LocalConfig{
		Database:        "shop",
		Type:            databaseType,
		TargetDSN:       dsn,
		SchemaOverrides: overrides,
	}, logger: slog.New(slog.DiscardHandler)}
}

func resolveNamespaces(t *testing.T, c *LocalClient, namespaces ...string) (map[string]string, error) {
	t.Helper()
	resp, err := c.ResolveNamespaces(t.Context(), &ternv1.ResolveNamespacesRequest{Database: "shop", Namespaces: namespaces})
	if err != nil {
		return nil, err
	}
	return resp.GetSchemas(), nil
}

// A local data plane resolves each namespace to the schema its plans and
// applies connect to, so the control plane can tell when two namespaces are
// one schema on the target.
func TestLocalClientResolveNamespaces(t *testing.T) {
	t.Run("a MySQL DSN naming a database pins every namespace to it", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypeMySQL, "root@tcp(localhost:3306)/shop_eu", nil)
		schemas, err := resolveNamespaces(t, c, "shop", "shop_sharded")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop_eu", "shop_sharded": "shop_eu"}, schemas)
	})

	t.Run("a pinned MySQL DSN wins over schema overrides", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypeMySQL, "root@tcp(localhost:3306)/shop_eu", map[string]string{"shop": "shop_other"})
		schemas, err := resolveNamespaces(t, c, "shop", "ledger")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop_eu", "ledger": "shop_eu"}, schemas)
	})

	t.Run("a MySQL DSN naming no database addresses each namespace itself", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypeMySQL, "root@tcp(localhost:3306)/", nil)
		schemas, err := resolveNamespaces(t, c, "shop", "shop_sharded")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop", "shop_sharded": "shop_sharded"}, schemas)
	})

	t.Run("schema overrides map each namespace to its physical schema", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypeMySQL, "root@tcp(localhost:3306)/", map[string]string{"shop": "shop_eu", "ledger": "shop_eu"})
		schemas, err := resolveNamespaces(t, c, "shop", "ledger")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop_eu", "ledger": "shop_eu"}, schemas)
	})

	t.Run("a namespace the overrides do not map is refused", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypePostgres, "postgres://localhost/shop", map[string]string{"shop": "shop_eu"})
		_, err := resolveNamespaces(t, c, "shop", "ledger")
		require.ErrorContains(t, err, `resolve namespace "ledger" for database shop`)
		assert.ErrorContains(t, err, "does not authorize namespace")
	})

	t.Run("an empty MySQL namespace on a namespace-free DSN is refused", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypeMySQL, "root@tcp(localhost:3306)/", nil)
		_, err := resolveNamespaces(t, c, "")
		require.ErrorContains(t, err, "MySQL namespace is required")
	})

	t.Run("Vitess keyspaces resolve to themselves", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypeVitess, "", nil)
		schemas, err := resolveNamespaces(t, c, "shop", "shop_sharded")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop", "shop_sharded": "shop_sharded"}, schemas)
	})

	t.Run("an engine type the client does not know is unsupported", func(t *testing.T) {
		c := newResolveTestClient("cassandra", "", nil)
		_, err := resolveNamespaces(t, c, "shop")
		require.ErrorIs(t, err, ErrResolveNamespacesUnsupported)
		assert.ErrorContains(t, err, "database shop")
	})

	t.Run("a nil request is refused", func(t *testing.T) {
		c := newResolveTestClient(storage.DatabaseTypeMySQL, "root@tcp(localhost:3306)/", nil)
		_, err := c.ResolveNamespaces(t.Context(), nil)
		require.ErrorContains(t, err, "resolve namespaces request is required")
	})
}

type resolveAnsweringClient struct {
	Client
	resp *ternv1.ResolveNamespacesResponse
	err  error
}

func (c *resolveAnsweringClient) ResolveNamespaces(context.Context, *ternv1.ResolveNamespacesRequest) (*ternv1.ResolveNamespacesResponse, error) {
	return c.resp, c.err
}

// Over gRPC, a data plane that predates the RPC and one whose engine cannot
// resolve namespaces both reach the control plane as
// ErrResolveNamespacesUnsupported, so the caller fails closed the same way for
// each. Any other failure stays an ordinary error naming the database and
// target, and a resolution passes through unchanged.
func TestGRPCClientResolveNamespaces(t *testing.T) {
	req := &ternv1.ResolveNamespacesRequest{Database: "shop", Type: storage.DatabaseTypeMySQL, Target: "shop-eu", Namespaces: []string{"shop"}}
	logger := slog.New(slog.DiscardHandler)

	t.Run("a resolution passes through", func(t *testing.T) {
		client := newRetryTestClient(t, NewServer(&resolveAnsweringClient{resp: &ternv1.ResolveNamespacesResponse{Schemas: map[string]string{"shop": "shop_eu"}}}, logger))
		resp, err := client.ResolveNamespaces(t.Context(), req)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop_eu"}, resp.GetSchemas())
	})
	t.Run("an engine that cannot resolve namespaces", func(t *testing.T) {
		client := newRetryTestClient(t, NewServer(&resolveAnsweringClient{err: ErrResolveNamespacesUnsupported}, logger))
		_, err := client.ResolveNamespaces(t.Context(), req)
		require.ErrorIs(t, err, ErrResolveNamespacesUnsupported)
		assert.ErrorContains(t, err, "database shop target shop-eu")
	})
	t.Run("a data plane that predates the RPC", func(t *testing.T) {
		client := newRetryTestClient(t, &ternv1.UnimplementedTernServer{})
		_, err := client.ResolveNamespaces(t.Context(), req)
		require.ErrorIs(t, err, ErrResolveNamespacesUnsupported)
	})
	t.Run("a resolution that fails", func(t *testing.T) {
		client := newRetryTestClient(t, NewServer(&resolveAnsweringClient{err: errors.New(`target does not authorize namespace "shop"`)}, logger))
		_, err := client.ResolveNamespaces(t.Context(), req)
		require.ErrorContains(t, err, "does not authorize namespace")
		assert.ErrorContains(t, err, "database shop target shop-eu")
		assert.NotErrorIs(t, err, ErrResolveNamespacesUnsupported)
	})
}

// Through the TargetRouter, a namespace resolution is answered from the
// resolved target's own connection and schema override, so the control plane
// learns what the data plane's plans would connect to, not what the request
// assumed. A target whose connection names a database stores every namespace
// there, one with an override stores its one namespace under the override's
// name and refuses any other, and one with neither stores each namespace under
// its own name.
func TestTargetRouterResolveNamespacesAnswersFromTheResolvedTarget(t *testing.T) {
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: inventoryTargets{
			"shop-pinned":   {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(localhost:3306)/shop_main"},
			"shop-override": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(localhost:3306)/", SchemaOverrides: map[string]string{"shop": "shop_eu"}},
			"shop-unpinned": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(localhost:3306)/"},
		},
		Storage: targetRouterStorage{applies: targetRouterApplyStore{}},
		Logger:  slog.New(slog.DiscardHandler),
		LocalClientFactory: func(cfg LocalConfig, _ storage.Storage, logger *slog.Logger) (Client, error) {
			return &LocalClient{config: cfg, logger: logger}, nil
		},
	})
	require.NoError(t, err)
	resolve := func(t *testing.T, target string, namespaces ...string) (map[string]string, error) {
		t.Helper()
		resp, err := router.ResolveNamespaces(t.Context(), &ternv1.ResolveNamespacesRequest{
			Database:    "shop",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Target:      target,
			Namespaces:  namespaces,
		})
		return resp.GetSchemas(), err
	}

	t.Run("a connection naming a database", func(t *testing.T) {
		schemas, err := resolve(t, "shop-pinned", "shop", "shop_sharded")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop_main", "shop_sharded": "shop_main"}, schemas)
	})
	t.Run("a schema override", func(t *testing.T) {
		schemas, err := resolve(t, "shop-override", "shop")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop_eu"}, schemas)

		_, err = resolve(t, "shop-override", "shop", "shop_sharded")
		require.ErrorContains(t, err, `does not authorize MySQL namespace "shop_sharded"`)
	})
	t.Run("neither", func(t *testing.T) {
		schemas, err := resolve(t, "shop-unpinned", "shop", "shop_sharded")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"shop": "shop", "shop_sharded": "shop_sharded"}, schemas)
	})
}

// inventoryTargets resolves each named target to its record as given. Unlike
// the static inventory, it accepts a MySQL DSN that names a database, which
// other inventories can return.
type inventoryTargets map[string]inventory.Target

func (r inventoryTargets) ResolveTarget(_ context.Context, req inventory.Request) (*inventory.Target, error) {
	target, ok := r[req.Target]
	if !ok {
		return nil, fmt.Errorf("target %q is not in the inventory", req.Target)
	}
	target.Target = req.Target
	return &target, nil
}
