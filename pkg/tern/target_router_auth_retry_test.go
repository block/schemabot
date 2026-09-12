package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/targetauth"
)

func authRetryError() error {
	return targetauth.Wrap(&mysql.MySQLError{Number: 1045, Message: "access denied"})
}

type authRetryResolver struct {
	dsn       string
	calls     int
	failAfter int
}

func (r *authRetryResolver) ResolveTarget(_ context.Context, req inventory.Request) (*inventory.Target, error) {
	r.calls++
	if r.failAfter > 0 && r.calls >= r.failAfter {
		return nil, errors.New("secret backend unavailable")
	}
	return &inventory.Target{Target: req.Target, DatabaseType: storage.DatabaseTypeMySQL, DSN: r.dsn}, nil
}

func newAuthRetryRouter(t *testing.T, resolver inventory.Resolver, configure func(int, *targetRouterRecordingClient), applyStore storage.ApplyStore) (*TargetRouter, *[]*targetRouterRecordingClient) {
	t.Helper()
	clients := make([]*targetRouterRecordingClient, 0, 2)
	if applyStore == nil {
		applyStore = targetRouterApplyStore{}
	}
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: resolver,
		Storage:  targetRouterStorage{applies: applyStore},
		Logger:   slog.Default(),
		LocalClientFactory: func(LocalConfig, storage.Storage, *slog.Logger) (Client, error) {
			client := &targetRouterRecordingClient{}
			configure(len(clients), client)
			clients = append(clients, client)
			return client, nil
		},
	})
	require.NoError(t, err)
	return router, &clients
}

func TestTargetRouterAuthRetryEligibleReads(t *testing.T) {
	reader := newTernMetricsReader(t)
	for _, operation := range []string{"pull_schema", "plan", "plan_diff"} {
		t.Run(operation, func(t *testing.T) {
			resolver := &authRetryResolver{dsn: rotationOldDSN}
			dispatches := 0
			router, clients := newAuthRetryRouter(t, resolver, func(index int, client *targetRouterRecordingClient) {
				result := func() error {
					dispatches++
					if index == 0 {
						return authRetryError()
					}
					return nil
				}
				client.onPullSchema = func() (*ternv1.PullSchemaResponse, error) {
					return &ternv1.PullSchemaResponse{Database: "orders"}, result()
				}
				client.onPlanResult = func() (*ternv1.PlanResponse, error) { return &ternv1.PlanResponse{PlanId: "retried"}, result() }
				client.onPlanDiff = func() (*ternv1.PlanDiffResponse, error) {
					return &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_PLANETSCALE}, result()
				}
			}, nil)
			switch operation {
			case "pull_schema":
				resp, err := router.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
				require.NoError(t, err)
				assert.Equal(t, "orders", resp.Database)
			case "plan":
				resp, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
				require.NoError(t, err)
				assert.Equal(t, "retried", resp.PlanId)
			case "plan_diff":
				resp, err := router.PlanDiff(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
				require.NoError(t, err)
				assert.Equal(t, ternv1.Engine_ENGINE_PLANETSCALE, resp.Engine)
			default:
				require.Fail(t, "unknown operation", operation)
			}
			assert.Equal(t, 2, resolver.calls)
			assert.Equal(t, 2, dispatches)
			require.Len(t, *clients, 2)
			assert.True(t, (*clients)[0].closed)
			assert.False(t, (*clients)[1].closed)
		})
	}
	retryPoints := collectCounterPoints(t, reader, "schemabot.target.auth_retries.total")
	require.Len(t, retryPoints, 3)
	operations := make([]string, 0, len(retryPoints))
	for _, point := range retryPoints {
		operations = append(operations, counterAttr(t, point, "operation"))
		assert.Equal(t, "auth_invalid_credentials", counterAttr(t, point, "classification"))
		assert.Equal(t, "success", counterAttr(t, point, "outcome"))
	}
	assert.ElementsMatch(t, []string{"pull_schema", "plan", "plan_diff"}, operations)
	evictionPoints := collectCounterPoints(t, reader, "schemabot.target.client_evictions.total")
	require.Len(t, evictionPoints, 1)
	assert.Equal(t, int64(3), evictionPoints[0].Value)
	assert.Equal(t, "auth_invalid_credentials", counterAttr(t, evictionPoints[0], "reason"))
}

func TestTargetRouterAuthRetryStopsAfterSecondFailure(t *testing.T) {
	reader := newTernMetricsReader(t)
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	dispatches := 0
	router, clients := newAuthRetryRouter(t, resolver, func(_ int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) { dispatches++; return nil, authRetryError() }
	}, nil)
	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.Error(t, err)
	assert.Equal(t, 2, dispatches)
	assert.Len(t, *clients, 2)
	points := collectCounterPoints(t, reader, "schemabot.target.auth_retries.total")
	require.Len(t, points, 1)
	assert.Equal(t, "failed", counterAttr(t, points[0], "outcome"))
}

func TestTargetRouterDoesNotRetryUnclassifiedFailure(t *testing.T) {
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	dispatches := 0
	router, clients := newAuthRetryRouter(t, resolver, func(_ int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) { dispatches++; return nil, errors.New("storage unavailable") }
	}, nil)
	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.Error(t, err)
	assert.Equal(t, 1, dispatches)
	assert.Len(t, *clients, 1)
	assert.False(t, (*clients)[0].closed)
}

func TestTargetRouterAuthRetryResolveFailurePreservesCauses(t *testing.T) {
	reader := newTernMetricsReader(t)
	resolver := &authRetryResolver{dsn: rotationOldDSN, failAfter: 2}
	router, _ := newAuthRetryRouter(t, resolver, func(_ int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) { return nil, authRetryError() }
	}, nil)
	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.Error(t, err)
	assert.ErrorContains(t, err, "authentication failure")
	assert.ErrorContains(t, err, "secret backend unavailable")
	points := collectCounterPoints(t, reader, "schemabot.target.auth_retries.total")
	require.Len(t, points, 1)
	assert.Equal(t, "resolve_error", counterAttr(t, points[0], "outcome"))
}

func TestTargetRouterAuthRetryDoesNotEvictPeerReplacement(t *testing.T) {
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	var router *TargetRouter
	var peer *targetClientGeneration
	router, clients := newAuthRetryRouter(t, resolver, func(index int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) {
			if index != 0 {
				return &ternv1.PlanResponse{PlanId: "peer"}, nil
			}
			resolver.dsn = rotationNewDSN
			var err error
			peer, _, err = router.clientForTarget(t.Context(), "target", storage.DatabaseTypeMySQL, "", "orders")
			require.NoError(t, err)
			router.release(peer)
			return nil, authRetryError()
		}
	}, nil)
	resp, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)
	assert.Equal(t, "peer", resp.PlanId)
	require.Len(t, *clients, 2)
	router.mu.Lock()
	assert.Same(t, peer, router.current[peer.key])
	router.mu.Unlock()
	assert.False(t, (*clients)[1].closed)
}

func TestTargetRouterApplyAuthenticationFailureIsNotRetried(t *testing.T) {
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	dispatches := 0
	router, clients := newAuthRetryRouter(t, resolver, func(_ int, client *targetRouterRecordingClient) {
		client.onApply = func() (*ternv1.ApplyResponse, error) { dispatches++; return nil, authRetryError() }
	}, nil)
	_, err := router.Apply(t.Context(), &ternv1.ApplyRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.Error(t, err)
	assert.Equal(t, 1, dispatches)
	assert.Len(t, *clients, 1)
	assert.False(t, (*clients)[0].closed)
}

func TestTargetRouterAuthEvictedApplyOwnerRemainsVisibleToShutdown(t *testing.T) {
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	apply, store := runningOrdersApply(t, "apply-routed")
	var first *targetRouterRecordingClient
	router, clients := newAuthRetryRouter(t, resolver, func(index int, client *targetRouterRecordingClient) {
		if index == 0 {
			first = client
		}
		client.onPlanResult = func() (*ternv1.PlanResponse, error) {
			if index == 0 {
				return nil, authRetryError()
			}
			return &ternv1.PlanResponse{PlanId: fmt.Sprintf("plan-%d", index)}, nil
		}
	}, store)
	_, err := router.Apply(t.Context(), &ternv1.ApplyRequest{Database: "orders", Target: "dsid-orders-prod", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, apply.State)
	_, err = router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "dsid-orders-prod", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)
	assert.False(t, first.closed)
	router.mu.Lock()
	_, retiring := router.retiring[router.applyOwners[apply.ApplyIdentifier]]
	router.mu.Unlock()
	assert.True(t, retiring)
	require.NoError(t, router.HaltForShutdown(t.Context()))
	assert.True(t, first.halted)
	require.NoError(t, router.Close())
	assert.True(t, first.closed)
	assert.Len(t, *clients, 2)
}
