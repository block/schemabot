package tern

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

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
			// Each cache miss resolves twice: once for the route key and
			// once under the creation slot to publish the identity it
			// resolved; the failed first build and the retry's build make
			// two misses.
			assert.Equal(t, 4, resolver.calls)
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

// A rebuilt client the target refuses again ends the retry: the error names
// both refusals and keeps the classified cause, and the route's cooldown is
// armed because re-resolution did not fix the failure.
func TestTargetRouterAuthRetryStopsAfterSecondFailure(t *testing.T) {
	reader := newTernMetricsReader(t)
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	dispatches := 0
	router, clients := newAuthRetryRouter(t, resolver, func(_ int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) { dispatches++; return nil, authRetryError() }
	}, nil)
	var logged bytes.Buffer
	router.logger = slog.New(slog.NewTextHandler(&logged, nil))
	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.Error(t, err)
	var authErr *targetauth.Error
	require.ErrorAs(t, err, &authErr)
	assert.Equal(t, targetauth.AuthInvalidCredentials, authErr.Classification)
	assert.ErrorContains(t, err, "failed authentication")
	assert.ErrorContains(t, err, "original failure")
	assert.Equal(t, 2, dispatches)
	assert.Len(t, *clients, 2)
	points := collectCounterPoints(t, reader, "schemabot.target.auth_retries.total")
	require.Len(t, points, 1)
	assert.Equal(t, "failed", counterAttr(t, points[0], "outcome"))
	assert.Contains(t, selfHealLogLine(t, logged.String()), "cooldown_armed=true")
	router.mu.Lock()
	assert.Len(t, router.authRetryBlockedUntil, 1)
	router.mu.Unlock()
}

// A retry that fails for a reason the classifier does not recognise says
// nothing about the re-resolved credential, so it does not arm the cooldown:
// the returned error still carries the classified original cause, and the next
// classified failure on the route evicts and retries as if the first retry had
// never run.
func TestTargetRouterUnclassifiedRetryFailureLeavesRouteEligible(t *testing.T) {
	reader := newTernMetricsReader(t)
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	dispatches := 0
	router, clients := newAuthRetryRouter(t, resolver, func(index int, client *targetRouterRecordingClient) {
		calls := 0
		client.onPlanResult = func() (*ternv1.PlanResponse, error) {
			dispatches++
			calls++
			switch {
			case index == 0:
				return nil, authRetryError()
			case index == 1 && calls == 1:
				return nil, errors.New("plan on rebuilt client: context deadline exceeded")
			case index == 1:
				return nil, authRetryError()
			default:
				return &ternv1.PlanResponse{PlanId: "healed"}, nil
			}
		}
	}, nil)
	var logged bytes.Buffer
	router.logger = slog.New(slog.NewTextHandler(&logged, nil))
	plan := func() (*ternv1.PlanResponse, error) {
		return router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	}

	// The first read is refused, evicts, and its retry on the rebuilt client
	// hits an unclassified failure.
	_, err := plan()
	require.Error(t, err)
	var authErr *targetauth.Error
	require.ErrorAs(t, err, &authErr)
	assert.Equal(t, targetauth.AuthInvalidCredentials, authErr.Classification)
	assert.ErrorContains(t, err, "context deadline exceeded")
	assert.ErrorContains(t, err, "access denied")
	assert.Equal(t, 2, dispatches)
	require.Len(t, *clients, 2)
	assert.Contains(t, selfHealLogLine(t, logged.String()), "cooldown_armed=false")
	router.mu.Lock()
	assert.Empty(t, router.authRetryBlockedUntil)
	router.mu.Unlock()

	// The next classified failure on the route is not suppressed: it evicts
	// the rebuilt client and its retry on a third client succeeds.
	logged.Reset()
	resp, err := plan()
	require.NoError(t, err)
	assert.Equal(t, "healed", resp.PlanId)
	assert.Equal(t, 4, dispatches)
	require.Len(t, *clients, 3)
	assert.True(t, (*clients)[1].closed)
	assert.NotContains(t, logged.String(), "within the self-heal cooldown")

	outcomes := map[string]int64{}
	for _, point := range collectCounterPoints(t, reader, "schemabot.target.auth_retries.total") {
		outcomes[counterAttr(t, point, "outcome")] += point.Value
	}
	assert.Equal(t, map[string]int64{"failed": 1, "success": 1}, outcomes)
}

// A retry that cannot re-resolve the target arms the cooldown like a refused
// rebuild does: until it lapses, a classified failure on the route is returned
// without another eviction even once the resolver has recovered.
func TestTargetRouterAuthRetryResolveFailureArmsCooldown(t *testing.T) {
	reader := newTernMetricsReader(t)
	// The first build consumes two resolutions; the third is the retry's.
	resolver := &authRetryResolver{dsn: rotationOldDSN, failAfter: 3}
	dispatches := 0
	router, clients := newAuthRetryRouter(t, resolver, func(_ int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) { dispatches++; return nil, authRetryError() }
	}, nil)
	plan := func() (*ternv1.PlanResponse, error) {
		return router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	}
	_, err := plan()
	require.Error(t, err)
	assert.ErrorContains(t, err, "secret backend unavailable")
	assert.Equal(t, 1, dispatches)
	require.Len(t, *clients, 1)

	// The resolver recovers. The route has no current generation, so this
	// read builds one; its classified failure is then suppressed rather than
	// evicting the generation it just built.
	resolver.failAfter = 0
	_, err = plan()
	require.Error(t, err)
	var authErr *targetauth.Error
	require.ErrorAs(t, err, &authErr)
	assert.Equal(t, 2, dispatches)
	require.Len(t, *clients, 2)
	assert.False(t, (*clients)[1].closed)

	outcomes := map[string]int64{}
	for _, point := range collectCounterPoints(t, reader, "schemabot.target.auth_retries.total") {
		outcomes[counterAttr(t, point, "outcome")] += point.Value
	}
	assert.Equal(t, map[string]int64{"resolve_error": 1, "suppressed": 1}, outcomes)
}

// A successful retry drops a cooldown that a concurrent request on the same
// route armed after this request passed the eligibility check: the success
// shows the re-resolved credential works, so the route stays eligible. The
// peer's arming is modelled from inside the retry's dispatch.
func TestTargetRouterSuccessfulRetryClearsConcurrentlyArmedCooldown(t *testing.T) {
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	var router *TargetRouter
	key := targetClientKey{target: "target", databaseType: storage.DatabaseTypeMySQL, database: "orders"}
	router, clients := newAuthRetryRouter(t, resolver, func(index int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) {
			if index == 0 {
				return nil, authRetryError()
			}
			router.armAuthRetryCooldown(key)
			return &ternv1.PlanResponse{PlanId: "healed"}, nil
		}
	}, nil)
	resp, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)
	assert.Equal(t, "healed", resp.PlanId)
	require.Len(t, *clients, 2)
	router.mu.Lock()
	assert.Empty(t, router.authRetryBlockedUntil)
	router.mu.Unlock()
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

// A retry whose re-resolution fails reports both causes, and its metric and
// log carry the database type the first resolution established even when the
// request left the type for the resolver to fill in.
func TestTargetRouterAuthRetryResolveFailurePreservesCauses(t *testing.T) {
	reader := newTernMetricsReader(t)
	// The first build consumes two resolutions; the third is the retry's.
	resolver := &authRetryResolver{dsn: rotationOldDSN, failAfter: 3}
	router, _ := newAuthRetryRouter(t, resolver, func(_ int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) { return nil, authRetryError() }
	}, nil)
	var logged bytes.Buffer
	router.logger = slog.New(slog.NewTextHandler(&logged, nil))
	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed authentication")
	assert.ErrorContains(t, err, "secret backend unavailable")
	assert.ErrorContains(t, err, "access denied")
	points := collectCounterPoints(t, reader, "schemabot.target.auth_retries.total")
	require.Len(t, points, 1)
	assert.Equal(t, "resolve_error", counterAttr(t, points[0], "outcome"))
	assert.Equal(t, storage.DatabaseTypeMySQL, counterAttr(t, points[0], "database_type"))
	selfHeal := selfHealLogLine(t, logged.String())
	assert.Contains(t, selfHeal, "database_type="+storage.DatabaseTypeMySQL)
	assert.Contains(t, selfHeal, "outcome=resolve_error")
	assert.Contains(t, selfHeal, "evicted=true")
}

// A retry that does not repair the failure arms a per-route cooldown: until it
// lapses, a classified failure on the route is returned as it is, without
// evicting the generation or re-resolving the target, so a secret the server
// keeps rejecting costs one resolution per window rather than one per read.
// The lapse alone re-enables the route; the retry it then permits leaves no
// cooldown entry behind when it succeeds.
func TestTargetRouterAuthRetryCooldownBoundsResolverLoad(t *testing.T) {
	reader := newTernMetricsReader(t)
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	dispatches := 0
	router, clients := newAuthRetryRouter(t, resolver, func(index int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) {
			dispatches++
			if index < 2 {
				return nil, authRetryError()
			}
			return &ternv1.PlanResponse{PlanId: "healed"}, nil
		}
	}, nil)
	clock := time.Unix(1_700_000_000, 0)
	router.now = func() time.Time { return clock }
	var logged bytes.Buffer
	router.logger = slog.New(slog.NewTextHandler(&logged, nil))
	plan := func() (*ternv1.PlanResponse, error) {
		return router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	}

	// The first failure evicts and retries; the retry fails too and arms the
	// cooldown.
	_, err := plan()
	require.Error(t, err)
	assert.Equal(t, 2, dispatches)
	assert.Equal(t, 4, resolver.calls)
	require.Len(t, *clients, 2)
	assert.True(t, (*clients)[0].closed)

	// Inside the cooldown the failure comes straight back: one dispatch on
	// the current generation, only the routing resolution that every read
	// pays, no eviction, no rebuild, typed error intact.
	logged.Reset()
	_, err = plan()
	require.Error(t, err)
	var authErr *targetauth.Error
	require.ErrorAs(t, err, &authErr)
	assert.Equal(t, targetauth.AuthInvalidCredentials, authErr.Classification)
	assert.Equal(t, 3, dispatches)
	assert.Equal(t, 5, resolver.calls)
	assert.Len(t, *clients, 2)
	assert.False(t, (*clients)[1].closed)
	suppressed := logged.String()
	assert.Contains(t, suppressed, "within the self-heal cooldown")
	assert.Contains(t, suppressed, "outcome=suppressed")
	assert.Contains(t, suppressed, "attempt=1")
	assert.Contains(t, suppressed, "cooldown_remaining_ms="+fmt.Sprint(authRetryCooldown.Milliseconds()))
	assert.NotContains(t, suppressed, rotationOldDSN)

	// Just short of the window's end the failure is still suppressed.
	clock = clock.Add(authRetryCooldown - time.Millisecond)
	_, err = plan()
	require.Error(t, err)
	assert.Equal(t, 4, dispatches)
	assert.Equal(t, 6, resolver.calls)

	// Once it lapses the route is eligible again: the routing resolution,
	// then the eviction and the retry's two-resolution rebuild. This retry
	// repairs the failure and clears the cooldown.
	clock = clock.Add(time.Millisecond)
	resp, err := plan()
	require.NoError(t, err)
	assert.Equal(t, "healed", resp.PlanId)
	assert.Equal(t, 6, dispatches)
	assert.Equal(t, 9, resolver.calls)
	require.Len(t, *clients, 3)
	assert.True(t, (*clients)[1].closed)
	router.mu.Lock()
	assert.Empty(t, router.authRetryBlockedUntil)
	router.mu.Unlock()

	outcomes := map[string]int64{}
	for _, point := range collectCounterPoints(t, reader, "schemabot.target.auth_retries.total") {
		outcomes[counterAttr(t, point, "outcome")] += point.Value
	}
	assert.Equal(t, map[string]int64{"failed": 1, "suppressed": 2, "success": 1}, outcomes)
}

// The generation an authentication failure evicts is swept like one a
// rotation replaced: when the apply it owns has already finished, the
// eviction closes it instead of leaving it open until the next
// size-triggered sweep.
func TestTargetRouterAuthEvictionSweepsSettledOwnership(t *testing.T) {
	resolver := &authRetryResolver{dsn: rotationOldDSN}
	apply, store := runningOrdersApply(t, "apply-routed")
	router, clients := newAuthRetryRouter(t, resolver, func(index int, client *targetRouterRecordingClient) {
		client.onPlanResult = func() (*ternv1.PlanResponse, error) {
			if index == 0 {
				return nil, authRetryError()
			}
			return &ternv1.PlanResponse{PlanId: "plan"}, nil
		}
	}, store)
	_, err := router.Apply(t.Context(), &ternv1.ApplyRequest{Database: "orders", Target: "dsid-orders-prod", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)
	router.mu.Lock()
	assert.Len(t, router.applyOwners, 1)
	router.mu.Unlock()

	apply.State = state.Apply.Completed
	_, err = router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "dsid-orders-prod", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)
	require.Len(t, *clients, 2)
	assert.True(t, (*clients)[0].closed)
	assert.False(t, (*clients)[1].closed)
	router.mu.Lock()
	assert.Empty(t, router.applyOwners)
	assert.Empty(t, router.retiring)
	router.mu.Unlock()
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
	var logged bytes.Buffer
	router.logger = slog.New(slog.NewTextHandler(&logged, nil))
	resp, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "target", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)
	assert.Equal(t, "peer", resp.PlanId)
	require.Len(t, *clients, 2)
	router.mu.Lock()
	assert.Same(t, peer, router.current[peer.key])
	router.mu.Unlock()
	assert.False(t, (*clients)[1].closed)

	// The self-heal log carries its full field contract even when a peer
	// already replaced the failed generation: one definitive log per
	// self-heal, with evicted saying whether this request did the replacing.
	selfHeal := selfHealLogLine(t, logged.String())
	assert.Contains(t, selfHeal, "authentication self-heal retry succeeded")
	assert.Contains(t, selfHeal, "evicted=false")
	assert.Contains(t, selfHeal, "attempt=2")
	assert.Contains(t, selfHeal, "outcome=success")
	assert.Contains(t, selfHeal, "old_dsn_hash="+connectionIdentityHash(&inventory.Target{Target: "target", DatabaseType: storage.DatabaseTypeMySQL, DSN: rotationOldDSN}))
	assert.Contains(t, selfHeal, "new_dsn_hash="+peer.dsnHash)
	assert.NotContains(t, logged.String(), rotationOldDSN)
	assert.NotContains(t, logged.String(), rotationNewDSN)
}

// selfHealLogLine returns the single authentication self-heal log line from
// captured text-handler output, failing when there is not exactly one.
func selfHealLogLine(t *testing.T, logs string) string {
	t.Helper()
	var lines []string
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if strings.Contains(line, "authentication self-heal retry") {
			lines = append(lines, line)
		}
	}
	require.Len(t, lines, 1, "expected exactly one self-heal log, got:\n%s", logs)
	return lines[0]
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
