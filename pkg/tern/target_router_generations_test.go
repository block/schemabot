package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// rotatingResolver resolves one MySQL target whose DSN a test can change
// between requests, standing in for a dsn_from resolver whose secret was
// re-synced after a credential rotation.
type rotatingResolver struct {
	dsn string
}

func (r *rotatingResolver) ResolveTarget(_ context.Context, req inventory.Request) (*inventory.Target, error) {
	return &inventory.Target{
		Target:       req.Target,
		DatabaseType: storage.DatabaseTypeMySQL,
		DSN:          r.dsn,
	}, nil
}

const (
	rotationOldDSN = "app:old-secret@tcp(orders-db:3306)/"
	rotationNewDSN = "app:new-secret@tcp(orders-db:3306)/"
)

func planOrders(t *testing.T, router *TargetRouter) {
	t.Helper()
	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{
		Database:    "orders",
		Type:        storage.DatabaseTypeMySQL,
		Environment: "production",
		Target:      "dsid-orders-prod",
	})
	require.NoError(t, err)
}

func runningOrdersApply(t *testing.T, identifier string) (*storage.Apply, targetRouterApplyStore) {
	t.Helper()
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: identifier,
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "production",
		State:           state.Apply.Running,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "dsid-orders-prod"})
	return apply, targetRouterApplyStore{
		byID:         map[int64]*storage.Apply{42: apply},
		byIdentifier: map[string]*storage.Apply{identifier: apply},
	}
}

// The same connection identity resolved twice serves one client; a rotated
// DSN publishes a second client and, because the first drives no apply,
// closes it on replacement. New work after the rotation goes to the new
// client only.
func TestTargetRouterRotationReplacesIdleGeneration(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)

	planOrders(t, router)
	planOrders(t, router)
	require.Len(t, created, 1, "an unchanged connection identity reuses the generation")
	first := created["orders"]
	assert.Equal(t, rotationOldDSN, first.targetDSN)

	resolver.dsn = rotationNewDSN
	planOrders(t, router)

	require.Len(t, created, 2)
	second := created["orders#2"]
	require.NotNil(t, second)
	assert.Equal(t, rotationNewDSN, second.targetDSN)
	assert.True(t, first.closed, "a replaced generation that owns no apply is closed")
	assert.False(t, second.closed)

	first.planReq = nil
	planOrders(t, router)
	assert.Nil(t, first.planReq, "no new work reaches the retired generation")
	assert.NotNil(t, second.planReq)
}

// An apply started on a generation keeps that generation after a rotation:
// the router does not close it, routes the apply's requests to it, and sends
// new work to the current generation. Once the apply is terminal the old
// generation gives it up and is closed.
func TestTargetRouterRetiringGenerationDrivesItsApplyUntilTerminal(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-routed")
	router := newTargetRouterForTest(t, resolver, store, nil, created)

	resp, err := router.Apply(t.Context(), &ternv1.ApplyRequest{
		Database:    "orders",
		Type:        storage.DatabaseTypeMySQL,
		Environment: "production",
		Target:      "dsid-orders-prod",
	})
	require.NoError(t, err)
	require.Equal(t, apply.ApplyIdentifier, resp.ApplyId)
	first := created["orders"]
	require.NotNil(t, first)

	resolver.dsn = rotationNewDSN
	planOrders(t, router)
	second := created["orders#2"]
	require.NotNil(t, second)
	assert.False(t, first.closed, "the generation driving a running apply survives the rotation")

	_, err = router.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "production"})
	require.NoError(t, err)
	assert.Equal(t, apply.ApplyIdentifier, first.progressReq.GetApplyId(), "the owning generation serves the apply")
	assert.Nil(t, second.progressReq)

	router.SetObserver(apply.ID, targetRouterNoopObserver{})
	assert.Equal(t, apply.ID, first.observerApplyID, "observers attach to the owning generation")
	assert.Zero(t, second.observerApplyID)

	apply.State = state.Apply.Completed
	_, err = router.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "production"})
	require.NoError(t, err)
	assert.Equal(t, apply.ApplyIdentifier, second.progressReq.GetApplyId(), "a terminal apply routes to the current generation")
	assert.True(t, first.closed, "the retired generation closes once its last apply is terminal")
	assert.False(t, second.closed)
}

// A re-dispatch of an apply that is already queued or driving is accepted by
// the current generation, which adopts the existing apply rather than
// starting one. Ownership stays with the generation that first accepted it:
// that is where the operator's claim drives the apply and where its observer
// is registered, so the current generation's acceptance neither closes the
// owner nor redirects the apply's requests (AV-2).
func TestTargetRouterRedispatchLeavesOwnershipWithDrivingGeneration(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-routed")
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	dispatch := func() {
		resp, err := router.Apply(t.Context(), &ternv1.ApplyRequest{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Target:      "dsid-orders-prod",
		})
		require.NoError(t, err)
		require.Equal(t, apply.ApplyIdentifier, resp.ApplyId)
	}

	dispatch()
	first := created["orders"]
	require.NotNil(t, first)

	resolver.dsn = rotationNewDSN
	dispatch()
	second := created["orders#2"]
	require.NotNil(t, second)
	assert.NotNil(t, second.applyReq, "the re-dispatch itself is served by the current generation")
	assert.False(t, first.closed, "accepting the re-dispatch does not close the generation driving the apply")

	_, err := router.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "production"})
	require.NoError(t, err)
	assert.Equal(t, apply.ApplyIdentifier, first.progressReq.GetApplyId(), "the apply's requests still reach the generation that owns it")
	assert.Nil(t, second.progressReq)

	apply.State = state.Apply.Completed
	_, err = router.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "production"})
	require.NoError(t, err)
	assert.True(t, first.closed, "the owner closes once the apply is terminal")
	assert.False(t, second.closed)
}

// A local drive runs inside the resume call for the life of the schema
// change. Requests that arrive while it runs — progress, observer attachment
// — must reach the generation driving it even when a rotation has since
// published a new current generation, so ownership is recorded before the
// drive starts rather than when it returns (AV-2, OW-3).
func TestTargetRouterRequestsDuringDriveReachDrivingGeneration(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-42")
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	planOrders(t, router)
	first := created["orders"]
	require.NotNil(t, first)

	first.onResume = func() {
		first.onResume = nil
		resolver.dsn = rotationNewDSN
		planOrders(t, router)
		second := created["orders#2"]
		require.NotNil(t, second, "the rotation mid-drive publishes a new current generation")
		assert.False(t, first.closed, "the driving generation survives the rotation")

		_, err := router.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "production"})
		require.NoError(t, err)
		assert.Equal(t, apply.ApplyIdentifier, first.progressReq.GetApplyId(), "progress mid-drive reaches the driving generation")
		assert.Nil(t, second.progressReq)

		router.SetObserver(apply.ID, targetRouterNoopObserver{})
		assert.Equal(t, apply.ID, first.observerApplyID, "an observer attached mid-drive reaches the driving generation")
		assert.Zero(t, second.observerApplyID)
	}
	require.NoError(t, router.ResumeApply(t.Context(), apply))
	require.Nil(t, first.onResume, "the drive ran")
	assert.False(t, first.closed, "the apply is still running, so its generation stays open after the resume returns")
}

// Two requests that miss on the same route at once build one client between
// them: the second waits for the first build to publish and reuses it, so a
// slower build can never publish a stale identity over a newer generation.
func TestTargetRouterConcurrentMissesBuildOneClient(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	entered := make(chan struct{})
	proceed := make(chan struct{})
	var builds int
	var client targetRouterRecordingClient
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: resolver,
		Storage:  targetRouterStorage{applies: targetRouterApplyStore{}},
		LocalClientFactory: func(LocalConfig, storage.Storage, *slog.Logger) (Client, error) {
			builds++
			if builds == 1 {
				close(entered)
				<-proceed
			}
			return &client, nil
		},
	})
	require.NoError(t, err)

	firstDone := make(chan error, 1)
	go func() {
		_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Type: storage.DatabaseTypeMySQL, Environment: "production", Target: "dsid-orders-prod"})
		firstDone <- err
	}()
	<-entered
	secondDone := make(chan error, 1)
	go func() {
		_, err := router.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders", Type: storage.DatabaseTypeMySQL, Environment: "production", Target: "dsid-orders-prod"})
		secondDone <- err
	}()
	close(proceed)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)

	assert.Equal(t, 1, builds, "the waiting request reuses the published generation instead of building its own")
	assert.NotNil(t, client.planReq)
	assert.NotNil(t, client.pullReq)
	assert.False(t, client.closed)
}

// Every rotation sweeps: a generation retired two rotations ago whose apply
// has since finished is closed at the later rotation even when the generation
// replaced by that rotation is idle and closes outright.
func TestTargetRouterRotationSweepsOlderRetiringGenerations(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-42")
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	require.NoError(t, router.ResumeApply(t.Context(), apply))
	first := created["orders"]

	resolver.dsn = rotationNewDSN
	planOrders(t, router)
	second := created["orders#2"]
	require.NotNil(t, second)
	assert.False(t, first.closed, "the first generation still drives the apply")

	apply.State = state.Apply.Completed
	resolver.dsn = "app:third-secret@tcp(orders-db:3306)/"
	planOrders(t, router)
	third := created["orders#3"]
	require.NotNil(t, third)

	assert.True(t, second.closed, "the idle generation replaced by this rotation closes")
	assert.True(t, first.closed, "the older retiring generation closes too now that its apply is terminal")
	assert.False(t, third.closed)
}

// The sweep also reclaims finished applies from a route's current generation,
// which no request for the apply would otherwise release, so a long-lived
// generation does not keep an entry for every apply it ever drove.
func TestTargetRouterSweepReclaimsFinishedAppliesFromCurrentGeneration(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-42")
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	require.NoError(t, router.ResumeApply(t.Context(), apply))
	orders := created["orders"]
	planBilling := func() {
		_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "billing", Type: storage.DatabaseTypeMySQL, Environment: "production", Target: "dsid-billing-prod"})
		require.NoError(t, err)
	}
	planBilling()
	require.Contains(t, router.applyOwners, apply.ApplyIdentifier)

	apply.State = state.Apply.Completed
	resolver.dsn = rotationNewDSN
	planBilling()

	router.mu.Lock()
	defer router.mu.Unlock()
	assert.NotContains(t, router.applyOwners, apply.ApplyIdentifier, "a rotation on another route sweeps the finished apply off its owner")
	assert.Empty(t, router.current[targetClientKey{target: "dsid-orders-prod", databaseType: storage.DatabaseTypeMySQL, environment: "production", database: "orders"}].owned)
	assert.False(t, orders.closed, "the current generation of the orders route stays open")
}

// A stopped apply is terminal: its drive has exited, so a resume after a
// rotation lands on the current generation and the stale one is released.
func TestTargetRouterResumeOfStoppedApplyMovesToCurrentGeneration(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-42")
	router := newTargetRouterForTest(t, resolver, store, nil, created)

	require.NoError(t, router.ResumeApply(t.Context(), apply))
	first := created["orders"]
	require.NotNil(t, first)
	assert.Equal(t, apply.ApplyIdentifier, first.resumeApply.ApplyIdentifier)

	resolver.dsn = rotationNewDSN
	planOrders(t, router)
	second := created["orders#2"]
	require.NotNil(t, second)
	assert.False(t, first.closed, "a running apply keeps its generation")

	apply.State = state.Apply.Stopped
	require.NoError(t, router.ResumeApplyOperation(t.Context(), apply, 7))
	assert.Equal(t, apply.ApplyIdentifier, second.resumeApply.ApplyIdentifier, "the resume opens with the rotated credential")
	assert.True(t, first.closed)

	apply.State = state.Apply.Running
	_, err := router.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "production"})
	require.NoError(t, err)
	assert.Equal(t, apply.ApplyIdentifier, second.progressReq.GetApplyId(), "the resumed apply is owned by the generation that resumed it")
	assert.Nil(t, first.progressReq)
}

// A rotation sweeps retiring generations: one whose owned apply finished
// without any further request for it is closed at the rotation rather than
// staying open until a request happens to arrive.
func TestTargetRouterRotationSweepsFinishedAppliesOffRetiringGeneration(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-42")
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	require.NoError(t, router.ResumeApply(t.Context(), apply))
	first := created["orders"]

	apply.State = state.Apply.Failed
	resolver.dsn = rotationNewDSN
	planOrders(t, router)

	assert.True(t, first.closed, "a retiring generation whose applies are all terminal is closed by the sweep")
	assert.False(t, created["orders#2"].closed)
}

// A storage error during the sweep keeps the generation open: the router
// cannot tell whether the apply is still running, and closing a client under
// a running apply is the outcome ownership exists to prevent.
func TestTargetRouterSweepKeepsGenerationWhenApplyLookupFails(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-42")
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	require.NoError(t, router.ResumeApply(t.Context(), apply))
	first := created["orders"]

	store.lookupErr = errors.New("storage unavailable")
	router.storage = targetRouterStorage{applies: store}
	resolver.dsn = rotationNewDSN
	planOrders(t, router)

	assert.False(t, first.closed)
	require.NoError(t, router.Close())
	assert.True(t, first.closed, "Close still reaches the retiring generation")
}

// A generation replaced while it is serving a request is not closed under
// that request; it closes when the request releases it.
func TestTargetRouterReplacedGenerationOutlivesInFlightRequest(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)
	planOrders(t, router)
	first := created["orders"]

	first.onPlan = func() {
		first.onPlan = nil
		resolver.dsn = rotationNewDSN
		planOrders(t, router)
		assert.False(t, first.closed, "the generation is still serving the outer request")
	}
	planOrders(t, router)

	require.Len(t, created, 2)
	assert.True(t, first.closed, "released after the in-flight request returned")
	assert.False(t, created["orders#2"].closed)
}

// Shutdown reaches retiring generations: they still drive the applies they
// own, and a process that halted only the current generation would leave
// those targets held after it stopped renewing leases.
func TestTargetRouterHaltAndCloseCoverRetiringGenerations(t *testing.T) {
	resolver := &rotatingResolver{dsn: rotationOldDSN}
	created := make(map[string]*targetRouterRecordingClient)
	apply, store := runningOrdersApply(t, "apply-42")
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	require.NoError(t, router.ResumeApply(t.Context(), apply))
	resolver.dsn = rotationNewDSN
	planOrders(t, router)
	first, second := created["orders"], created["orders#2"]
	require.NotNil(t, second)

	require.NoError(t, router.HaltForShutdown(t.Context()))
	assert.True(t, first.halted)
	assert.True(t, second.halted)

	require.NoError(t, router.Close())
	assert.True(t, first.closed)
	assert.True(t, second.closed)
}

// The connection identity hash changes exactly when the credential material
// changes: the DSN for DSN-connected engines, the API metadata for Vitess.
// It is short and never contains the DSN it summarizes.
func TestConnectionIdentityHash(t *testing.T) {
	mysqlOld := &inventory.Target{Target: "t", DatabaseType: storage.DatabaseTypeMySQL, DSN: rotationOldDSN}
	mysqlNew := &inventory.Target{Target: "t", DatabaseType: storage.DatabaseTypeMySQL, DSN: rotationNewDSN}

	assert.Equal(t, connectionIdentityHash(mysqlOld), connectionIdentityHash(mysqlOld))
	assert.NotEqual(t, connectionIdentityHash(mysqlOld), connectionIdentityHash(mysqlNew))
	assert.Len(t, connectionIdentityHash(mysqlOld), 12)
	assert.NotContains(t, connectionIdentityHash(mysqlOld), "old-secret")

	vitess := func(database, token string) *inventory.Target {
		return &inventory.Target{Target: "t", DatabaseType: storage.DatabaseTypeVitess, Metadata: map[string]string{
			inventory.MetadataOrganization: "acme",
			inventory.MetadataDatabase:     database,
			inventory.MetadataTokenName:    "tok-id",
			inventory.MetadataTokenValue:   token,
			inventory.MetadataAPIURL:       "https://api.example.test",
		}}
	}
	assert.Equal(t, connectionIdentityHash(vitess("orders", "a")), connectionIdentityHash(vitess("orders", "a")))
	assert.NotEqual(t, connectionIdentityHash(vitess("orders", "a")), connectionIdentityHash(vitess("orders", "b")), "a rotated service token is a new connection identity")
	assert.NotEqual(t, connectionIdentityHash(vitess("orders", "a")), connectionIdentityHash(vitess("orders-v2", "a")), "a re-pointed PlanetScale database is a new connection identity")

	postgres := func(caRef string) *inventory.Target {
		return &inventory.Target{Target: "t", DatabaseType: storage.DatabaseTypePostgres, DSN: "postgres://app:secret@orders-db:5432/orders", Metadata: map[string]string{
			inventory.MetadataPostgresCARef: caRef,
		}}
	}
	assert.Equal(t, connectionIdentityHash(postgres("ca-2026")), connectionIdentityHash(postgres("ca-2026")))
	assert.NotEqual(t, connectionIdentityHash(postgres("ca-2026")), connectionIdentityHash(postgres("ca-2027")), "a rotated CA is a new connection identity even when the DSN is unchanged")

	mysqlWithMetadata := &inventory.Target{Target: "t", DatabaseType: storage.DatabaseTypeMySQL, DSN: rotationOldDSN, Metadata: map[string]string{"pending_drops": "true"}}
	assert.Equal(t, connectionIdentityHash(mysqlOld), connectionIdentityHash(mysqlWithMetadata), "non-credential metadata does not change the identity")
}
