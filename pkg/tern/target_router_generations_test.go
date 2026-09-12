package tern

import (
	"context"
	"errors"
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

	vitess := func(token string) *inventory.Target {
		return &inventory.Target{Target: "t", DatabaseType: storage.DatabaseTypeVitess, Metadata: map[string]string{
			inventory.MetadataOrganization: "acme",
			inventory.MetadataTokenName:    "tok-id",
			inventory.MetadataTokenValue:   token,
			inventory.MetadataAPIURL:       "https://api.example.test",
		}}
	}
	assert.Equal(t, connectionIdentityHash(vitess("a")), connectionIdentityHash(vitess("a")))
	assert.NotEqual(t, connectionIdentityHash(vitess("a")), connectionIdentityHash(vitess("b")), "a rotated service token is a new connection identity")

	mysqlWithMetadata := &inventory.Target{Target: "t", DatabaseType: storage.DatabaseTypeMySQL, DSN: rotationOldDSN, Metadata: map[string]string{"pending_drops": "true"}}
	assert.Equal(t, connectionIdentityHash(mysqlOld), connectionIdentityHash(mysqlWithMetadata), "non-credential metadata does not change the identity")
}
