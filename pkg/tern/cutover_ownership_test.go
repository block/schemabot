package tern

import (
	"log/slog"
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A cutover RPC can land on any instance sharing the route's storage. It must
// record a durable cutover request for the apply owner to process, never invoke
// the receiving instance's local engine — which may be running a different
// schema change or none.
func TestCutoverQueuesRequestForOwner(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-cutover-queue",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.WaitingForCutover,
	}
	controlRequests := &testControlRequestStore{}
	eng := &fakeControlEngine{}
	client := &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeMySQL},
		storage: &mockStorage{
			applies:         &mockApplyStore{apply: apply},
			controlRequests: controlRequests,
		},
		spiritEngine: eng,
		logger:       slog.Default(),
	}

	resp, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted, "cutover request should be accepted (queued)")

	require.Len(t, controlRequests.requests, 1, "a durable cutover request should be queued for the owner")
	assert.Equal(t, storage.ControlOperationCutover, controlRequests.requests[0].Operation)
	assert.Equal(t, apply.ID, controlRequests.requests[0].ApplyID)
	assert.Equal(t, storage.ControlRequestPending, controlRequests.requests[0].Status)

	assert.Zero(t, eng.cutoverCount, "the receiving instance must not invoke its local engine; the owner performs the cutover")
}
