package api

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// drivePassesDeadline bounds a run of claim passes in these tests, and how
// long a test waits for a running operator to settle a rollout whose units
// become claimable one after another. It is far below the poll interval the
// tests configure, so only a driver that claims the next unit as soon as the
// previous one settles can meet it.
const drivePassesDeadline = 20 * time.Second

// drivePassesWithin runs one driver's claim passes under drivePassesDeadline
// and fails the test if the run is still going when the deadline ends it. A
// cancelled context ends the run, so a pass that keeps reporting settled work
// fails here instead of hanging the suite.
func drivePassesWithin(t *testing.T, svc *Service, stop <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), drivePassesDeadline)
	defer cancel()
	svc.drivePasses(ctx, 1, stop)
	require.NoError(t, ctx.Err(), "the driver's claim passes must end before the deadline")
}

// The operation claim fails against storage on every attempt. A failed claim
// settles nothing, so the driver must make one attempt per wake or tick and
// leave the next to the poll cadence rather than retrying in a tight loop
// against a store that is already struggling.
func TestDrivePassesDoesNotRetryAFailedClaim(t *testing.T) {
	ops := &claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		claimErr:              errors.New("storage unavailable"),
	}
	applies := &operationClaimApplyStore{}
	store := &mockStorageWithApplyStores{applies: applies, operations: ops}
	svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))

	drivePassesWithin(t, svc, openClaimGate())

	assert.Equal(t, 1, ops.claims, "a failed claim must not send the driver into another pass")
	assert.Equal(t, 1, applies.stopProbes, "the ladder runs exactly once")
}
