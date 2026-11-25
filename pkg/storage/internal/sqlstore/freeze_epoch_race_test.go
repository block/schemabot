//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// TestFreezeCreateWaitsForAnAdmissionWrite runs the freeze/admission race
// against MySQL; TestPostgresStorageParity runs the same scenario against
// PostgreSQL.
func TestFreezeCreateWaitsForAnAdmissionWrite(t *testing.T) {
	testFreezeCreateWaitsForAnAdmissionWrite(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	}, mysqlLockWaiter(testDB))
}

// testFreezeCreateWaitsForAnAdmissionWrite pins the serialization between a
// freeze being created and a change being admitted. An admission write that
// passed its epoch check holds the epoch row shared until it commits, so the
// freeze's epoch bump waits for it: the freeze is acknowledged only after the
// change it did not see is recorded. An admission that checks after the
// freeze commits sees the new epoch and is refused.
func testFreezeCreateWaitsForAnAdmissionWrite(t *testing.T, newStore func(t *testing.T) *Storage, rowWaiting lockWaiter) {
	ctx := t.Context()
	store := newStore(t)
	epoch, err := store.Freezes().Epoch(ctx, "staging")
	require.NoError(t, err)
	guarded := storage.WithFreezeEpochGuard(ctx, storage.FreezeEpochGuard{Environment: "staging", Epoch: epoch})

	admission, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := admission.Rollback()
		if errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
			return
		}
		assert.NoError(t, err, "roll back the admission")
	})
	require.NoError(t, checkFreezeEpochGuard(guarded, admission), "the admission checks before any freeze")

	created := make(chan error, 1)
	go func() {
		_, err := store.Freezes().Create(ctx, &storage.EnvironmentFreeze{
			Identifier:  engine.NewFreezeID(),
			Environment: "staging",
			Kind:        storage.FreezeKindOnDemand,
			Running:     storage.FreezeRunningStop,
			StartsAt:    time.Now().UTC(),
			Reason:      "SEV-1234 payments latency",
			CreatedBy:   "alice",
			Source:      storage.FreezeSourceManual,
		})
		created <- err
	}()
	require.Eventually(t, func() bool { return rowWaiting(t, "freeze_epochs (environment, epoch) VALUES") },
		startTakeoverRaceDeadline, 20*time.Millisecond, "the freeze's epoch bump waits on the admission's lock")
	select {
	case err := <-created:
		require.FailNow(t, "the freeze was created while an admission held the epoch", "err = %v", err)
	default:
	}

	require.NoError(t, admission.Commit())
	select {
	case err := <-created:
		require.NoError(t, err, "the freeze is created once the admission commits")
	case <-time.After(startTakeoverRaceDeadline):
		require.FailNow(t, "the freeze was not created after the admission committed")
	}

	now, err := store.Freezes().Epoch(ctx, "staging")
	require.NoError(t, err)
	assert.Equal(t, epoch+1, now)
	late, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	defer rollbackTx(ctx, late, "late admission")
	require.ErrorIs(t, checkFreezeEpochGuard(guarded, late), storage.ErrFreezeEpochChanged,
		"an admission that checks after the freeze is refused")
}
