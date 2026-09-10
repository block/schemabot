package storagetest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// pendingDropAt builds a quarantined ledger row for one source table on the
// canonical test target, stamped at the given quarantine time.
func pendingDropAt(originalTable, quarantinedName, runID string, quarantinedAt time.Time) *storage.PendingDrop {
	return &storage.PendingDrop{
		Target:          "shard-a",
		Environment:     "staging",
		DatabaseName:    "orders",
		OriginalTable:   originalTable,
		QuarantinedName: quarantinedName,
		QuarantinedAt:   quarantinedAt,
		RunID:           runID,
		Engine:          "spirit",
		State:           storage.PendingDropQuarantined,
	}
}

// TestPendingDrops runs the behavioral parity suite for
// storage.PendingDropStore.
func TestPendingDrops(t *testing.T, h Harness) {
	// The suite pins wall-clock-independent times so ordering and cutoff
	// assertions cannot depend on how long a subtest takes to run.
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	// PostgreSQL compares identity strings byte-wise, so a row written with one
	// spelling of an environment or database name would be invisible to a
	// lookup that used another. Both sides of the boundary fold, and the table
	// identifiers do not: those must keep the target server's own spelling.
	t.Run("CanonicalizesIdentityKeys", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		drop := pendingDropAt("Orders_Archive", "20260801120000000_Orders_Archive", "run-1", base)
		drop.Environment = "Staging"
		drop.DatabaseName = "Orders_DB"
		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{drop}))

		stored, err := store.PendingDrops().LatestForTable(ctx, "shard-a", "STAGING", "ORDERS_DB", "Orders_Archive")
		require.NoError(t, err)
		require.NotNil(t, stored, "a lookup must find the row whatever spelling it asks with")
		require.Equal(t, "staging", stored.Environment)
		require.Equal(t, "orders_db", stored.DatabaseName)
		require.Equal(t, "Orders_Archive", stored.OriginalTable,
			"a table identifier keeps the spelling the target server uses")
		require.Equal(t, "20260801120000000_Orders_Archive", stored.QuarantinedName)
	})

	t.Run("RecordAndListQuarantined", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Written out of quarantine order so the ordering assertion cannot pass
		// by insertion order alone.
		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("shipments", "20260801120500000_shipments", "run-1", base.Add(5*time.Minute)),
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
		}))

		drops, err := store.PendingDrops().ListQuarantined(ctx, "shard-a", "staging")
		require.NoError(t, err)
		require.Len(t, drops, 2)
		require.Equal(t, "invoices", drops[0].OriginalTable)
		require.Equal(t, "shipments", drops[1].OriginalTable)

		oldest := drops[0]
		require.Equal(t, "shard-a", oldest.Target)
		require.Equal(t, "staging", oldest.Environment)
		require.Equal(t, "orders", oldest.DatabaseName)
		require.Equal(t, "20260801120000000_invoices", oldest.QuarantinedName)
		require.Equal(t, "run-1", oldest.RunID)
		require.Equal(t, "spirit", oldest.Engine)
		require.Equal(t, storage.PendingDropQuarantined, oldest.State)
		require.WithinDuration(t, base, oldest.QuarantinedAt.UTC(), time.Second)
	})

	t.Run("RecordIsIdempotentPerQuarantinedName", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		drop := pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base)
		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{drop}))

		// A retried write of the same quarantined name must neither fail nor
		// duplicate the row: the reaper would otherwise try to drop one table
		// twice and count the second attempt as an error.
		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{drop}))

		drops, err := store.PendingDrops().ListQuarantined(ctx, "shard-a", "staging")
		require.NoError(t, err)
		require.Len(t, drops, 1)
	})

	t.Run("RecordEmptyIsNoOp", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, nil))

		drops, err := store.PendingDrops().ListQuarantined(ctx, "shard-a", "staging")
		require.NoError(t, err)
		require.Empty(t, drops)
	})

	t.Run("ListQuarantinedIsScopedToTargetAndEnvironment", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		otherEnv := pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base)
		otherEnv.Environment = "production"
		otherTarget := pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base)
		otherTarget.Target = "shard-b"

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
			otherEnv,
			otherTarget,
		}))

		// The same quarantined name on a different target or environment is a
		// different server's table, so the unique key admits all three rows and
		// a sweep of one target sees only its own.
		drops, err := store.PendingDrops().ListQuarantined(ctx, "shard-a", "staging")
		require.NoError(t, err)
		require.Len(t, drops, 1)
		require.Equal(t, "shard-a", drops[0].Target)
		require.Equal(t, "staging", drops[0].Environment)
	})

	t.Run("LatestForTableReturnsNewestRecord", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
			pendingDropAt("invoices", "20260801130000000_invoices", "run-2", base.Add(time.Hour)),
		}))

		// Two applies dropping the same table leave rows whose quarantined names
		// differ only by timestamp. The caller compares RunID to decide whether
		// the newest one is its own proof or an earlier apply's data.
		latest, err := store.PendingDrops().LatestForTable(ctx, "shard-a", "staging", "orders", "invoices")
		require.NoError(t, err)
		require.NotNil(t, latest)
		require.Equal(t, "run-2", latest.RunID)
		require.Equal(t, "20260801130000000_invoices", latest.QuarantinedName)
	})

	t.Run("LatestForTableReturnsNilWhenUnrecorded", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
		}))

		// A nil result is what tells a re-run that the missing source table is
		// drift from outside SchemaBot rather than its own completed move, so it
		// must not be confused with an error.
		latest, err := store.PendingDrops().LatestForTable(ctx, "shard-a", "staging", "orders", "shipments")
		require.NoError(t, err)
		require.Nil(t, latest)
	})

	t.Run("ListExpiredReturnsOnlyElapsedQuarantines", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
			pendingDropAt("shipments", "20260808120000000_shipments", "run-2", base.Add(7*24*time.Hour)),
		}))

		expired, err := store.PendingDrops().ListExpired(ctx, base.Add(24*time.Hour), 10)
		require.NoError(t, err)
		require.Len(t, expired, 1)
		require.Equal(t, "invoices", expired[0].OriginalTable)
	})

	t.Run("ListExpiredIsOldestFirstAndBounded", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("c", "20260801120200000_c", "run-1", base.Add(2*time.Minute)),
			pendingDropAt("a", "20260801120000000_a", "run-1", base),
			pendingDropAt("b", "20260801120100000_b", "run-1", base.Add(time.Minute)),
		}))

		expired, err := store.PendingDrops().ListExpired(ctx, base.Add(time.Hour), 2)
		require.NoError(t, err)
		require.Len(t, expired, 2)
		require.Equal(t, "a", expired[0].OriginalTable)
		require.Equal(t, "b", expired[1].OriginalTable)
	})

	t.Run("SetStateRemovesRowsFromDiscovery", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
			pendingDropAt("shipments", "20260801120100000_shipments", "run-1", base.Add(time.Minute)),
		}))

		expired, err := store.PendingDrops().ListExpired(ctx, base.Add(time.Hour), 10)
		require.NoError(t, err)
		require.Len(t, expired, 2)

		require.NoError(t, store.PendingDrops().SetState(ctx, []int64{expired[0].ID}, storage.PendingDropReaped))
		require.NoError(t, store.PendingDrops().SetState(ctx, []int64{expired[1].ID}, storage.PendingDropVanished))

		// A terminal row is no longer a sweep candidate, but it is still the
		// proof a re-run reads, so it stays queryable by table.
		remaining, err := store.PendingDrops().ListExpired(ctx, base.Add(time.Hour), 10)
		require.NoError(t, err)
		require.Empty(t, remaining)

		latest, err := store.PendingDrops().LatestForTable(ctx, "shard-a", "staging", "orders", "invoices")
		require.NoError(t, err)
		require.NotNil(t, latest)
		require.Equal(t, storage.PendingDropReaped, latest.State)
	})

	t.Run("SetStateEmptyIsNoOp", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().SetState(ctx, nil, storage.PendingDropReaped))
	})

	t.Run("PruneRemovesOnlyTerminalRows", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
			pendingDropAt("shipments", "20260801120100000_shipments", "run-1", base.Add(time.Minute)),
		}))

		expired, err := store.PendingDrops().ListExpired(ctx, base.Add(time.Hour), 10)
		require.NoError(t, err)
		require.Len(t, expired, 2)
		require.NoError(t, store.PendingDrops().SetState(ctx, []int64{expired[0].ID}, storage.PendingDropReaped))

		// SetState stamps updated_at with the server's clock, so the cutoff has
		// to be in the future rather than derived from the pinned base time.
		pruned, err := store.PendingDrops().Prune(ctx, time.Now().Add(time.Hour), 10)
		require.NoError(t, err)
		require.Equal(t, int64(1), pruned)

		// Pruning must never reach a still-quarantined row: it describes a table
		// that is still sitting on the target, and dropping the row would strand
		// it outside discovery.
		remaining, err := store.PendingDrops().ListQuarantined(ctx, "shard-a", "staging")
		require.NoError(t, err)
		require.Len(t, remaining, 1)
		require.Equal(t, "shipments", remaining[0].OriginalTable)
	})

	t.Run("PruneIsBounded", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("a", "20260801120000000_a", "run-1", base),
			pendingDropAt("b", "20260801120100000_b", "run-1", base.Add(time.Minute)),
			pendingDropAt("c", "20260801120200000_c", "run-1", base.Add(2*time.Minute)),
		}))

		expired, err := store.PendingDrops().ListExpired(ctx, base.Add(time.Hour), 10)
		require.NoError(t, err)
		require.Len(t, expired, 3)
		ids := []int64{expired[0].ID, expired[1].ID, expired[2].ID}
		require.NoError(t, store.PendingDrops().SetState(ctx, ids, storage.PendingDropReaped))

		pruned, err := store.PendingDrops().Prune(ctx, time.Now().Add(time.Hour), 2)
		require.NoError(t, err)
		require.Equal(t, int64(2), pruned)

		pruned, err = store.PendingDrops().Prune(ctx, time.Now().Add(time.Hour), 2)
		require.NoError(t, err)
		require.Equal(t, int64(1), pruned)
	})

	t.Run("AdoptedRowsCarryArrivalTargetWithoutOrigin", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// A sweep that finds an unrecorded table in the quarantine schema cannot
		// recover its origin from the name, so it records where it was found and
		// leaves attribution empty rather than guessing.
		require.NoError(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{{
			Target:          "shard-a",
			Environment:     "staging",
			QuarantinedName: "20260724152851280_days",
			QuarantinedAt:   base,
			Engine:          "spirit",
			State:           storage.PendingDropQuarantined,
			ArrivalTarget:   "shard-a",
		}}))

		drops, err := store.PendingDrops().ListQuarantined(ctx, "shard-a", "staging")
		require.NoError(t, err)
		require.Len(t, drops, 1)
		require.Empty(t, drops[0].DatabaseName)
		require.Empty(t, drops[0].OriginalTable)
		require.Empty(t, drops[0].RunID)
		require.Equal(t, "shard-a", drops[0].ArrivalTarget)
	})

	t.Run("SurfacesConnectionFailures", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewUnreachableStorage(t)

		require.Error(t, store.PendingDrops().Record(ctx, []*storage.PendingDrop{
			pendingDropAt("invoices", "20260801120000000_invoices", "run-1", base),
		}))

		_, err := store.PendingDrops().LatestForTable(ctx, "shard-a", "staging", "orders", "invoices")
		require.Error(t, err)

		_, err = store.PendingDrops().ListExpired(ctx, base, 10)
		require.Error(t, err)

		_, err = store.PendingDrops().ListQuarantined(ctx, "shard-a", "staging")
		require.Error(t, err)

		require.Error(t, store.PendingDrops().SetState(ctx, []int64{1}, storage.PendingDropReaped))

		_, err = store.PendingDrops().Prune(ctx, base, 10)
		require.Error(t, err)
	})
}
