package postgres

import (
	"context"
	"log/slog"
	"math"

	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

// tableSizeEstimate is one table's plan-time size, for display only. A nil
// field is a statistic the catalog could not report.
type tableSizeEstimate struct {
	rows  *int64
	bytes *int64
}

// sizeFromPreflight reads the display estimate from the size the native-safe
// ceiling already measured, so the plan shows the same figure the gate judged.
// TotalBytes is pg_total_relation_size summed over the partition tree (heap,
// indexes, and TOAST). RelTuples is the catalog's row estimate, which is
// negative until the table is first analyzed; that is an unknown count, not
// an empty table.
func sizeFromPreflight(database, namespace string, proof preflight.PreflightedTable) tableSizeEstimate {
	bytes := proof.TotalBytes()
	est := tableSizeEstimate{bytes: &bytes}
	if proof.RelTuples() < 0 {
		// Routine for a table autovacuum has not reached yet, so Info, not Warn.
		slog.Info("PostgreSQL table has never been analyzed; the plan will omit its row count",
			"database", database, "namespace", namespace, "table", proof.Table())
		return est
	}
	rows := int64(math.Round(proof.RelTuples()))
	est.rows = &rows
	return est
}

// sizeFromSizeRefusal reads the display estimate from a size ceiling refusal,
// which carries the measured bytes but no row estimate.
func sizeFromSizeRefusal(database, namespace, table string, refusal *preflight.SizeError) tableSizeEstimate {
	slog.Info("PostgreSQL size ceiling refusal carries no row estimate; the plan will omit the table's row count",
		"database", database, "namespace", namespace, "table", table)
	bytes := refusal.TotalBytes
	return tableSizeEstimate{bytes: &bytes}
}

// attachTableSize gives every change to the table its estimate, as Spirit does
// for MySQL, so a PostgreSQL schema change shows the same size clause. Each
// change gets its own copy, so editing one change's estimate does not edit the
// others. An ALTER TYPE change names a type, not a table, even when a table
// shares its name, so it gets none.
func attachTableSize(changes []engine.TableChange, table string, est tableSizeEstimate) {
	for i := range changes {
		if changes[i].Table != table || changes[i].Operation == ddl.StatementAlterType {
			continue
		}
		changes[i].EstimatedRows = copyEstimate(est.rows)
		changes[i].EstimatedBytes = copyEstimate(est.bytes)
	}
}

// sizeDroppedTables gives each undeclared table the plan drops its size. No
// size ceiling applies to a drop, so the preflight runs with no limit, only to
// measure the table. Sizes are display-only: the reads share one
// engine.TableSizeProbeTimeout budget, and a failed read logs and leaves that
// drop without a size rather than failing the plan.
func sizeDroppedTables(ctx context.Context, pool *pgxpool.Pool, database, namespace string, drops []engine.TableChange) {
	if len(drops) == 0 {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, engine.TableSizeProbeTimeout)
	defer cancel()
	for i := range drops {
		proof, err := preflight.CheckTable(probeCtx, pool, namespace, drops[i].Table, math.MaxInt64)
		if err != nil {
			slog.Warn("PostgreSQL table size unavailable for a table the plan drops; the plan will show its size as unavailable",
				"database", database, "namespace", namespace, "table", drops[i].Table, "error", err)
			continue
		}
		attachTableSize(drops[i:i+1], drops[i].Table, sizeFromPreflight(database, namespace, proof))
	}
}

func copyEstimate(v *int64) *int64 {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
