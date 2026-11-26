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
// indexes, and TOAST).
func sizeFromPreflight(database, namespace string, proof preflight.PreflightedTable) tableSizeEstimate {
	return sizeEstimate(database, namespace, proof.Table(), proof.TotalBytes(), proof.RelTuples())
}

// sizeFromSizeRefusal reads the display estimate from a size ceiling refusal,
// which carries the same measured bytes and row estimate as a table that
// passed, so a table over the ceiling keeps its rows line.
func sizeFromSizeRefusal(database, namespace, table string, refusal *preflight.SizeError) tableSizeEstimate {
	return sizeEstimate(database, namespace, table, refusal.TotalBytes, refusal.RelTuples)
}

// sizeEstimate builds the display estimate from the measured bytes and the
// catalog's row estimate. RelTuples is negative until the table is first
// analyzed; that is an unknown count, not an empty table, so the rows are
// left out rather than reported as zero.
func sizeEstimate(database, namespace, table string, totalBytes int64, relTuples float64) tableSizeEstimate {
	est := tableSizeEstimate{bytes: &totalBytes}
	if relTuples < 0 {
		// Routine for a table autovacuum has not reached yet, so Info, not Warn.
		slog.Info("PostgreSQL table has never been analyzed; the plan will omit its row count",
			"database", database, "namespace", namespace, "table", table)
		return est
	}
	rows := int64(math.Round(relTuples))
	est.rows = &rows
	return est
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

// sizeDroppedTables gives each table the plan drops its size. A PostgreSQL
// plan drops only the live tables no schema file declares. No size ceiling
// applies to a drop, so the preflight runs with no limit, only to measure the
// table. Sizes are display-only: every read in the plan shares one
// engine.TableSizeProbeTimeout budget, the reads stop once it runs out, and a
// failed read leaves that drop without a size rather than failing the plan.
func sizeDroppedTables(ctx context.Context, pool *pgxpool.Pool, database string, changes []engine.SchemaChange) {
	probeCtx, cancel := context.WithTimeout(ctx, engine.TableSizeProbeTimeout)
	defer cancel()
	for i := range changes {
		namespace := changes[i].Namespace
		for j := range changes[i].TableChanges {
			drop := &changes[i].TableChanges[j]
			if drop.Operation != ddl.StatementDropTable {
				continue
			}
			if err := probeCtx.Err(); err != nil {
				slog.Warn("PostgreSQL table size budget ran out; the plan will show the remaining dropped tables' sizes as unavailable",
					"database", database, "namespace", namespace, "first_unsized_table", drop.Table, "error", err)
				return
			}
			proof, err := preflight.CheckTable(probeCtx, pool, namespace, drop.Table, math.MaxInt64)
			if err != nil {
				slog.Warn("PostgreSQL table size unavailable for a table the plan drops; the plan will show its size as unavailable",
					"database", database, "namespace", namespace, "table", drop.Table, "error", err)
				continue
			}
			est := sizeFromPreflight(database, namespace, proof)
			drop.EstimatedRows = copyEstimate(est.rows)
			drop.EstimatedBytes = copyEstimate(est.bytes)
		}
	}
}

func copyEstimate(v *int64) *int64 {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
