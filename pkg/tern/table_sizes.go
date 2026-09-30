package tern

import (
	"github.com/block/schemabot/pkg/engine"
)

// tableSizeAccumulator folds one table's per-shard size estimates into the
// namespace-level size view: how many planned shards the change spans and,
// when every shard reported an estimate, the summed rows and bytes plus the
// largest single shard's row count.
//
// Rows and bytes are tracked independently because an engine can report one
// without the other: a source that carries bytes only still yields a byte
// total, and its absent row estimates do not suppress it. There is no
// largest-shard byte counterpart to largestShardRows: the largest shard's rows
// bound the biggest chunk a shard-at-a-time apply works through at once, while
// bytes only convey the change's magnitude, for which the total is the number
// an operator reads.
type tableSizeAccumulator struct {
	shardCount int
	sum        int64
	largest    int64
	bytesSum   int64
	// missingRows and missingBytes record that at least one shard carried no
	// estimate of that kind. Each total is all-or-nothing: a partial sum would
	// understate the table's size, so that kind's values are omitted entirely
	// while the shard count survives. The largest shard follows the row sum:
	// the shard with no estimate could be the largest, so a maximum over the
	// shards that reported would present a smaller shard as the largest.
	missingRows  bool
	missingBytes bool
	// seenShards records the shards already folded into this table's totals,
	// so a shard that plans several statements against one table contributes
	// once rather than once per statement.
	seenShards map[string]bool
}

// sizes returns the namespace-level display values for the accumulated table:
// the shard count, the total estimated rows and bytes across shards, and the
// largest single shard's row estimate. Row values are nil when any shard lacked
// a row estimate, and the byte total is nil when any shard lacked a byte
// estimate (see missingRows and missingBytes).
func (a *tableSizeAccumulator) sizes() (shardCount int, estimatedRows, largestShardRows, estimatedBytes *int64) {
	if !a.missingRows {
		sum, largest := a.sum, a.largest
		estimatedRows, largestShardRows = &sum, &largest
	}
	if !a.missingBytes {
		bytesSum := a.bytesSum
		estimatedBytes = &bytesSum
	}
	return a.shardCount, estimatedRows, largestShardRows, estimatedBytes
}

// aggregateShardTableSizes computes namespace-level table-size aggregates for
// a sharded plan, keyed by (plan namespace, table). The namespace view of a
// sharded plan keeps one TableChange per table while the same table repeats
// across the keyspace's shards; this fold gives that single entry the
// cross-shard totals: sum of per-shard estimates, largest single shard (the
// biggest chunk a shard-at-a-time apply works through at once), and the number
// of planned shards.
//
// A shard contributes each of its tables once even when it plans several
// statements against that table (a partition-type change needs its own
// REMOVE PARTITIONING statement), so the shard count is a count of shards and
// the totals are not inflated by statement count.
//
// Only sharded SchemaChanges (non-empty Shard.Name) contribute: an engine that
// aggregates a sharded target itself emits unsharded changes, and its own
// values pass through the namespace view untouched.
func (c *LocalClient) aggregateShardTableSizes(changes []engine.SchemaChange) map[string]map[string]*tableSizeAccumulator {
	agg := make(map[string]map[string]*tableSizeAccumulator)
	for _, sc := range changes {
		// The same predicate the namespace view's dedup uses, so the entry that
		// receives an aggregate is the one this fold was computed for.
		if !sc.Sharded() {
			// Unsharded change: nothing to fold, the engine's own per-table
			// size values are already namespace-level.
			continue
		}
		shard := sc.ShardName()
		ns := c.planNamespace(sc.Namespace)
		byTable := agg[ns]
		if byTable == nil {
			byTable = make(map[string]*tableSizeAccumulator)
			agg[ns] = byTable
		}
		for _, tc := range sc.TableChanges {
			a := byTable[tc.Table]
			if a == nil {
				a = &tableSizeAccumulator{seenShards: make(map[string]bool)}
				byTable[tc.Table] = a
			}
			if a.seenShards[shard] {
				continue
			}
			a.seenShards[shard] = true
			a.shardCount++
			switch {
			case tc.EstimatedRows == nil:
				c.logger.Debug("shard reports no row estimate; the table's row total will be omitted",
					"namespace", ns, "shard", shard, "table", tc.Table)
				a.missingRows = true
			case *tc.EstimatedRows < 0:
				c.logger.Warn("shard reports a negative row estimate; the table's row total will be omitted",
					"namespace", ns, "shard", shard, "table", tc.Table, "estimated_rows", *tc.EstimatedRows)
				a.missingRows = true
			default:
				a.sum += *tc.EstimatedRows
				if *tc.EstimatedRows > a.largest {
					a.largest = *tc.EstimatedRows
				}
			}
			switch {
			case tc.EstimatedBytes == nil:
				c.logger.Debug("shard reports no byte estimate; the table's byte total will be omitted",
					"namespace", ns, "shard", shard, "table", tc.Table)
				a.missingBytes = true
			case *tc.EstimatedBytes < 0:
				c.logger.Warn("shard reports a negative byte estimate; the table's byte total will be omitted",
					"namespace", ns, "shard", shard, "table", tc.Table, "estimated_bytes", *tc.EstimatedBytes)
				a.missingBytes = true
			default:
				a.bytesSum += *tc.EstimatedBytes
			}
		}
	}
	return agg
}
