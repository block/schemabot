package tern

import (
	"strings"

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
	// while the shard count survives.
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
		shard := strings.TrimSpace(sc.Shard.Name)
		if shard == "" {
			// Unsharded change: nothing to fold, the engine's own per-table
			// size values are already namespace-level.
			continue
		}
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
			if tc.EstimatedRows == nil {
				a.missingRows = true
			} else {
				a.sum += *tc.EstimatedRows
				if *tc.EstimatedRows > a.largest {
					a.largest = *tc.EstimatedRows
				}
			}
			if tc.EstimatedBytes == nil {
				a.missingBytes = true
			} else {
				a.bytesSum += *tc.EstimatedBytes
			}
		}
	}
	return agg
}
