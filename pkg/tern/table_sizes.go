package tern

import (
	"strings"

	"github.com/block/schemabot/pkg/engine"
)

// tableSizeAccumulator folds one table's per-shard row estimates into the
// namespace-level size view: how many planned shards the change spans and,
// when every shard reported an estimate, the summed and largest-shard row
// counts.
type tableSizeAccumulator struct {
	shardCount int
	sum        int64
	largest    int64
	bytesSum   int64
	// missing records that at least one shard carried no estimate. Row totals
	// are all-or-nothing: a partial sum would understate the table's size, so
	// the row values are omitted entirely while the shard count survives.
	missing bool
}

// sizes returns the namespace-level display values for the accumulated table:
// the shard count, the total estimated rows and bytes across shards, and the
// largest single shard's row estimate. The size values are nil when any shard
// lacked an estimate (see missing).
func (a *tableSizeAccumulator) sizes() (shardCount int, estimatedRows, largestShardRows, estimatedBytes *int64) {
	if a.missing {
		return a.shardCount, nil, nil, nil
	}
	sum, largest, bytesSum := a.sum, a.largest, a.bytesSum
	return a.shardCount, &sum, &largest, &bytesSum
}

// aggregateShardTableSizes computes namespace-level table-size aggregates for
// a sharded plan, keyed by (plan namespace, table). The namespace view of a
// sharded plan keeps one TableChange per table while the same table repeats
// across the keyspace's shards; this fold gives that single entry the
// cross-shard totals — sum of per-shard estimates, largest single shard (the
// biggest chunk a shard-at-a-time apply works through at once), and the
// number of
// planned shards. Only sharded SchemaChanges (non-empty Shard.Name)
// contribute: an engine that aggregates a sharded target itself emits
// unsharded changes, and its own values pass through the namespace view
// untouched.
func (c *LocalClient) aggregateShardTableSizes(changes []engine.SchemaChange) map[string]map[string]*tableSizeAccumulator {
	agg := make(map[string]map[string]*tableSizeAccumulator)
	for _, sc := range changes {
		if strings.TrimSpace(sc.Shard.Name) == "" {
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
				a = &tableSizeAccumulator{}
				byTable[tc.Table] = a
			}
			a.shardCount++
			if tc.EstimatedRows == nil || tc.EstimatedBytes == nil {
				a.missing = true
				continue
			}
			a.sum += *tc.EstimatedRows
			a.bytesSum += *tc.EstimatedBytes
			if *tc.EstimatedRows > a.largest {
				a.largest = *tc.EstimatedRows
			}
		}
	}
	return agg
}
