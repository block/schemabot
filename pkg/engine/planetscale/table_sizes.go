package planetscale

import (
	"context"
	"fmt"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// fetchKeyspaceShardCounts returns each keyspace on the branch, keyed by
// keyspace name, with its shard count. An unsharded keyspace maps to zero:
// the API reports it as one shard, but a shard count is only meaningful for a
// sharded keyspace, and TableChange.ShardCount reserves zero for "no shard
// count to render". Shard counts are display-only plan context; the caller
// treats a lookup failure as "topology unknown" rather than failing the plan.
//
// Every keyspace on the branch is present, sharded or not, so the caller can
// tell a single-keyspace branch from a multi-keyspace one.
func (e *Engine) fetchKeyspaceShardCounts(ctx context.Context, client psclient.PSClient, org, database, branch string) (map[string]int, error) {
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		return nil, fmt.Errorf("list keyspaces for %s/%s branch %s: %w", org, database, branch, err)
	}
	counts := make(map[string]int, len(keyspaces))
	for _, ks := range keyspaces {
		if ks == nil {
			// Defensive skip: the SDK should not return nil entries, and a nil
			// keyspace carries no name to key by.
			continue
		}
		if !ks.Sharded {
			counts[ks.Name] = 0
			continue
		}
		counts[ks.Name] = ks.Shards
	}
	return counts, nil
}

// fetchBranchTableBytes returns each table's storage bytes on the branch,
// keyed by bare table name, or nil when no byte estimate can be attributed.
//
// The metrics endpoint is branch-scoped and its keys carry no keyspace, so on
// a branch with more than one keyspace a bare table name cannot be attributed
// to the keyspace that owns it — two keyspaces commonly hold a table of the
// same name, and stamping both with one value would render a confidently wrong
// size. Bytes are therefore read only when the branch has exactly one
// keyspace, where the attribution is unambiguous. An unknown topology (the
// keyspace lookup failed) is treated the same way, since uniqueness cannot be
// established without it.
//
// Best effort and budget-bound: sizes are display-only, so a slow or failed
// metrics read logs and the plan proceeds without byte estimates.
func (e *Engine) fetchBranchTableBytes(ctx context.Context, client psclient.PSClient, org, database, branch string, shardCounts map[string]int) map[string]int64 {
	if len(shardCounts) != 1 {
		e.logger.Info("branch table metrics skipped; table byte estimates need a single-keyspace branch to attribute",
			"organization", org, "database", database, "branch", branch, "keyspaces", len(shardCounts))
		return nil
	}
	metricsCtx, cancelMetrics := context.WithTimeout(ctx, engine.TableSizeProbeTimeout)
	defer cancelMetrics()
	tableBytes, err := client.BranchTableMetrics(metricsCtx, org, database, branch)
	if err != nil {
		e.logger.Warn("branch table metrics unavailable; the plan will omit table byte estimates",
			"organization", org, "database", database, "branch", branch, "error", err)
		return nil
	}
	return tableBytes
}

// attachTableSizes decorates a keyspace's planned table changes with
// display-only size context: the shard count the change spans and, when the
// branch table metrics could be attributed to this keyspace (see
// fetchBranchTableBytes), each table's storage bytes. The metrics endpoint
// reports bytes only — no row counts and no per-shard breakdown — so
// EstimatedRows and LargestShardRows stay absent on PlanetScale changes.
func attachTableSizes(shardCount int, tableBytes map[string]int64, changes []engine.TableChange) {
	for i := range changes {
		changes[i].ShardCount = shardCount
		storageBytes, ok := tableBytes[changes[i].Table]
		if !ok {
			// A table being created — or one absent from the branch metrics —
			// has no byte estimate; the shard count above still applies.
			continue
		}
		changes[i].EstimatedBytes = &storageBytes
	}
}
