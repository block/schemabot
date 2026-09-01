package planetscale

import (
	"context"
	"fmt"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// fetchKeyspaceShardCounts returns each keyspace's shard count from the
// PlanetScale API, keyed by keyspace name. Shard counts are display-only plan
// context; the caller treats a lookup failure as "counts unknown" rather than
// failing the plan.
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
		counts[ks.Name] = ks.Shards
	}
	return counts, nil
}

// attachTableSizes decorates a keyspace's planned table changes with
// display-only size context: the shard count the change spans and, when the
// branch table metrics are available, each table's storage bytes. The metrics
// endpoint reports bytes only — no row counts and no per-shard breakdown — so
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
