package planetscale

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/spirit/pkg/utils"
)

// tableRowEstimate aggregates one table's per-shard row estimates across a
// keyspace: the summed total and the largest single shard (the write-blocking
// blast radius of a shard-at-a-time apply). Values come from
// information_schema TABLE_ROWS — approximate and possibly stale, display
// only.
type tableRowEstimate struct {
	total      int64
	largest    int64
	totalBytes int64
}

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
// display-only size data: the shard count the change spans and, when a vtgate
// DSN is available, per-table row estimates summed across the keyspace's
// shards with the largest single shard alongside. Best-effort by design —
// sizes are informational, so a probe failure logs and leaves the changes
// without estimates instead of failing the plan. The live vtgate topology
// outranks the API's shard count when both are available.
func (e *Engine) attachTableSizes(ctx context.Context, creds *engine.Credentials, keyspace string, apiShardCount int, changes []engine.TableChange) {
	shardCount := apiShardCount
	var estimates map[string]*tableRowEstimate
	if creds != nil && creds.DSN != "" {
		probed, probedShards, err := e.fetchKeyspaceTableRowEstimates(ctx, creds.DSN, keyspace)
		if err != nil {
			e.logger.Warn("table row estimates unavailable; the plan will omit them for this keyspace",
				"keyspace", keyspace, "error", err)
		} else {
			estimates = probed
			if probedShards > 0 {
				shardCount = probedShards
			}
		}
	}
	for i := range changes {
		changes[i].ShardCount = shardCount
		est, ok := estimates[changes[i].Table]
		if !ok {
			// A table being created — or one absent from shard statistics —
			// has no row estimate; the shard count above still applies.
			continue
		}
		total, largest, totalBytes := est.total, est.largest, est.totalBytes
		changes[i].EstimatedRows = &total
		changes[i].LargestShardRows = &largest
		changes[i].EstimatedBytes = &totalBytes
	}
}

// fetchKeyspaceTableRowEstimates probes every shard of a keyspace over vtgate
// and returns per-table row estimates (summed total and largest single shard),
// plus the number of shards probed. The per-shard read targets
// information_schema through a `keyspace/shard`-scoped connection so each
// shard reports its own statistics. Any shard failing fails the whole probe:
// a partial sum would silently understate the table's size.
func (e *Engine) fetchKeyspaceTableRowEstimates(ctx context.Context, dsn, keyspace string) (map[string]*tableRowEstimate, int, error) {
	shards, err := e.listKeyspaceShards(ctx, dsn, keyspace)
	if err != nil {
		return nil, 0, fmt.Errorf("list shards for keyspace %s: %w", keyspace, err)
	}
	estimates := make(map[string]*tableRowEstimate)
	for _, shard := range shards {
		if err := e.accumulateShardRowEstimates(ctx, dsn, keyspace, shard, estimates); err != nil {
			return nil, 0, fmt.Errorf("row estimates for shard %s/%s: %w", keyspace, shard, err)
		}
	}
	return estimates, len(shards), nil
}

// accumulateShardRowEstimates reads one shard's per-table row statistics and
// folds them into estimates.
func (e *Engine) accumulateShardRowEstimates(ctx context.Context, dsn, keyspace, shard string, estimates map[string]*tableRowEstimate) error {
	db, err := e.getVtgateKeyspaceDB(ctx, dsn, keyspace+"/"+shard)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, table_rows, data_length + index_length
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'`)
	if err != nil {
		return fmt.Errorf("query table row estimates: %w", err)
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var name string
		var tableRows, tableBytes sql.NullInt64
		if err := rows.Scan(&name, &tableRows, &tableBytes); err != nil {
			return fmt.Errorf("scan table row estimate: %w", err)
		}
		if !tableRows.Valid || !tableBytes.Valid {
			// NULL statistics (e.g. a view): nothing to fold in.
			continue
		}
		est := estimates[name]
		if est == nil {
			est = &tableRowEstimate{}
			estimates[name] = est
		}
		est.total += tableRows.Int64
		est.totalBytes += tableBytes.Int64
		if tableRows.Int64 > est.largest {
			est.largest = tableRows.Int64
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table row estimates: %w", err)
	}
	return nil
}

// listKeyspaceShards returns the keyspace's shard names from the live vtgate
// topology, sorted for deterministic probing order.
func (e *Engine) listKeyspaceShards(ctx context.Context, dsn, keyspace string) ([]string, error) {
	db, err := e.getVtgateKeyspaceDB(ctx, dsn, keyspace)
	if err != nil {
		return nil, fmt.Errorf("connect to keyspace %s: %w", keyspace, err)
	}
	rows, err := db.QueryContext(ctx, "SHOW VITESS_SHARDS")
	if err != nil {
		return nil, fmt.Errorf("show vitess shards: %w", err)
	}
	defer utils.CloseAndLog(rows)

	prefix := keyspace + "/"
	var shards []string
	for rows.Next() {
		var qualified string
		if err := rows.Scan(&qualified); err != nil {
			return nil, fmt.Errorf("scan shard name: %w", err)
		}
		shard, ok := strings.CutPrefix(qualified, prefix)
		if !ok {
			// SHOW VITESS_SHARDS lists every keyspace; other keyspaces'
			// shards are not this probe's concern.
			continue
		}
		shards = append(shards, shard)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate shard names: %w", err)
	}
	sort.Strings(shards)
	return shards, nil
}
